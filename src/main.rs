use askama::Template;
use askama_web::WebTemplate;
use axum::{
    extract::{Multipart, State, Form, Path, Query},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{sse::{Event, Sse}, Html, IntoResponse, Redirect, Json},
    routing::{get, post, delete},
    Router,
};
use futures_util::stream::Stream;
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{convert::Infallible, process::Stdio, sync::Arc, collections::{HashMap, HashSet}, path::{Component, Path as StdPath, PathBuf}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tower_http::services::ServeDir;
use uuid::Uuid;
use std::fs;

mod startup;
mod system_stats;

use startup::{check_asmo_readiness, detect_panel_host_ip, print_startup_banner, shutdown_signal};
use system_stats::{
    HostCpuSnapshot,
    format_uptime_human,
    read_host_cpu_snapshot,
    read_host_disk_mb,
    read_host_loadavg_1m,
    read_host_memory_mb,
    read_host_uptime_seconds,
};

#[derive(Template, WebTemplate)]
#[template(path = "index.html")]
struct IndexTemplate {
    instances: Vec<InstanceInfo>,
}

// FIX: fields are read by the Askama template engine at runtime, not by Rust directly.
// #[allow(dead_code)] silences the false-positive compiler warning.
#[allow(dead_code)]
#[derive(Template, WebTemplate)]
#[template(path = "console.html")]
struct ConsoleTemplate {
    instance_name: String,
    history: Vec<String>,
    instances: Vec<InstanceInfo>,
    allow_device_host_edit: bool,
    default_device_host: String,
}

#[allow(dead_code)]
#[derive(Template, WebTemplate)]
#[template(path = "instance_settings.html")]
struct InstanceSettingsTemplate {
    instance_name: String,
    settings: Vec<ServerProperty>,
}

#[allow(dead_code)]
#[derive(Template, WebTemplate)]
#[template(path = "instance_files.html")]
struct InstanceFilesTemplate {
    instance_name: String,
    current_path: String,
    parent_path: String,
    has_parent: bool,
    breadcrumbs: Vec<FileBreadcrumb>,
    entries: Vec<InstanceFileEntry>,
    directory_count: usize,
    file_count: usize,
}

#[derive(Template, WebTemplate)]
#[template(path = "device.html")]
struct DeviceTemplate {
    allow_device_host_edit: bool,
    default_device_host: String,
}

#[derive(Template, WebTemplate)]
#[template(path = "backups.html")]
struct BackupsTemplate {}

#[derive(Clone)]
struct InstanceInfo {
    name: String,
}

#[derive(Clone)]
struct ServerProperty {
    key: String,
    value: String,
}

#[derive(Clone)]
struct FileBreadcrumb {
    label: String,
    path: String,
    is_current: bool,
}

#[derive(Clone)]
struct InstanceFileEntry {
    name: String,
    is_directory: bool,
    size_label: String,
    relative_path: String,
}

#[derive(Deserialize)]
struct ConsoleInput {
    command: String,
}

#[derive(Deserialize)]
struct CreateInstanceRequest {
    name: String,
    version: String,
}

#[derive(Deserialize)]
struct RenameInstanceRequest {
    new_name: String,
}

#[derive(Serialize)]
struct VersionResponse {
    release: Option<String>,
    preview: Option<String>,
}

#[derive(Serialize)]
struct CreateInstanceResponse {
    status: String,
    message: String,
}

#[derive(Deserialize)]
struct CreateBackupRequest {
    instance_name: String,
}

#[derive(Deserialize)]
struct RestoreBackupRequest {
    mode: String,
    new_instance_name: Option<String>,
}

#[derive(Deserialize)]
struct InstanceFilesQuery {
    path: Option<String>,
}

#[derive(Deserialize)]
struct SaveSettingItem {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct SaveSettingsRequest {
    settings: Vec<SaveSettingItem>,
}

#[derive(Deserialize)]
struct CreateFolderRequest {
    current_path: Option<String>,
    folder_name: String,
}

#[derive(Deserialize)]
struct DeleteEntryRequest {
    path: String,
}

#[derive(Deserialize)]
struct DownloadFileQuery {
    path: String,
}

#[derive(Deserialize)]
struct FileContentQuery {
    path: String,
}

#[derive(Deserialize)]
struct SaveFileContentRequest {
    path: String,
    #[serde(default)]
    original_path: Option<String>,
    content: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct BackupMetadata {
    id: String,
    source_instance: String,
    created_at_unix: u64,
    size_bytes: u64,
    file_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_instance_proof_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_instance_id: Option<String>,
}

struct InstanceState {
    process: Mutex<Option<Child>>,
    tx: broadcast::Sender<String>,
    logs: Arc<Mutex<Vec<String>>>,
}

struct AppState {
    instances: RwLock<HashMap<String, Arc<InstanceState>>>,
    device_host: Mutex<Option<String>>,
    development_mode: bool,
    fixed_device_host: String,
    progress: RwLock<HashMap<String, Arc<std::sync::atomic::AtomicU64>>>,
    host_cpu_snapshot: Mutex<Option<HostCpuSnapshot>>,
}

const INSTANCE_ID_FILENAME: &str = ".minara_instance_id";
const STOP_GRACEFUL_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_CTRL_C_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_FORCE_KILL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
struct StaleProcessCleanupOutcome {
    matched: usize,
    cleaned: usize,
    remaining: usize,
}

impl StaleProcessCleanupOutcome {
    fn none() -> Self {
        Self {
            matched: 0,
            cleaned: 0,
            remaining: 0,
        }
    }
}

#[cfg(target_os = "linux")]
fn list_stale_instance_runtime_pids(instance_root: &StdPath) -> Vec<i32> {
    let canonical_root = fs::canonicalize(instance_root).unwrap_or_else(|_| instance_root.to_path_buf());
    let canonical_root_text = canonical_root.to_string_lossy().to_string();
    let canonical_root_prefix = format!("{}/", canonical_root_text);
    let mut pids = Vec::new();
    let self_pid = i32::try_from(std::process::id()).unwrap_or_default();

    let proc_entries = match fs::read_dir("/proc") {
        Ok(entries) => entries,
        Err(_) => return pids,
    };

    for entry in proc_entries.flatten() {
        let pid_text = entry.file_name().to_string_lossy().to_string();
        if !pid_text.chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }

        let Ok(pid) = pid_text.parse::<i32>() else {
            continue;
        };

        if pid <= 1 || pid == self_pid {
            continue;
        }

        let proc_path = entry.path();
        let comm_lower = fs::read_to_string(proc_path.join("comm"))
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_default();

        let cmdline_bytes = fs::read(proc_path.join("cmdline")).unwrap_or_default();
        let cmdline = String::from_utf8_lossy(&cmdline_bytes).replace('\0', " ");
        let cmdline_lower = cmdline.to_ascii_lowercase();

        let runtime_name_match = matches!(comm_lower.as_str(), "bedrock_server" | "bionilux" | "box64");
        let runtime_cmdline_match = cmdline_lower.contains("bedrock_server")
            || cmdline_lower.contains("bionilux")
            || cmdline_lower.contains("box64");

        if !runtime_name_match && !runtime_cmdline_match {
            continue;
        }

        let cmdline_matches_instance = cmdline_bytes
            .split(|byte| *byte == 0)
            .filter(|arg| !arg.is_empty())
            .any(|arg| {
                let arg_text = String::from_utf8_lossy(arg);
                arg_text == canonical_root_text || arg_text.starts_with(&canonical_root_prefix)
            });
        let cwd_matches_instance = fs::read_link(proc_path.join("cwd"))
            .ok()
            .map(|path| path.starts_with(&canonical_root))
            .unwrap_or(false);
        let exe_matches_instance = fs::read_link(proc_path.join("exe"))
            .ok()
            .map(|path| path.starts_with(&canonical_root))
            .unwrap_or(false);

        if cmdline_matches_instance || cwd_matches_instance || exe_matches_instance {
            pids.push(pid);
        }
    }

    pids.sort_unstable();
    pids.dedup();
    pids
}

#[cfg(target_os = "linux")]
fn pid_is_alive(pid: i32) -> bool {
    let stat_path = PathBuf::from("/proc").join(pid.to_string()).join("stat");
    let Ok(stat_text) = fs::read_to_string(stat_path) else {
        return false;
    };

    let Some(state) = stat_text.split_whitespace().nth(2) else {
        return true;
    };

    state != "Z"
}

#[cfg(target_os = "linux")]
fn signal_pid(pid: i32, signal: libc::c_int) {
    let _ = unsafe { libc::kill(pid, signal) };
}

#[cfg(target_os = "linux")]
async fn cleanup_stale_instance_processes(instance_root: &StdPath) -> StaleProcessCleanupOutcome {
    let matched_pids = list_stale_instance_runtime_pids(instance_root);
    if matched_pids.is_empty() {
        return StaleProcessCleanupOutcome::none();
    }

    for pid in &matched_pids {
        signal_pid(*pid, libc::SIGTERM);
    }

    let term_deadline = Instant::now() + Duration::from_millis(1200);
    loop {
        let alive_after_term = matched_pids
            .iter()
            .copied()
            .filter(|pid| pid_is_alive(*pid))
            .collect::<Vec<_>>();

        if alive_after_term.is_empty() {
            break;
        }

        if Instant::now() >= term_deadline {
            for pid in &alive_after_term {
                signal_pid(*pid, libc::SIGKILL);
            }
            break;
        }

        tokio::time::sleep(Duration::from_millis(120)).await;
    }

    let kill_deadline = Instant::now() + Duration::from_millis(1200);
    loop {
        let alive_after_kill = matched_pids
            .iter()
            .copied()
            .filter(|pid| pid_is_alive(*pid))
            .count();

        if alive_after_kill == 0 {
            return StaleProcessCleanupOutcome {
                matched: matched_pids.len(),
                cleaned: matched_pids.len(),
                remaining: 0,
            };
        }

        if Instant::now() >= kill_deadline {
            return StaleProcessCleanupOutcome {
                matched: matched_pids.len(),
                cleaned: matched_pids.len().saturating_sub(alive_after_kill),
                remaining: alive_after_kill,
            };
        }

        tokio::time::sleep(Duration::from_millis(120)).await;
    }
}

#[cfg(not(target_os = "linux"))]
async fn cleanup_stale_instance_processes(_instance_root: &StdPath) -> StaleProcessCleanupOutcome {
    StaleProcessCleanupOutcome::none()
}

fn process_slot_is_running(process: &mut Option<Child>) -> bool {
    let Some(child) = process.as_mut() else {
        return false;
    };

    match child.try_wait() {
        Ok(Some(_)) => {
            *process = None;
            false
        }
        Ok(None) => true,
        Err(_) => {
            *process = None;
            false
        }
    }
}

async fn wait_for_slot_exit(process: &mut Option<Child>, timeout: Duration) -> std::io::Result<bool> {
    let deadline = Instant::now() + timeout;

    loop {
        if !process_slot_is_running(process) {
            return Ok(true);
        }

        if Instant::now() >= deadline {
            return Ok(false);
        }

        tokio::time::sleep(Duration::from_millis(120)).await;
    }
}

#[cfg(unix)]
fn send_ctrl_c_to_slot(process: &Option<Child>) -> std::io::Result<()> {
    let Some(child) = process.as_ref() else {
        return Ok(());
    };

    let pid = child.id().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "child pid is unavailable")
    })?;
    let pid = i32::try_from(pid).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::Other, "child pid is out of range")
    })?;

    let signal_result = unsafe { libc::kill(-pid, libc::SIGINT) };
    if signal_result == 0 {
        return Ok(());
    }

    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }

    Err(error)
}

#[cfg(not(unix))]
fn send_ctrl_c_to_slot(_process: &Option<Child>) -> std::io::Result<()> {
    Ok(())
}

fn force_kill_slot(process: &mut Option<Child>) -> std::io::Result<()> {
    let Some(child) = process.as_mut() else {
        return Ok(());
    };

    match child.start_kill() {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[derive(Debug)]
enum StopProcessOutcome {
    NoProcess,
    Graceful,
    CtrlCFallback,
    ForceKilled,
    ForceKillTimedOut,
    Error(String),
}

fn stop_outcome_to_html(outcome: StopProcessOutcome) -> Html<String> {
    match outcome {
        StopProcessOutcome::NoProcess => Html("<span class='text-slate-500'>No process found</span>".into()),
        StopProcessOutcome::Graceful => {
            Html("<span class='text-rose-400'>Shutdown complete (graceful)</span>".into())
        }
        StopProcessOutcome::CtrlCFallback => {
            Html("<span class='text-amber-300'>Shutdown complete (Ctrl+C fallback)</span>".into())
        }
        StopProcessOutcome::ForceKilled => {
            Html("<span class='text-amber-300'>Shutdown complete (force killed)</span>".into())
        }
        StopProcessOutcome::ForceKillTimedOut => {
            Html("<span class='text-red-500'>Force kill timed out</span>".into())
        }
        StopProcessOutcome::Error(error) => {
            Html(format!("<span class='text-red-500'>{}</span>", error))
        }
    }
}

async fn stop_process_with_escalation(process: &mut Option<Child>) -> StopProcessOutcome {
    if !process_slot_is_running(process) {
        return StopProcessOutcome::NoProcess;
    }

    if let Some(child) = process.as_mut() {
        if let Some(stdin) = child.stdin.as_mut() {
            let _ = stdin.write_all(b"stop\n").await;
            let _ = stdin.flush().await;
        }
    }

    match wait_for_slot_exit(process, STOP_GRACEFUL_TIMEOUT).await {
        Ok(true) => return StopProcessOutcome::Graceful,
        Ok(false) => {}
        Err(error) => {
            return StopProcessOutcome::Error(format!("Stop check failed: {}", error));
        }
    }

    let mut ctrl_c_error: Option<String> = None;
    match send_ctrl_c_to_slot(process) {
        Ok(()) => {
            match wait_for_slot_exit(process, STOP_CTRL_C_TIMEOUT).await {
                Ok(true) => return StopProcessOutcome::CtrlCFallback,
                Ok(false) => {}
                Err(error) => {
                    return StopProcessOutcome::Error(format!(
                        "Ctrl+C stop check failed: {}",
                        error
                    ));
                }
            }
        }
        Err(error) => {
            ctrl_c_error = Some(error.to_string());
        }
    }

    if let Err(error) = force_kill_slot(process) {
        if let Some(ctrl_c_error) = ctrl_c_error {
            return StopProcessOutcome::Error(format!(
                "Ctrl+C fallback failed ({}) and force kill failed: {}",
                ctrl_c_error, error
            ));
        }

        return StopProcessOutcome::Error(format!("Force kill failed: {}", error));
    }

    match wait_for_slot_exit(process, STOP_FORCE_KILL_TIMEOUT).await {
        Ok(true) => StopProcessOutcome::ForceKilled,
        Ok(false) => StopProcessOutcome::ForceKillTimedOut,
        Err(error) => StopProcessOutcome::Error(format!("Force kill check failed: {}", error)),
    }
}

fn validate_instance_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Instance name cannot be empty".to_string());
    }
    if name.len() > 64 {
        return Err("Instance name is too long (max 64 characters)".to_string());
    }

    let invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t'];
    for ch in invalid_chars {
        if name.contains(ch) {
            return Err(format!("Instance name cannot contain '{}'", ch));
        }
    }

    let reserved = [".", "..", "CON", "PRN", "AUX", "NUL"];
    if reserved.contains(&name.to_uppercase().as_str()) {
        return Err(format!("'{}' is a reserved name", name));
    }

    Ok(())
}

fn validate_version_format(version: &str) -> Result<(), String> {
    let pattern = Regex::new(r"^\d{1,2}\.\d{1,3}\.\d{1,3}\.\d{1,3}$").unwrap();
    if pattern.is_match(version) {
        Ok(())
    } else {
        Err("Invalid version format. Use format like: 1.21.0.3".to_string())
    }
}

fn get_minara_data_dir() -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(".local").join("share").join("minara"),
        None => PathBuf::from(".minara"),
    }
}

fn get_instances_dir() -> String {
    get_minara_data_dir()
        .join("instances")
        .to_string_lossy()
        .into_owned()
}

fn get_backups_dir() -> String {
    get_minara_data_dir()
        .join("backups")
        .to_string_lossy()
        .into_owned()
}

fn ensure_minara_data_dirs() -> Result<(), String> {
    let data_root = get_minara_data_dir();
    let instances_root = data_root.join("instances");
    let backups_root = data_root.join("backups");

    fs::create_dir_all(&instances_root)
        .map_err(|e| format!("Failed to create instances directory {}: {}", instances_root.display(), e))?;
    fs::create_dir_all(&backups_root)
        .map_err(|e| format!("Failed to create backups directory {}: {}", backups_root.display(), e))?;

    Ok(())
}

fn format_bytes(size: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let size_f = size as f64;
    if size_f < KB {
        format!("{} B", size)
    } else if size_f < MB {
        format!("{:.1} KB", size_f / KB)
    } else if size_f < GB {
        format!("{:.1} MB", size_f / MB)
    } else {
        format!("{:.1} GB", size_f / GB)
    }
}

fn parse_server_properties(instance_path: &StdPath) -> Vec<ServerProperty> {
    let file_path = instance_path.join("server.properties");
    let content = match fs::read_to_string(file_path) {
        Ok(text) => text,
        Err(_) => return Vec::new(),
    };

    let mut settings = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = trimmed.split_once('=') {
            settings.push(ServerProperty {
                key: key.trim().to_string(),
                value: value.trim().to_string(),
            });
        }
    }

    settings
}

fn parse_property_key_from_line(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let (key, _) = trimmed.split_once('=')?;
    let key = key.trim();
    if key.is_empty() {
        None
    } else {
        Some(key.to_string())
    }
}

fn merge_server_properties_content(existing_content: &str, updates_in_order: &[(String, String)]) -> String {
    let update_lookup: HashMap<&str, &str> = updates_in_order
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();

    let mut seen_keys: HashSet<String> = HashSet::new();
    let mut output_lines: Vec<String> = Vec::new();

    for line in existing_content.lines() {
        if let Some(key) = parse_property_key_from_line(line) {
            if let Some(new_value) = update_lookup.get(key.as_str()) {
                output_lines.push(format!("{}={}", key, new_value));
                seen_keys.insert(key);
                continue;
            }
        }

        output_lines.push(line.to_string());
    }

    let mut has_missing_keys = false;
    for (key, _) in updates_in_order {
        if !seen_keys.contains(key) {
            has_missing_keys = true;
            break;
        }
    }

    if has_missing_keys {
        if !output_lines.is_empty() {
            let last_is_blank = output_lines
                .last()
                .map(|line| line.trim().is_empty())
                .unwrap_or(false);
            if !last_is_blank {
                output_lines.push(String::new());
            }
        }

        for (key, value) in updates_in_order {
            if !seen_keys.contains(key) {
                output_lines.push(format!("{}={}", key, value));
            }
        }
    }

    let mut merged = output_lines.join("\n");
    if !merged.is_empty() {
        merged.push('\n');
    }

    merged
}

fn normalize_relative_path(raw_path: &str) -> Result<PathBuf, String> {
    let cleaned = raw_path.trim().replace('\\', "/");
    if cleaned.is_empty() {
        return Ok(PathBuf::new());
    }

    let candidate = StdPath::new(&cleaned);
    if candidate.is_absolute() {
        return Err("Absolute paths are not allowed".to_string());
    }

    let mut normalized = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => normalized.push(segment),
            _ => return Err("Invalid path".to_string()),
        }
    }

    Ok(normalized)
}

fn relative_path_to_string(path: &StdPath) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(segment) => Some(segment.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn resolve_instance_subpath(instance_root: &StdPath, requested_path: Option<&str>) -> Result<(PathBuf, String), String> {
    let relative_path = match requested_path {
        Some(raw) if !raw.trim().is_empty() => normalize_relative_path(raw)?,
        _ => PathBuf::new(),
    };

    let relative_string = relative_path_to_string(&relative_path);
    Ok((instance_root.join(relative_path), relative_string))
}

fn sanitize_upload_filename(raw_name: &str) -> Option<String> {
    let trimmed = raw_name.trim();
    if trimmed.is_empty() {
        return None;
    }

    let base_name = StdPath::new(trimmed).file_name()?.to_str()?.trim();
    if base_name.is_empty() || base_name == "." || base_name == ".." {
        return None;
    }

    Some(base_name.to_string())
}

fn parent_relative_path(path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }

    let mut parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.pop().is_none() {
        return None;
    }

    if parts.is_empty() {
        Some(String::new())
    } else {
        Some(parts.join("/"))
    }
}

fn build_file_breadcrumbs(current_path: &str) -> Vec<FileBreadcrumb> {
    let mut breadcrumbs = vec![FileBreadcrumb {
        label: "root".to_string(),
        path: String::new(),
        is_current: current_path.is_empty(),
    }];

    if current_path.is_empty() {
        return breadcrumbs;
    }

    let mut segments: Vec<String> = Vec::new();
    for segment in current_path.split('/').filter(|part| !part.is_empty()) {
        segments.push(segment.to_string());
        breadcrumbs.push(FileBreadcrumb {
            label: segment.to_string(),
            path: segments.join("/"),
            is_current: false,
        });
    }

    if let Some(last) = breadcrumbs.last_mut() {
        last.is_current = true;
    }

    breadcrumbs
}

fn collect_instance_entries(directory_path: &StdPath, current_path: &str) -> Vec<InstanceFileEntry> {
    let mut entries = Vec::new();

    if let Ok(read_dir) = fs::read_dir(directory_path) {
        for entry in read_dir.flatten() {
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };

            if name == INSTANCE_ID_FILENAME {
                continue;
            }

            let metadata = entry.metadata().ok();
            let is_directory = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let size_label = if is_directory {
                "-".to_string()
            } else {
                metadata
                    .as_ref()
                    .map(|m| format_bytes(m.len()))
                    .unwrap_or_else(|| "--".to_string())
            };

            let relative_path = if current_path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", current_path, name)
            };

            entries.push(InstanceFileEntry {
                name,
                is_directory,
                size_label,
                relative_path,
            });
        }
    }

    entries.sort_by(|a, b| {
        if a.is_directory != b.is_directory {
            return b.is_directory.cmp(&a.is_directory);
        }

        a.name.to_lowercase().cmp(&b.name.to_lowercase())
    });

    entries
}

fn validate_backup_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("Backup id cannot be empty".to_string());
    }
    if id.len() > 128 {
        return Err("Backup id is too long".to_string());
    }

    let pattern = Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap();
    if !pattern.is_match(id) {
        return Err("Backup id contains invalid characters".to_string());
    }

    Ok(())
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn instance_identity_path(instance_path: &StdPath) -> PathBuf {
    instance_path.join(INSTANCE_ID_FILENAME)
}

fn read_instance_identity(instance_path: &StdPath) -> Option<String> {
    let identity_path = instance_identity_path(instance_path);
    let text = fs::read_to_string(identity_path).ok()?;
    let identity = text.trim().to_string();
    if identity.is_empty() {
        None
    } else {
        Some(identity)
    }
}

fn write_instance_identity(instance_path: &StdPath, identity: &str) -> Result<(), String> {
    if identity.trim().is_empty() {
        return Err("Instance identity cannot be empty".to_string());
    }

    let identity_path = instance_identity_path(instance_path);
    fs::write(&identity_path, format!("{}\n", identity.trim()))
        .map_err(|e| format!("Failed to write instance identity {}: {}", identity_path.display(), e))
}

fn assign_new_instance_identity(instance_path: &StdPath) -> Result<String, String> {
    let new_identity = Uuid::new_v4().to_string();
    write_instance_identity(instance_path, &new_identity)?;
    Ok(new_identity)
}

fn ensure_instance_identity(instance_path: &StdPath) -> Result<String, String> {
    if let Some(existing) = read_instance_identity(instance_path) {
        return Ok(existing);
    }

    assign_new_instance_identity(instance_path)
}

fn backup_source_identity(metadata: &BackupMetadata, backup_files: &StdPath) -> Option<String> {
    metadata
        .source_instance_id
        .clone()
        .or_else(|| read_instance_identity(backup_files))
}

fn find_instance_by_identity_proof(instances_root: &StdPath, proof_sha256: &str) -> Result<Option<(String, PathBuf)>, String> {
    if !instances_root.is_dir() {
        return Ok(None);
    }

    let mut matched: Option<(String, PathBuf)> = None;

    for entry in fs::read_dir(instances_root)
        .map_err(|e| format!("Failed to read instances directory {}: {}", instances_root.display(), e))?
    {
        let entry = entry.map_err(|e| format!("Failed to read instance entry: {}", e))?;
        let instance_path = entry.path();
        if !instance_path.is_dir() {
            continue;
        }

        let instance_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let Some(instance_identity) = read_instance_identity(&instance_path) else {
            continue;
        };

        if sha256_hex(&instance_identity) == proof_sha256 {
            if matched.is_some() {
                return Err("Multiple instances match this backup identity proof; refusing unsafe override".to_string());
            }
            matched = Some((instance_name, instance_path));
        }
    }

    Ok(matched)
}

fn build_instance_proof_index(instances_root: &StdPath) -> Result<HashMap<String, String>, String> {
    let mut proof_index: HashMap<String, String> = HashMap::new();

    if !instances_root.is_dir() {
        return Ok(proof_index);
    }

    for entry in fs::read_dir(instances_root)
        .map_err(|e| format!("Failed to read instances directory {}: {}", instances_root.display(), e))?
    {
        let entry = entry.map_err(|e| format!("Failed to read instance entry: {}", e))?;
        let instance_path = entry.path();
        if !instance_path.is_dir() {
            continue;
        }

        let instance_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let instance_identity = match ensure_instance_identity(&instance_path) {
            Ok(id) => id,
            Err(_) => continue,
        };

        let proof = sha256_hex(&instance_identity);
        proof_index.entry(proof).or_insert(instance_name);
    }

    Ok(proof_index)
}

fn relink_backup_metadata_source_names(old_name: &str, new_name: &str, new_instance_id: &str, new_proof_sha256: &str) -> Result<u64, String> {
    let root = PathBuf::from(get_backups_dir());
    if !root.is_dir() {
        return Ok(0);
    }

    let instances_root = PathBuf::from(get_instances_dir());

    let mut updated_count: u64 = 0;

    for instance_entry in fs::read_dir(&root).map_err(|e| format!("Failed to read backups root: {}", e))? {
        let instance_entry = instance_entry.map_err(|e| format!("Failed to read backups entry: {}", e))?;
        let instance_path = instance_entry.path();
        if !instance_path.is_dir() {
            continue;
        }

        for backup_entry in fs::read_dir(&instance_path).map_err(|e| format!("Failed to read instance backup folder: {}", e))? {
            let backup_entry = backup_entry.map_err(|e| format!("Failed to read backup entry: {}", e))?;
            let backup_path = backup_entry.path();
            if !backup_path.is_dir() {
                continue;
            }

            let meta_path = backup_path.join("meta.json");
            if !meta_path.is_file() {
                continue;
            }

            let meta_text = match fs::read_to_string(&meta_path) {
                Ok(text) => text,
                Err(_) => continue,
            };

            let mut metadata: BackupMetadata = match serde_json::from_str(&meta_text) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };

            let id_match = metadata.source_instance_id.as_deref() == Some(new_instance_id);
            let proof_match = metadata.source_instance_proof_sha256.as_deref() == Some(new_proof_sha256);
            let legacy_name_match = metadata.source_instance == old_name && metadata.source_instance_proof_sha256.is_none();

            let orphan_name_rebind = if metadata.source_instance == old_name && !id_match && !proof_match {
                if let Some(proof) = metadata.source_instance_proof_sha256.as_deref() {
                    matches!(find_instance_by_identity_proof(&instances_root, proof), Ok(None))
                } else {
                    false
                }
            } else {
                false
            };

            if !id_match && !proof_match && !legacy_name_match && !orphan_name_rebind {
                continue;
            }

            let mut changed = false;
            if metadata.source_instance != new_name {
                metadata.source_instance = new_name.to_string();
                changed = true;
            }

            if metadata.source_instance_proof_sha256.as_deref() != Some(new_proof_sha256) {
                metadata.source_instance_proof_sha256 = Some(new_proof_sha256.to_string());
                changed = true;
            }

            if metadata.source_instance_id.as_deref() != Some(new_instance_id) {
                metadata.source_instance_id = Some(new_instance_id.to_string());
                changed = true;
            }

            if !changed {
                continue;
            }

            let updated_text = serde_json::to_string_pretty(&metadata)
                .map_err(|e| format!("Failed to encode backup metadata {}: {}", meta_path.display(), e))?;
            fs::write(&meta_path, updated_text)
                .map_err(|e| format!("Failed to write backup metadata {}: {}", meta_path.display(), e))?;

            updated_count += 1;
        }
    }

    Ok(updated_count)
}

fn copy_dir_recursive(src: &StdPath, dst: &StdPath) -> Result<(), String> {
    if !src.is_dir() {
        return Err(format!("Source path is not a directory: {}", src.display()));
    }

    fs::create_dir_all(dst).map_err(|e| format!("Failed to create directory {}: {}", dst.display(), e))?;

    for entry in fs::read_dir(src).map_err(|e| format!("Failed to read directory {}: {}", src.display(), e))? {
        let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type().map_err(|e| format!("Failed to get file type for {}: {}", src_path.display(), e))?;

        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            fs::copy(&src_path, &dst_path)
                .map_err(|e| format!("Failed to copy {} to {}: {}", src_path.display(), dst_path.display(), e))?;

            if let Ok(meta) = fs::metadata(&src_path) {
                let _ = fs::set_permissions(&dst_path, meta.permissions());
            }
        }
    }

    Ok(())
}

fn compute_dir_stats(path: &StdPath) -> Result<(u64, u64), String> {
    if path.is_file() {
        let size = fs::metadata(path)
            .map_err(|e| format!("Failed to read file metadata {}: {}", path.display(), e))?
            .len();
        return Ok((size, 1));
    }

    if !path.is_dir() {
        return Ok((0, 0));
    }

    let mut total_size: u64 = 0;
    let mut total_files: u64 = 0;

    for entry in fs::read_dir(path).map_err(|e| format!("Failed to read directory {}: {}", path.display(), e))? {
        let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
        let child = entry.path();
        let (size, files) = compute_dir_stats(&child)?;
        total_size += size;
        total_files += files;
    }

    Ok((total_size, total_files))
}

fn collect_backups() -> Result<Vec<BackupMetadata>, String> {
    let root = PathBuf::from(get_backups_dir());
    if !root.is_dir() {
        return Ok(Vec::new());
    }

    let instances_root = PathBuf::from(get_instances_dir());
    let proof_index = build_instance_proof_index(&instances_root).unwrap_or_default();

    let mut backups = Vec::new();

    for instance_entry in fs::read_dir(&root).map_err(|e| format!("Failed to read backups root: {}", e))? {
        let instance_entry = instance_entry.map_err(|e| format!("Failed to read backups entry: {}", e))?;
        let instance_path = instance_entry.path();
        if !instance_path.is_dir() {
            continue;
        }

        for backup_entry in fs::read_dir(&instance_path).map_err(|e| format!("Failed to read instance backup folder: {}", e))? {
            let backup_entry = backup_entry.map_err(|e| format!("Failed to read backup entry: {}", e))?;
            let backup_path = backup_entry.path();
            if !backup_path.is_dir() {
                continue;
            }

            let meta_path = backup_path.join("meta.json");
            if !meta_path.is_file() {
                continue;
            }

            let meta_text = match fs::read_to_string(&meta_path) {
                Ok(text) => text,
                Err(_) => continue,
            };

            let mut metadata: BackupMetadata = match serde_json::from_str(&meta_text) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };

            let mut changed = false;

            let backup_files = backup_path.join("files");
            if metadata.source_instance_id.is_none() {
                if let Some(source_id) = read_instance_identity(&backup_files) {
                    metadata.source_instance_id = Some(source_id);
                    changed = true;
                }
            }

            if metadata.source_instance_proof_sha256.is_none() {
                if let Some(source_id) = metadata.source_instance_id.as_deref() {
                    metadata.source_instance_proof_sha256 = Some(sha256_hex(source_id));
                    changed = true;
                }
            }

            // Auto-heal source names when an instance was renamed but keeps the same identity proof.
            if let Some(proof) = metadata.source_instance_proof_sha256.as_deref() {
                if let Some(current_name) = proof_index.get(proof) {
                    if metadata.source_instance != *current_name {
                        metadata.source_instance = current_name.clone();
                        changed = true;
                    }
                }
            }

            if changed {
                if let Ok(updated_meta) = serde_json::to_string_pretty(&metadata) {
                    let _ = fs::write(&meta_path, updated_meta);
                }
            }

            backups.push(metadata);
        }
    }

    backups.sort_by(|a, b| b.created_at_unix.cmp(&a.created_at_unix));
    Ok(backups)
}

fn find_backup_by_id(id: &str) -> Result<(PathBuf, BackupMetadata), String> {
    let root = PathBuf::from(get_backups_dir());
    if !root.is_dir() {
        return Err("No backups found".to_string());
    }

    for instance_entry in fs::read_dir(&root).map_err(|e| format!("Failed to read backups root: {}", e))? {
        let instance_entry = instance_entry.map_err(|e| format!("Failed to read backups entry: {}", e))?;
        let instance_path = instance_entry.path();
        if !instance_path.is_dir() {
            continue;
        }

        let backup_path = instance_path.join(id);
        if !backup_path.is_dir() {
            continue;
        }

        let meta_path = backup_path.join("meta.json");
        if !meta_path.is_file() {
            return Err("Backup metadata is missing".to_string());
        }

        let meta_text = fs::read_to_string(&meta_path)
            .map_err(|e| format!("Failed to read backup metadata {}: {}", meta_path.display(), e))?;
        let metadata: BackupMetadata = serde_json::from_str(&meta_text)
            .map_err(|e| format!("Invalid backup metadata {}: {}", meta_path.display(), e))?;

        return Ok((backup_path, metadata));
    }

    Err("Backup not found".to_string())
}

async fn fetch_versions() -> Result<VersionResponse, String> {
    let wiki_url = "https://minecraft.wiki/w/Bedrock_Dedicated_Server";

    match tokio::process::Command::new("curl")
        .args(&["-s", wiki_url])
        .output()
        .await
    {
        Ok(output) => {
            if !output.status.success() {
                return Err("Failed to fetch wiki page".to_string());
            }

            let html = String::from_utf8_lossy(&output.stdout);

            let release_pattern = Regex::new(r#"<b>Release:</b>.*?<a[^>]*>(\d+\.\d+\.\d+\.\d+)</a>"#).unwrap();
            let preview_pattern = Regex::new(r#"<b>Preview:</b>.*?<a[^>]*>(\d+\.\d+\.\d+\.\d+)</a>"#).unwrap();

            let release = release_pattern
                .captures(&html)
                .and_then(|caps| caps.get(1))
                .map(|m| m.as_str().to_string());

            let preview = preview_pattern
                .captures(&html)
                .and_then(|caps| caps.get(1))
                .map(|m| m.as_str().to_string());

            Ok(VersionResponse { release, preview })
        }
        Err(e) => Err(format!("curl not found or failed: {}", e)),
    }
}

async fn download_and_install(
    instance_name: &str,
    version: &str,
    progress: Arc<std::sync::atomic::AtomicU64>,
) -> Result<String, String> {
    use std::sync::atomic::Ordering;

    let instances_dir = get_instances_dir();
    let instance_path = format!("{}/{}", instances_dir, instance_name);

    if std::path::Path::new(&instance_path).exists() {
        return Err("Instance already exists".to_string());
    }

    let urls_to_try = vec![
        format!("https://www.minecraft.net/bedrockdedicatedserver/bin-linux/bedrock-server-{}.zip", version),
        format!("https://www.minecraft.net/bedrockdedicatedserver/bin-linux-preview/bedrock-server-{}.zip", version),
    ];

    let mut url = None;
    for test_url in urls_to_try.iter() {
        if check_url_exists(test_url).await {
            url = Some(test_url.clone());
            break;
        }
    }

    let url = match url {
        Some(u) => u,
        None => return Err(format!("Version {} not found on any server", version)),
    };

    let client = Client::new();
    let response = client.get(&url).send().await
        .map_err(|e| format!("Failed to connect: {}", e))?;

    let total_bytes = response.content_length().unwrap_or(0);
    let mut received: u64 = 0;
    let mut buf: Vec<u8> = if total_bytes > 0 { Vec::with_capacity(total_bytes as usize) } else { Vec::new() };

    let mut stream = response.bytes_stream();
    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk = chunk.map_err(|e| format!("Download error: {}", e))?;
        received += chunk.len() as u64;
        buf.extend_from_slice(&chunk);
        if total_bytes > 0 {
            // Reserve 0–90 for downloading, 90–100 for extracting
            let pct = (received * 90 / total_bytes).min(90);
            progress.store(pct, Ordering::Relaxed);
        }
    }

    // Extraction phase: set to 90, then 100 when done
    progress.store(90, Ordering::Relaxed);

    fs::create_dir_all(&instance_path)
        .map_err(|e| format!("Failed to create instance directory: {}", e))?;

    let cursor = std::io::Cursor::new(buf);
    match zip::ZipArchive::new(cursor) {
        Ok(mut archive) => {
            archive.extract(&instance_path)
                .map_err(|e| format!("Failed to extract: {}", e))?;

            let server_bin = format!("{}/bedrock_server", instance_path);
            if StdPath::new(&server_bin).exists() {
                #[cfg(unix)]
                {
                    use std::fs::Permissions;
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&server_bin, Permissions::from_mode(0o755))
                        .map_err(|e| format!("Failed to set executable: {}", e))?;
                }
            }

            if let Err(e) = assign_new_instance_identity(StdPath::new(&instance_path)) {
                let _ = fs::remove_dir_all(&instance_path);
                return Err(e);
            }

            progress.store(100, Ordering::Relaxed);
            Ok(format!("Instance '{}' created successfully!", instance_name))
        }
        Err(e) => {
            let _ = fs::remove_dir_all(&instance_path);
            Err(format!("Failed to extract zip: {}", e))
        }
    }
}

async fn check_url_exists(url: &str) -> bool {
    let client = reqwest::Client::new();
    let timeout = std::time::Duration::from_secs(5);

    match client.head(url).timeout(timeout).send().await {
        Ok(response) => {
            if response.status() == 200 {
                return true;
            }
        }
        Err(_) => {}
    }

    match client.get(url)
        .header("Range", "bytes=0-1")
        .timeout(timeout)
        .send()
        .await
    {
        Ok(response) => response.status() == 200 || response.status() == 206,
        Err(_) => false,
    }
}

#[tokio::main]
async fn main() {
    let development_mode = std::env::args().any(|arg| arg == "-d");
    let fixed_device_host = "localhost:3000".to_string();

    if let Err(e) = ensure_minara_data_dirs() {
        eprintln!("Failed to prepare Minara data directories: {}", e);
        std::process::exit(1);
    }

    let instances_dir = get_instances_dir();
    let mut instance_list = Vec::new();

    if let Ok(entries) = std::fs::read_dir(instances_dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_dir() {
                    let _ = ensure_instance_identity(&entry.path());
                    if let Some(name) = entry.file_name().into_string().ok() {
                        instance_list.push(InstanceInfo { name });
                    }
                }
            }
        }
    }

    instance_list.sort_by(|a, b| a.name.cmp(&b.name));

    let mut instances = HashMap::new();
    for info in &instance_list {
        let (tx, _) = broadcast::channel(100);
        instances.insert(
            info.name.clone(),
            Arc::new(InstanceState {
                process: Mutex::new(None),
                tx,
                logs: Arc::new(Mutex::new(Vec::new())),
            }),
        );
    }

    let shared_state = Arc::new(AppState {
        instances: RwLock::new(instances),
        device_host: Mutex::new(Some(fixed_device_host.clone())),
        development_mode,
        fixed_device_host: fixed_device_host.clone(),
        progress: RwLock::new(HashMap::new()),
        host_cpu_snapshot: Mutex::new(None),
    });

    let app = Router::new()
        .nest_service("/assets", ServeDir::new("assets"))
        .route("/", get(index))
        .route("/instance/{name}", get(console))
        .route("/instance/{name}/main", get(console))
        .route("/instance/{name}/settings", get(instance_settings))
        .route("/instance/{name}/files", get(instance_files))
        .route("/instance/{name}/files/download", get(download_instance_file))
        .route("/instance/{name}/status", get(get_status))
        .route("/instance/{name}/start", post(start_server))
        .route("/instance/{name}/stop", post(stop_server))
        .route("/instance/{name}/input", post(send_input))
        .route("/instance/{name}/stream", get(stream_logs))
        .route("/api/versions", get(api_versions))
        .route("/api/instances", get(api_instances))
        .route("/api/create-instance", post(api_create_instance))
        .route("/api/instance-ready/{name}", get(api_instance_ready))
        .route("/api/check-version/{version}", get(api_check_version))
        .route("/api/instance-progress/{name}", get(api_instance_progress))
        .route("/api/instance/{name}/settings", post(api_save_instance_settings))
        .route(
            "/api/instance/{name}/files/content",
            get(api_get_instance_file_content).post(api_save_instance_file_content),
        )
        .route("/api/instance/{name}/files/upload", post(api_upload_instance_files))
        .route("/api/instance/{name}/files/folder", post(api_create_instance_folder))
        .route("/api/instance/{name}/files/delete", post(api_delete_instance_entry))
        .route("/api/backups", get(api_backups).post(api_create_backup))
        .route("/api/backups/{id}/restore", post(api_restore_backup))
        .route("/api/backups/{id}", delete(api_delete_backup))
        .route("/api/rename-instance/{name}", post(api_rename_instance))
        .route("/api/delete-instance/{name}", delete(api_delete_instance))
        .route("/api/host-stats", get(api_host_stats))
        .route("/api/device-stats", get(api_device_stats))
        .route("/api/process-stats/{name}", get(api_process_stats))
        .route("/device", get(device))
        .route("/backups", get(backups))
        .with_state(shared_state.clone());

    let addr = "0.0.0.0:7777";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let panel_host_ip = detect_panel_host_ip();
    let panel_url = format!("http://{}:7777", panel_host_ip);

    let asmo_ready = check_asmo_readiness(&fixed_device_host).await;

    print_startup_banner(development_mode, &panel_url, asmo_ready);

    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(err) = result {
                eprintln!("Server error: {}", err);
            }
        }
        _ = shutdown_signal() => {
            eprintln!("Shutdown signal received. Exiting Minara immediately.");
            std::process::exit(0);
        }
    }
}

// FIX: _state prefix tells the compiler the unused State extractor is intentional.
// It's kept in the signature because Axum requires consistent extractor patterns
// when the router uses .with_state().
async fn index(State(_state): State<Arc<AppState>>) -> IndexTemplate {
    let instances_dir = get_instances_dir();
    let mut instance_list = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&instances_dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_dir() {
                    if let Some(name) = entry.file_name().into_string().ok() {
                        instance_list.push(InstanceInfo { name });
                    }
                }
            }
        }
    }

    instance_list.sort_by(|a, b| a.name.cmp(&b.name));

    IndexTemplate {
        instances: instance_list,
    }
}

async fn console(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> impl IntoResponse {
    let instances_dir = get_instances_dir();
    let instance_path = format!("{}/{}", instances_dir, name);
    let allow_device_host_edit = state.development_mode;
    let default_device_host = state.fixed_device_host.clone();

    if !std::path::Path::new(&instance_path).is_dir() {
        return Redirect::to("/").into_response();
    }

    let instances = state.instances.read().await;
    let history = if let Some(instance) = instances.get(&name) {
        instance.logs.lock().await.clone()
    } else {
        Vec::new()
    };

    let mut instance_list = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&instances_dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_dir() {
                    if let Some(inst_name) = entry.file_name().into_string().ok() {
                        instance_list.push(InstanceInfo { name: inst_name });
                    }
                }
            }
        }
    }

    instance_list.sort_by(|a, b| a.name.cmp(&b.name));

    ConsoleTemplate {
        instance_name: name,
        history,
        instances: instance_list,
        allow_device_host_edit,
        default_device_host,
    }.into_response()
}

async fn instance_settings(Path(name): Path<String>) -> impl IntoResponse {
    let instances_dir = get_instances_dir();
    let instance_path = StdPath::new(&instances_dir).join(&name);

    if !instance_path.is_dir() {
        return Redirect::to("/").into_response();
    }

    let settings = parse_server_properties(&instance_path);

    InstanceSettingsTemplate {
        instance_name: name,
        settings,
    }
    .into_response()
}

async fn instance_files(Path(name): Path<String>, Query(query): Query<InstanceFilesQuery>) -> impl IntoResponse {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);

    if !instance_root.is_dir() {
        return Redirect::to("/").into_response();
    }

    let (candidate_path, candidate_relative) = match resolve_instance_subpath(&instance_root, query.path.as_deref()) {
        Ok(resolved) => resolved,
        Err(_) => (instance_root.clone(), String::new()),
    };

    let (current_directory, current_path) = if candidate_path.is_dir() {
        (candidate_path, candidate_relative)
    } else {
        (instance_root.clone(), String::new())
    };

    let entries = collect_instance_entries(&current_directory, &current_path);
    let directory_count = entries.iter().filter(|entry| entry.is_directory).count();
    let file_count = entries.len().saturating_sub(directory_count);
    let breadcrumbs = build_file_breadcrumbs(&current_path);
    let parent_path = parent_relative_path(&current_path);

    InstanceFilesTemplate {
        instance_name: name,
        current_path,
        has_parent: parent_path.is_some(),
        parent_path: parent_path.unwrap_or_default(),
        breadcrumbs,
        entries,
        directory_count,
        file_count,
    }
    .into_response()
}

async fn api_save_instance_settings(
    Path(name): Path<String>,
    Json(payload): Json<SaveSettingsRequest>,
) -> Json<serde_json::Value> {
    let instances_dir = get_instances_dir();
    let instance_path = StdPath::new(&instances_dir).join(&name);

    if !instance_path.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Instance does not exist"
        }));
    }

    let mut serialized_settings: Vec<(String, String)> = Vec::new();
    let mut seen_keys = HashSet::new();

    for item in payload.settings {
        let key = item.key.trim();
        if key.is_empty() {
            continue;
        }

        if key.contains('=') || key.contains('\n') || key.contains('\r') {
            return Json(json!({
                "status": "error",
                "message": format!("Invalid setting key: {}", key)
            }));
        }

        if !seen_keys.insert(key.to_string()) {
            continue;
        }

        let value = item.value.replace('\n', " ").replace('\r', " ");
        serialized_settings.push((key.to_string(), value.trim().to_string()));
    }

    if serialized_settings.is_empty() {
        return Json(json!({
            "status": "error",
            "message": "Add at least one valid setting before saving"
        }));
    }

    let file_path = instance_path.join("server.properties");

    let existing_content = if file_path.exists() {
        match fs::read_to_string(&file_path) {
            Ok(content) => content,
            Err(error) => {
                return Json(json!({
                    "status": "error",
                    "message": format!("Failed to read existing server.properties: {}", error)
                }));
            }
        }
    } else {
        String::new()
    };

    let file_content = merge_server_properties_content(&existing_content, &serialized_settings);

    match fs::write(&file_path, file_content) {
        Ok(_) => Json(json!({
            "status": "success",
            "message": "server.properties saved",
            "count": serialized_settings.len()
        })),
        Err(error) => Json(json!({
            "status": "error",
            "message": format!("Failed to write server.properties: {}", error)
        })),
    }
}

async fn api_get_instance_file_content(
    Path(name): Path<String>,
    Query(query): Query<FileContentQuery>,
) -> Json<serde_json::Value> {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);

    if !instance_root.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Instance does not exist"
        }));
    }

    let (file_path, relative_path) = match resolve_instance_subpath(&instance_root, Some(&query.path)) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Json(json!({
                "status": "error",
                "message": error
            }));
        }
    };

    if relative_path.is_empty() || !file_path.is_file() {
        return Json(json!({
            "status": "error",
            "message": "File does not exist"
        }));
    }

    if file_path.file_name().and_then(|name| name.to_str()) == Some(INSTANCE_ID_FILENAME) {
        return Json(json!({
            "status": "error",
            "message": "This file is protected"
        }));
    }

    let bytes = match fs::read(&file_path) {
        Ok(content) => content,
        Err(error) => {
            return Json(json!({
                "status": "error",
                "message": format!("Failed to read file: {}", error)
            }));
        }
    };

    let content = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => {
            return Json(json!({
                "status": "error",
                "message": "This file is not UTF-8 text and cannot be edited here"
            }));
        }
    };

    Json(json!({
        "status": "success",
        "path": relative_path,
        "content": content
    }))
}

async fn api_save_instance_file_content(
    Path(name): Path<String>,
    Json(payload): Json<SaveFileContentRequest>,
) -> Json<serde_json::Value> {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);

    if !instance_root.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Instance does not exist"
        }));
    }

    let source_reference = payload
        .original_path
        .as_deref()
        .unwrap_or(payload.path.as_str());

    let (source_path, source_relative_path) = match resolve_instance_subpath(&instance_root, Some(source_reference)) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Json(json!({
                "status": "error",
                "message": error
            }));
        }
    };

    let (target_path, target_relative_path) = match resolve_instance_subpath(&instance_root, Some(payload.path.as_str())) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Json(json!({
                "status": "error",
                "message": error
            }));
        }
    };

    if source_relative_path.is_empty() || target_relative_path.is_empty() {
        return Json(json!({
            "status": "error",
            "message": "Invalid file path"
        }));
    }

    if source_path.file_name().and_then(|name| name.to_str()) == Some(INSTANCE_ID_FILENAME)
        || target_path.file_name().and_then(|name| name.to_str()) == Some(INSTANCE_ID_FILENAME)
    {
        return Json(json!({
            "status": "error",
            "message": "This file is protected"
        }));
    }

    if !source_path.is_file() {
        return Json(json!({
            "status": "error",
            "message": "Source file does not exist"
        }));
    }

    if target_path.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Cannot save to a directory"
        }));
    }

    if let Some(parent) = target_path.parent() {
        if !parent.is_dir() {
            return Json(json!({
                "status": "error",
                "message": "Parent directory does not exist"
            }));
        }
    }

    if source_path != target_path {
        if target_path.exists() {
            return Json(json!({
                "status": "error",
                "message": "Target file already exists"
            }));
        }

        if let Err(error) = fs::rename(&source_path, &target_path) {
            return Json(json!({
                "status": "error",
                "message": format!("Failed to rename file: {}", error)
            }));
        }
    }

    match fs::write(&target_path, payload.content) {
        Ok(_) => Json(json!({
            "status": "success",
            "message": if source_relative_path != target_relative_path {
                "File renamed and saved"
            } else {
                "File saved"
            },
            "path": target_relative_path
        })),
        Err(error) => Json(json!({
            "status": "error",
            "message": format!("Failed to save file: {}", error)
        })),
    }
}

async fn api_upload_instance_files(
    Path(name): Path<String>,
    Query(query): Query<InstanceFilesQuery>,
    mut multipart: Multipart,
) -> Json<serde_json::Value> {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);

    if !instance_root.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Instance does not exist"
        }));
    }

    let (target_dir, target_relative) = match resolve_instance_subpath(&instance_root, query.path.as_deref()) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Json(json!({
                "status": "error",
                "message": error
            }));
        }
    };

    if !target_dir.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Upload target is not a directory"
        }));
    }

    let mut uploaded_names: Vec<String> = Vec::new();

    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(error) => {
                return Json(json!({
                    "status": "error",
                    "message": format!("Upload stream error: {}", error)
                }));
            }
        };

        let Some(raw_name) = field.file_name().map(|name| name.to_string()) else {
            continue;
        };

        let Some(file_name) = sanitize_upload_filename(&raw_name) else {
            continue;
        };

        if file_name == INSTANCE_ID_FILENAME {
            continue;
        }

        let bytes = match field.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Json(json!({
                    "status": "error",
                    "message": format!("Failed to read uploaded data: {}", error)
                }));
            }
        };

        let destination = target_dir.join(&file_name);
        if let Err(error) = fs::write(&destination, &bytes) {
            return Json(json!({
                "status": "error",
                "message": format!("Failed to store {}: {}", file_name, error)
            }));
        }

        uploaded_names.push(file_name);
    }

    if uploaded_names.is_empty() {
        return Json(json!({
            "status": "error",
            "message": "No files were uploaded"
        }));
    }

    Json(json!({
        "status": "success",
        "message": format!("Uploaded {} file(s)", uploaded_names.len()),
        "path": target_relative,
        "files": uploaded_names
    }))
}

async fn api_create_instance_folder(
    Path(name): Path<String>,
    Json(payload): Json<CreateFolderRequest>,
) -> Json<serde_json::Value> {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);

    if !instance_root.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Instance does not exist"
        }));
    }

    let folder_name = payload.folder_name.trim();
    if folder_name.is_empty() {
        return Json(json!({
            "status": "error",
            "message": "Folder name cannot be empty"
        }));
    }

    if folder_name == "." || folder_name == ".." || folder_name.contains('/') || folder_name.contains('\\') {
        return Json(json!({
            "status": "error",
            "message": "Folder name contains invalid characters"
        }));
    }

    let (parent_path, parent_relative) = match resolve_instance_subpath(&instance_root, payload.current_path.as_deref()) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Json(json!({
                "status": "error",
                "message": error
            }));
        }
    };

    if !parent_path.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Target directory does not exist"
        }));
    }

    let new_folder = parent_path.join(folder_name);
    if new_folder.exists() {
        return Json(json!({
            "status": "error",
            "message": "A file or folder with that name already exists"
        }));
    }

    match fs::create_dir(&new_folder) {
        Ok(_) => {
            let created_path = if parent_relative.is_empty() {
                folder_name.to_string()
            } else {
                format!("{}/{}", parent_relative, folder_name)
            };

            Json(json!({
                "status": "success",
                "message": "Folder created",
                "path": created_path
            }))
        }
        Err(error) => Json(json!({
            "status": "error",
            "message": format!("Failed to create folder: {}", error)
        })),
    }
}

async fn api_delete_instance_entry(
    Path(name): Path<String>,
    Json(payload): Json<DeleteEntryRequest>,
) -> Json<serde_json::Value> {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);

    if !instance_root.is_dir() {
        return Json(json!({
            "status": "error",
            "message": "Instance does not exist"
        }));
    }

    let (target_path, relative_path) = match resolve_instance_subpath(&instance_root, Some(&payload.path)) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Json(json!({
                "status": "error",
                "message": error
            }));
        }
    };

    if relative_path.is_empty() {
        return Json(json!({
            "status": "error",
            "message": "Cannot delete the instance root"
        }));
    }

    if target_path.file_name().and_then(|name| name.to_str()) == Some(INSTANCE_ID_FILENAME) {
        return Json(json!({
            "status": "error",
            "message": "This file is protected"
        }));
    }

    if !target_path.exists() {
        return Json(json!({
            "status": "error",
            "message": "Path does not exist"
        }));
    }

    let (kind, delete_result) = if target_path.is_dir() {
        ("folder", fs::remove_dir_all(&target_path))
    } else {
        ("file", fs::remove_file(&target_path))
    };

    match delete_result {
        Ok(_) => Json(json!({
            "status": "success",
            "message": format!("{} deleted", if kind == "folder" { "Folder" } else { "File" }),
            "path": relative_path,
            "kind": kind
        })),
        Err(error) => Json(json!({
            "status": "error",
            "message": format!("Failed to delete {}: {}", kind, error)
        })),
    }
}

async fn download_instance_file(
    Path(name): Path<String>,
    Query(query): Query<DownloadFileQuery>,
) -> impl IntoResponse {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);

    if !instance_root.is_dir() {
        return (StatusCode::NOT_FOUND, "Instance not found").into_response();
    }

    let (file_path, relative_path) = match resolve_instance_subpath(&instance_root, Some(&query.path)) {
        Ok(resolved) => resolved,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };

    if relative_path.is_empty() || !file_path.is_file() {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    }

    let bytes = match fs::read(&file_path) {
        Ok(content) => content,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read file: {}", error),
            )
                .into_response()
        }
    };

    let file_name = file_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download.bin")
        .replace('"', "");

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));

    if let Ok(disposition) = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", file_name)) {
        headers.insert(header::CONTENT_DISPOSITION, disposition);
    }

    (headers, bytes).into_response()
}

async fn get_status(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Json<serde_json::Value> {
    let instances = state.instances.read().await;

    match instances.get(&name) {
        Some(instance) => {
            let mut lock = instance.process.lock().await;
            let running = process_slot_is_running(&mut lock);
            Json(json!({ "running": running }))
        }
        None => Json(json!({ "running": false })),
    }
}

async fn start_server(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Html<String> {
    let instances_dir = get_instances_dir();
    let instance_path = format!("{}/{}", instances_dir, name);
    let instance_root = PathBuf::from(&instance_path);

    if !std::path::Path::new(&instance_path).is_dir() {
        return Html("<span class='text-red-500'>Instance not found</span>".into());
    }

    let mut instances = state.instances.write().await;
    if !instances.contains_key(&name) {
        let (tx, _) = broadcast::channel(100);
        instances.insert(
            name.clone(),
            Arc::new(InstanceState {
                process: Mutex::new(None),
                tx,
                logs: Arc::new(Mutex::new(Vec::new())),
            }),
        );
    }

    let instance = instances.get(&name).unwrap().clone();
    drop(instances);

    let mut lock = instance.process.lock().await;
    if process_slot_is_running(&mut lock) {
        return Html("<span class='text-yellow-500'>Server is already running.</span>".into());
    }

    let stale_cleanup = cleanup_stale_instance_processes(&instance_root).await;
    if stale_cleanup.matched > 0 {
        let cleanup_message = if stale_cleanup.remaining == 0 {
            format!(
                "Recovered: cleaned {} stale process(es) before startup.",
                stale_cleanup.cleaned
            )
        } else {
            format!(
                "Warning: detected {} stale process(es), cleaned {}, {} still running.",
                stale_cleanup.matched,
                stale_cleanup.cleaned,
                stale_cleanup.remaining
            )
        };

        let _ = instance.tx.send(cleanup_message.clone());
        let mut logs = instance.logs.lock().await;
        logs.push(cleanup_message);
        if logs.len() > 200 {
            logs.remove(0);
        }
    }

    if stale_cleanup.remaining > 0 {
        return Html(format!(
            "<span class='text-red-500'>Found {} stale process(es) still running for this instance. Please press Stop again and retry.</span>",
            stale_cleanup.remaining
        ));
    }

    let host_arch = std::env::consts::ARCH;
    let use_bionilux = if state.development_mode {
        host_arch == "aarch64"
    } else {
        true
    };

    let mut cmd = if use_bionilux {
        let mut c = Command::new("bionilux");
        c.arg("./bedrock_server");
        c.env("BIONILUX_WAKELOCK", "1");
        // Ensure Box64 startup diagnostics are visible in the panel console.
        c.env("BOX64_LOG", "1");
        c.env("BOX64_NOBANNER", "0");
        c
    } else {
        Command::new("./bedrock_server")
    };

    if use_bionilux {
        let launch_line = "$ bionilux ./bedrock_server".to_string();
        let _ = instance.tx.send(launch_line.clone());
        let mut logs = instance.logs.lock().await;
        logs.push(launch_line);
        if logs.len() > 200 {
            logs.remove(0);
        }
        drop(logs);

        match std::env::var_os("HOME") {
            Some(home) => {
                let rcfile_path = PathBuf::from(home).join(".box64rc");
                cmd.env("BOX64_RCFILE", rcfile_path.as_os_str());

                if let Err(error) = fs::File::open(&rcfile_path) {
                    let warning = if error.kind() == std::io::ErrorKind::NotFound {
                        format!(
                            "Warning: BOX64_RCFILE not found at {}",
                            rcfile_path.display()
                        )
                    } else {
                        format!(
                            "Warning: BOX64_RCFILE is not readable at {} ({})",
                            rcfile_path.display(),
                            error
                        )
                    };

                    let _ = instance.tx.send(warning.clone());
                    let mut logs = instance.logs.lock().await;
                    logs.push(warning);
                    if logs.len() > 200 {
                        logs.remove(0);
                    }
                }
            }
            None => {
                let warning = "Warning: HOME is not set, cannot resolve ~/.box64rc".to_string();
                let _ = instance.tx.send(warning.clone());
                let mut logs = instance.logs.lock().await;
                logs.push(warning);
                if logs.len() > 200 {
                    logs.remove(0);
                }
            }
        }
    }

    #[cfg(unix)]
    unsafe {
        // Spawn in a dedicated process group so Ctrl+C fallback reaches
        // both bionilux and the bedrock_server child process tree.
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }

            #[cfg(target_os = "linux")]
            {
                // If Minara exits unexpectedly, ensure child server processes
                // are also terminated instead of becoming orphaned ghosts.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }

            Ok(())
        });
    }

    match cmd
        .current_dir(&instance_path)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(mut child) => {
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();
            let tx = instance.tx.clone();
            let logs_buffer = instance.logs.clone();

            let tx_clone = tx.clone();
            let logs_clone = logs_buffer.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    let mut l = logs_clone.lock().await;
                    l.push(line.clone());
                    if l.len() > 200 { l.remove(0); }
                    let _ = tx_clone.send(line);
                }
            });

            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    let mut l = logs_buffer.lock().await;
                    l.push(line.clone());
                    if l.len() > 200 { l.remove(0); }
                    let _ = tx.send(line);
                }
            });

            *lock = Some(child);
            Html("<span class='text-green-400'>Server started</span>".into())
        }
        Err(e) => {
            let error_msg = format!("Failed to start server: {}", e);
            let _ = instance.tx.send(error_msg.clone());
            // FIX: Use .lock().await instead of blocking_lock() to avoid
            // freezing the Tokio thread in an async context.
            let mut l = instance.logs.lock().await;
            l.push(error_msg);
            Html(format!("<span class='text-red-500'>Failed to start server: {}</span>", e))
        }
    }
}

async fn send_input(State(state): State<Arc<AppState>>, Path(name): Path<String>, Form(input): Form<ConsoleInput>) -> Html<String> {
    let instances = state.instances.read().await;

    match instances.get(&name) {
        Some(instance) => {
            let raw_command = input.command.trim().to_string();
            let command = raw_command.to_lowercase();

            if command.is_empty() {
                return Html("<span class='text-slate-500'>Command is empty</span>".into());
            }

            let echo = format!("> {}", raw_command);
            let _ = instance.tx.send(echo.clone());
            let mut l = instance.logs.lock().await;
            l.push(echo);
            if l.len() > 200 {
                l.remove(0);
            }
            drop(l);

            if command == "start" {
                drop(instances);
                return start_server(State(state), Path(name)).await;
            }

            if command == "stop" {
                drop(instances);
                return stop_server(State(state), Path(name)).await;
            }

            if command == "restart" {
                drop(instances);

                let stop_result = stop_server(State(state.clone()), Path(name.clone())).await;

                // If instance vanished between checks, bubble that response.
                if stop_result.0.contains("Instance not found") {
                    return stop_result;
                }

                // Graceful stop needs a brief delay before starting again.
                if stop_result.0.contains("Shutdown complete") {
                    tokio::time::sleep(std::time::Duration::from_millis(900)).await;
                }

                return start_server(State(state), Path(name)).await;
            }

            let mut lock = instance.process.lock().await;
            if !process_slot_is_running(&mut lock) {
                return Html("<span class='text-red-500'>Server Offline</span>".into());
            }

            if let Some(child) = lock.as_mut() {
                if let Some(stdin) = child.stdin.as_mut() {
                    let _ = stdin.write_all(format!("{}\n", raw_command).as_bytes()).await;
                    let _ = stdin.flush().await;
                    return Html("<span class='text-blue-400'>Command sent</span>".into());
                }
            }
            Html("<span class='text-red-500'>Server Offline</span>".into())
        }
        None => Html("<span class='text-red-500'>Instance not found</span>".into()),
    }
}

async fn stop_server(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Html<String> {
    let instances_dir = get_instances_dir();
    let instance_root = StdPath::new(&instances_dir).join(&name);
    let instances = state.instances.read().await;

    match instances.get(&name) {
        Some(instance) => {
            let mut lock = instance.process.lock().await;
            let stop_outcome = stop_process_with_escalation(&mut lock).await;
            drop(lock);

            if matches!(stop_outcome, StopProcessOutcome::NoProcess) {
                let stale_cleanup = cleanup_stale_instance_processes(&instance_root).await;
                if stale_cleanup.matched > 0 {
                    if stale_cleanup.remaining == 0 {
                        return Html(format!(
                            "<span class='text-amber-300'>Shutdown complete (cleaned {} stale process{})</span>",
                            stale_cleanup.cleaned,
                            if stale_cleanup.cleaned == 1 { "" } else { "es" }
                        ));
                    }

                    return Html(format!(
                        "<span class='text-red-500'>Cleaned {} stale process(es), but {} still running</span>",
                        stale_cleanup.cleaned,
                        stale_cleanup.remaining
                    ));
                }
            }

            stop_outcome_to_html(stop_outcome)
        }
        None => Html("<span class='text-red-500'>Instance not found</span>".into()),
    }
}

async fn stream_logs(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let instances = state.instances.read().await;

    let rx = if let Some(instance) = instances.get(&name) {
        instance.tx.subscribe()
    } else {
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        let _ = tx.send("Instance not found".to_string());
        rx
    };

    let stream = BroadcastStream::new(rx).map(|msg| {
        match msg {
            Ok(text) => Ok(Event::default().data(format!("<div>{}</div>", text))),
            Err(_) => Ok(Event::default().data("<div>[Log Overflow]</div>")),
        }
    });

    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

#[derive(Deserialize)]
struct DeviceStatsQuery {
    host: Option<String>,
}

async fn api_process_stats(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    let instances = state.instances.read().await;
    let running = if let Some(inst) = instances.get(&name) {
        let mut lock = inst.process.lock().await;
        process_slot_is_running(&mut lock)
    } else {
        false
    };

    if !running {
        return Json(json!({ "running": false, "ram_mb": null }));
    }

    // Read bedrock_server process RAM from /proc — find child pid of bionilux
    // by scanning /proc for processes named bedrock_server
    let ram_mb: Option<f64> = (|| -> Option<f64> {
        let entries = std::fs::read_dir("/proc").ok()?;
        for entry in entries.flatten() {
            let pid_str = entry.file_name().into_string().ok()?;
            if !pid_str.chars().all(|c| c.is_ascii_digit()) { continue; }
            let comm = std::fs::read_to_string(format!("/proc/{}/comm", pid_str)).ok()?;
            if comm.trim() != "bedrock_server" { continue; }
            // Read VmRSS from status
            let status = std::fs::read_to_string(format!("/proc/{}/status", pid_str)).ok()?;
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
                    return Some(kb / 1024.0);
                }
            }
        }
        None
    })();

    Json(json!({ "running": true, "ram_mb": ram_mb }))
}

async fn api_host_stats(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let cpu_snapshot = match read_host_cpu_snapshot() {
        Ok(v) => v,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };

    let cpu_percent = {
        let mut prev = state.host_cpu_snapshot.lock().await;
        let pct = if let Some(last) = *prev {
            let total_diff = cpu_snapshot.total.saturating_sub(last.total);
            let idle_diff = cpu_snapshot.idle.saturating_sub(last.idle);

            if total_diff > 0 {
                Some(((total_diff.saturating_sub(idle_diff)) as f64 / total_diff as f64) * 100.0)
            } else {
                Some(0.0)
            }
        } else {
            None
        };

        *prev = Some(cpu_snapshot);
        pct.unwrap_or(0.0)
    };

    let (memory_total_mb, memory_used_mb) = match read_host_memory_mb() {
        Ok(v) => v,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };

    let memory_used_percent = if memory_total_mb > 0 {
        (memory_used_mb as f64 / memory_total_mb as f64) * 100.0
    } else {
        0.0
    };

    let (disk_total_mb, disk_used_mb) = match read_host_disk_mb(StdPath::new(".")) {
        Ok(v) => v,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };

    let disk_used_percent = if disk_total_mb > 0 {
        (disk_used_mb as f64 / disk_total_mb as f64) * 100.0
    } else {
        0.0
    };

    let uptime_seconds = match read_host_uptime_seconds() {
        Ok(v) => v,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };

    let load_avg_1m = match read_host_loadavg_1m() {
        Ok(v) => v,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };

    Json(json!({
        "status": "success",
        "cpu_percent": cpu_percent,
        "memory_total_mb": memory_total_mb,
        "memory_used_mb": memory_used_mb,
        "memory_used_percent": memory_used_percent,
        "disk_total_mb": disk_total_mb,
        "disk_used_mb": disk_used_mb,
        "disk_used_percent": disk_used_percent,
        "uptime_seconds": uptime_seconds,
        "uptime_human": format_uptime_human(uptime_seconds),
        "load_avg_1m": load_avg_1m
    }))
}

async fn api_device_stats(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<DeviceStatsQuery>,
) -> Json<serde_json::Value> {
    let host = if state.development_mode {
        let requested_host = q.host.unwrap_or_default().trim().to_string();

        if !requested_host.is_empty() {
            let mut saved_host = state.device_host.lock().await;
            *saved_host = Some(requested_host.clone());
            requested_host
        } else {
            let saved_host = state.device_host.lock().await.clone();
            saved_host.unwrap_or_else(|| state.fixed_device_host.clone())
        }
    } else {
        state.fixed_device_host.clone()
    };

    let normalized_host = host.trim().trim_end_matches('/').to_string();
    let url = if normalized_host.starts_with("http://") || normalized_host.starts_with("https://") {
        format!("{}/stats", normalized_host)
    } else {
        format!("http://{}/stats", normalized_host)
    };

    match Client::new()
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(resp) => match resp.text().await {
            Ok(text) => match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(data) => Json(json!({ "ok": true, "data": data })),
                Err(e) => Json(json!({ "ok": false, "error": format!("Failed to parse response: {}", e) })),
            },
            Err(e) => Json(json!({ "ok": false, "error": format!("Failed to read response: {}", e) })),
        },
        Err(e) => Json(json!({ "ok": false, "error": format!("Could not reach device: {}", e) })),
    }
}

async fn device(State(state): State<Arc<AppState>>) -> DeviceTemplate {
    DeviceTemplate {
        allow_device_host_edit: state.development_mode,
        default_device_host: state.fixed_device_host.clone(),
    }
}

async fn backups() -> BackupsTemplate {
    BackupsTemplate {}
}

async fn api_versions() -> Json<VersionResponse> {
    match fetch_versions().await {
        Ok(versions) => Json(versions),
        Err(_) => Json(VersionResponse { release: None, preview: None }),
    }
}

async fn api_instances() -> Json<Vec<String>> {
    let instances_dir = get_instances_dir();
    let mut instance_list = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&instances_dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_dir() {
                    let _ = ensure_instance_identity(&entry.path());
                    if let Some(name) = entry.file_name().into_string().ok() {
                        instance_list.push(name);
                    }
                }
            }
        }
    }

    instance_list.sort();
    Json(instance_list)
}

async fn api_backups() -> Json<serde_json::Value> {
    match collect_backups() {
        Ok(backups) => Json(json!({ "status": "success", "backups": backups })),
        Err(e) => Json(json!({ "status": "error", "message": e, "backups": [] })),
    }
}

async fn api_create_backup(
    Json(payload): Json<CreateBackupRequest>,
) -> Json<serde_json::Value> {
    if let Err(e) = validate_instance_name(&payload.instance_name) {
        return Json(json!({ "status": "error", "message": e }));
    }

    let source_path = PathBuf::from(get_instances_dir()).join(&payload.instance_name);
    if !source_path.is_dir() {
        return Json(json!({ "status": "error", "message": "Instance does not exist" }));
    }

    let source_instance_id = match ensure_instance_identity(&source_path) {
        Ok(id) => id,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };
    let source_instance_proof_sha256 = sha256_hex(&source_instance_id);

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let backup_id = format!("{}-{}", now.as_secs(), now.subsec_nanos());

    let backup_root = PathBuf::from(get_backups_dir())
        .join(&payload.instance_name)
        .join(&backup_id);
    let backup_files = backup_root.join("files");

    if let Err(e) = fs::create_dir_all(&backup_files) {
        return Json(json!({ "status": "error", "message": format!("Failed to create backup folder: {}", e) }));
    }

    if let Err(e) = copy_dir_recursive(&source_path, &backup_files) {
        let _ = fs::remove_dir_all(&backup_root);
        return Json(json!({ "status": "error", "message": e }));
    }

    let (size_bytes, file_count) = match compute_dir_stats(&backup_files) {
        Ok(stats) => stats,
        Err(e) => {
            let _ = fs::remove_dir_all(&backup_root);
            return Json(json!({ "status": "error", "message": e }));
        }
    };

    let metadata = BackupMetadata {
        id: backup_id,
        source_instance: payload.instance_name,
        created_at_unix: now.as_secs(),
        size_bytes,
        file_count,
        source_instance_proof_sha256: Some(source_instance_proof_sha256),
        source_instance_id: Some(source_instance_id),
    };

    let meta_path = backup_root.join("meta.json");
    let meta_text = match serde_json::to_string_pretty(&metadata) {
        Ok(v) => v,
        Err(e) => {
            let _ = fs::remove_dir_all(&backup_root);
            return Json(json!({ "status": "error", "message": format!("Failed to encode metadata: {}", e) }));
        }
    };

    if let Err(e) = fs::write(&meta_path, meta_text) {
        let _ = fs::remove_dir_all(&backup_root);
        return Json(json!({ "status": "error", "message": format!("Failed to write metadata: {}", e) }));
    }

    Json(json!({
        "status": "success",
        "message": "Backup created successfully",
        "backup": metadata
    }))
}

async fn api_restore_backup(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<RestoreBackupRequest>,
) -> Json<serde_json::Value> {
    if let Err(e) = validate_backup_id(&id) {
        return Json(json!({ "status": "error", "message": e }));
    }

    let (backup_path, metadata) = match find_backup_by_id(&id) {
        Ok(v) => v,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };

    let backup_files = backup_path.join("files");
    if !backup_files.is_dir() {
        return Json(json!({ "status": "error", "message": "Backup data is missing" }));
    }

    let instances_root = PathBuf::from(get_instances_dir());

    if payload.mode == "override" {
        let override_proof = metadata
            .source_instance_proof_sha256
            .clone()
            .or_else(|| metadata.source_instance_id.as_ref().map(|id| sha256_hex(id)));

        let (target_name, target_path) = if let Some(proof) = override_proof.as_deref() {
            match find_instance_by_identity_proof(&instances_root, proof) {
                Ok(Some(found)) => found,
                Ok(None) => {
                    return Json(json!({
                        "status": "error",
                        "message": "No matching instance identity was found for this backup override"
                    }));
                }
                Err(e) => return Json(json!({ "status": "error", "message": e })),
            }
        } else {
            let fallback_name = metadata.source_instance.clone();
            let fallback_path = instances_root.join(&fallback_name);
            if !fallback_path.is_dir() {
                return Json(json!({ "status": "error", "message": "Target instance does not exist for override" }));
            }
            (fallback_name, fallback_path)
        };

        let target_identity = match ensure_instance_identity(&target_path) {
            Ok(identity) => identity,
            Err(e) => return Json(json!({ "status": "error", "message": e })),
        };

        let instances = state.instances.read().await;
        if let Some(instance) = instances.get(&target_name) {
            let mut lock = instance.process.lock().await;
            if process_slot_is_running(&mut lock) {
                return Json(json!({ "status": "error", "message": "Stop the server before overriding this instance" }));
            }
        }
        drop(instances);

        if let Err(e) = fs::remove_dir_all(&target_path) {
            return Json(json!({ "status": "error", "message": format!("Failed to clear target instance: {}", e) }));
        }

        if let Err(e) = fs::create_dir_all(&target_path) {
            return Json(json!({ "status": "error", "message": format!("Failed to recreate target instance: {}", e) }));
        }

        if let Err(e) = copy_dir_recursive(&backup_files, &target_path) {
            return Json(json!({ "status": "error", "message": e }));
        }

        if let Err(e) = write_instance_identity(&target_path, &target_identity) {
            return Json(json!({ "status": "error", "message": e }));
        }

        return Json(json!({
            "status": "success",
            "message": format!("Backup restored over instance '{}'", target_name),
            "instance": target_name
        }));
    }

    if payload.mode == "new" {
        let requested_name = payload
            .new_instance_name
            .clone()
            .unwrap_or_else(|| metadata.source_instance.clone());
        let mut new_name = requested_name.trim().to_string();
        if new_name.is_empty() {
            new_name = metadata.source_instance.trim().to_string();
        }

        if let Err(e) = validate_instance_name(&new_name) {
            return Json(json!({ "status": "error", "message": e }));
        }

        let lineage_identity_for_new_restore = if let Some(proof) = metadata.source_instance_proof_sha256.as_deref() {
            match find_instance_by_identity_proof(&instances_root, proof) {
                Ok(None) => backup_source_identity(&metadata, &backup_files),
                _ => None,
            }
        } else {
            None
        };

        if let Err(e) = fs::create_dir_all(&instances_root) {
            return Json(json!({
                "status": "error",
                "message": format!("Failed to prepare instances directory: {}", e)
            }));
        }

        let target_path = instances_root.join(&new_name);
        if target_path.exists() {
            return Json(json!({ "status": "error", "message": "Target instance already exists" }));
        }

        if let Err(e) = fs::create_dir_all(&target_path) {
            return Json(json!({ "status": "error", "message": format!("Failed to create new instance directory: {}", e) }));
        }

        if let Err(e) = copy_dir_recursive(&backup_files, &target_path) {
            let _ = fs::remove_dir_all(&target_path);
            return Json(json!({ "status": "error", "message": e }));
        }

        if let Some(source_identity) = lineage_identity_for_new_restore {
            if let Err(e) = write_instance_identity(&target_path, &source_identity) {
                let _ = fs::remove_dir_all(&target_path);
                return Json(json!({ "status": "error", "message": e }));
            }
        } else if let Err(e) = assign_new_instance_identity(&target_path) {
            let _ = fs::remove_dir_all(&target_path);
            return Json(json!({ "status": "error", "message": e }));
        }

        let mut instances = state.instances.write().await;
        if !instances.contains_key(&new_name) {
            let (tx, _) = broadcast::channel(100);
            instances.insert(
                new_name.clone(),
                Arc::new(InstanceState {
                    process: Mutex::new(None),
                    tx,
                    logs: Arc::new(Mutex::new(Vec::new())),
                }),
            );
        }

        return Json(json!({
            "status": "success",
            "message": format!("Backup restored into new instance '{}'", new_name),
            "instance": new_name
        }));
    }

    Json(json!({
        "status": "error",
        "message": "Invalid restore mode. Use 'new' or 'override'."
    }))
}

async fn api_delete_backup(
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    if let Err(e) = validate_backup_id(&id) {
        return Json(json!({ "status": "error", "message": e }));
    }

    let (backup_path, metadata) = match find_backup_by_id(&id) {
        Ok(v) => v,
        Err(e) => return Json(json!({ "status": "error", "message": e })),
    };

    if let Err(e) = fs::remove_dir_all(&backup_path) {
        return Json(json!({ "status": "error", "message": format!("Failed to delete backup: {}", e) }));
    }

    if let Some(parent) = backup_path.parent() {
        if let Ok(mut entries) = fs::read_dir(parent) {
            if entries.next().is_none() {
                let _ = fs::remove_dir(parent);
            }
        }
    }

    Json(json!({
        "status": "success",
        "message": "Backup deleted",
        "source_instance": metadata.source_instance
    }))
}

async fn api_create_instance(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateInstanceRequest>,
) -> Json<CreateInstanceResponse> {
    if let Err(e) = validate_instance_name(&payload.name) {
        return Json(CreateInstanceResponse { status: "error".to_string(), message: e });
    }

    if let Err(e) = validate_version_format(&payload.version) {
        return Json(CreateInstanceResponse { status: "error".to_string(), message: e });
    }

    let instance_name = payload.name.clone();
    let version = payload.version.clone();
    let response_message = format!("Creating instance '{}' with version {}...", instance_name, version);


    let prog = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let mut map = state.progress.write().await;
        map.insert(instance_name.clone(), prog.clone());
    }

    let state_clone = state.clone();
    let instance_name_clone = instance_name.clone();
    tokio::spawn(async move {
        match download_and_install(&instance_name_clone, &version, prog).await {
            Ok(_) => {
                let mut instances = state_clone.instances.write().await;
                if !instances.contains_key(&instance_name_clone) {
                    let (tx, _) = broadcast::channel(100);
                    instances.insert(
                        instance_name_clone.clone(),
                        Arc::new(InstanceState {
                            process: Mutex::new(None),
                            tx,
                            logs: Arc::new(Mutex::new(Vec::new())),
                        }),
                    );
                }
            }
            Err(e) => eprintln!("Failed to create instance '{}': {}", instance_name_clone, e),
        }
    });

    Json(CreateInstanceResponse { status: "downloading".to_string(), message: response_message })
}

async fn api_instance_progress(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    use std::sync::atomic::Ordering;
    let map = state.progress.read().await;
    let pct = map.get(&name).map(|p| p.load(Ordering::Relaxed)).unwrap_or(0);
    Json(json!({ "progress": pct }))
}

async fn api_instance_ready(Path(name): Path<String>) -> Json<serde_json::Value> {
    let instances_dir = get_instances_dir();
    let instance_path = format!("{}/{}", instances_dir, name);
    let server_bin = format!("{}/bedrock_server", instance_path);

    let ready = std::path::Path::new(&server_bin).exists();

    Json(json!({
        "ready": ready,
        "message": if ready { "Instance ready!" } else { "Still downloading and extracting..." }
    }))
}

async fn api_check_version(Path(version): Path<String>) -> Json<serde_json::Value> {
    if let Err(e) = validate_version_format(&version) {
        return Json(json!({ "found": false, "message": e }));
    }

    let urls_to_try = vec![
        format!("https://www.minecraft.net/bedrockdedicatedserver/bin-linux/bedrock-server-{}.zip", version),
        format!("https://www.minecraft.net/bedrockdedicatedserver/bin-linux-preview/bedrock-server-{}.zip", version),
    ];

    for url in &urls_to_try {
        if check_url_exists(url).await {
            return Json(json!({ "found": true, "message": "Version found!" }));
        }
    }

    Json(json!({
        "found": false,
        "message": format!("Version {} was not found on Minecraft's servers.", version)
    }))
}

async fn api_delete_instance(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    if let Err(e) = validate_instance_name(&name) {
        return Json(json!({ "status": "error", "message": e }));
    }

    let instances_dir = get_instances_dir();
    let instance_path = format!("{}/{}", instances_dir, name);

    if !std::path::Path::new(&instance_path).exists() {
        return Json(json!({ "status": "error", "message": "Instance does not exist" }));
    }

    let mut instances = state.instances.write().await;
    instances.remove(&name);
    drop(instances);

    match std::fs::remove_dir_all(&instance_path) {
        Ok(_) => Json(json!({ "status": "success", "message": "Instance deleted successfully" })),
        Err(e) => Json(json!({ "status": "error", "message": format!("Failed to delete instance: {}", e) })),
    }
}

async fn api_rename_instance(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(payload): Json<RenameInstanceRequest>,
) -> Json<serde_json::Value> {
    if let Err(e) = validate_instance_name(&name) {
        return Json(json!({ "status": "error", "message": e }));
    }

    let new_name = payload.new_name.trim().to_string();
    if let Err(e) = validate_instance_name(&new_name) {
        return Json(json!({ "status": "error", "message": e }));
    }

    if new_name == name {
        return Json(json!({
            "status": "success",
            "message": "Instance name is unchanged",
            "new_name": new_name
        }));
    }

    let instances_dir = PathBuf::from(get_instances_dir());
    let old_path = instances_dir.join(&name);
    let new_path = instances_dir.join(&new_name);

    if !old_path.is_dir() {
        return Json(json!({ "status": "error", "message": "Instance does not exist" }));
    }

    if new_path.exists() {
        return Json(json!({ "status": "error", "message": "An instance with the new name already exists" }));
    }

    {
        let instances = state.instances.read().await;
        if let Some(instance) = instances.get(&name) {
            let mut lock = instance.process.lock().await;
            if process_slot_is_running(&mut lock) {
                return Json(json!({ "status": "error", "message": "Stop the server before renaming this instance" }));
            }
        }
    }

    if let Err(e) = fs::rename(&old_path, &new_path) {
        return Json(json!({ "status": "error", "message": format!("Failed to rename instance: {}", e) }));
    }

    let mut relinked_backups: u64 = 0;
    let mut relink_warning: Option<String> = None;

    match ensure_instance_identity(&new_path) {
        Ok(new_instance_id) => {
            let new_proof_sha256 = sha256_hex(&new_instance_id);
            match relink_backup_metadata_source_names(&name, &new_name, &new_instance_id, &new_proof_sha256) {
                Ok(count) => relinked_backups = count,
                Err(e) => relink_warning = Some(e),
            }
        }
        Err(e) => {
            relink_warning = Some(format!("Rename succeeded but backup relink was skipped: {}", e));
        }
    }

    {
        let mut instances = state.instances.write().await;
        if let Some(instance_state) = instances.remove(&name) {
            instances.insert(new_name.clone(), instance_state);
        }
    }

    {
        let mut progress = state.progress.write().await;
        if let Some(progress_state) = progress.remove(&name) {
            progress.insert(new_name.clone(), progress_state);
        }
    }

    Json(json!({
        "status": "success",
        "message": format!("Instance renamed from '{}' to '{}'", name, new_name),
        "old_name": name,
        "new_name": new_name,
        "relinked_backups": relinked_backups,
        "warning": relink_warning
    }))
}
