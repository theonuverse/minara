use std::fs;
use std::path::Path as StdPath;

#[derive(Clone, Copy)]
pub struct HostCpuSnapshot {
    pub total: u64,
    pub idle: u64,
}

pub fn read_host_cpu_snapshot() -> Result<HostCpuSnapshot, String> {
    let stat_text = fs::read_to_string("/proc/stat")
        .map_err(|e| format!("Failed to read /proc/stat: {}", e))?;

    let cpu_line = stat_text
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or_else(|| "CPU stats line not found in /proc/stat".to_string())?;

    let values: Vec<u64> = cpu_line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse::<u64>().ok())
        .collect();

    if values.len() < 4 {
        return Err("CPU stats were incomplete".to_string());
    }

    let total = values.iter().sum::<u64>();
    let idle = values[3] + values.get(4).copied().unwrap_or(0);
    Ok(HostCpuSnapshot { total, idle })
}

pub fn read_host_memory_mb() -> Result<(u64, u64), String> {
    let meminfo = fs::read_to_string("/proc/meminfo")
        .map_err(|e| format!("Failed to read /proc/meminfo: {}", e))?;

    let mut mem_total_kb: Option<u64> = None;
    let mut mem_available_kb: Option<u64> = None;
    let mut mem_free_kb: Option<u64> = None;
    let mut buffers_kb: Option<u64> = None;
    let mut cached_kb: Option<u64> = None;

    for line in meminfo.lines() {
        let mut parts = line.split_whitespace();
        let key = match parts.next() {
            Some(k) => k,
            None => continue,
        };
        let value = parts.next().and_then(|v| v.parse::<u64>().ok());

        match key {
            "MemTotal:" => mem_total_kb = value,
            "MemAvailable:" => mem_available_kb = value,
            "MemFree:" => mem_free_kb = value,
            "Buffers:" => buffers_kb = value,
            "Cached:" => cached_kb = value,
            _ => {}
        }
    }

    let total_kb = mem_total_kb.ok_or_else(|| "MemTotal missing in /proc/meminfo".to_string())?;
    let available_kb = if let Some(v) = mem_available_kb {
        v
    } else {
        mem_free_kb.unwrap_or(0) + buffers_kb.unwrap_or(0) + cached_kb.unwrap_or(0)
    };

    let used_kb = total_kb.saturating_sub(available_kb);
    Ok((total_kb / 1024, used_kb / 1024))
}

pub fn read_host_disk_mb(path: &StdPath) -> Result<(u64, u64), String> {
    let output = std::process::Command::new("df")
        .args(["-kP", path.to_str().unwrap_or(".")])
        .output()
        .map_err(|e| format!("Failed to execute df: {}", e))?;

    if !output.status.success() {
        return Err("df command failed".to_string());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines();
    let _ = lines.next();
    let data_line = lines.next().ok_or_else(|| "No disk stats returned by df".to_string())?;

    let cols: Vec<&str> = data_line.split_whitespace().collect();
    if cols.len() < 3 {
        return Err("Unexpected df output format".to_string());
    }

    let total_kb = cols[1].parse::<u64>().map_err(|e| format!("Invalid disk total from df: {}", e))?;
    let used_kb = cols[2].parse::<u64>().map_err(|e| format!("Invalid disk used from df: {}", e))?;
    Ok((total_kb / 1024, used_kb / 1024))
}

pub fn read_host_uptime_seconds() -> Result<u64, String> {
    let uptime_text = fs::read_to_string("/proc/uptime")
        .map_err(|e| format!("Failed to read /proc/uptime: {}", e))?;
    let first = uptime_text
        .split_whitespace()
        .next()
        .ok_or_else(|| "Invalid /proc/uptime format".to_string())?;
    let seconds = first
        .parse::<f64>()
        .map_err(|e| format!("Invalid uptime value: {}", e))?;
    Ok(seconds as u64)
}

pub fn format_uptime_human(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;

    if days > 0 {
        format!("{}d {}h", days, hours)
    } else if hours > 0 {
        format!("{}h {}m", hours, minutes)
    } else {
        format!("{}m", minutes)
    }
}

pub fn read_host_loadavg_1m() -> Result<f64, String> {
    let load_text = fs::read_to_string("/proc/loadavg")
        .map_err(|e| format!("Failed to read /proc/loadavg: {}", e))?;
    let first = load_text
        .split_whitespace()
        .next()
        .ok_or_else(|| "Invalid /proc/loadavg format".to_string())?;
    first
        .parse::<f64>()
        .map_err(|e| format!("Invalid load average value: {}", e))
}
