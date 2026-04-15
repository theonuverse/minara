use reqwest::Client;
use std::future::pending;
use std::net::{IpAddr, Ipv4Addr, UdpSocket};

pub async fn check_asmo_readiness(host: &str) -> bool {
    let trimmed_host = host.trim().trim_end_matches('/');
    let base_url = if trimmed_host.starts_with("http://") || trimmed_host.starts_with("https://") {
        trimmed_host.to_string()
    } else {
        format!("http://{}", trimmed_host)
    };

    let client = Client::new();
    let timeout = std::time::Duration::from_secs(3);
    let stats_url = format!("{}/stats", base_url);

    match client.get(&stats_url).timeout(timeout).send().await {
        Ok(response) if response.status().is_success() => true,
        Ok(_) => {
            let root_url = format!("{}/", base_url);
            match client.get(&root_url).timeout(timeout).send().await {
                Ok(root_resp) => root_resp.status().is_success(),
                Err(_) => false,
            }
        }
        Err(_) => false,
    }
}

fn private_ipv4_priority(ip: Ipv4Addr) -> Option<u8> {
    let [a, b, _, _] = ip.octets();
    match (a, b) {
        (192, 168) => Some(0),
        (10, _) => Some(1),
        (172, 16..=31) => Some(2),
        (169, 254) => Some(3),
        _ => None,
    }
}

fn detect_private_ipv4_from_hostname() -> Option<Ipv4Addr> {
    let output = std::process::Command::new("hostname").arg("-I").output().ok()?;
    if !output.status.success() {
        return None;
    }

    let output_text = String::from_utf8_lossy(&output.stdout);
    let mut best: Option<(u8, Ipv4Addr)> = None;

    for token in output_text.split_whitespace() {
        let parsed = match token.parse::<IpAddr>() {
            Ok(IpAddr::V4(v4)) => v4,
            _ => continue,
        };

        let priority = match private_ipv4_priority(parsed) {
            Some(p) => p,
            None => continue,
        };

        match best {
            Some((current, _)) if current <= priority => {}
            _ => best = Some((priority, parsed)),
        }
    }

    best.map(|(_, ip)| ip)
}

fn detect_private_ipv4_from_udp_probe() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    if socket.connect("1.1.1.1:80").is_err() {
        return None;
    }

    let local_ip = socket.local_addr().ok()?.ip();
    match local_ip {
        IpAddr::V4(v4) if private_ipv4_priority(v4).is_some() => Some(v4),
        _ => None,
    }
}

pub fn detect_panel_host_ip() -> String {
    detect_private_ipv4_from_hostname()
        .or_else(detect_private_ipv4_from_udp_probe)
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "localhost".to_string())
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                let _ = sigterm.recv().await;
            }
            Err(_) => pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

pub fn print_startup_banner(development_mode: bool, panel_url: &str, asmo_ready: bool) {
    println!("Minara Panel");
    println!("Mode        : {}", if development_mode { "Development" } else { "Production" });
    println!("Panel URL   : {}", panel_url);
    println!("Asmo Status : {}", if asmo_ready { "Works" } else { "Setup required" });
    println!("Setup Guide : https://github.com/theonuverse/asmo");
}
