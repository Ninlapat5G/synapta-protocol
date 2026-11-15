//! Which addresses the lab's web page may listen on: loopback and this machine's Tailscale
//! address, nothing else (owner's decision, docs/web-sandbox-plan.md). Never 0.0.0.0 — that
//! would put the page on every network the machine is on.

use std::net::{IpAddr, Ipv4Addr};
use std::process::Command;

/// Tailscale gives each machine an address in 100.64.0.0/10 (IPv4) or fd7a:115c:a1e0::/48 (IPv6).
pub fn is_tailscale(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            a == 100 && (64..128).contains(&b)
        }
        IpAddr::V6(v6) => v6.segments()[..3] == [0xfd7a, 0x115c, 0xa1e0],
    }
}

/// `Ok` for loopback and Tailscale addresses; otherwise says why not.
pub fn check(ip: IpAddr) -> Result<(), String> {
    if ip.is_unspecified() {
        return Err(format!(
            "{ip} would listen on every network; use 127.0.0.1 or the Tailscale address"
        ));
    }
    if ip.is_loopback() || is_tailscale(ip) {
        Ok(())
    } else {
        Err(format!(
            "{ip} is neither loopback nor a Tailscale address (100.64.0.0/10)"
        ))
    }
}

/// This machine's Tailscale IPv4 address, from `tailscale ip -4`; `None` if Tailscale is not
/// installed or not up. (Not the UDP-route trick: that binds 0.0.0.0 for a moment.)
pub fn tailscale_ip() -> Option<IpAddr> {
    let output = Command::new("tailscale").args(["ip", "-4"]).output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .filter_map(|line| line.trim().parse().ok())
        .find(|ip| is_tailscale(*ip))
}

pub const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn only_loopback_and_tailscale_are_allowed() {
        for good in ["127.0.0.1", "::1", "100.64.0.1", "100.127.255.254", "fd7a:115c:a1e0::5"] {
            assert!(check(ip(good)).is_ok(), "{good}");
        }
        for bad in ["0.0.0.0", "::", "192.168.1.10", "100.128.0.1", "100.63.0.1", "10.0.0.1"] {
            assert!(check(ip(bad)).is_err(), "{bad}");
        }
    }
}
