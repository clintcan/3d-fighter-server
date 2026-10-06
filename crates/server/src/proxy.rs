//! Client IP resolution behind a reverse proxy (section 10).
//!
//! The TCP peer address is used as the client address unless the peer is a
//! configured trusted proxy, in which case `X-Forwarded-For` (right-most
//! untrusted entry) or `X-Real-IP` is used. Forwarded headers are ignored from
//! untrusted peers. IP-based strikes are skipped for loopback/private peers when
//! no proxy is trusted, so a shared address (a LAN, or Caddy on localhost) is
//! not banned wholesale.

use std::net::{IpAddr, Ipv4Addr};

use axum::http::HeaderMap;

/// A trusted proxy address or IPv4 CIDR block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpAllow {
    Exact(IpAddr),
    V4 { base: u32, prefix: u8 },
}

impl IpAllow {
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Some((addr, prefix)) = s.split_once('/') {
            let ip: Ipv4Addr = addr.parse().ok()?;
            let prefix: u8 = prefix.parse().ok()?;
            if prefix > 32 {
                return None;
            }
            return Some(IpAllow::V4 {
                base: u32::from(ip),
                prefix,
            });
        }
        s.parse::<IpAddr>().ok().map(IpAllow::Exact)
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match self {
            IpAllow::Exact(a) => *a == ip,
            IpAllow::V4 { base, prefix } => match ip {
                IpAddr::V4(v4) => {
                    let mask = if *prefix == 0 {
                        0
                    } else {
                        u32::MAX << (32 - *prefix)
                    };
                    (u32::from(v4) & mask) == (*base & mask)
                }
                IpAddr::V6(_) => false,
            },
        }
    }
}

pub fn parse_trusted(list: &[String]) -> Vec<IpAllow> {
    list.iter().filter_map(|s| IpAllow::parse(s)).collect()
}

fn is_trusted(trusted: &[IpAllow], ip: IpAddr) -> bool {
    trusted.iter().any(|t| t.contains(ip))
}

/// True for loopback, private, link-local and unspecified addresses.
pub fn is_private_or_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.octets()[0] == 0
        }
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified(),
    }
}

/// Resolve the client address from the peer and forwarded headers.
pub fn resolve_client_ip(peer: IpAddr, headers: &HeaderMap, trusted: &[IpAllow]) -> IpAddr {
    if !is_trusted(trusted, peer) {
        return peer;
    }
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        // Take the right-most entry that is not itself a trusted proxy.
        let mut chosen = None;
        for part in xff.split(',') {
            if let Ok(ip) = part.trim().parse::<IpAddr>() {
                if !is_trusted(trusted, ip) {
                    chosen = Some(ip);
                }
            }
        }
        if let Some(ip) = chosen {
            return ip;
        }
    }
    if let Some(ip) = headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<IpAddr>().ok())
    {
        return ip;
    }
    peer
}

/// The address an IP-based strike should be recorded against, if any.
pub fn strike_ip(peer: IpAddr, client_ip: IpAddr, trusted: &[IpAllow]) -> Option<IpAddr> {
    if is_trusted(trusted, peer) {
        // Never ban the proxy itself; only the forwarded client.
        (client_ip != peer).then_some(client_ip)
    } else if is_private_or_loopback(peer) {
        None
    } else {
        Some(peer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn cidr_and_exact_match() {
        let exact = IpAllow::parse("127.0.0.1").unwrap();
        assert!(exact.contains(ip("127.0.0.1")));
        assert!(!exact.contains(ip("127.0.0.2")));
        let cidr = IpAllow::parse("10.0.0.0/8").unwrap();
        assert!(cidr.contains(ip("10.1.2.3")));
        assert!(!cidr.contains(ip("11.1.2.3")));
        let all = IpAllow::parse("0.0.0.0/0").unwrap();
        assert!(all.contains(ip("8.8.8.8")));
    }

    #[test]
    fn forwarded_only_from_trusted_peer() {
        let trusted = parse_trusted(&["10.0.0.1".into()]);
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.5".parse().unwrap());
        assert_eq!(
            resolve_client_ip(ip("10.0.0.1"), &headers, &trusted),
            ip("203.0.113.5")
        );
        // Untrusted peer: header ignored.
        assert_eq!(
            resolve_client_ip(ip("198.51.100.9"), &headers, &trusted),
            ip("198.51.100.9")
        );
    }

    #[test]
    fn takes_rightmost_untrusted_entry() {
        let trusted = parse_trusted(&["10.0.0.0/8".into()]);
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.5, 10.0.0.7".parse().unwrap());
        assert_eq!(
            resolve_client_ip(ip("10.0.0.1"), &headers, &trusted),
            ip("203.0.113.5")
        );
    }

    #[test]
    fn local_peers_are_not_strike_targets() {
        let trusted = parse_trusted(&[]);
        assert_eq!(strike_ip(ip("127.0.0.1"), ip("127.0.0.1"), &trusted), None);
        assert_eq!(
            strike_ip(ip("203.0.113.9"), ip("203.0.113.9"), &trusted),
            Some(ip("203.0.113.9"))
        );
    }
}
