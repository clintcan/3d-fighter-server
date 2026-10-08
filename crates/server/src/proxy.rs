//! Client IP resolution behind a reverse proxy (section 10).
//!
//! The TCP peer address is used as the client address unless the peer is a
//! configured trusted proxy, in which case `X-Forwarded-For` (right-most
//! untrusted entry) or `X-Real-IP` is used. Forwarded headers are ignored from
//! untrusted peers. IP-based strikes are skipped for loopback/private peers when
//! no proxy is trusted, so a shared address (a LAN, or Caddy on localhost) is
//! not banned wholesale.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use axum::http::HeaderMap;

/// A trusted proxy address or CIDR block (IPv4 or IPv6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpAllow {
    Exact(IpAddr),
    V4 { base: u32, prefix: u8 },
    V6 { base: u128, prefix: u8 },
}

impl IpAllow {
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Some((addr, prefix)) = s.split_once('/') {
            let prefix: u8 = prefix.parse().ok()?;
            if let Ok(v4) = addr.parse::<Ipv4Addr>() {
                if prefix > 32 {
                    return None;
                }
                return Some(IpAllow::V4 {
                    base: u32::from(v4),
                    prefix,
                });
            }
            if let Ok(v6) = addr.parse::<Ipv6Addr>() {
                if prefix > 128 {
                    return None;
                }
                return Some(IpAllow::V6 {
                    base: u128::from(v6),
                    prefix,
                });
            }
            return None;
        }
        s.parse::<IpAddr>().ok().map(IpAllow::Exact)
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match self {
            IpAllow::Exact(a) => *a == ip,
            IpAllow::V4 { base, prefix } => match ip {
                IpAddr::V4(v4) => {
                    let mask = v4_mask(*prefix);
                    (u32::from(v4) & mask) == (*base & mask)
                }
                IpAddr::V6(_) => false,
            },
            IpAllow::V6 { base, prefix } => match ip {
                IpAddr::V6(v6) => {
                    let mask = v6_mask(*prefix);
                    (u128::from(v6) & mask) == (*base & mask)
                }
                IpAddr::V4(_) => false,
            },
        }
    }
}

fn v4_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn v6_mask(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    }
}

/// Normalise an address to the key used for per-IP limits and bans: IPv4 stays
/// as-is, an IPv4-mapped IPv6 address becomes IPv4, and any other IPv6 address
/// is reduced to its /64 prefix (issue #27).
pub fn key_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                IpAddr::V4(v4)
            } else {
                let mut octets = v6.octets();
                for b in octets[8..].iter_mut() {
                    *b = 0;
                }
                IpAddr::V6(Ipv6Addr::from(octets))
            }
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
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.octets()[0] & 0xfe) == 0xfc // unique local fc00::/7
                || (v6.octets()[0] == 0xfe && (v6.octets()[1] & 0xc0) == 0x80) // link-local fe80::/10
        }
    }
}

/// Resolve the client address from the peer and forwarded headers. The result is
/// already normalised with [`key_ip`] (IPv6 reduced to /64), so it is the right
/// key for per-IP limits and bans.
pub fn resolve_client_ip(peer: IpAddr, headers: &HeaderMap, trusted: &[IpAllow]) -> IpAddr {
    let raw = resolve_raw_client_ip(peer, headers, trusted);
    key_ip(raw)
}

fn resolve_raw_client_ip(peer: IpAddr, headers: &HeaderMap, trusted: &[IpAllow]) -> IpAddr {
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
        (client_ip != key_ip(peer)).then_some(client_ip)
    } else if is_private_or_loopback(peer) {
        None
    } else {
        Some(client_ip)
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

    #[test]
    fn ipv6_is_keyed_by_slash64() {
        let a: IpAddr = "2001:db8:1:2:3:4:5:6".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff::1".parse().unwrap();
        assert_eq!(key_ip(a), key_ip(b));
        let mapped: IpAddr = "::ffff:203.0.113.5".parse().unwrap();
        assert_eq!(key_ip(mapped), ip("203.0.113.5"));
    }

    #[test]
    fn v6_cidr_matches() {
        let cidr = IpAllow::parse("2001:db8::/32").unwrap();
        assert!(cidr.contains("2001:db8:1:2::1".parse().unwrap()));
        assert!(!cidr.contains("2001:db9::1".parse().unwrap()));
        let exact = IpAllow::parse("2001:db8::1").unwrap();
        assert!(exact.contains("2001:db8::1".parse().unwrap()));
    }
}
