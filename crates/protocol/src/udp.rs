//! UDP datagrams for rendezvous and relay (section 7.1).
//!
//! Type bytes are `0xF0`-`0xF5`; `0xF6`/`0xF7` are reserved for IPv6. The server
//! drops anything malformed or unknown, so every decoder returns [`CodecError`].

use std::net::SocketAddrV4;

use crate::error::{le, CodecError};

pub const TYPE_BIND: u8 = 0xF0;
pub const TYPE_BOUND: u8 = 0xF1;
pub const TYPE_RELAY: u8 = 0xF2;
pub const TYPE_RELAYED: u8 = 0xF3;
pub const TYPE_PING: u8 = 0xF4;
pub const TYPE_PONG: u8 = 0xF5;

/// Minimum payload carried in a RELAY/RELAYED datagram.
pub const MIN_RELAY_PAYLOAD: usize = 1;
/// Maximum payload carried in a RELAY/RELAYED datagram.
pub const MAX_RELAY_PAYLOAD: usize = 1200;
/// Maximum local candidates in a BIND.
pub const MAX_BIND_CANDIDATES: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindRole {
    Host,
    Guest,
}

impl BindRole {
    fn to_byte(self) -> u8 {
        match self {
            BindRole::Host => 0,
            BindRole::Guest => 1,
        }
    }

    fn from_byte(b: u8) -> Result<Self, CodecError> {
        match b {
            0 => Ok(BindRole::Host),
            1 => Ok(BindRole::Guest),
            _ => Err(CodecError::BadValue("bind role")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UdpDatagram {
    /// client -> server: prove the source endpoint and list local candidates.
    Bind {
        session_token: [u8; 16],
        role: BindRole,
        candidates: Vec<SocketAddrV4>,
    },
    /// server -> client: the public endpoint the server observed.
    Bound { observed: SocketAddrV4 },
    /// client -> server: wrap one game datagram for forwarding.
    Relay {
        relay_key: [u8; 8],
        payload: Vec<u8>,
    },
    /// server -> client: a forwarded game datagram.
    Relayed { payload: Vec<u8> },
    /// client -> server: latency probe.
    Ping { nonce: u32, client_time_ms: u32 },
    /// server -> client: latency probe answer.
    Pong { nonce: u32, client_time_ms: u32 },
}

impl UdpDatagram {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            UdpDatagram::Bind {
                session_token,
                role,
                candidates,
            } => {
                out.push(TYPE_BIND);
                out.extend_from_slice(session_token);
                out.push(role.to_byte());
                out.push(candidates.len() as u8);
                for c in candidates {
                    out.extend_from_slice(&c.ip().octets());
                    le::put_u16(&mut out, c.port());
                }
            }
            UdpDatagram::Bound { observed } => {
                out.push(TYPE_BOUND);
                out.extend_from_slice(&observed.ip().octets());
                le::put_u16(&mut out, observed.port());
            }
            UdpDatagram::Relay { relay_key, payload } => {
                out.push(TYPE_RELAY);
                out.extend_from_slice(relay_key);
                out.extend_from_slice(payload);
            }
            UdpDatagram::Relayed { payload } => {
                out.push(TYPE_RELAYED);
                out.extend_from_slice(payload);
            }
            UdpDatagram::Ping {
                nonce,
                client_time_ms,
            }
            | UdpDatagram::Pong {
                nonce,
                client_time_ms,
            } => {
                out.push(match self {
                    UdpDatagram::Ping { .. } => TYPE_PING,
                    _ => TYPE_PONG,
                });
                le::put_u32(&mut out, *nonce);
                le::put_u32(&mut out, *client_time_ms);
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let type_byte = *buf.first().ok_or(CodecError::Truncated)?;
        let mut pos = 1usize;
        match type_byte {
            TYPE_BIND => {
                let session_token = le::get_bytes::<16>(buf, &mut pos)?;
                let role = BindRole::from_byte(le::get_u8(buf, &mut pos)?)?;
                let n = le::get_u8(buf, &mut pos)?;
                if n > MAX_BIND_CANDIDATES {
                    return Err(CodecError::BadValue("bind candidate count"));
                }
                let mut candidates = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let ip = le::get_bytes::<4>(buf, &mut pos)?;
                    let port = le::get_u16(buf, &mut pos)?;
                    candidates.push(SocketAddrV4::new(ip.into(), port));
                }
                if pos != buf.len() {
                    return Err(CodecError::BadLength);
                }
                Ok(UdpDatagram::Bind {
                    session_token,
                    role,
                    candidates,
                })
            }
            TYPE_BOUND => {
                let ip = le::get_bytes::<4>(buf, &mut pos)?;
                let port = le::get_u16(buf, &mut pos)?;
                if pos != buf.len() {
                    return Err(CodecError::BadLength);
                }
                Ok(UdpDatagram::Bound {
                    observed: SocketAddrV4::new(ip.into(), port),
                })
            }
            TYPE_RELAY => {
                let relay_key = le::get_bytes::<8>(buf, &mut pos)?;
                let payload = buf.get(pos..).ok_or(CodecError::Truncated)?.to_vec();
                if !(MIN_RELAY_PAYLOAD..=MAX_RELAY_PAYLOAD).contains(&payload.len()) {
                    return Err(CodecError::BadValue("relay payload length"));
                }
                Ok(UdpDatagram::Relay { relay_key, payload })
            }
            TYPE_RELAYED => {
                let payload = buf.get(pos..).ok_or(CodecError::Truncated)?.to_vec();
                if !(MIN_RELAY_PAYLOAD..=MAX_RELAY_PAYLOAD).contains(&payload.len()) {
                    return Err(CodecError::BadValue("relayed payload length"));
                }
                Ok(UdpDatagram::Relayed { payload })
            }
            TYPE_PING | TYPE_PONG => {
                let nonce = le::get_u32(buf, &mut pos)?;
                let client_time_ms = le::get_u32(buf, &mut pos)?;
                if pos != buf.len() {
                    return Err(CodecError::BadLength);
                }
                if type_byte == TYPE_PING {
                    Ok(UdpDatagram::Ping {
                        nonce,
                        client_time_ms,
                    })
                } else {
                    Ok(UdpDatagram::Pong {
                        nonce,
                        client_time_ms,
                    })
                }
            }
            other => Err(CodecError::BadType(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn ping_vector_matches_spec() {
        let dg = UdpDatagram::Ping {
            nonce: 0x0102_0304,
            client_time_ms: 1000,
        };
        assert_eq!(
            dg.encode(),
            vec![0xf4, 0x04, 0x03, 0x02, 0x01, 0xe8, 0x03, 0x00, 0x00]
        );
        assert_eq!(UdpDatagram::decode(&dg.encode()).unwrap(), dg);
    }

    #[test]
    fn bind_round_trip() {
        let dg = UdpDatagram::Bind {
            session_token: [7u8; 16],
            role: BindRole::Guest,
            candidates: vec![SocketAddrV4::new(Ipv4Addr::new(192, 168, 0, 5), 7777)],
        };
        let bytes = dg.encode();
        assert_eq!(bytes.len(), 19 + 6);
        assert_eq!(UdpDatagram::decode(&bytes).unwrap(), dg);
    }

    #[test]
    fn bound_round_trip() {
        let dg = UdpDatagram::Bound {
            observed: SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 9), 40000),
        };
        assert_eq!(dg.encode().len(), 7);
        assert_eq!(UdpDatagram::decode(&dg.encode()).unwrap(), dg);
    }

    #[test]
    fn relay_round_trip_and_bounds() {
        let dg = UdpDatagram::Relay {
            relay_key: [1, 2, 3, 4, 5, 6, 7, 8],
            payload: vec![0xAB; 600],
        };
        assert_eq!(UdpDatagram::decode(&dg.encode()).unwrap(), dg);
        assert_eq!(
            UdpDatagram::decode(&[TYPE_RELAY, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(CodecError::BadValue("relay payload length"))
        );
    }

    #[test]
    fn rejects_unknown_type_and_trailing_bytes() {
        assert_eq!(
            UdpDatagram::decode(&[0xF7, 0]),
            Err(CodecError::BadType(0xF7))
        );
        assert_eq!(
            UdpDatagram::decode(&[TYPE_BOUND, 1, 2, 3, 4, 5, 6, 9]),
            Err(CodecError::BadLength)
        );
        assert_eq!(UdpDatagram::decode(&[]), Err(CodecError::Truncated));
    }

    #[test]
    fn rejects_too_many_candidates() {
        let mut bytes = vec![TYPE_BIND];
        bytes.extend_from_slice(&[0u8; 16]);
        bytes.push(0);
        bytes.push(5);
        assert_eq!(
            UdpDatagram::decode(&bytes),
            Err(CodecError::BadValue("bind candidate count"))
        );
    }

    #[test]
    fn replies_are_never_larger_than_requests() {
        let bind = UdpDatagram::Bind {
            session_token: [0u8; 16],
            role: BindRole::Host,
            candidates: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1)],
        };
        let bound = UdpDatagram::Bound {
            observed: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1),
        };
        assert!(bound.encode().len() <= bind.encode().len());

        let ping = UdpDatagram::Ping {
            nonce: 1,
            client_time_ms: 2,
        };
        let pong = UdpDatagram::Pong {
            nonce: 1,
            client_time_ms: 2,
        };
        assert_eq!(pong.encode().len(), ping.encode().len());

        let relay = UdpDatagram::Relay {
            relay_key: [0u8; 8],
            payload: vec![0u8; 1200],
        };
        let relayed = UdpDatagram::Relayed {
            payload: vec![0u8; 1200],
        };
        assert!(relayed.encode().len() <= relay.encode().len());
    }
}
