//! Wire protocol for the 3D Fighter online server.
//!
//! This crate has no I/O. It contains the JSON lobby messages (section 6), the
//! binary UDP datagrams (section 7), the spectator feed frames (section 8), text
//! cleaning (section 10.4) and a few shared utilities. Every parser returns
//! [`Result`] and never panics on untrusted input.

#![forbid(unsafe_code)]

pub mod clock;
pub mod error;
pub mod feed;
pub mod ids;
pub mod json;
pub mod ratelimit;
pub mod text;
pub mod udp;

pub use clock::{Clock, RealClock, TestClock};
pub use error::CodecError;
pub use json::{
    parse_client_message, ClientEnvelope, ClientMessage, ClientParseError, ServerMessage,
};
