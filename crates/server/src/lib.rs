//! 3D Fighter online server: lobby, HTTP and (from M2) UDP rendezvous/relay.

#![forbid(unsafe_code)]

pub mod app;
pub mod config;
pub mod http;
pub mod lobby;
pub mod metrics;
pub mod moderation;
pub mod proxy;
pub mod relay;
pub mod replays;
pub mod serve;
pub mod spectate;
pub mod udp;
pub mod ws;
