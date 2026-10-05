//! `fighter-server`: lobby + HTTP in M1, UDP rendezvous/relay from M2.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use fighter_protocol::clock::{Clock, RealClock};
use fighter_server::{config, serve};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = std::env::var("FIGHTER_CONFIG")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            let default = PathBuf::from("config/server.toml");
            default.exists().then_some(default)
        });
    let config = config::Config::load(config_path.as_deref())?;
    init_tracing(&config);

    let clock: Arc<dyn Clock> = Arc::new(RealClock);
    serve::run(config, clock).await
}

fn init_tracing(config: &config::Config) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if config.server.log_format == "json" {
        builder.json().init();
    } else {
        builder.init();
    }
}
