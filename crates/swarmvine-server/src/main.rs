//! swarmVine server binary — epidemic gossip engine.
//!
//! Protocol evolution (G64 → G65 Cephalization):
//! - **Phase 2**: Dual-socket (`.sock` + `.tarpc.sock`) — current default
//! - **Phase 3 (G65)**: Single-socket with protocol negotiation — `--negotiate`
//!
//! The primal does not choose its transport. The launcher or Songbird provides it.

mod announce;
mod dispatch;
mod method_gate;
mod server;
mod tarpc_server;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "swarmvine", about = "swarmVine — epidemic gossip engine for the Tower Atomic mesh")]
struct Cli {
    /// Family ID for socket naming (production mode).
    #[arg(long, env = "FAMILY_ID")]
    family_id: Option<String>,

    /// Gate identity (e.g. "sporeGate"). Defaults to hostname.
    #[arg(long, env = "GATE_ID")]
    gate_id: Option<String>,

    /// Transport endpoint override (JSON, e.g. '{"transport":"tcp","host":"0.0.0.0","port":7800}').
    #[arg(long, env = "TRANSPORT_ENDPOINT")]
    transport_endpoint: Option<String>,

    /// Disable tarpc binary listener (JSON-RPC only mode).
    #[arg(long, env = "DISABLE_TARPC")]
    disable_tarpc: bool,

    /// G65: Enable protocol negotiation on a single socket.
    #[arg(long, env = "NEGOTIATE_PROTOCOL")]
    negotiate: bool,

    /// Gossip eviction interval in seconds (default 120).
    #[arg(long, env = "GOSSIP_EVICT_INTERVAL", default_value = "120")]
    evict_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let node_id = cli
        .gate_id
        .clone()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_owned())
        });

    let mut primal = swarmvine_core::SwarmVinePrimal::with_node_id(node_id.clone());
    swarmvine_core::PrimalLifecycle::start(&mut primal)
        .await
        .map_err(|e| anyhow::anyhow!("startup failed: {e}"))?;

    tracing::info!(node_id = %node_id, "swarmVine started — vine spreads, bat validates");

    let primal_arc = Arc::new(primal);

    // Periodic gossip eviction task
    let evict_primal = Arc::clone(&primal_arc);
    let evict_interval = cli.evict_interval;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(evict_interval),
        );
        loop {
            interval.tick().await;
            let evicted = evict_primal.engine().evict_expired().await;
            if evicted > 0 {
                tracing::info!(evicted, "Gossip eviction cycle");
            }
        }
    });

    if !cli.disable_tarpc && !cli.negotiate {
        tarpc_server::start_tarpc_listener(
            "swarmvine",
            cli.family_id.as_deref(),
            primal_arc.clone(),
        )
        .await?;
    }

    let endpoint: Option<server::TransportEndpoint> = cli
        .transport_endpoint
        .as_deref()
        .map(|s| serde_json::from_str(s))
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid TRANSPORT_ENDPOINT: {e}"))?;

    server::run(
        "swarmvine",
        cli.family_id.as_deref(),
        endpoint.as_ref(),
        &*primal_arc,
        cli.negotiate,
    )
    .await
}
