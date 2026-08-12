// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! swarmVine server library — dual-protocol gossip engine (JSON-RPC + tarpc).

#![allow(clippy::redundant_pub_crate)]

pub(crate) mod announce;
pub(crate) mod dispatch;
pub(crate) mod method_gate;
pub(crate) mod rpc_types;
pub(crate) mod server;
pub(crate) mod spread;
pub(crate) mod tarpc_server;

/// Test harness for integration tests (not part of the stable public API).
#[doc(hidden)]
pub mod test_support;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;

/// CLI arguments for the swarmVine server binary.
#[derive(Parser)]
#[command(
    name = "swarmvine",
    about = "swarmVine — epidemic gossip engine for the Tower Atomic mesh"
)]
pub struct Cli {
    /// Family ID for socket naming (production mode).
    #[arg(long, env = "FAMILY_ID")]
    pub family_id: Option<String>,

    /// Gate identity (e.g. "sporeGate"). Defaults to hostname.
    #[arg(long, env = "GATE_ID")]
    pub gate_id: Option<String>,

    /// Transport endpoint override (JSON).
    #[arg(long, env = "TRANSPORT_ENDPOINT")]
    pub transport_endpoint: Option<String>,

    /// Disable tarpc binary listener (JSON-RPC only mode).
    #[arg(long, env = "DISABLE_TARPC")]
    pub disable_tarpc: bool,

    /// Legacy: run separate JSON-RPC and tarpc sockets instead of G65 negotiation.
    #[arg(long, env = "LEGACY_DUAL_SOCKET")]
    pub legacy_dual_socket: bool,

    /// Gossip eviction interval in seconds (default 120).
    #[arg(long, env = "GOSSIP_EVICT_INTERVAL", default_value = "120")]
    pub evict_interval: u64,

    /// Epidemic sweep interval in seconds (default 30).
    #[arg(long, env = "GOSSIP_SPREAD_INTERVAL", default_value = "30")]
    pub spread_interval: u64,

    /// TCP port for cross-gate gossip (default 7800).
    #[arg(long, env = "GOSSIP_PORT", default_value = "7800")]
    pub gossip_port: u16,
}

/// Run the swarmVine server with parsed CLI arguments.
///
/// # Errors
///
/// Returns an error if startup or the main accept loop fails.
pub async fn run(cli: Cli) -> Result<()> {
    let node_id = cli
        .gate_id
        .clone()
        .unwrap_or_else(swarmvine_core::resolve_node_id);

    if cli.gate_id.is_none() {
        tracing::warn!(
            node_id = %node_id,
            "GATE_ID not set — using hostname as node identity. \
             Set GATE_ID env or --gate-id for stable mesh identity."
        );
    }

    let mut primal = swarmvine_core::SwarmVinePrimal::with_node_id(node_id.clone());
    swarmvine_core::PrimalLifecycle::start(&mut primal)
        .await
        .map_err(|e| anyhow::anyhow!("startup failed: {e}"))?;

    tracing::info!(node_id = %node_id, "swarmVine started — vine spreads, bat validates");

    let primal_arc = Arc::new(primal);
    let gossip_port = cli.gossip_port;

    announce::inject_endpoint_alive(&primal_arc, gossip_port).await;

    let evict_primal = Arc::clone(&primal_arc);
    let evict_interval = cli.evict_interval;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(evict_interval));
        loop {
            interval.tick().await;
            let evicted = evict_primal.engine().evict_expired().await;
            if evicted > 0 {
                tracing::info!(evicted, "Gossip eviction cycle");
            }
            announce::inject_endpoint_alive(&evict_primal, gossip_port).await;
        }
    });

    let spread_primal = Arc::clone(&primal_arc);
    let spread_interval = cli.spread_interval;
    tokio::spawn(async move {
        spread::epidemic_loop(spread_primal, spread_interval).await;
    });

    let gossip_primal = Arc::clone(&primal_arc);
    tokio::spawn(async move {
        if let Err(e) = run_gossip_listener(gossip_port, gossip_primal).await {
            tracing::error!(error = %e, "Cross-gate gossip listener failed");
        }
    });

    if !cli.disable_tarpc && cli.legacy_dual_socket {
        tarpc_server::start_tarpc_listener(
            "swarmvine",
            cli.family_id.as_deref(),
            primal_arc.clone(),
        )
        .await?;
    }

    let endpoint: Option<swarmvine_core::TransportEndpoint> = cli
        .transport_endpoint
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid TRANSPORT_ENDPOINT: {e}"))?;

    server::run(
        "swarmvine",
        cli.family_id.as_deref(),
        endpoint.as_ref(),
        &primal_arc,
        !cli.legacy_dual_socket,
    )
    .await
}

/// TCP listener for cross-gate gossip exchange.
///
/// Remote swarmVine instances connect here to push `gossip.spread` entries.
///
/// # Errors
///
/// Returns an error if binding the TCP listener fails or accepting a connection fails.
pub async fn run_gossip_listener(
    port: u16,
    primal: Arc<swarmvine_core::SwarmVinePrimal>,
) -> Result<()> {
    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(port, "Cross-gate gossip listener started");

    let gate = method_gate::MethodGate::permissive();

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        tracing::debug!(peer = %peer_addr, "Cross-gate gossip connection");
        server::handle_tcp_connection(stream, &primal, &gate).await;
    }
}
