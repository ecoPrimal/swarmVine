// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! Integration test harness — starts JSON-RPC and gossip listeners on ephemeral endpoints.
//!
//! This module is Unix-only (UDS transport). On non-Unix platforms the test harness
//! is not available and integration tests that depend on it are `#[cfg(unix)]` gated.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use swarmvine_core::{PrimalLifecycle, SwarmVinePrimal, TransportEndpoint};
use tokio::task::JoinHandle;

/// Running test server with UDS JSON-RPC and TCP gossip endpoints.
pub struct TestServer {
    /// Unix socket path for JSON-RPC.
    pub uds_path: PathBuf,
    /// TCP port for cross-gate gossip.
    pub gossip_port: u16,
    primal: Arc<SwarmVinePrimal>,
    tasks: Vec<JoinHandle<()>>,
}

impl TestServer {
    /// Start a test server bound to unique temp paths derived from `test_name`.
    ///
    /// # Panics
    ///
    /// Panics if binding or startup fails.
    pub async fn start(test_name: &str, legacy_dual_socket: bool) -> Self {
        let uds_path = std::env::temp_dir().join(format!(
            "swarmvine-it-{test_name}-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&uds_path);

        let gossip_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gossip port");
        let gossip_port = gossip_listener.local_addr().expect("gossip addr").port();
        drop(gossip_listener);

        let mut primal = SwarmVinePrimal::with_node_id(format!("it-{test_name}"));
        primal.start().await.expect("primal start");
        let primal = Arc::new(primal);

        let endpoint = TransportEndpoint::Uds {
            path: uds_path.to_string_lossy().into_owned(),
        };

        let primal_run = Arc::clone(&primal);
        let endpoint_for_run = endpoint.clone();
        let server_task = tokio::spawn(async move {
            let _ = crate::server::run(
                "swarmvine",
                None,
                Some(&endpoint_for_run),
                &primal_run,
                !legacy_dual_socket,
            )
            .await;
        });

        let primal_gossip = Arc::clone(&primal);
        let gossip_task = tokio::spawn(async move {
            let _ = crate::run_gossip_listener("127.0.0.1", gossip_port, primal_gossip).await;
        });

        wait_for_uds(&uds_path).await;

        Self {
            uds_path,
            gossip_port,
            primal,
            tasks: vec![server_task, gossip_task],
        }
    }

    /// Access the shared primal instance (for lifecycle assertions).
    #[must_use]
    pub fn primal(&self) -> &SwarmVinePrimal {
        &self.primal
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.uds_path);
    }
}

async fn wait_for_uds(path: &std::path::Path) {
    for _ in 0..100 {
        if tokio::net::UnixStream::connect(path).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("UDS server did not become ready at {}", path.display());
}
