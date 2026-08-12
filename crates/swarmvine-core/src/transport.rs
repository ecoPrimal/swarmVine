// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project

//! G66 Transport Abstraction — silicon-agnostic IPC.
//!
//! Eliminates silicon deism: primals express *what* they connect to (a service,
//! a capability) without encoding *how* bytes move. `#[cfg(unix)]` lives here —
//! not scattered across business logic.
//!
//! Components:
//! - `TransportEndpoint` — where to connect (UDS / TCP / `MeshRelay`)
//! - `TransportStream` — the connected byte pipe (`AsyncRead` + `AsyncWrite`)
//! - `TransportListener` — server-side accept loop
//! - `connect_transport()` / `bind_transport()` — the bridges

use serde::{Deserialize, Serialize};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Where to connect — platform-neutral endpoint descriptor.
///
/// The primal never constructs this directly in production. The launcher,
/// biomeOS, or songBird injects via `TRANSPORT_ENDPOINT` env var.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport")]
pub enum TransportEndpoint {
    /// Unix domain socket.
    #[serde(rename = "uds")]
    Uds {
        /// Socket path.
        path: String,
    },
    /// TCP socket.
    #[serde(rename = "tcp")]
    Tcp {
        /// Hostname or IP.
        host: String,
        /// Port number.
        port: u16,
    },
    /// Mesh relay via songBird.
    #[serde(rename = "mesh_relay")]
    MeshRelay {
        /// Remote peer identifier.
        peer_id: String,
        /// Capability to route to.
        capability: String,
    },
}

impl TransportEndpoint {
    /// Platform default: UDS on Unix, TCP localhost elsewhere.
    ///
    /// Uses `platform_paths::runtime_socket_dir()` for consistent socket
    /// directory resolution (no duplicated `/tmp` fallbacks).
    #[must_use]
    pub fn platform_default(primal_name: &str, family_id: Option<&str>) -> Self {
        if cfg!(unix) {
            let socket_dir = crate::platform_paths::runtime_socket_dir();
            let filename = family_id
                .filter(|id| !id.is_empty() && *id != "default")
                .map_or_else(
                    || format!("{primal_name}.sock"),
                    |fid| format!("{primal_name}-{fid}.sock"),
                );
            Self::Uds {
                path: socket_dir.join(&filename).to_string_lossy().into_owned(),
            }
        } else {
            Self::Tcp {
                host: "127.0.0.1".to_owned(),
                port: 0,
            }
        }
    }

    /// Parse from `TRANSPORT_ENDPOINT` env var, falling back to platform default.
    #[must_use]
    pub fn from_env_or_default(primal_name: &str, family_id: Option<&str>) -> Self {
        std::env::var("TRANSPORT_ENDPOINT")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| Self::platform_default(primal_name, family_id))
    }

    /// Whether the endpoint is local (same-host).
    #[must_use]
    pub fn is_local(&self) -> bool {
        match self {
            Self::Uds { .. } => true,
            Self::Tcp { host, .. } => host == "127.0.0.1" || host == "::1" || host == "localhost",
            Self::MeshRelay { .. } => false,
        }
    }
}

/// Transport-agnostic connected stream.
#[derive(Debug)]
pub enum TransportStream {
    /// Unix domain socket stream.
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    /// TCP stream.
    Tcp(tokio::net::TcpStream),
}

impl TransportStream {
    /// Transport name as a static string.
    #[must_use]
    pub const fn transport_name(&self) -> &'static str {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => "uds",
            Self::Tcp(_) => "tcp",
        }
    }
}

impl AsyncRead for TransportStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_flush(cx),
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Connect to a service via its endpoint.
///
/// # Errors
///
/// Returns an error if the underlying socket connection fails or the endpoint
/// type is unsupported on this platform.
pub async fn connect_transport(endpoint: &TransportEndpoint) -> io::Result<TransportStream> {
    match endpoint {
        #[cfg(unix)]
        TransportEndpoint::Uds { path } => {
            let stream = tokio::net::UnixStream::connect(path).await?;
            Ok(TransportStream::Unix(stream))
        }
        #[cfg(not(unix))]
        TransportEndpoint::Uds { path } => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("UDS not available on this platform for {path}"),
        )),
        TransportEndpoint::Tcp { host, port } => {
            let stream = tokio::net::TcpStream::connect(format!("{host}:{port}")).await?;
            Ok(TransportStream::Tcp(stream))
        }
        TransportEndpoint::MeshRelay { .. } => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "mesh relay requires songBird routing",
        )),
    }
}

/// Transport-agnostic listener.
#[derive(Debug)]
pub enum TransportListener {
    /// Unix domain socket listener.
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
    /// TCP listener.
    Tcp(tokio::net::TcpListener),
}

impl TransportListener {
    /// Accept the next connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying accept call fails.
    pub async fn accept(&self) -> io::Result<TransportStream> {
        match self {
            #[cfg(unix)]
            Self::Unix(l) => {
                let (stream, _) = l.accept().await?;
                Ok(TransportStream::Unix(stream))
            }
            Self::Tcp(l) => {
                let (stream, _) = l.accept().await?;
                Ok(TransportStream::Tcp(stream))
            }
        }
    }

    /// Transport name as a static string.
    #[must_use]
    pub const fn transport_name(&self) -> &'static str {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => "uds",
            Self::Tcp(_) => "tcp",
        }
    }

    /// Whether this listener accepts only local connections.
    #[must_use]
    pub fn is_local(&self) -> bool {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => true,
            Self::Tcp(l) => l.local_addr().is_ok_and(|a| a.ip().is_loopback()),
        }
    }
}

/// Bind a listener on the given endpoint.
///
/// # Errors
///
/// Returns an error if directory creation, socket binding, or the endpoint type
/// is unsupported on this platform.
pub async fn bind_transport(endpoint: &TransportEndpoint) -> io::Result<TransportListener> {
    match endpoint {
        #[cfg(unix)]
        TransportEndpoint::Uds { path } => {
            let socket_path = std::path::PathBuf::from(path);
            if let Some(parent) = socket_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let _ = std::fs::remove_file(&socket_path);
            let listener = tokio::net::UnixListener::bind(&socket_path)?;
            Ok(TransportListener::Unix(listener))
        }
        #[cfg(not(unix))]
        TransportEndpoint::Uds { path } => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("UDS not available on this platform for {path}"),
        )),
        TransportEndpoint::Tcp { host, port } => {
            let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
            Ok(TransportListener::Tcp(listener))
        }
        TransportEndpoint::MeshRelay { .. } => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "mesh relay cannot be bound — register with songBird",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{TransportEndpoint, TransportListener, bind_transport, connect_transport};
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn mesh_relay_endpoint() -> TransportEndpoint {
        TransportEndpoint::MeshRelay {
            peer_id: "peer-1".to_owned(),
            capability: "gossip".to_owned(),
        }
    }

    #[test]
    fn platform_default_is_uds_on_unix() {
        if cfg!(unix) {
            let endpoint = TransportEndpoint::platform_default("swarmVine", None);
            assert!(matches!(endpoint, TransportEndpoint::Uds { .. }));
            if let TransportEndpoint::Uds { path } = endpoint {
                assert!(path.contains("swarmVine.sock"));
            }
        }
    }

    #[test]
    fn from_env_or_default_without_env() {
        temp_env::with_var_unset("TRANSPORT_ENDPOINT", || {
            let default = TransportEndpoint::platform_default("testPrimal", Some("fam"));
            let resolved = TransportEndpoint::from_env_or_default("testPrimal", Some("fam"));
            assert_eq!(resolved, default);
        });
    }

    #[test]
    fn is_local_classifies_endpoints() {
        assert!(
            TransportEndpoint::Uds {
                path: "/tmp/x.sock".to_owned()
            }
            .is_local()
        );
        assert!(
            TransportEndpoint::Tcp {
                host: "127.0.0.1".to_owned(),
                port: 8080
            }
            .is_local()
        );
        assert!(
            TransportEndpoint::Tcp {
                host: "localhost".to_owned(),
                port: 8080
            }
            .is_local()
        );
        assert!(
            TransportEndpoint::Tcp {
                host: "::1".to_owned(),
                port: 8080
            }
            .is_local()
        );
        assert!(
            !TransportEndpoint::Tcp {
                host: "example.com".to_owned(),
                port: 443
            }
            .is_local()
        );
        assert!(!mesh_relay_endpoint().is_local());
    }

    #[test]
    fn serde_roundtrip_each_variant() {
        let variants = [
            TransportEndpoint::Uds {
                path: "/run/user/0/biomeos/swarmVine.sock".to_owned(),
            },
            TransportEndpoint::Tcp {
                host: "127.0.0.1".to_owned(),
                port: 9000,
            },
            mesh_relay_endpoint(),
        ];

        for endpoint in variants {
            let json = serde_json::to_string(&endpoint).unwrap();
            let decoded: TransportEndpoint = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, endpoint);
        }
    }

    #[tokio::test]
    async fn transport_stream_and_listener_names_and_locality() {
        let endpoint = TransportEndpoint::Tcp {
            host: "127.0.0.1".to_owned(),
            port: 0,
        };
        let listener = bind_transport(&endpoint).await.unwrap();
        assert_eq!(listener.transport_name(), "tcp");
        assert!(listener.is_local());

        let addr = match &listener {
            TransportListener::Tcp(l) => l.local_addr().unwrap(),
            #[cfg(unix)]
            TransportListener::Unix(_) => unreachable!("tcp test uses tcp listener"),
        };

        let stream = connect_transport(&TransportEndpoint::Tcp {
            host: addr.ip().to_string(),
            port: addr.port(),
        })
        .await
        .unwrap();
        assert_eq!(stream.transport_name(), "tcp");

        #[cfg(unix)]
        {
            let uds_path = std::env::temp_dir().join(format!(
                "swarmvine-transport-test-{}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&uds_path);
            let uds_listener = bind_transport(&TransportEndpoint::Uds {
                path: uds_path.to_string_lossy().into_owned(),
            })
            .await
            .unwrap();
            assert_eq!(uds_listener.transport_name(), "uds");
            assert!(uds_listener.is_local());

            let uds_stream = connect_transport(&TransportEndpoint::Uds {
                path: uds_path.to_string_lossy().into_owned(),
            })
            .await
            .unwrap();
            assert_eq!(uds_stream.transport_name(), "uds");
            let _ = std::fs::remove_file(&uds_path);
        }
    }

    #[tokio::test]
    async fn uds_connect_and_accept_roundtrip() {
        #[cfg(unix)]
        {
            let path =
                std::env::temp_dir().join(format!("swarmvine-uds-e2e-{}.sock", std::process::id()));
            let _ = std::fs::remove_file(&path);
            let listener = bind_transport(&TransportEndpoint::Uds {
                path: path.to_string_lossy().into_owned(),
            })
            .await
            .unwrap();

            let client_path = path.clone();
            let client = tokio::spawn(async move {
                let mut stream = connect_transport(&TransportEndpoint::Uds {
                    path: client_path.to_string_lossy().into_owned(),
                })
                .await
                .unwrap();
                stream.write_all(b"ping").await.unwrap();
            });

            let mut server = listener.accept().await.unwrap();
            let mut buf = [0_u8; 4];
            server.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            client.await.unwrap();
            let _ = std::fs::remove_file(&path);
        }
    }

    #[tokio::test]
    async fn connect_mesh_relay_unsupported() {
        let err = connect_transport(&mesh_relay_endpoint()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(err.to_string().contains("mesh relay"));
    }

    #[tokio::test]
    async fn bind_mesh_relay_unsupported() {
        let err = bind_transport(&mesh_relay_endpoint()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(err.to_string().contains("mesh relay"));
    }
}
