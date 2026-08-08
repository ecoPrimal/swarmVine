//! G66 Transport Abstraction — silicon-agnostic IPC.
//!
//! Eliminates silicon deism: primals express *what* they connect to (a service,
//! a capability) without encoding *how* bytes move. `#[cfg(unix)]` lives here —
//! not scattered across business logic.
//!
//! Components:
//! - `TransportEndpoint` — where to connect (UDS / TCP / MeshRelay)
//! - `TransportStream` — the connected byte pipe (AsyncRead + AsyncWrite)
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
    #[serde(rename = "uds")]
    Uds { path: String },
    #[serde(rename = "tcp")]
    Tcp { host: String, port: u16 },
    #[serde(rename = "mesh_relay")]
    MeshRelay { peer_id: String, capability: String },
}

impl TransportEndpoint {
    /// Platform default: UDS on Unix, TCP localhost elsewhere.
    #[must_use]
    pub fn platform_default(primal_name: &str, family_id: Option<&str>) -> Self {
        if cfg!(unix) {
            let runtime_dir = std::env::var(crate::env_keys::XDG_RUNTIME_DIR)
                .unwrap_or_else(|_| "/tmp".to_owned());
            let filename = match family_id.filter(|id| !id.is_empty() && *id != "default") {
                Some(fid) => format!("{primal_name}-{fid}.sock"),
                None => format!("{primal_name}.sock"),
            };
            Self::Uds { path: format!("{runtime_dir}/biomeos/{filename}") }
        } else {
            Self::Tcp { host: "127.0.0.1".to_owned(), port: 0 }
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
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    Tcp(tokio::net::TcpStream),
}

impl TransportStream {
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
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
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
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
    Tcp(tokio::net::TcpListener),
}

impl TransportListener {
    /// Accept the next connection.
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

    #[must_use]
    pub const fn transport_name(&self) -> &'static str {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => "uds",
            Self::Tcp(_) => "tcp",
        }
    }

    #[must_use]
    pub fn is_local(&self) -> bool {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => true,
            Self::Tcp(l) => l.local_addr().map(|a| a.ip().is_loopback()).unwrap_or(false),
        }
    }
}

/// Bind a listener on the given endpoint.
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
