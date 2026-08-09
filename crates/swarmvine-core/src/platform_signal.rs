//! Platform-aware shutdown signal handling.
//!
//! Provides a single `shutdown_signal()` future that resolves when the process
//! should begin graceful shutdown, regardless of platform:
//! - **Unix** (Linux, macOS, iOS, Android, BSD): SIGTERM or SIGINT
//! - **Windows**: Ctrl+C

/// Wait for a platform-appropriate shutdown signal.
///
/// Use with `tokio::select!` to race against your main loop.
pub async fn shutdown_signal() {
    let signal = platform_signal_impl().await;
    tracing::info!(signal = %signal, "shutdown signal received");
}

/// Wait for shutdown and return the signal name.
pub async fn shutdown_signal_named() -> &'static str {
    let signal = platform_signal_impl().await;
    tracing::info!(signal = %signal, "shutdown signal received");
    signal
}

#[cfg(unix)]
async fn platform_signal_impl() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT",
    }
}

#[cfg(not(unix))]
async fn platform_signal_impl() -> &'static str {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to register Ctrl+C handler");
    "ctrl_c"
}

/// Register a shutdown hook.
///
/// The cleanup future runs after the shutdown signal is received.
pub fn on_shutdown<F, Fut>(cleanup: F) -> tokio::task::JoinHandle<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        shutdown_signal().await;
        cleanup().await;
    })
}
