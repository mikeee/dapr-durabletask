//! Shared test harness for integration and performance tests.
//!
//! Provides [`TestEnv`] (sidecar lifecycle), [`WorkerGuard`] (worker

#![allow(dead_code)]
//! lifecycle), and helpers for locating the sidecar binary and allocating
//! free ports.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::worker::TaskHubGrpcWorker;

/// Locate the sidecar binary, checking `DURABLETASK_SIDECAR_BIN` first,
/// then falling back to `<workspace>/tmp/durabletask-sidecar`.
pub fn sidecar_bin() -> Option<String> {
    let bin = std::env::var("DURABLETASK_SIDECAR_BIN").unwrap_or_else(|_| {
        let workspace = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        format!("{workspace}/tmp/durabletask-sidecar")
    });
    std::path::Path::new(&bin).exists().then_some(bin)
}

/// Ask the OS for an ephemeral free port.
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Per-test sidecar process bound to a free port.
///
/// The sidecar is killed when `TestEnv` is dropped.
pub struct TestEnv {
    pub address: String,
    sidecar: Child,
}

impl TestEnv {
    /// Spawn a sidecar on a free port and poll until it's ready (up to 4 s).
    /// Returns `None` if the sidecar binary is absent.
    pub async fn start() -> Option<Self> {
        let bin = sidecar_bin()?;
        let port = free_port();
        let address = format!("http://127.0.0.1:{port}");

        let sidecar = Command::new(&bin)
            .args(["--port", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start sidecar '{bin}': {e}"));

        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Some(Self { address, sidecar });
            }
        }
        eprintln!("[harness] Sidecar on port {port} failed to start within 4 s");
        None
    }

    pub fn new_worker(&self) -> TaskHubGrpcWorker {
        TaskHubGrpcWorker::new(&self.address)
    }

    pub async fn new_client(&self) -> TaskHubGrpcClient {
        TaskHubGrpcClient::new(&self.address)
            .await
            .expect("failed to connect to sidecar")
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = self.sidecar.kill();
        let _ = self.sidecar.wait();
    }
}

/// Convenience macro: start a [`TestEnv`] or panic with a helpful message.
#[macro_export]
macro_rules! setup {
    ($name:ident) => {
        let $name = harness::TestEnv::start().await.expect(
            "sidecar not available — run `nix develop` to build it, or set DURABLETASK_SIDECAR_BIN",
        );
    };
}

/// RAII guard that spawns a worker task and cancels it on [`stop()`](WorkerGuard::stop).
pub struct WorkerGuard {
    pub shutdown: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl WorkerGuard {
    pub fn start(worker: TaskHubGrpcWorker) -> Self {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let token = shutdown.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = worker.start(token).await {
                eprintln!("Worker error: {e}");
            }
        });
        Self { shutdown, handle }
    }

    pub async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.handle.await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Read an environment variable, parse it, or return a default.
pub fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
