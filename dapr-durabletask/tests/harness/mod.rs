//! Shared test harness for integration and performance tests.
//!
//! Provides [`TestEnv`] (sidecar lifecycle), [`WorkerGuard`] (worker

#![allow(dead_code)]
//! lifecycle), and helpers for locating the sidecar binary and allocating
//! free ports.

use std::net::TcpListener;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::worker::TaskHubGrpcWorker;

/// Locate the sidecar binary, checking `DURABLETASK_SIDECAR_BIN` first,
/// then falling back to package and workspace `tmp/durabletask-sidecar` paths.
pub fn sidecar_bin() -> Option<String> {
    if let Ok(bin) = std::env::var("DURABLETASK_SIDECAR_BIN")
        && std::path::Path::new(&bin).exists()
    {
        return Some(bin);
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    [
        format!("{manifest_dir}/tmp/durabletask-sidecar"),
        format!("{manifest_dir}/../tmp/durabletask-sidecar"),
    ]
    .into_iter()
    .find(|bin| std::path::Path::new(bin).exists())
}

/// Kill a child process and wait for it to exit, ensuring no zombie is left.
///
/// Returns the exit status if the child was successfully reaped.
/// This helper prevents the common mistake of calling `kill()` without a
/// subsequent `wait()`, which leaves zombie processes on Unix.
pub fn kill_and_wait(child: &mut Child) -> Option<ExitStatus> {
    let _ = child.kill();
    child.wait().ok()
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

        let mut sidecar = Command::new(&bin)
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
        kill_and_wait(&mut sidecar);
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
        kill_and_wait(&mut self.sidecar);
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

#[cfg(test)]
mod process_lifecycle_tests {
    use super::*;
    use std::process::Command;

    /// Spawn a long-running `sleep` process (acts as a stand-in for the sidecar).
    fn spawn_sleep() -> Child {
        Command::new("sleep")
            .arg("300")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn sleep process")
    }

    #[cfg(target_os = "linux")]
    fn process_state(pid: u32) -> Option<char> {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        let state = status.lines().find(|line| line.starts_with("State:"))?;
        state.split_whitespace().nth(1)?.chars().next()
    }

    #[cfg(target_os = "linux")]
    fn wait_for_process_state(pid: u32, expected: char) -> Option<char> {
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut last_state = process_state(pid);
        while std::time::Instant::now() < deadline {
            if last_state == Some(expected) {
                return last_state;
            }
            std::thread::sleep(Duration::from_millis(10));
            last_state = process_state(pid);
        }
        last_state
    }

    #[cfg(target_os = "linux")]
    fn assert_reaped(pid: u32, context: &str) {
        assert_eq!(
            process_state(pid),
            None,
            "{context}: child process {pid} should be fully reaped"
        );
    }

    /// Verifies that `kill_and_wait` actually reaps the child process (no zombie).
    #[test]
    fn kill_and_wait_reaps_child() {
        let mut child = spawn_sleep();
        let pid = child.id();

        let status = kill_and_wait(&mut child);
        assert!(status.is_some(), "kill_and_wait must return an ExitStatus");

        // On Unix, /proc/<pid>/status should not exist for a fully reaped child.
        #[cfg(target_os = "linux")]
        assert_reaped(pid, "kill_and_wait");
    }

    /// Verifies that `kill_and_wait` is idempotent — calling it on an already-exited
    /// child does not panic.
    #[test]
    fn kill_and_wait_idempotent_on_exited_child() {
        let mut child = Command::new("true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn `true`");

        let _ = child.wait();

        let status = kill_and_wait(&mut child);
        // On most platforms, wait after a previous wait returns the cached status.
        // The important thing is it doesn't panic or leave a zombie.
        let _ = status;
    }

    /// Regression: TestEnv::Drop must reap the child so no zombie remains.
    /// Simulates the TestEnv lifecycle with a real long-running process.
    #[test]
    fn test_env_drop_reaps_child() {
        let child = spawn_sleep();
        let pid = child.id();

        let env = TestEnv {
            address: "http://127.0.0.1:0".to_string(),
            sidecar: child,
        };
        std::mem::drop(env);

        #[cfg(target_os = "linux")]
        assert_reaped(pid, "TestEnv drop");
    }

    /// Regression: kill without wait leaves a zombie. Demonstrates that our
    /// helper avoids this by requiring wait after kill.
    #[cfg(target_os = "linux")]
    #[test]
    fn kill_without_wait_leaves_zombie() {
        let mut child = spawn_sleep();
        let pid = child.id();

        // Kill but intentionally do NOT wait — simulates the bug we're preventing.
        let _ = child.kill();

        let state_before_wait = wait_for_process_state(pid, 'Z');

        let _ = child.wait();

        assert_eq!(
            state_before_wait,
            Some('Z'),
            "kill without wait should leave process {pid} in zombie state before wait"
        );
    }

    /// Ensures the SidecarHandle (from e2e.rs) pattern of kill+wait is correct.
    /// Tests the same pattern inline: kill then wait must result in full cleanup.
    #[test]
    fn sidecar_handle_pattern_reaps() {
        let mut child = spawn_sleep();
        let pid = child.id();

        // Mirror SidecarHandle::kill pattern: kill then wait.
        let _ = child.kill();
        let wait_result = child.wait();
        assert!(
            wait_result.is_ok(),
            "wait after kill must succeed for proper reaping"
        );

        #[cfg(target_os = "linux")]
        assert_reaped(pid, "SidecarHandle kill pattern");
    }
}
