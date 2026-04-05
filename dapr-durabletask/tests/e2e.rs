//! End-to-end tests for the durabletask Rust SDK.
//!
//! Each test spawns its own sidecar process on a dynamically-assigned free
//! port and tears it down when done via `TestEnv`'s `Drop` impl. Tests are
//! fully isolated and run in parallel under both `cargo test` and
//! `cargo nextest run` — no shared global state, no serialisation needed.
//!
//! The sidecar binary is expected at `tmp/durabletask-sidecar` (built by the
//! nix flake shellHook) or at the path in `DURABLETASK_SIDECAR_BIN`.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use dapr_durabletask::api::OrchestrationStatus;
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{ActivityContext, when_all, when_any};
use dapr_durabletask::worker::{ReconnectPolicy, TaskHubGrpcWorker, WorkerOptions};

const TIMEOUT: Duration = Duration::from_secs(30);

// ── Per-test sidecar environment ─────────────────────────────────────────────

fn sidecar_bin() -> Option<String> {
    let bin = std::env::var("DURABLETASK_SIDECAR_BIN").unwrap_or_else(|_| {
        let workspace = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        format!("{}/tmp/durabletask-sidecar", workspace)
    });
    if std::path::Path::new(&bin).exists() {
        Some(bin)
    } else {
        None
    }
}

fn free_port() -> u16 {
    // Bind to port 0 to let the OS assign a free port, then release it.
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct TestEnv {
    pub address: String,
    sidecar: Child,
}

impl TestEnv {
    /// Spawns a sidecar on a free port and waits up to 4 s for it to be ready.
    /// Returns `None` if the sidecar binary is absent.
    async fn start() -> Option<Self> {
        let bin = sidecar_bin()?;
        let port = free_port();
        let address = format!("http://127.0.0.1:{}", port);

        let sidecar = Command::new(&bin)
            .args(["--port", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start sidecar '{}': {}", bin, e));

        // Poll until the sidecar binds the port (up to 4 s).
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Some(Self { address, sidecar });
            }
        }
        eprintln!("[e2e] Sidecar on port {} failed to start within 4s", port);
        None
    }

    fn new_worker(&self) -> TaskHubGrpcWorker {
        TaskHubGrpcWorker::new(&self.address)
    }

    async fn new_client(&self) -> TaskHubGrpcClient {
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

// ── Controllable sidecar for reconnect tests ──────────────────────────────────

/// A sidecar process bound to a specific port that can be stopped and
/// restarted independently, used by reconnect tests.
struct SidecarHandle {
    port: u16,
    process: Child,
}

impl SidecarHandle {
    /// Launch a sidecar on the given port. Does NOT wait for it to be ready.
    /// Returns `None` if the sidecar binary is absent.
    fn launch(port: u16) -> Option<Self> {
        let bin = sidecar_bin()?;
        let process = Command::new(&bin)
            .args(["--port", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start sidecar '{}': {}", bin, e));
        Some(Self { port, process })
    }

    /// Returns the gRPC address of this sidecar.
    fn address(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Poll until the port is listening (up to `timeout`).
    async fn wait_ready(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// Kill the process and wait for it to exit, freeing the port.
    fn kill(mut self) -> u16 {
        let _ = self.process.kill();
        let _ = self.process.wait();
        self.port
    }

    /// Kill the process, wait for it to exit, then re-launch on the same port.
    async fn restart(self) -> Option<Self> {
        let port = self.kill();
        // Brief pause to let the OS fully release the port.
        tokio::time::sleep(Duration::from_millis(100)).await;
        SidecarHandle::launch(port)
    }
}

impl Drop for SidecarHandle {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Shorthand for a `ReconnectPolicy` suitable for reconnect tests: short
/// delays, no jitter, no attempt cap.
fn test_reconnect_policy() -> ReconnectPolicy {
    ReconnectPolicy::new()
        .with_initial_delay(Duration::from_millis(50))
        .with_max_delay(Duration::from_millis(200))
        .with_multiplier(2.0)
        .with_jitter(false)
}

/// Starts a per-test sidecar and binds it to `$name`, or panics.
macro_rules! setup {
    ($name:ident) => {
        let $name = TestEnv::start()
            .await
            .expect("sidecar not available — run `nix develop` to build it, or set DURABLETASK_SIDECAR_BIN");
    };
}

// ── Worker lifecycle helper ───────────────────────────────────────────────────

struct WorkerGuard {
    shutdown: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl WorkerGuard {
    fn start(worker: TaskHubGrpcWorker) -> Self {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let token = shutdown.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = worker.start(token).await {
                eprintln!("Worker error: {}", e);
            }
        });
        Self { shutdown, handle }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.handle.await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// ===========================================================================
// E2E Tests — each test has its own isolated sidecar on a free port.
// ===========================================================================

#[tokio::test]
async fn test_empty_orchestration() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("empty_orch", |_ctx| async move { Ok(None) });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("empty_orch", None, None, None)
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.name, "empty_orch");
    assert_eq!(state.instance_id, id);
    assert!(state.failure_details.is_none());
    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

    guard.stop().await;
}

#[tokio::test]
async fn test_single_orchestration_without_activity() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("no_activity_orch", |ctx| async move {
            let input: i32 = ctx.get_input()?;
            Ok(Some(serde_json::to_string(&(input + 1)).unwrap()))
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration(
            "no_activity_orch",
            Some(serde_json::to_string(&15).unwrap()),
            None,
            None,
        )
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.name, "no_activity_orch");
    assert_eq!(state.instance_id, id);
    assert!(state.failure_details.is_none());
    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert_eq!(state.serialized_input, Some("15".to_string()));
    assert_eq!(state.serialized_output, Some("16".to_string()));

    guard.stop().await;
}

#[tokio::test]
async fn test_activity_sequence() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("sequence_orch", |ctx| async move {
            let start_val: i32 = ctx.get_input()?;
            let mut numbers = vec![start_val];
            let mut current = start_val;

            for _ in 0..10 {
                let result = ctx.call_activity("plus_one", current).await?;
                current = serde_json::from_str(result.as_deref().unwrap_or("0")).unwrap_or(0);
                numbers.push(current);
            }

            ctx.set_custom_status("foobaz");
            Ok(Some(serde_json::to_string(&numbers).unwrap()))
        });
    worker.registry_mut().add_named_activity(
        "plus_one",
        |_ctx: ActivityContext, input: Option<String>| async move {
            let val: i32 = serde_json::from_str(input.as_deref().unwrap_or("0")).unwrap_or(0);
            Ok(Some(serde_json::to_string(&(val + 1)).unwrap()))
        },
    );
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration(
            "sequence_orch",
            Some(serde_json::to_string(&1).unwrap()),
            None,
            None,
        )
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.name, "sequence_orch");
    assert_eq!(state.instance_id, id);
    assert!(state.failure_details.is_none());
    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert_eq!(state.serialized_input, Some("1".to_string()));
    assert_eq!(
        state.serialized_output,
        Some(serde_json::to_string(&vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]).unwrap())
    );
    assert_eq!(state.serialized_custom_status, Some("foobaz".to_string()));

    guard.stop().await;
}

#[tokio::test]
async fn test_fan_out_fan_in() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("fanout_orch", |ctx| async move {
            let count: i32 = ctx.get_input()?;
            let mut tasks = Vec::new();
            for _ in 0..count {
                tasks.push(ctx.call_activity("increment", ()));
            }
            when_all(tasks).await?;
            Ok(None)
        });
    worker.registry_mut().add_named_activity(
        "increment",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration(
            "fanout_orch",
            Some(serde_json::to_string(&10).unwrap()),
            None,
            None,
        )
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert!(state.failure_details.is_none());

    guard.stop().await;
}

#[tokio::test]
async fn test_sub_orchestration() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("child_orch", |ctx| async move {
            ctx.call_activity("increment", ()).await?;
            Ok(None)
        });
    worker
        .registry_mut()
        .add_named_orchestrator("parent_orch", |ctx| async move {
            ctx.call_sub_orchestrator("child_orch", (), None).await?;
            Ok(None)
        });
    worker.registry_mut().add_named_activity(
        "increment",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("parent_orch", None, None, None)
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert!(state.failure_details.is_none());

    guard.stop().await;
}

#[tokio::test]
async fn test_sub_orchestration_fan_out() {
    setup!(env);
    const ACTIVITY_COUNT: i32 = 2;

    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("child_fanout_orch", |ctx| async move {
            let count: i32 = ctx.get_input()?;
            for _ in 0..count {
                ctx.call_activity("increment", ()).await?;
            }
            Ok(None)
        });
    worker
        .registry_mut()
        .add_named_orchestrator("parent_fanout_orch", |ctx| async move {
            let count: i32 = ctx.get_input()?;
            let mut tasks = Vec::new();
            for _ in 0..count {
                tasks.push(ctx.call_sub_orchestrator("child_fanout_orch", ACTIVITY_COUNT, None));
            }
            when_all(tasks).await?;
            Ok(None)
        });
    worker.registry_mut().add_named_activity(
        "increment",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration(
            "parent_fanout_orch",
            Some(serde_json::to_string(&2).unwrap()),
            None,
            None,
        )
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(Duration::from_secs(45)))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert!(state.failure_details.is_none());

    guard.stop().await;
}

#[tokio::test]
async fn test_multiple_external_events() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("multi_event_orch", |ctx| async move {
            let a = ctx.wait_for_external_event("A").await?;
            let b = ctx.wait_for_external_event("B").await?;
            let c = ctx.wait_for_external_event("C").await?;

            let values: Vec<String> = [a, b, c]
                .iter()
                .map(|opt| {
                    serde_json::from_str(opt.as_deref().unwrap_or("\"\"")).unwrap_or_default()
                })
                .collect();
            Ok(Some(serde_json::to_string(&values).unwrap()))
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("multi_event_orch", None, None, None)
        .await
        .unwrap();

    client
        .raise_orchestration_event(&id, "A", Some("\"a\"".to_string()))
        .await
        .unwrap();
    client
        .raise_orchestration_event(&id, "B", Some("\"b\"".to_string()))
        .await
        .unwrap();
    client
        .raise_orchestration_event(&id, "C", Some("\"c\"".to_string()))
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert_eq!(
        state.serialized_output,
        Some(serde_json::to_string(&vec!["a", "b", "c"]).unwrap())
    );

    guard.stop().await;
}

#[tokio::test]
async fn test_single_timer() {
    setup!(env);
    let delay_secs: i64 = 3;
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("timer_orch", |ctx| async move {
            ctx.create_timer(Duration::from_secs(3)).await?;
            Ok(None)
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("timer_orch", None, None, None)
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.name, "timer_orch");
    assert_eq!(state.instance_id, id);
    assert!(state.failure_details.is_none());
    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert!(state.created_at.is_some());
    assert!(state.last_updated_at.is_some());

    if let (Some(created), Some(updated)) = (&state.created_at, &state.last_updated_at) {
        let elapsed = *updated - *created;
        assert!(elapsed >= chrono::Duration::seconds(delay_secs));
    }

    guard.stop().await;
}

#[tokio::test]
async fn test_external_event_with_timeout_approved() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("timeout_approved_orch", |ctx| async move {
            let approval = ctx.wait_for_external_event("Approval");
            let timeout = ctx.create_timer(Duration::from_secs(3));
            let winner = when_any(vec![approval, timeout]).await?;

            if winner == 0 {
                Ok(Some("\"approved\"".to_string()))
            } else {
                Ok(Some("\"timed out\"".to_string()))
            }
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("timeout_approved_orch", None, None, None)
        .await
        .unwrap();

    client
        .raise_orchestration_event(&id, "Approval", None)
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert_eq!(state.serialized_output, Some("\"approved\"".to_string()));

    guard.stop().await;
}

#[tokio::test]
async fn test_external_event_with_timeout_expired() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("timeout_expired_orch", |ctx| async move {
            let approval = ctx.wait_for_external_event("Approval");
            let timeout = ctx.create_timer(Duration::from_secs(3));
            let winner = when_any(vec![approval, timeout]).await?;

            if winner == 0 {
                Ok(Some("\"approved\"".to_string()))
            } else {
                Ok(Some("\"timed out\"".to_string()))
            }
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("timeout_expired_orch", None, None, None)
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert_eq!(state.serialized_output, Some("\"timed out\"".to_string()));

    guard.stop().await;
}

#[tokio::test]
async fn test_terminate_orchestration() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("terminate_orch", |ctx| async move {
            let result = ctx.wait_for_external_event("my_event").await?;
            Ok(result)
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("terminate_orch", None, None, None)
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_start(&id, false, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");
    assert_eq!(state.runtime_status, OrchestrationStatus::Running);

    client
        .terminate_orchestration(
            &id,
            Some("\"some reason for termination\"".to_string()),
            false,
        )
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Terminated);
    assert_eq!(
        state.serialized_output,
        Some("\"some reason for termination\"".to_string())
    );

    guard.stop().await;
}

#[tokio::test]
async fn test_continue_as_new() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("continue_orch", |ctx| async move {
            let input: i32 = ctx.get_input()?;
            if input < 10 {
                ctx.continue_as_new(input + 1, true);
                Ok(None)
            } else {
                Ok(Some(serde_json::to_string(&input).unwrap()))
            }
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration(
            "continue_orch",
            Some(serde_json::to_string(&1).unwrap()),
            None,
            None,
        )
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert_eq!(state.serialized_output, Some("10".to_string()));

    guard.stop().await;
}

#[tokio::test]
async fn test_suspend_resume() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("suspend_orch", |ctx| async move {
            let result = ctx.wait_for_external_event("continue").await?;
            Ok(result)
        });
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration("suspend_orch", None, None, None)
        .await
        .unwrap();

    client
        .wait_for_orchestration_start(&id, false, Some(TIMEOUT))
        .await
        .unwrap();

    client
        .suspend_orchestration(&id, Some("pausing".to_string()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let state = client
        .get_orchestration_state(&id, false)
        .await
        .unwrap()
        .expect("no state returned");
    assert_eq!(state.runtime_status, OrchestrationStatus::Suspended);

    client
        .resume_orchestration(&id, Some("continuing".to_string()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .raise_orchestration_event(&id, "continue", Some("\"resumed ok\"".to_string()))
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

    guard.stop().await;
}

#[tokio::test]
async fn test_purge_orchestration() {
    setup!(env);
    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("purge_orch", |ctx| async move {
            let input: i32 = ctx.get_input()?;
            let result = ctx.call_activity("plus_one", input).await?;
            Ok(result)
        });
    worker.registry_mut().add_named_activity(
        "plus_one",
        |_ctx: ActivityContext, input: Option<String>| async move {
            let val: i32 = serde_json::from_str(input.as_deref().unwrap_or("0")).unwrap_or(0);
            Ok(Some(serde_json::to_string(&(val + 1)).unwrap()))
        },
    );
    let guard = WorkerGuard::start(worker);

    let mut client = env.new_client().await;
    let id = client
        .schedule_new_orchestration(
            "purge_orch",
            Some(serde_json::to_string(&1).unwrap()),
            None,
            None,
        )
        .await
        .unwrap();

    let state = client
        .wait_for_orchestration_completion(&id, true, Some(TIMEOUT))
        .await
        .unwrap()
        .expect("no state returned");

    assert_eq!(state.name, "purge_orch");
    assert_eq!(state.instance_id, id);
    assert!(state.failure_details.is_none());
    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
    assert_eq!(state.serialized_input, Some("1".to_string()));
    assert_eq!(state.serialized_output, Some("2".to_string()));

    let deleted = client.purge_orchestration(&id, false).await.unwrap();
    assert_eq!(deleted, 1);

    let state = client.get_orchestration_state(&id, false).await.unwrap();
    assert!(state.is_none());

    guard.stop().await;
}

#[tokio::test]
async fn test_human_interaction_three_orchestrations() {
    setup!(env);

    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("hi_approval_workflow", |ctx| async move {
            let order: String = ctx.get_input().unwrap_or_else(|_| "unknown".into());

            ctx.call_activity("hi_send_approval_request", &order)
                .await?;

            let approval_task = ctx.wait_for_external_event("approval");
            let timeout_task = ctx.create_timer(Duration::from_secs(30));

            let winner = when_any(vec![approval_task, timeout_task]).await?;

            if winner == 1 {
                Ok(Some(serde_json::to_string("timed out").unwrap()))
            } else {
                let result = ctx.call_activity("hi_process_order", &order).await?;
                Ok(result)
            }
        });
    worker.registry_mut().add_named_activity(
        "hi_send_approval_request",
        |_ctx, input| async move {
            let order: String = serde_json::from_str(input.as_deref().unwrap_or("\"\""))?;
            eprintln!("[e2e] Sending approval request for: {}", order);
            Ok(None)
        },
    );
    worker
        .registry_mut()
        .add_named_activity("hi_process_order", |_ctx, input| async move {
            let order: String = serde_json::from_str(input.as_deref().unwrap_or("\"\""))?;
            eprintln!("[e2e] Processing order: {}", order);
            Ok(Some(serde_json::to_string(&format!(
                "Processed: {}",
                order
            ))?))
        });

    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    // 1. Start orchestrations 1, 2, 3 ─────────────────────────────────────────
    let orders = ["order-1", "order-2", "order-3"];
    let mut instance_ids: Vec<String> = Vec::with_capacity(3);

    for order in &orders {
        let input = serde_json::to_string(order).unwrap();
        let id = client
            .schedule_new_orchestration("hi_approval_workflow", Some(input), None, None)
            .await
            .unwrap();
        eprintln!("[e2e] Started orchestration for {} → {}", order, id);
        instance_ids.push(id);
    }

    // Wait for all three to reach Running (approval-request activity completes).
    for id in &instance_ids {
        client
            .wait_for_orchestration_start(id, false, Some(TIMEOUT))
            .await
            .unwrap()
            .expect("orchestration did not start");
    }

    // 2. Print / assert status of all three ───────────────────────────────────
    for (i, id) in instance_ids.iter().enumerate() {
        let state = client
            .get_orchestration_state(id, false)
            .await
            .unwrap()
            .expect("state missing");
        eprintln!(
            "[e2e] Orchestration {} ({}): {}",
            i + 1,
            id,
            state.runtime_status
        );
        assert_eq!(
            state.runtime_status,
            OrchestrationStatus::Running,
            "orchestration {} should be Running while awaiting approval",
            i + 1
        );
    }

    // 3. Approve 1 → 2 → 3, waiting for each to complete before the next ─────
    let mut completed_outputs: Vec<String> = Vec::with_capacity(3);

    for (i, id) in instance_ids.iter().enumerate() {
        let payload = serde_json::to_string(&serde_json::json!({"approved": true})).unwrap();

        eprintln!("[e2e] Sending approval for orchestration {}", i + 1);
        client
            .raise_orchestration_event(id, "approval", Some(payload))
            .await
            .unwrap();

        let state = client
            .wait_for_orchestration_completion(id, true, Some(TIMEOUT))
            .await
            .unwrap()
            .expect("no state returned");

        assert_eq!(
            state.runtime_status,
            OrchestrationStatus::Completed,
            "orchestration {} should be Completed",
            i + 1
        );

        let output = state.serialized_output.expect("expected output");
        eprintln!("[e2e] Orchestration {} completed with: {}", i + 1, output);
        completed_outputs.push(output);
    }

    assert_eq!(
        completed_outputs[0],
        serde_json::to_string("Processed: order-1").unwrap()
    );
    assert_eq!(
        completed_outputs[1],
        serde_json::to_string("Processed: order-2").unwrap()
    );
    assert_eq!(
        completed_outputs[2],
        serde_json::to_string("Processed: order-3").unwrap()
    );

    guard.stop().await;
}

// ===========================================================================
// Reconnect / backoff tests
// ===========================================================================

/// The worker should connect and process work even when the sidecar starts
/// *after* the worker does.
///
/// Scenario:
///   1. Pick a free port but do NOT start the sidecar yet.
///   2. Start the worker with a fast retry policy.
///   3. After 150 ms, launch the sidecar.
///   4. Worker should connect and complete an orchestration.
#[tokio::test]
async fn test_worker_connects_after_sidecar_starts_late() {
    if sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = free_port();
    let address = format!("http://127.0.0.1:{}", port);

    let options = WorkerOptions::new().with_reconnect_policy(test_reconnect_policy());
    let mut worker = TaskHubGrpcWorker::with_options(&address, options);
    worker
        .registry_mut()
        .add_named_orchestrator("late_sidecar_orch", |_ctx| async move { Ok(None) });

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let worker_handle = tokio::spawn(async move {
        worker.start(shutdown_clone).await.ok();
    });

    // Give the worker a moment to attempt its first connection.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now launch the sidecar — the worker should pick it up on the next retry.
    let sidecar = SidecarHandle::launch(port).expect("sidecar binary not found");
    assert!(
        sidecar.wait_ready(Duration::from_secs(5)).await,
        "sidecar did not start within 5 s"
    );

    // Allow time for the worker to reconnect (max backoff is 200 ms).
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut client = TaskHubGrpcClient::new(&sidecar.address())
        .await
        .expect("failed to connect client");

    let id = client
        .schedule_new_orchestration("late_sidecar_orch", None, None, None)
        .await
        .expect("failed to schedule orchestration");

    let state = client
        .wait_for_orchestration_completion(&id, false, Some(TIMEOUT))
        .await
        .expect("wait failed")
        .expect("no state");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

    shutdown.cancel();
    worker_handle.await.ok();
}

/// After the sidecar is killed mid-run the worker should detect the dropped
/// stream, retry with backoff, reconnect once the sidecar is restarted on
/// the same port, and successfully complete a new orchestration.
#[tokio::test]
async fn test_worker_reconnects_after_sidecar_restart() {
    if sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = free_port();
    let sidecar = SidecarHandle::launch(port).expect("sidecar binary not found");
    assert!(
        sidecar.wait_ready(Duration::from_secs(5)).await,
        "initial sidecar did not start"
    );

    let address = sidecar.address();

    let options = WorkerOptions::new().with_reconnect_policy(test_reconnect_policy());
    let mut worker = TaskHubGrpcWorker::with_options(&address, options);
    worker
        .registry_mut()
        .add_named_orchestrator("reconnect_orch", |_ctx| async move { Ok(None) });

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let worker_handle = tokio::spawn(async move {
        worker.start(shutdown_clone).await.ok();
    });

    // Let the worker establish its stream.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Kill the sidecar and restart on the same port.
    let sidecar = sidecar.restart().await.expect("sidecar binary not found");
    assert!(
        sidecar.wait_ready(Duration::from_secs(5)).await,
        "restarted sidecar did not come up"
    );

    // Allow the worker time to reconnect (max backoff 200 ms; 600 ms is ample).
    tokio::time::sleep(Duration::from_millis(600)).await;

    let mut client = TaskHubGrpcClient::new(&sidecar.address())
        .await
        .expect("failed to connect client after restart");

    let id = client
        .schedule_new_orchestration("reconnect_orch", None, None, None)
        .await
        .expect("failed to schedule after restart");

    let state = client
        .wait_for_orchestration_completion(&id, false, Some(TIMEOUT))
        .await
        .expect("wait failed")
        .expect("no state");

    assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

    shutdown.cancel();
    worker_handle.await.ok();
}

/// A worker configured with `max_attempts = N` should give up and return an
/// error once N connection attempts have been exhausted.
#[tokio::test]
async fn test_worker_stops_after_max_attempts() {
    if sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    // Nothing is listening on this port.
    let port = free_port();
    let address = format!("http://127.0.0.1:{}", port);

    let policy = ReconnectPolicy::new()
        .with_initial_delay(Duration::from_millis(30))
        .with_max_delay(Duration::from_millis(100))
        .with_multiplier(1.0)
        .with_max_attempts(3)
        .with_jitter(false);

    let options = WorkerOptions::new().with_reconnect_policy(policy);
    let worker = TaskHubGrpcWorker::with_options(&address, options);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let result = tokio::time::timeout(Duration::from_secs(5), worker.start(shutdown))
        .await
        .expect("worker.start did not finish within timeout");

    assert!(
        result.is_err(),
        "expected an error after exhausting max_attempts, got Ok"
    );
}

/// The cancellation token should interrupt the backoff sleep immediately,
/// even when the configured delay is very long.
#[tokio::test]
async fn test_worker_shutdown_interrupts_reconnect_wait() {
    if sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = free_port(); // nothing listening
    let address = format!("http://127.0.0.1:{}", port);

    // 60 s backoff — we expect the shutdown to fire long before this.
    let policy = ReconnectPolicy::new()
        .with_initial_delay(Duration::from_secs(60))
        .with_jitter(false);
    let options = WorkerOptions::new().with_reconnect_policy(policy);
    let worker = TaskHubGrpcWorker::with_options(&address, options);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let handle = tokio::spawn(async move { worker.start(shutdown_clone).await });

    // Cancel after a short pause — well before the 60 s sleep would expire.
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("worker did not exit promptly after cancellation");

    assert!(
        result.unwrap().is_ok(),
        "expected Ok(()) on clean shutdown, not an error"
    );
}

/// Sidecar killed multiple times (double-bounce). The worker should recover
/// from each bounce, with work completing after each restart.
#[tokio::test]
async fn test_worker_survives_multiple_sidecar_restarts() {
    if sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = free_port();
    let mut sidecar = SidecarHandle::launch(port).expect("sidecar binary not found");
    assert!(
        sidecar.wait_ready(Duration::from_secs(5)).await,
        "initial sidecar did not start"
    );

    let address = sidecar.address();

    let options = WorkerOptions::new().with_reconnect_policy(test_reconnect_policy());
    let mut worker = TaskHubGrpcWorker::with_options(&address, options);
    worker
        .registry_mut()
        .add_named_orchestrator("multi_restart_orch", |_ctx| async move { Ok(None) });

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let worker_handle = tokio::spawn(async move {
        worker.start(shutdown_clone).await.ok();
    });

    // Two restart cycles.
    for bounce in 1..=2u32 {
        // Let worker connect.
        tokio::time::sleep(Duration::from_millis(400)).await;

        sidecar = sidecar.restart().await.expect("sidecar binary not found");
        assert!(
            sidecar.wait_ready(Duration::from_secs(5)).await,
            "sidecar bounce {bounce} did not come up"
        );

        // Let the worker reconnect.
        tokio::time::sleep(Duration::from_millis(600)).await;

        let mut client = TaskHubGrpcClient::new(&sidecar.address())
            .await
            .expect("client failed after bounce {bounce}");

        let id = client
            .schedule_new_orchestration("multi_restart_orch", None, None, None)
            .await
            .unwrap_or_else(|e| panic!("schedule failed on bounce {bounce}: {e}"));

        let state = client
            .wait_for_orchestration_completion(&id, false, Some(TIMEOUT))
            .await
            .expect("wait failed")
            .expect("no state");

        assert_eq!(
            state.runtime_status,
            OrchestrationStatus::Completed,
            "bounce {bounce} orchestration did not complete"
        );
    }

    shutdown.cancel();
    worker_handle.await.ok();
}

/// Graceful drain: an activity that takes some time is dispatched, then
/// shutdown is requested. The worker should wait for the activity to finish
/// and report its result before `start()` returns — not abandon the task.
#[tokio::test]
async fn test_worker_drains_in_flight_activity_on_shutdown() {
    setup!(env);

    const ACTIVITY_DELAY_MS: u64 = 1000;

    // Shared flag: the activity sets this to `true` the moment it starts
    // executing. We wait for it before signalling shutdown, so we know the
    // task is definitely inside the worker's JoinSet.
    let activity_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = activity_started.clone();

    let mut worker = env.new_worker();
    worker
        .registry_mut()
        .add_named_orchestrator("drain_orch", |ctx| async move {
            ctx.call_activity("slow_activity", &()).await?;
            Ok(None)
        });
    worker
        .registry_mut()
        .add_named_activity("slow_activity", move |_ctx, _input| {
            let flag = flag.clone();
            async move {
                flag.store(true, std::sync::atomic::Ordering::Release);
                tokio::time::sleep(Duration::from_millis(ACTIVITY_DELAY_MS)).await;
                Ok(None)
            }
        });

    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = worker.start(shutdown_clone).await {
            eprintln!("Worker error: {e}");
        }
    });

    let mut client = env.new_client().await;

    client
        .schedule_new_orchestration("drain_orch", None, None, None)
        .await
        .unwrap();

    // Poll until the activity confirms it has started inside the worker.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if activity_started.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for activity to start"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Cancel while the activity is definitely still sleeping (ACTIVITY_DELAY_MS
    // started just a few ms ago).
    let t_shutdown = tokio::time::Instant::now();
    shutdown.cancel();

    // The worker must drain the in-flight activity before returning.
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("worker did not exit within timeout")
        .ok();

    let elapsed = t_shutdown.elapsed();

    // The drain must have taken a meaningful amount of time (the activity's
    // remaining sleep). With 1000 ms total and some scheduling overhead, we
    // just verify the worker didn't exit instantly — anything >= 200 ms shows
    // it blocked on the in-flight task rather than dropping it.
    assert!(
        elapsed.as_millis() >= 200,
        "worker exited too early ({}ms); expected to drain the in-flight activity",
        elapsed.as_millis()
    );

    // Note: we do NOT check orchestration completion here. The worker has
    // shut down, so no one can process the subsequent orchestration replay
    // that the sidecar would dispatch after receiving the activity result.
    // The timing assertion above is sufficient to prove the drain worked.
}
