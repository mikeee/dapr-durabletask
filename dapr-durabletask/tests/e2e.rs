//! End-to-end tests for the durabletask Rust SDK.
//!
//! Each test spawns its own sidecar process on a dynamically-assigned free
//! port and tears it down when done via `TestEnv`'s `Drop` impl. Tests are
//! fully isolated and run in parallel under both `cargo test` and
//! `cargo nextest run` — no shared global state, no serialisation needed.
//!
//! The sidecar binary is expected at `tmp/durabletask-sidecar` (built by the
//! nix flake shellHook) or at the path in `DURABLETASK_SIDECAR_BIN`.

mod harness;

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use dapr_durabletask::api::OrchestrationStatus;
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{ActivityContext, when_all, when_any};
use dapr_durabletask::worker::{ReconnectPolicy, TaskHubGrpcWorker, WorkerOptions};

use harness::WorkerGuard;

const TIMEOUT: Duration = Duration::from_secs(30);

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
        let bin = harness::sidecar_bin()?;
        let process = Command::new(&bin)
            .args(["--port", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start sidecar '{bin}': {e}"));
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
        harness::kill_and_wait(&mut self.process);
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
        harness::kill_and_wait(&mut self.process);
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
            let input: i32 = ctx.input()?;
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
            let start_val: i32 = ctx.input()?;
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
            let count: i32 = ctx.input()?;
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
            let count: i32 = ctx.input()?;
            for _ in 0..count {
                ctx.call_activity("increment", ()).await?;
            }
            Ok(None)
        });
    worker
        .registry_mut()
        .add_named_orchestrator("parent_fanout_orch", |ctx| async move {
            let count: i32 = ctx.input()?;
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
            let input: i32 = ctx.input()?;
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
            let input: i32 = ctx.input()?;
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
            let order: String = ctx.input().unwrap_or_else(|_| "unknown".into());

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
            eprintln!("[e2e] Sending approval request for: {order}");
            Ok(None)
        },
    );
    worker
        .registry_mut()
        .add_named_activity("hi_process_order", |_ctx, input| async move {
            let order: String = serde_json::from_str(input.as_deref().unwrap_or("\"\""))?;
            eprintln!("[e2e] Processing order: {order}");
            Ok(Some(serde_json::to_string(&format!("Processed: {order}"))?))
        });

    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let orders = ["order-1", "order-2", "order-3"];
    let mut instance_ids: Vec<String> = Vec::with_capacity(3);

    for order in &orders {
        let input = serde_json::to_string(order).unwrap();
        let id = client
            .schedule_new_orchestration("hi_approval_workflow", Some(input), None, None)
            .await
            .unwrap();
        eprintln!("[e2e] Started orchestration for {order} → {id}");
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
    if harness::sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = harness::free_port();
    let address = format!("http://127.0.0.1:{port}");

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

    tokio::time::sleep(Duration::from_millis(150)).await;

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
    if harness::sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = harness::free_port();
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

    tokio::time::sleep(Duration::from_millis(300)).await;

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
    if harness::sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = harness::free_port();
    let address = format!("http://127.0.0.1:{port}");

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
    if harness::sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = harness::free_port(); // nothing listening
    let address = format!("http://127.0.0.1:{port}");

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
    if harness::sidecar_bin().is_none() {
        eprintln!("[e2e] SKIP — sidecar not available");
        return;
    }

    let port = harness::free_port();
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

    for bounce in 1..=2u32 {
        tokio::time::sleep(Duration::from_millis(400)).await;

        sidecar = sidecar.restart().await.expect("sidecar binary not found");
        assert!(
            sidecar.wait_ready(Duration::from_secs(5)).await,
            "sidecar bounce {bounce} did not come up"
        );

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
            ctx.call_activity("slow_activity", ()).await?;
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

/// The process-wide in-memory span exporter behind the global tracer provider.
///
/// The OTel global provider can be installed only once per process, so every
/// module that asserts on spans shares this exporter and filters spans by
/// instance ID (or by a user span name unique to one test).
#[cfg(feature = "opentelemetry")]
fn span_exporter() -> opentelemetry_sdk::trace::InMemorySpanExporter {
    use std::sync::OnceLock;
    static EXPORTER: OnceLock<opentelemetry_sdk::trace::InMemorySpanExporter> = OnceLock::new();
    EXPORTER
        .get_or_init(|| {
            let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            opentelemetry::global::set_tracer_provider(provider);
            exporter
        })
        .clone()
}

mod orchestrations {
    //! In Go these run against an in-process sqlite backend with a Go worker. Here
    //! they run end-to-end: Rust `TaskHubGrpcClient` + `TaskHubGrpcWorker` against
    //! a durabletask-go sidecar (same sqlite backend, spawned per test by
    //! `harness::TestEnv`).
    //!
    //! ## OpenTelemetry assertions
    //!
    //! Go validates spans with `utils.AssertSpanSequence`. Those spans come from
    //! two places, and are ported differently:
    //!
    //! 1. Spans the Rust SDK emits itself (with the `opentelemetry` feature) are
    //!    asserted directly by [`spans::assert_span_sequence`], as an ordered
    //!    sequence (by span end time, i.e. the order Go's in-memory exporter
    //!    receives them) including the `durabletask.*` attributes Go checks:
    //!    - `create_orchestration||<name>` (client),
    //!    - `orchestration||<name>` carrying `durabletask.runtime_status`
    //!      (worker). The Rust worker emits one per execution but only the
    //!      execution that finishes a generation carries a status; that mirrors
    //!      Go's backend, which exports the span only on completion or
    //!      continue-as-new and "cancels" it otherwise,
    //!    - `activity||<name>` with `durabletask.task.task_id` (worker),
    //!    - user spans created inside activities.
    //! 2. Spans, span events and attributes that only the durabletask-go
    //!    *backend* emits (`backend/orchestration.go`). Here the backend runs in
    //!    the sidecar process, whose spans are exported nowhere the test can see:
    //!    - `timer` spans: one per `TimerFired` event (`StartAndEndNewTimerSpan`),
    //!      carrying the timer id as `durabletask.task.task_id` and
    //!      `durabletask.fire_at`,
    //!    - span events "Received external event" (name, size), "Execution
    //!      suspended", "Execution resumed", built by `addNotableEventsToSpan`
    //!      from the full history when the orchestration completes,
    //!    - the `applied_patches` attribute, copied from the `version` the SDK
    //!      reports, which the backend also stores on that turn's
    //!      `WorkflowStarted` event.
    //!
    //!    For these the tests read the sidecar's persisted history (raw
    //!    `GetInstanceHistory` gRPC; `TaskHubGrpcClient` has no history API) and
    //!    assert the exact events the backend derives them from; see [`history`].
    //!    Only the span encoding itself is lost. Each test says which of these it
    //!    substitutes.

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use dapr_durabletask::api::{
        DurableTaskError, ExternalEventResult, OrchestrationState, OrchestrationStatus, RetryPolicy,
    };
    use dapr_durabletask::client::TaskHubGrpcClient;
    use dapr_durabletask::task::{
        ActivityContext, ActivityOptions, DetachedWorkflowOptions, SubOrchestratorOptions,
    };
    use dapr_durabletask::worker::{TaskHubGrpcWorker, WorkerOptions};

    use crate::harness::{self, WorkerGuard};
    use crate::setup;
    use history::{Notable, Step};

    const TIMEOUT: Duration = Duration::from_secs(30);

    // ── Helpers ──────────────────────────────────────────────────────────────────

    fn json<T: serde::Serialize>(v: T) -> Option<String> {
        Some(serde_json::to_string(&v).unwrap())
    }

    fn err(msg: &str) -> DurableTaskError {
        DurableTaskError::Other(msg.to_string())
    }

    async fn wait_done(client: &mut TaskHubGrpcClient, id: &str) -> OrchestrationState {
        wait_done_within(client, id, TIMEOUT).await
    }

    async fn wait_done_within(
        client: &mut TaskHubGrpcClient,
        id: &str,
        timeout: Duration,
    ) -> OrchestrationState {
        client
            .wait_for_orchestration_completion(id, true, Some(timeout))
            .await
            .unwrap_or_else(|e| panic!("wait_for_orchestration_completion({id}) failed: {e}"))
            .unwrap_or_else(|| panic!("no state returned for {id}"))
    }

    async fn fetch(client: &mut TaskHubGrpcClient, id: &str) -> Option<OrchestrationState> {
        client.get_orchestration_state(id, true).await.unwrap()
    }

    fn assert_last_updated_after_created(state: &OrchestrationState, min_delta: Duration) {
        let created = state.created_at.expect("created_at missing");
        let updated = state.last_updated_at.expect("last_updated_at missing");
        assert!(
            updated >= created + chrono::Duration::from_std(min_delta).unwrap(),
            "last_updated_at {updated} should be >= created_at {created} + {min_delta:?}"
        );
    }

    /// One expected entry of Go's `utils.AssertSpanSequence` (SDK-emitted spans
    /// only; see the module docs).
    #[cfg_attr(not(feature = "opentelemetry"), allow(dead_code))]
    enum Expected<'a> {
        /// `utils.AssertWorkflowCreated(name, id)`
        Created(&'a str, &'a str),
        /// `utils.AssertWorkflowExecuted(name, id, status)`
        Executed(&'a str, &'a str, &'a str),
        /// `utils.AssertActivity(name, id, taskID)`
        Activity(&'a str, &'a str, i64),
        /// `utils.AssertSpan(name)` for a user-created span.
        Named(&'a str),
    }

    use Expected as E;

    #[cfg(feature = "opentelemetry")]
    mod spans {
        use opentelemetry::Value;
        use opentelemetry_sdk::trace::SpanData;

        use super::Expected;

        const INSTANCE_ID: &str = "durabletask.task.instance_id";
        const RUNTIME_STATUS: &str = "durabletask.runtime_status";

        /// Install a global tracer provider backed by an in-memory exporter.
        ///
        /// Tests run in parallel in one process, so spans are always filtered by
        /// `durabletask.task.instance_id` (or by a user span name unique to one
        /// test).
        pub fn init_tracing() {
            crate::span_exporter();
        }

        fn attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| &kv.value)
        }

        fn attr_str(span: &SpanData, key: &str) -> Option<String> {
            match attr(span, key) {
                Some(Value::String(s)) => Some(s.as_str().to_string()),
                _ => None,
            }
        }

        fn attr_display(span: &SpanData, key: &str) -> String {
            match attr(span, key) {
                Some(Value::String(s)) => s.as_str().to_string(),
                Some(Value::I64(v)) => v.to_string(),
                Some(other) => format!("{other:?}"),
                None => "<missing>".to_string(),
            }
        }

        /// All finished spans in the order Go's exporter would receive them, i.e.
        /// by end time, with one exception: `create_orchestration||` spans are
        /// placed by their *start* time.
        ///
        /// Go's in-process backend client ends that span as soon as the instance
        /// row is written. The sidecar's gRPC `StartInstance`
        /// (`grpcExecutor.StartInstance`) instead blocks in `WaitForInstanceStart`
        /// until the instance leaves PENDING, so the Rust client's span ends after
        /// the orchestration already ran (often after it completed). Its start
        /// precedes the request that creates the instance, so it is still
        /// causally before every worker span, as the created span is in Go.
        fn finished() -> Vec<SpanData> {
            let mut spans = crate::span_exporter().get_finished_spans().unwrap();
            spans.sort_by_key(|s| {
                if s.name.starts_with("create_orchestration||") {
                    s.start_time
                } else {
                    s.end_time
                }
            });
            spans
        }

        fn render(span: &SpanData) -> String {
            let name = span.name.as_ref();
            let common = format!(
                "type={}, name={}, instance_id={}",
                attr_display(span, "durabletask.type"),
                attr_display(span, "durabletask.task.name"),
                attr_display(span, INSTANCE_ID),
            );
            if name.starts_with("create_orchestration||") {
                format!("{name} [{common}]")
            } else if name.starts_with("orchestration||") {
                format!(
                    "{name} [{common}, status={}]",
                    attr_display(span, RUNTIME_STATUS)
                )
            } else if name.starts_with("activity||") {
                let task_id = attr_display(span, "durabletask.task.task_id");
                format!("{name} [{common}, task_id={task_id}]")
            } else {
                name.to_string()
            }
        }

        fn render_expected(e: &Expected) -> String {
            match *e {
                Expected::Created(name, id) => format!(
                    "create_orchestration||{name} [type=orchestration, name={name}, instance_id={id}]"
                ),
                Expected::Executed(name, id, status) => format!(
                    "orchestration||{name} [type=orchestration, name={name}, instance_id={id}, status={status}]"
                ),
                Expected::Activity(name, id, task_id) => format!(
                    "activity||{name} [type=activity, name={name}, instance_id={id}, task_id={task_id}]"
                ),
                Expected::Named(name) => name.to_string(),
            }
        }

        fn relevant(span: &SpanData, ids: &[&str], names: &[&str]) -> bool {
            let name = span.name.as_ref();
            if names.contains(&name) {
                return true;
            }
            let Some(id) = attr_str(span, INSTANCE_ID) else {
                return false;
            };
            ids.contains(&id.as_str())
                && (name.starts_with("create_orchestration||")
                    || name.starts_with("activity||")
                    || (name.starts_with("orchestration||")
                        && attr(span, RUNTIME_STATUS).is_some()))
        }

        /// Go: `utils.AssertSpanSequence(t, spans, ...)` restricted to the spans
        /// the Rust SDK emits (the instance IDs and user span names are taken
        /// from `expected`). Unlike Go, which only checks a prefix, the filtered
        /// sequence must match exactly.
        pub fn assert_span_sequence(expected: &[Expected]) {
            let ids: Vec<&str> = expected
                .iter()
                .filter_map(|e| match *e {
                    Expected::Created(_, id)
                    | Expected::Executed(_, id, _)
                    | Expected::Activity(_, id, _) => Some(id),
                    Expected::Named(_) => None,
                })
                .collect();
            let names: Vec<&str> = expected
                .iter()
                .filter_map(|e| match *e {
                    Expected::Named(n) => Some(n),
                    _ => None,
                })
                .collect();
            let actual: Vec<String> = finished()
                .iter()
                .filter(|s| relevant(s, &ids, &names))
                .map(render)
                .collect();
            let expected: Vec<String> = expected.iter().map(render_expected).collect();
            assert_eq!(actual, expected, "span sequence mismatch");
        }

        fn only<'a>(
            spans: &'a [SpanData],
            what: &str,
            f: impl Fn(&SpanData) -> bool,
        ) -> &'a SpanData {
            let matching: Vec<&SpanData> = spans.iter().filter(|s| f(s)).collect();
            assert_eq!(matching.len(), 1, "expected exactly one {what} span");
            matching[0]
        }

        fn activity_span<'a>(spans: &'a [SpanData], activity: &str, id: &str) -> &'a SpanData {
            let name = format!("activity||{activity}");
            only(spans, &name, |s| {
                s.name == name && attr_str(s, INSTANCE_ID).as_deref() == Some(id)
            })
        }

        /// Go: `assert.Equal(t, spans[child].Parent().SpanID(), spans[activity].SpanContext().SpanID())`.
        pub fn assert_parent_is_activity(child: &str, activity: &str, id: &str) {
            let spans = finished();
            let child_span = only(&spans, child, |s| s.name == child);
            let parent = activity_span(&spans, activity, id);
            assert_eq!(
                child_span.parent_span_id,
                parent.span_context.span_id(),
                "'{child}' should be a child of the 'activity||{activity}' span"
            );
        }

        /// Task ids of the `activity||<activity>` spans for `id`, in end order.
        pub fn activity_task_ids(id: &str, activity: &str) -> Vec<i64> {
            let name = format!("activity||{activity}");
            finished()
                .iter()
                .filter(|s| s.name == name && attr_str(s, INSTANCE_ID).as_deref() == Some(id))
                .filter_map(|s| match attr(s, "durabletask.task.task_id") {
                    Some(Value::I64(v)) => Some(*v),
                    _ => None,
                })
                .collect()
        }

        /// W3C traceparent of the `activity||<activity>` span for `id`.
        pub fn activity_trace_parent(activity: &str, id: &str) -> String {
            let spans = finished();
            let sc = &activity_span(&spans, activity, id).span_context;
            format!(
                "00-{}-{}-{:02x}",
                sc.trace_id(),
                sc.span_id(),
                sc.trace_flags().to_u8()
            )
        }
    }

    #[cfg(not(feature = "opentelemetry"))]
    mod spans {
        use super::Expected;

        pub fn init_tracing() {}
        pub fn assert_span_sequence(_expected: &[Expected]) {}
        pub fn assert_parent_is_activity(_child: &str, _activity: &str, _id: &str) {}
        pub fn activity_task_ids(_id: &str, _activity: &str) -> Vec<i64> {
            Vec::new()
        }
        pub fn activity_trace_parent(_activity: &str, _id: &str) -> String {
            String::new()
        }
    }

    use spans::{
        activity_task_ids, activity_trace_parent, assert_parent_is_activity, assert_span_sequence,
        init_tracing,
    };

    /// W3C traceparent of the current OTel context, built with the SDK's own
    /// `otel::trace_context_from_span_context` (what it sends as a proto
    /// `TraceContext`). Empty if there is no valid, sampled current span.
    #[cfg(feature = "opentelemetry")]
    fn current_trace_parent() -> String {
        use opentelemetry::trace::TraceContextExt;
        let cx = opentelemetry::Context::current();
        dapr_durabletask::otel::trace_context_from_span_context(cx.span().span_context())
            .map(|tc| tc.trace_parent)
            .unwrap_or_default()
    }

    #[cfg(not(feature = "opentelemetry"))]
    fn current_trace_parent() -> String {
        String::new()
    }

    /// Start and end a span named `name` from `parent` with the test tracer.
    #[cfg(feature = "opentelemetry")]
    fn user_span(name: &'static str, parent: &opentelemetry::Context) {
        use opentelemetry::trace::{Span as _, Tracer as _};
        let tracer = opentelemetry::global::tracer("workflow-test");
        let mut span = tracer.start_with_context(name, parent);
        span.end();
    }

    /// Observations of the sidecar's persisted history, standing in for the
    /// backend-emitted spans (see the module docs).
    mod history {
        use dapr_durabletask_proto as proto;
        use proto::history_event::EventType;
        use proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient;

        pub type Events = Vec<proto::HistoryEvent>;

        pub async fn fetch(address: &str, id: &str) -> Events {
            let mut client = TaskHubSidecarServiceClient::connect(address.to_string())
                .await
                .expect("failed to connect to sidecar for GetInstanceHistory");
            client
                .get_instance_history(proto::GetInstanceHistoryRequest {
                    instance_id: id.to_string(),
                })
                .await
                .unwrap_or_else(|e| panic!("GetInstanceHistory({id}) failed: {e}"))
                .into_inner()
                .events
        }

        /// A step of the backend-side span sequence, in history order.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Step {
            /// Activity result (`TaskCompleted`/`TaskFailed`) for task id.
            Activity(i32),
            /// Child result (`ChildWorkflowInstanceCompleted`/`Failed`) for task id.
            Child(i32),
            /// `TimerFired` for timer id: Go emits one `timer` span per event.
            Timer(i32),
        }

        /// Go's `AssertTimer`: `durabletask.fire_at` parses, is before now and
        /// less than one hour old.
        fn assert_timer_fired(t: &proto::TimerFiredEvent, now: chrono::DateTime<chrono::Utc>) {
            let ts = t.fire_at.as_ref().expect("TimerFired without fire_at");
            let fire_at = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                .expect("invalid TimerFired fire_at");
            assert!(
                fire_at < now && fire_at > now - chrono::Duration::hours(1),
                "timer {} fire_at {fire_at} should be within the past hour (now {now})",
                t.timer_id
            );
        }

        /// Activity/child results and fired timers in history order.
        ///
        /// Go's span sequence interleaves `activity||`/child `orchestration||`
        /// spans (ended when the result is produced) with `timer` spans (one per
        /// `TimerFired`); this is the same interleaving read from the history.
        pub fn timeline(events: &[proto::HistoryEvent]) -> Vec<Step> {
            let now = chrono::Utc::now();
            events
                .iter()
                .filter_map(|e| match e.event_type.as_ref()? {
                    EventType::TaskCompleted(t) => Some(Step::Activity(t.task_scheduled_id)),
                    EventType::TaskFailed(t) => Some(Step::Activity(t.task_scheduled_id)),
                    EventType::ChildWorkflowInstanceCompleted(c) => {
                        Some(Step::Child(c.task_scheduled_id))
                    }
                    EventType::ChildWorkflowInstanceFailed(c) => {
                        Some(Step::Child(c.task_scheduled_id))
                    }
                    EventType::TimerFired(t) => {
                        assert_timer_fired(t, now);
                        Some(Step::Timer(t.timer_id))
                    }
                    _ => None,
                })
                .collect()
        }

        /// Go's `addNotableEventsToSpan` output, one entry per span event.
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum Notable {
            /// "Received external event" with `name` and `size` attributes.
            External(String, usize),
            /// "Execution suspended"
            Suspended,
            /// "Execution resumed"
            Resumed,
            /// "Execution stalled"
            Stalled,
        }

        pub fn external(name: &str, size: usize) -> Notable {
            Notable::External(name.to_string(), size)
        }

        /// The span events Go's backend attaches to the completed orchestration
        /// span: `addNotableEventsToSpan(OldEvents)` + `(NewEvents)`, i.e. the
        /// whole history in order.
        pub fn notable_events(events: &[proto::HistoryEvent]) -> Vec<Notable> {
            events
                .iter()
                .filter_map(|e| match e.event_type.as_ref()? {
                    EventType::EventRaised(r) => Some(Notable::External(
                        r.name.clone(),
                        r.input.as_deref().map_or(0, str::len),
                    )),
                    EventType::ExecutionSuspended(_) => Some(Notable::Suspended),
                    EventType::ExecutionResumed(_) => Some(Notable::Resumed),
                    EventType::ExecutionStalled(_) => Some(Notable::Stalled),
                    _ => None,
                })
                .collect()
        }

        /// Patches recorded on the last `WorkflowStarted` event: exactly what Go's
        /// backend copies into `applied_patches` on the exported (final-turn)
        /// orchestration span.
        pub fn last_applied_patches(events: &[proto::HistoryEvent]) -> Option<Vec<String>> {
            events
                .iter()
                .rev()
                .find_map(|e| match e.event_type.as_ref()? {
                    EventType::WorkflowStarted(ws) => Some(
                        ws.version
                            .as_ref()
                            .map(|v| v.patches.clone())
                            .unwrap_or_default(),
                    ),
                    _ => None,
                })
        }

        /// Ids of `TimerCreated` events, in creation order.
        pub fn created_timer_ids(events: &[proto::HistoryEvent]) -> Vec<i32> {
            events
                .iter()
                .filter(|e| matches!(e.event_type, Some(EventType::TimerCreated(_))))
                .map(|e| e.event_id)
                .collect()
        }

        /// History index of the `TimerFired` event for `timer_id`.
        pub fn timer_fired_index(events: &[proto::HistoryEvent], timer_id: i32) -> Option<usize> {
            events.iter().position(
            |e| matches!(&e.event_type, Some(EventType::TimerFired(t)) if t.timer_id == timer_id),
        )
        }

        /// History indices of `EventRaised` events named `name`.
        pub fn event_raised_indices(events: &[proto::HistoryEvent], name: &str) -> Vec<usize> {
            events
            .iter()
            .enumerate()
            .filter(
                |(_, e)| matches!(&e.event_type, Some(EventType::EventRaised(r)) if r.name == name),
            )
            .map(|(i, _)| i)
            .collect()
        }
    }

    fn register_single_activity(worker: &mut TaskHubGrpcWorker, orch_name: &str) {
        worker
            .registry_mut()
            .add_named_orchestrator(orch_name, |ctx| async move {
                let input: String = ctx.input()?;
                let output = ctx.call_activity("SayHello", input).await?;
                Ok(output)
            });
        worker.registry_mut().add_named_activity(
            "SayHello",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let name: String = serde_json::from_str(input.as_deref().unwrap_or("null"))?;
                Ok(json(format!("Hello, {name}!")))
            },
        );
    }

    fn register_say_hello_plain(worker: &mut TaskHubGrpcWorker) {
        worker.registry_mut().add_named_activity(
            "SayHello",
            |_ctx: ActivityContext, _input: Option<String>| async move { Ok(json("Hello")) },
        );
    }

    // ── tests/orchestrations_test.go ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_empty_workflow() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("EmptyWorkflow", |_ctx| async move { Ok(None) });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("EmptyWorkflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

        assert_span_sequence(&[
            E::Created("EmptyWorkflow", &id),
            E::Executed("EmptyWorkflow", &id, "COMPLETED"),
        ]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_single_timer() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("SingleTimer", |ctx| async move {
                ctx.create_timer(Duration::ZERO).await?;
                Ok(None)
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("SingleTimer", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_last_updated_after_created(&state, Duration::ZERO);

        // Go: created, timer, executed. The backend `timer` span is replaced by
        // its source event: one TimerFired (fire_at within the past hour).
        assert_span_sequence(&[
            E::Created("SingleTimer", &id),
            E::Executed("SingleTimer", &id, "COMPLETED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        assert_eq!(history::timeline(&events), [Step::Timer(0)]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_concurrent_timers() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("TimerFanOut", |ctx| async move {
                let tasks: Vec<_> = (0..3)
                    .map(|_| ctx.create_timer(Duration::from_secs(1)))
                    .collect();
                for t in tasks {
                    t.await?;
                }
                Ok(None)
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("TimerFanOut", None, None, None)
            .await
            .unwrap();
        // Go bounds the whole test by a 5s context.
        let state = wait_done_within(&mut client, &id, Duration::from_secs(5)).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_last_updated_after_created(&state, Duration::ZERO);

        // Go: created, timer x3 (no ids asserted), executed. The backend `timer`
        // spans are replaced by three TimerFired events (one per timer).
        assert_span_sequence(&[
            E::Created("TimerFanOut", &id),
            E::Executed("TimerFanOut", &id, "COMPLETED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        let mut timers = history::timeline(&events);
        timers.sort_by_key(|s| match s {
            Step::Timer(id) => *id,
            other => panic!("unexpected step {other:?}"),
        });
        assert_eq!(timers, [Step::Timer(0), Step::Timer(1), Step::Timer(2)]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_is_replaying() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("IsReplayingOrch", |ctx| async move {
                let mut values = vec![ctx.is_replaying()];
                let _ = ctx.create_timer(Duration::ZERO).await;
                values.push(ctx.is_replaying());
                let _ = ctx.create_timer(Duration::ZERO).await;
                values.push(ctx.is_replaying());
                Ok(json(values))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("IsReplayingOrch", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some("[true,true,false]")
        );

        // Go: created, timer, timer, executed (timers via history, see docs).
        assert_span_sequence(&[
            E::Created("IsReplayingOrch", &id),
            E::Executed("IsReplayingOrch", &id, "COMPLETED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        assert_eq!(history::timeline(&events), [Step::Timer(0), Step::Timer(1)]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_single_activity() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        register_single_activity(&mut worker, "SingleActivity");
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("SingleActivity", json("世界"), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );

        assert_span_sequence(&[
            E::Created("SingleActivity", &id),
            E::Activity("SayHello", &id, 0),
            E::Executed("SingleActivity", &id, "COMPLETED"),
        ]);
        guard.stop().await;
    }

    #[cfg_attr(
        not(feature = "opentelemetry"),
        ignore = "requires the opentelemetry feature (asserts span parentage)"
    )]
    #[tokio::test]
    async fn test_single_activity_task_span() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("SingleActivity", |ctx| async move {
                let input: String = ctx.input()?;
                ctx.call_activity("SayHello", input).await
            });
        worker.registry_mut().add_named_activity(
            "SayHello",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let name: String = serde_json::from_str(input.as_deref().unwrap_or("null"))?;
                // Go: `tracer.Start(ctx.Context(), "activityChild")`. The Rust
                // counterpart of Go's ActivityContext.Context() is the current OTel
                // context, which the worker sets to the activity span while the
                // activity runs.
                #[cfg(feature = "opentelemetry")]
                user_span("activityChild", &opentelemetry::Context::current());
                Ok(json(format!("Hello, {name}!")))
            },
        );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("SingleActivity", json("世界"), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );

        assert_span_sequence(&[
            E::Created("SingleActivity", &id),
            E::Named("activityChild"),
            E::Activity("SayHello", &id, 0),
            E::Executed("SingleActivity", &id, "COMPLETED"),
        ]);
        // Go: assert.Equal(t, spans[1].Parent().SpanID(), spans[2].SpanContext().SpanID())
        assert_parent_is_activity("activityChild", "SayHello", &id);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_activity_chain() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ActivityChain", |ctx| async move {
                let mut val = 0i32;
                for _ in 0..10 {
                    let out = ctx.call_activity("PlusOne", val).await?;
                    val = serde_json::from_str(out.as_deref().unwrap_or("null"))?;
                }
                Ok(json(val))
            });
        worker.registry_mut().add_named_activity(
            "PlusOne",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let v: i32 = serde_json::from_str(input.as_deref().unwrap_or("null"))?;
                Ok(json(v + 1))
            },
        );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ActivityChain", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output.as_deref(), Some("10"));

        let mut expected = vec![E::Created("ActivityChain", &id)];
        expected.extend((0..10).map(|task_id| E::Activity("PlusOne", &id, task_id)));
        expected.push(E::Executed("ActivityChain", &id, "COMPLETED"));
        assert_span_sequence(&expected);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_activity_retries() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ActivityRetries", |ctx| async move {
                ctx.call_activity_with_options(
                    "FailActivity",
                    (),
                    ActivityOptions::new()
                        .with_retry_policy(RetryPolicy::new(3, Duration::from_millis(10))),
                )
                .await?;
                Ok(None)
            });
        worker.registry_mut().add_named_activity(
        "FailActivity",
        |_ctx: ActivityContext, _input: Option<String>| async move { Err(err("activity failure")) },
    );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ActivityRetries", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Failed);
        // With 3 max attempts there will be two retries with 10 millis delay before each
        assert_last_updated_after_created(&state, Duration::from_millis(2 * 10));

        // Go: created, activity(0), timer(1), activity(2), timer(3), activity(4),
        // executed(FAILED). SDK spans are asserted in order; the interleaved
        // backend `timer` spans are checked via the history timeline.
        assert_span_sequence(&[
            E::Created("ActivityRetries", &id),
            E::Activity("FailActivity", &id, 0),
            E::Activity("FailActivity", &id, 2),
            E::Activity("FailActivity", &id, 4),
            E::Executed("ActivityRetries", &id, "FAILED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        assert_eq!(
            history::timeline(&events),
            [
                Step::Activity(0),
                Step::Timer(1),
                Step::Activity(2),
                Step::Timer(3),
                Step::Activity(4),
            ]
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_activity_fan_out() {
        init_tracing();
        setup!(env);
        let mut worker = TaskHubGrpcWorker::with_options(
            &env.address,
            WorkerOptions::new().with_max_concurrent_work_items(10),
        );
        worker
            .registry_mut()
            .add_named_orchestrator("ActivityFanOut", |ctx| async move {
                let tasks: Vec<_> = (0..10).map(|i| ctx.call_activity("ToString", i)).collect();
                let mut results: Vec<String> = Vec::new();
                for t in tasks {
                    let out = t.await?;
                    results.push(serde_json::from_str(out.as_deref().unwrap_or("null"))?);
                }
                results.sort();
                results.reverse();
                Ok(json(results))
            });
        worker.registry_mut().add_named_activity(
            "ToString",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let v: i32 = serde_json::from_str(input.as_deref().unwrap_or("null"))?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(json(format!("{v}")))
            },
        );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ActivityFanOut", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#"["9","8","7","6","5","4","3","2","1","0"]"#)
        );
        // Because all the activities run in parallel, they should complete very quickly
        let elapsed = state.last_updated_at.unwrap() - state.created_at.unwrap();
        assert!(
            elapsed < chrono::Duration::seconds(3),
            "fan-out took {elapsed}, expected < 3s"
        );

        // Go asserts only the first span (created) because the order of the
        // activity spans is non-deterministic (see its TODO). Here: created first,
        // then one span per activity (task ids 0..10, in whatever order they
        // ended), then executed(COMPLETED).
        let order = activity_task_ids(&id, "ToString");
        #[cfg(feature = "opentelemetry")]
        {
            let mut sorted = order.clone();
            sorted.sort_unstable();
            assert_eq!(
                sorted,
                (0..10).collect::<Vec<i64>>(),
                "one span per activity"
            );
        }
        let mut expected = vec![E::Created("ActivityFanOut", &id)];
        expected.extend(order.iter().map(|&t| E::Activity("ToString", &id, t)));
        expected.push(E::Executed("ActivityFanOut", &id, "COMPLETED"));
        assert_span_sequence(&expected);
        guard.stop().await;
    }

    fn register_parent_child_failed(worker: &mut TaskHubGrpcWorker) {
        worker
            .registry_mut()
            .add_named_orchestrator("Child", |_ctx| async move { Err(err("Child failed")) });
    }

    #[tokio::test]
    async fn test_single_child_workflow_completed() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Parent", |ctx| async move {
                let input: serde_json::Value = ctx.input()?;
                let child_id = format!("{}_child", ctx.instance_id());
                ctx.call_sub_orchestrator("Child", input, Some(&child_id))
                    .await
            });
        worker
            .registry_mut()
            .add_named_orchestrator("Child", |ctx| async move {
                let input: String = ctx.input()?;
                Ok(json(input))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Parent", json("Hello, world!"), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, world!""#)
        );

        let child_id = format!("{id}_child");
        assert_span_sequence(&[
            E::Created("Parent", &id),
            E::Executed("Child", &child_id, "COMPLETED"),
            E::Executed("Parent", &id, "COMPLETED"),
        ]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_single_child_workflow_failed() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Parent", |ctx| async move {
                let child_id = format!("{}_child", ctx.instance_id());
                ctx.call_sub_orchestrator("Child", (), Some(&child_id))
                    .await?;
                Ok(None)
            });
        register_parent_child_failed(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Parent", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Failed);
        let fd = state.failure_details.expect("failure details missing");
        assert!(
            fd.message.contains("Child failed"),
            "failure message {:?} should contain 'Child failed'",
            fd.message
        );

        let child_id = format!("{id}_child");
        assert_span_sequence(&[
            E::Created("Parent", &id),
            E::Executed("Child", &child_id, "FAILED"),
            E::Executed("Parent", &id, "FAILED"),
        ]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_single_child_workflow_failed_retries() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Parent", |ctx| async move {
                let child_id = format!("{}_child", ctx.instance_id());
                ctx.call_sub_orchestrator_with_options(
                    "Child",
                    (),
                    SubOrchestratorOptions::new()
                        .with_instance_id(child_id)
                        .with_retry_policy(
                            RetryPolicy::new(3, Duration::from_millis(10))
                                .with_backoff_coefficient(2.0),
                        ),
                )
                .await?;
                Ok(None)
            });
        register_parent_child_failed(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Parent", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Failed);
        let fd = state.failure_details.expect("failure details missing");
        assert!(fd.message.contains("Child failed"), "got {:?}", fd.message);

        // Go: created, child(FAILED), timer(1), child(FAILED), timer(3),
        // child(FAILED), parent(FAILED). The backend `timer` spans are checked via
        // the parent's history timeline.
        let child_id = format!("{id}_child");
        assert_span_sequence(&[
            E::Created("Parent", &id),
            E::Executed("Child", &child_id, "FAILED"),
            E::Executed("Child", &child_id, "FAILED"),
            E::Executed("Child", &child_id, "FAILED"),
            E::Executed("Parent", &id, "FAILED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        assert_eq!(
            history::timeline(&events),
            [
                Step::Child(0),
                Step::Timer(1),
                Step::Child(2),
                Step::Timer(3),
                Step::Child(4),
            ]
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_single_child_workflow_failed_retries_auto_instance_id() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Parent", |ctx| async move {
                // No explicit instance ID — each retry gets a different
                // auto-generated instance ID.
                ctx.call_sub_orchestrator_with_options(
                    "Child",
                    (),
                    SubOrchestratorOptions::new().with_retry_policy(
                        RetryPolicy::new(3, Duration::from_millis(10))
                            .with_backoff_coefficient(2.0),
                    ),
                )
                .await?;
                Ok(None)
            });
        register_parent_child_failed(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Parent", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Failed);
        let fd = state.failure_details.expect("failure details missing");
        assert!(fd.message.contains("Child failed"), "got {:?}", fd.message);

        // Each retry gets a different, deterministic auto-generated instance ID
        // "<parent>:<actionID as %04x>" (action IDs 0, 2, 4). Also checked
        // directly on the child instances.
        let child_id = |action_id: u32| format!("{id}:{action_id:04x}");
        for action_id in [0, 2, 4] {
            let cid = child_id(action_id);
            let child = fetch(&mut client, &cid)
                .await
                .unwrap_or_else(|| panic!("expected child instance {cid} to exist"));
            assert_eq!(child.name, "Child");
            assert_eq!(child.runtime_status, OrchestrationStatus::Failed);
        }

        // Go: created, child0(FAILED), timer(1), child2(FAILED), timer(3),
        // child4(FAILED), parent(FAILED); timers via the parent's history.
        let (c0, c2, c4) = (child_id(0), child_id(2), child_id(4));
        assert_span_sequence(&[
            E::Created("Parent", &id),
            E::Executed("Child", &c0, "FAILED"),
            E::Executed("Child", &c2, "FAILED"),
            E::Executed("Child", &c4, "FAILED"),
            E::Executed("Parent", &id, "FAILED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        assert_eq!(
            history::timeline(&events),
            [
                Step::Child(0),
                Step::Timer(1),
                Step::Child(2),
                Step::Timer(3),
                Step::Child(4),
            ]
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_detached_workflow_happy_path() {
        // Go: "Caller" calls ctx.ScheduleNewDetachedWorkflow("Spawned",
        //   WithDetachedWorkflowInstanceID(id+"_spawned"), WithDetachedWorkflowInput("payload"))
        // and returns the spawned ID. "Spawned" returns "spawned-saw:"+input.
        // Asserts: caller COMPLETED; spawned instance (id+"_spawned") COMPLETED
        // with output containing "spawned-saw:".
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Caller", |ctx| async move {
                let spawned = ctx.schedule_new_detached_workflow(
                    "Spawned",
                    "payload",
                    DetachedWorkflowOptions::new()
                        .with_instance_id(format!("{}_spawned", ctx.instance_id())),
                )?;
                Ok(json(spawned))
            });
        worker
            .registry_mut()
            .add_named_orchestrator("Spawned", |ctx| async move {
                let input: String = ctx.input()?;
                Ok(json(format!("spawned-saw:{input}")))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Caller", None, None, None)
            .await
            .unwrap();
        let caller = wait_done(&mut client, &id).await;
        assert_eq!(caller.runtime_status, OrchestrationStatus::Completed);

        let spawned_id = format!("{id}_spawned");
        let spawned = wait_done(&mut client, &spawned_id).await;
        assert_eq!(spawned.runtime_status, OrchestrationStatus::Completed);
        let output = spawned.serialized_output.unwrap_or_default();
        assert!(output.contains("spawned-saw:"), "got {output:?}");
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_detached_workflow_default_instance_id() {
        // Go: "Caller" calls ScheduleNewDetachedWorkflow("Spawned") with no ID and
        // returns the spawned ID; "Spawned" returns "ok".
        // Asserts: caller COMPLETED, output contains id+"-0"; instance id+"-0"
        // exists and is COMPLETED.
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Caller", |ctx| async move {
                let spawned = ctx.schedule_new_detached_workflow(
                    "Spawned",
                    (),
                    DetachedWorkflowOptions::new(),
                )?;
                Ok(json(spawned))
            });
        worker
            .registry_mut()
            .add_named_orchestrator("Spawned", |_ctx| async move { Ok(json("ok")) });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Caller", None, None, None)
            .await
            .unwrap();
        let caller = wait_done(&mut client, &id).await;
        assert_eq!(caller.runtime_status, OrchestrationStatus::Completed);
        let expected_spawned_id = format!("{id}-0");
        let output = caller.serialized_output.unwrap_or_default();
        assert!(output.contains(&expected_spawned_id), "got {output:?}");

        let spawned = wait_done(&mut client, &expected_spawned_id).await;
        assert_eq!(spawned.runtime_status, OrchestrationStatus::Completed);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_detached_workflow_no_completion_flows_back() {
        // Go: "Caller" spawns detached "FailingSpawned" (id+"_spawned") and returns
        // "caller-done"; "FailingSpawned" returns an error. Asserts: caller
        // COMPLETED with nil FailureDetails; the spawned instance ends FAILED.
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Caller", |ctx| async move {
                ctx.schedule_new_detached_workflow(
                    "FailingSpawned",
                    (),
                    DetachedWorkflowOptions::new()
                        .with_instance_id(format!("{}_spawned", ctx.instance_id())),
                )?;
                Ok(json("caller-done"))
            });
        worker
            .registry_mut()
            .add_named_orchestrator(
                "FailingSpawned",
                |_ctx| async move { Err(err("spawned boom")) },
            );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Caller", None, None, None)
            .await
            .unwrap();
        let caller = wait_done(&mut client, &id).await;
        assert_eq!(caller.runtime_status, OrchestrationStatus::Completed);
        assert!(
            caller.failure_details.is_none(),
            "a detached child's failure must not flow back: {:?}",
            caller.failure_details
        );

        let spawned = wait_done(&mut client, &format!("{id}_spawned")).await;
        assert_eq!(spawned.runtime_status, OrchestrationStatus::Failed);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_detached_workflow_replay_determinism() {
        // Go: "Caller" spawns detached "Spawned" (id+"_spawned") then waits for
        // event "Continue" (30s timeout) and returns the spawned ID. The test
        // waits for the spawned instance to complete, raises "Continue" (forcing
        // a replay of the spawn call), and asserts the caller COMPLETED.
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Caller", |ctx| async move {
                let spawned = ctx.schedule_new_detached_workflow(
                    "Spawned",
                    (),
                    DetachedWorkflowOptions::new()
                        .with_instance_id(format!("{}_spawned", ctx.instance_id())),
                )?;
                match ctx
                    .wait_for_external_event_with_timeout("Continue", Duration::from_secs(30))
                    .await?
                {
                    ExternalEventResult::Received(_) => Ok(json(spawned)),
                    ExternalEventResult::TimedOut => Err(err("timed out waiting for Continue")),
                }
            });
        worker
            .registry_mut()
            .add_named_orchestrator("Spawned", |_ctx| async move { Ok(json("ok")) });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Caller", None, None, None)
            .await
            .unwrap();
        let spawned_id = format!("{id}_spawned");
        let spawned = wait_done(&mut client, &spawned_id).await;
        assert_eq!(spawned.runtime_status, OrchestrationStatus::Completed);

        client
            .raise_orchestration_event(&id, "Continue", None)
            .await
            .unwrap();
        let caller = wait_done(&mut client, &id).await;
        assert_eq!(
            caller.runtime_status,
            OrchestrationStatus::Completed,
            "{:?}",
            caller.failure_details
        );
        assert!(
            caller
                .serialized_output
                .unwrap_or_default()
                .contains(&spawned_id)
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_continue_as_new() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ContinueAsNewTest", |ctx| async move {
                let input: i32 = ctx.input()?;
                if input < 10 {
                    ctx.create_timer(Duration::ZERO).await?;
                    ctx.continue_as_new(input + 1, false);
                }
                Ok(json(input))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ContinueAsNewTest", json(0), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output.as_deref(), Some("10"));

        // Go: created, then (timer, executed(CONTINUED_AS_NEW)) x10, then
        // executed(COMPLETED). Lost: the ten per-generation backend `timer` spans;
        // continue-as-new resets the history, so the sidecar keeps only the last
        // generation (which has no timer). Nearest observable: each generation
        // reaches CONTINUED_AS_NEW only after its timer fired.
        let mut expected = vec![E::Created("ContinueAsNewTest", &id)];
        expected.extend((0..10).map(|_| E::Executed("ContinueAsNewTest", &id, "CONTINUED_AS_NEW")));
        expected.push(E::Executed("ContinueAsNewTest", &id, "COMPLETED"));
        assert_span_sequence(&expected);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_continue_as_new_events() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ContinueAsNewTest", |ctx| async move {
                let input: i32 = ctx.input()?;
                let raw = ctx.wait_for_external_event("MyEvent").await?;
                let complete: bool = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
                if complete {
                    return Ok(json(input));
                }
                ctx.continue_as_new(input + 1, true);
                Ok(None)
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ContinueAsNewTest", json(0), None, None)
            .await
            .unwrap();
        for _ in 0..10 {
            client
                .raise_orchestration_event(&id, "MyEvent", json(false))
                .await
                .unwrap();
        }
        client
            .raise_orchestration_event(&id, "MyEvent", json(true))
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output.as_deref(), Some("10"));
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_external_event_contention() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ContinueAsNewTest", |ctx| async move {
                // Go ignores ErrTaskCanceled (timeout) here and keeps data == 0.
                let mut data = 0i32;
                if let ExternalEventResult::Received(raw) = ctx
                    .wait_for_external_event_with_timeout("MyEventData", Duration::from_secs(1))
                    .await?
                {
                    data = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
                }

                let raw = ctx.wait_for_external_event("MyEventSignal").await?;
                let complete: bool = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
                if complete {
                    return Ok(json(data));
                }
                ctx.continue_as_new((), true);
                Ok(None)
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ContinueAsNewTest", None, None, None)
            .await
            .unwrap();

        // Wait for the timer to elapse
        let res = client
            .wait_for_orchestration_completion(&id, true, Some(Duration::from_secs(3)))
            .await;
        assert!(
            matches!(res, Err(DurableTaskError::Timeout)),
            "expected timeout, got {res:?}"
        );

        // Now raise the event, which should queue correctly for the next time around
        client
            .raise_orchestration_event(&id, "MyEventData", json(42))
            .await
            .unwrap();
        client
            .raise_orchestration_event(&id, "MyEventSignal", json(false))
            .await
            .unwrap();
        client
            .raise_orchestration_event(&id, "MyEventSignal", json(true))
            .await
            .unwrap();

        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output.as_deref(), Some("42"));
        guard.stop().await;
    }

    /// Go's `WaitForSingleEvent("MyEvent", 5s).Await(&value)` with the error
    /// ignored: a timeout leaves `value` at 0.
    fn register_ten_event_workflow(worker: &mut TaskHubGrpcWorker, name: &str) {
        const EVENT_COUNT: i32 = 10;
        worker
            .registry_mut()
            .add_named_orchestrator(name, |ctx| async move {
                for i in 0..EVENT_COUNT {
                    let mut value = 0i32;
                    if let Ok(ExternalEventResult::Received(Some(raw))) = ctx
                        .wait_for_external_event_with_timeout("MyEvent", Duration::from_secs(5))
                        .await
                    {
                        value = serde_json::from_str(&raw).unwrap_or(0);
                    }
                    if value != i {
                        return Err(err("Unexpected value"));
                    }
                }
                Ok(json(true))
            });
    }

    #[tokio::test]
    async fn test_external_event_workflow() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        register_ten_event_workflow(&mut worker, "ExternalEventWorkflow");
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ExternalEventWorkflow", json(0), None, None)
            .await
            .unwrap();
        for i in 0..10 {
            client
                .raise_orchestration_event(&id, "MyEvent", json(i))
                .await
                .unwrap();
        }
        let state = wait_done_within(&mut client, &id, Duration::from_secs(5)).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

        // Go: created, executed(COMPLETED) carrying ten "Received external event"
        // span events (name "MyEvent", size 1), and no `timer` span in between.
        // The backend span events are checked on their source history events.
        assert_span_sequence(&[
            E::Created("ExternalEventWorkflow", &id),
            E::Executed("ExternalEventWorkflow", &id, "COMPLETED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        assert_eq!(
            history::notable_events(&events),
            vec![history::external("MyEvent", 1); 10]
        );
        assert_eq!(history::timeline(&events), []);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_external_event_timeout() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker.registry_mut().add_named_orchestrator(
            "ExternalEventWorkflowWithTimeout",
            |ctx| async move {
                match ctx
                    .wait_for_external_event_with_timeout("MyEvent", Duration::from_secs(2))
                    .await?
                {
                    ExternalEventResult::Received(_) => Ok(None),
                    // Go's Await returns task.ErrTaskCanceled ("the task was
                    // canceled"), which the orchestrator returns as-is. The Rust
                    // API reports a timeout as a value, not an error, so there is
                    // no SDK error text to return; the orchestrator surfaces
                    // Go's text itself. What the assertion below still checks is
                    // that the timeout branch is taken and that the orchestrator
                    // error reaches FailureDetails verbatim.
                    ExternalEventResult::TimedOut => Err(err("the task was canceled")),
                }
            },
        );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        // Run two variations, one where we raise the external event and one where we don't (timeout)
        for raise_event in [true, false] {
            let id = client
                .schedule_new_orchestration("ExternalEventWorkflowWithTimeout", None, None, None)
                .await
                .unwrap();
            if raise_event {
                client
                    .raise_orchestration_event(&id, "MyEvent", None)
                    .await
                    .unwrap();
            }
            let state = wait_done_within(&mut client, &id, Duration::from_secs(5)).await;
            assert!(
                state.runtime_status.is_terminal(),
                "raise_event={raise_event}"
            );
            let events = history::fetch(&env.address, &id).await;
            if raise_event {
                assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
                // Go: created, executed(COMPLETED) with exactly one "Received
                // external event" span event (name "MyEvent", size 0).
                assert_span_sequence(&[
                    E::Created("ExternalEventWorkflowWithTimeout", &id),
                    E::Executed("ExternalEventWorkflowWithTimeout", &id, "COMPLETED"),
                ]);
                assert_eq!(
                    history::notable_events(&events),
                    [history::external("MyEvent", 0)]
                );
                assert_eq!(history::timeline(&events), []);
            } else {
                assert_eq!(state.runtime_status, OrchestrationStatus::Failed);
                let fd = state.failure_details.expect("failure details missing");
                assert_eq!(fd.message, "the task was canceled");
                // Go: created, timer (the event timeout), executed(FAILED) with no
                // span events.
                assert_span_sequence(&[
                    E::Created("ExternalEventWorkflowWithTimeout", &id),
                    E::Executed("ExternalEventWorkflowWithTimeout", &id, "FAILED"),
                ]);
                assert_eq!(history::timeline(&events), [Step::Timer(0)]);
                assert_eq!(history::notable_events(&events), []);
            }
        }
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_suspend_resume_workflow() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        register_ten_event_workflow(&mut worker, "SuspendResumeWorkflow");
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        // Run the workflow, which will block waiting for external events
        let id = client
            .schedule_new_orchestration("SuspendResumeWorkflow", json(0), None, None)
            .await
            .unwrap();
        // Wait for the workflow to finish starting
        client
            .wait_for_orchestration_start(&id, false, Some(TIMEOUT))
            .await
            .unwrap();

        client.suspend_orchestration(&id, None).await.unwrap();

        // Raise a bunch of events to the workflow (they should get buffered but not consumed)
        for i in 0..10 {
            client
                .raise_orchestration_event(&id, "MyEvent", json(i))
                .await
                .unwrap();
        }

        // Make sure the workflow *doesn't* complete
        let res = client
            .wait_for_orchestration_completion(&id, true, Some(Duration::from_secs(3)))
            .await;
        assert!(
            matches!(res, Err(DurableTaskError::Timeout)),
            "expected timeout, got {res:?}"
        );

        let state = fetch(&mut client, &id).await.expect("state missing");
        assert!(!state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Suspended);

        // Resume the workflow and wait for it to complete
        client.resume_orchestration(&id, None).await.unwrap();
        let state = wait_done_within(&mut client, &id, Duration::from_secs(3)).await;
        assert!(state.runtime_status.is_terminal());
        // Go checks COMPLETED through the executed span's status.
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

        // Go: created, executed(COMPLETED) with span events: suspended, ten
        // "Received external event" (MyEvent, size 1), resumed. Checked on the
        // history events the backend builds them from.
        assert_span_sequence(&[
            E::Created("SuspendResumeWorkflow", &id),
            E::Executed("SuspendResumeWorkflow", &id, "COMPLETED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        let mut expected = vec![Notable::Suspended];
        expected.extend(vec![history::external("MyEvent", 1); 10]);
        expected.push(Notable::Resumed);
        assert_eq!(history::notable_events(&events), expected);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_terminate_workflow() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("MyWorkflow", |ctx| async move {
                let _ = ctx.create_timer(Duration::from_secs(3)).await;
                Ok(None)
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("MyWorkflow", None, None, None)
            .await
            .unwrap();
        // Terminate the workflow before the timer expires
        client
            .terminate_orchestration(&id, json("You got terminated!"), false)
            .await
            .unwrap();

        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Terminated);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""You got terminated!""#)
        );

        assert_span_sequence(&[
            E::Created("MyWorkflow", &id),
            E::Executed("MyWorkflow", &id, "TERMINATED"),
        ]);
        guard.stop().await;
    }

    /// Poll until `check` returns true, or panic after `within`.
    async fn eventually<F, Fut>(within: Duration, what: &str, mut check: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if check().await {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("condition not met within {within:?}: {what}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn all_have_status(address: &str, expected: &[(String, OrchestrationStatus)]) -> bool {
        let mut client = TaskHubGrpcClient::new(address).await.unwrap();
        for (id, status) in expected {
            match fetch(&mut client, id).await {
                Some(s) if s.runtime_status == *status => {}
                _ => return false,
            }
        }
        true
    }

    #[tokio::test]
    async fn test_terminate_workflow_recursive() {
        const DELAY: Duration = Duration::from_secs(4);
        let executed_activity = Arc::new(AtomicBool::new(false));

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Root", |ctx| async move {
                let tasks: Vec<_> = (0..5)
                    .map(|i| {
                        let cid = format!("{}_L1_{i}", ctx.instance_id());
                        ctx.call_sub_orchestrator("L1", (), Some(&cid))
                    })
                    .collect();
                for t in tasks {
                    let _ = t.await;
                }
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L1", |ctx| async move {
                let cid = format!("{}_L2", ctx.instance_id());
                let _ = ctx.call_sub_orchestrator("L2", (), Some(&cid)).await;
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L2", |ctx| async move {
                let _ = ctx.create_timer(DELAY).await;
                let _ = ctx.call_activity("Fail", ()).await;
                Ok(None)
            });
        {
            let executed_activity = executed_activity.clone();
            worker.registry_mut().add_named_activity(
                "Fail",
                move |_ctx: ActivityContext, _input: Option<String>| {
                    let executed_activity = executed_activity.clone();
                    async move {
                        executed_activity.store(true, Ordering::SeqCst);
                        Err(err("Failed: Should not have executed the activity"))
                    }
                },
            );
        }
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        // Test terminating with and without recursion
        for recurse in [true, false] {
            let id = client
                .schedule_new_orchestration("Root", None, None, None)
                .await
                .unwrap();

            // Wait long enough to ensure all workflows have started (but not longer than the timer delay)
            let mut ids = vec![(id.clone(), OrchestrationStatus::Running)];
            for i in 0..5 {
                ids.push((format!("{id}_L1_{i}"), OrchestrationStatus::Running));
                ids.push((format!("{id}_L1_{i}_L2"), OrchestrationStatus::Running));
            }
            let address = env.address.clone();
            eventually(Duration::from_secs(2), "all workflows running", || {
                let address = address.clone();
                let ids = ids.clone();
                async move { all_have_status(&address, &ids).await }
            })
            .await;

            // Terminate the root workflow and mark whether a recursive termination
            let output = format!("Recursive termination = {recurse}");
            client
                .terminate_orchestration(&id, json(&output), recurse)
                .await
                .unwrap();

            let state = wait_done(&mut client, &id).await;
            assert_eq!(state.runtime_status, OrchestrationStatus::Terminated);
            assert_eq!(state.serialized_output, json(&output));

            // Wait for all L2 child workflows to complete
            for i in 0..5 {
                wait_done(&mut client, &format!("{id}_L1_{i}_L2")).await;
            }
            // Verify that none of the L2 child workflows executed the activity in case of recursive termination
            assert_ne!(
                recurse,
                executed_activity.load(Ordering::SeqCst),
                "recurse={recurse}"
            );
        }
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_terminate_workflow_recursive_terminate_completed_child_workflow() {
        const DELAY: Duration = Duration::from_secs(4);

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Root", |ctx| async move {
                // Create L1 child workflow and wait for it to complete
                let cid = format!("{}_L1", ctx.instance_id());
                let _ = ctx.call_sub_orchestrator("L1", (), Some(&cid)).await;
                let _ = ctx.create_timer(DELAY).await;
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L1", |ctx| async move {
                // Create L2 child workflow but don't wait for it to complete
                let cid = format!("{}_L2", ctx.instance_id());
                drop(ctx.call_sub_orchestrator("L2", (), Some(&cid)));
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L2", |ctx| async move {
                let _ = ctx.create_timer(DELAY).await;
                Ok(None)
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        for recurse in [true, false] {
            let id = client
                .schedule_new_orchestration("Root", None, None, None)
                .await
                .unwrap();

            // Wait long enough to ensure that all L1 workflows have completed but Root and L2 are still running
            let ids = vec![
                (id.clone(), OrchestrationStatus::Running),
                (format!("{id}_L1"), OrchestrationStatus::Completed),
                (format!("{id}_L1_L2"), OrchestrationStatus::Running),
            ];
            let address = env.address.clone();
            eventually(
                Duration::from_secs(2),
                "L1 completed, Root/L2 running",
                || {
                    let address = address.clone();
                    let ids = ids.clone();
                    async move { all_have_status(&address, &ids).await }
                },
            )
            .await;

            let output = format!("Recursive termination = {recurse}");
            client
                .terminate_orchestration(&id, json(&output), recurse)
                .await
                .unwrap();

            let state = wait_done(&mut client, &id).await;
            assert_eq!(state.runtime_status, OrchestrationStatus::Terminated);
            assert_eq!(state.serialized_output, json(&output));

            let (l2_status, l2_output) = if recurse {
                (OrchestrationStatus::Terminated, format!("\"{output}\""))
            } else {
                (OrchestrationStatus::Completed, String::new())
            };
            // In recursive case, L1 workflow is not terminated because it was already completed when the root workflow was terminated
            let l1 = wait_done(&mut client, &format!("{id}_L1")).await;
            assert_eq!(l1.runtime_status, OrchestrationStatus::Completed);
            assert_eq!(l1.serialized_output.as_deref().unwrap_or(""), "");

            // In recursive case, L2 is terminated because it was still running when the root workflow was terminated
            let l2 = wait_done(&mut client, &format!("{id}_L1_L2")).await;
            assert_eq!(l2.runtime_status, l2_status, "recurse={recurse}");
            assert_eq!(
                l2.serialized_output.as_deref().unwrap_or(""),
                l2_output,
                "recurse={recurse}"
            );
        }
        guard.stop().await;
    }

    /// Go maps both a gRPC error wrapping `ErrInstanceNotFound` and a zero
    /// deleted-instance count to `api.ErrInstanceNotFound`.
    fn assert_purge_not_found(res: dapr_durabletask::api::Result<i32>) {
        match res {
            Ok(0) => {}
            Err(e) if e.to_string().contains("no such instance exists") => {}
            other => panic!("expected instance-not-found purge result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_purge_completed_workflow() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ExternalEventWorkflow", |ctx| async move {
                match ctx
                    .wait_for_external_event_with_timeout("MyEvent", Duration::from_secs(30))
                    .await?
                {
                    ExternalEventResult::Received(_) => Ok(None),
                    ExternalEventResult::TimedOut => Err(err("the task was canceled")),
                }
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("ExternalEventWorkflow", None, None, None)
            .await
            .unwrap();
        client
            .wait_for_orchestration_start(&id, false, Some(TIMEOUT))
            .await
            .unwrap();

        // Try to purge the workflow state before it completes and verify that it fails with ErrNotCompleted
        let res = client.purge_orchestration(&id, false).await;
        match &res {
            Err(e)
                if e.to_string()
                    .contains("orchestration has not yet completed") => {}
            other => panic!("expected ErrNotCompleted, got {other:?}"),
        }

        // Raise an event to the workflow so that it can complete
        client
            .raise_orchestration_event(&id, "MyEvent", None)
            .await
            .unwrap();
        wait_done(&mut client, &id).await;

        // Try to purge the workflow state again and verify that it succeeds
        let deleted = client.purge_orchestration(&id, false).await.unwrap();
        assert!(deleted > 0, "expected purge to delete the instance");

        // Try to fetch the workflow metadata and verify that it fails with ErrInstanceNotFound
        assert!(fetch(&mut client, &id).await.is_none());

        // Try to purge again and verify that it also fails with ErrInstanceNotFound
        assert_purge_not_found(client.purge_orchestration(&id, false).await);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_purge_workflow_recursive() {
        const DELAY: Duration = Duration::from_secs(4);

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Root", |ctx| async move {
                let cid = format!("{}_L1", ctx.instance_id());
                let _ = ctx.call_sub_orchestrator("L1", (), Some(&cid)).await;
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L1", |ctx| async move {
                let cid = format!("{}_L2", ctx.instance_id());
                let _ = ctx.call_sub_orchestrator("L2", (), Some(&cid)).await;
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L2", |ctx| async move {
                let _ = ctx.create_timer(DELAY).await;
                Ok(None)
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        for recurse in [true, false] {
            let id = client
                .schedule_new_orchestration("Root", None, None, None)
                .await
                .unwrap();
            let state = wait_done(&mut client, &id).await;
            assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

            // Purge the root workflow
            let deleted = client.purge_orchestration(&id, recurse).await;
            assert!(
                matches!(deleted, Ok(n) if n > 0),
                "purge failed (recurse={recurse}): {deleted:?}"
            );

            // Verify that root Workflow has been purged
            assert!(fetch(&mut client, &id).await.is_none());

            let l1 = fetch(&mut client, &format!("{id}_L1")).await;
            let l2 = fetch(&mut client, &format!("{id}_L1_L2")).await;
            if recurse {
                // Verify that L1 and L2 workflows have been purged
                assert!(l1.is_none(), "L1 should be purged");
                assert!(l2.is_none(), "L2 should be purged");
            } else {
                // Verify that L1 and L2 workflows are not purged
                assert_eq!(
                    l1.expect("L1 should exist").runtime_status,
                    OrchestrationStatus::Completed
                );
                assert_eq!(
                    l2.expect("L2 should exist").runtime_status,
                    OrchestrationStatus::Completed
                );
            }
        }
        guard.stop().await;
    }

    #[ignore = "skipped upstream (t.Skip: needs durabletask-go issue #42, recreate completed workflow)"]
    #[tokio::test]
    async fn test_recreate_completed_workflow() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        register_single_activity(&mut worker, "HelloWorkflow");
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        // Run the first workflow
        let id = client
            .schedule_new_orchestration("HelloWorkflow", json("世界"), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );

        // Run the second workflow with the same ID as the first
        let new_id = client
            .schedule_new_orchestration("HelloWorkflow", json("World"), Some(id.clone()), None)
            .await
            .unwrap();
        assert_eq!(new_id, id);
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, World!""#)
        );

        // Go names the spans "SingleActivity" here (an upstream copy/paste slip
        // in a skipped test); the registered workflow is "HelloWorkflow".
        assert_span_sequence(&[
            E::Created("HelloWorkflow", &id),
            E::Activity("SayHello", &id, 0),
            E::Executed("HelloWorkflow", &id, "COMPLETED"),
            E::Created("HelloWorkflow", &id),
            E::Activity("SayHello", &id, 0),
            E::Executed("HelloWorkflow", &id, "COMPLETED"),
        ]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_single_activity_reuse_instance_id_error() {
        setup!(env);
        let mut worker = env.new_worker();
        register_single_activity(&mut worker, "SingleActivity");
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let instance_id = "ERROR_IF_RUNNING_OR_COMPLETED".to_string();
        let id = client
            .schedule_new_orchestration("SingleActivity", json("世界"), Some(instance_id), None)
            .await
            .unwrap();
        let res = client
            .schedule_new_orchestration("SingleActivity", json("World"), Some(id), None)
            .await;
        match res {
            Err(e) => assert!(
                e.to_string()
                    .contains("orchestration instance already exists"),
                "unexpected error: {e}"
            ),
            Ok(id) => panic!("expected duplicate-instance error, got Ok({id})"),
        }
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_single_activity_enforce_unique_instance_id() {
        use dapr_durabletask::client::NewOrchestrationOptions;

        /// Go: ErrDuplicateInstance, which the sidecar maps to gRPC AlreadyExists.
        fn assert_duplicate(res: dapr_durabletask::api::Result<String>) {
            match res {
                Err(DurableTaskError::GrpcError(status)) => {
                    assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status}");
                    assert!(
                        status
                            .message()
                            .contains("orchestration instance already exists"),
                        "unexpected message: {status}"
                    );
                }
                Err(e) => panic!("expected AlreadyExists gRPC error, got {e}"),
                Ok(id) => panic!("expected duplicate-instance error, got Ok({id})"),
            }
        }

        setup!(env);
        let mut worker = env.new_worker();
        register_single_activity(&mut worker, "SingleActivity");
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration(
                "SingleActivity",
                json("世界"),
                Some("ENFORCE_UNIQUE_INSTANCE_ID".to_string()),
                None,
            )
            .await
            .unwrap();
        let again = || {
            NewOrchestrationOptions::new()
                .with_input(json("World").unwrap())
                .with_instance_id(id.clone())
                .with_enforce_unique_instance_id()
        };

        // Scheduling again with the flag while the instance is active fails.
        assert_duplicate(
            client
                .schedule_new_orchestration_with_options("SingleActivity", again())
                .await,
        );

        let state = wait_done(&mut client, &id).await;
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );

        // After completion it also fails and does not restart the instance.
        assert_duplicate(
            client
                .schedule_new_orchestration_with_options("SingleActivity", again())
                .await,
        );
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );
        guard.stop().await;
    }

    fn retry_3x_10ms() -> ActivityOptions {
        ActivityOptions::new().with_retry_policy(RetryPolicy::new(3, Duration::from_millis(10)))
    }

    fn register_counting_fail_activity(
        worker: &mut TaskHubGrpcWorker,
        execution_map: Arc<Mutex<HashMap<String, i32>>>,
    ) {
        worker.registry_mut().add_named_activity(
            "FailActivity",
            move |ctx: ActivityContext, _input: Option<String>| {
                let execution_map = execution_map.clone();
                async move {
                    let mut map = execution_map.lock().unwrap();
                    let count = map.entry(ctx.task_execution_id().to_string()).or_insert(0);
                    *count += 1;
                    if *count == 3 {
                        return Ok(None);
                    }
                    Err(err("activity failure"))
                }
            },
        );
    }

    #[tokio::test]
    async fn test_task_execution_id() {
        // ── t.Run("SingleActivityWithRetry") ──
        {
            setup!(env);
            let execution_map = Arc::new(Mutex::new(HashMap::<String, i32>::new()));
            let mut worker = env.new_worker();
            worker
                .registry_mut()
                .add_named_orchestrator("TaskExecutionID", |ctx| async move {
                    ctx.call_activity_with_options("FailActivity", (), retry_3x_10ms())
                        .await?;
                    Ok(None)
                });
            register_counting_fail_activity(&mut worker, execution_map.clone());
            let guard = WorkerGuard::start(worker);
            let mut client = env.new_client().await;

            let id = client
                .schedule_new_orchestration("TaskExecutionID", None, None, None)
                .await
                .unwrap();
            let state = wait_done(&mut client, &id).await;
            assert_eq!(
                state.runtime_status,
                OrchestrationStatus::Completed,
                "SingleActivityWithRetry: failure={:?}, execution_map={:?}",
                state.failure_details,
                execution_map.lock().unwrap()
            );
            // With 3 max attempts there will be two retries with 10 millis delay before each
            assert_last_updated_after_created(&state, Duration::from_millis(2 * 10));
            let map = execution_map.lock().unwrap().clone();
            assert_eq!(map.len(), 1, "SingleActivityWithRetry: {map:?}");
            let (execution_id, count) = map.into_iter().next().unwrap();
            assert!(!execution_id.is_empty());
            assert_eq!(count, 3);
            guard.stop().await;
        }

        // ── t.Run("ParallelActivityWithRetry") ──
        {
            setup!(env);
            let execution_map = Arc::new(Mutex::new(HashMap::<String, i32>::new()));
            let mut worker = env.new_worker();
            worker
                .registry_mut()
                .add_named_orchestrator("TaskExecutionID", |ctx| async move {
                    // Go schedules both activities before awaiting either (its
                    // CallActivity is eager). `call_activity_with_options` returns
                    // a lazy future, so both are polled together to get the same
                    // schedule-both-then-await shape.
                    let t1 = ctx.call_activity_with_options("FailActivity", (), retry_3x_10ms());
                    let t2 = ctx.call_activity_with_options("FailActivity", (), retry_3x_10ms());
                    let (r1, r2) = futures::join!(t1, t2);
                    r1?;
                    r2?;
                    Ok(None)
                });
            register_counting_fail_activity(&mut worker, execution_map.clone());
            let guard = WorkerGuard::start(worker);
            let mut client = env.new_client().await;

            let id = client
                .schedule_new_orchestration("TaskExecutionID", None, None, None)
                .await
                .unwrap();
            let state = wait_done(&mut client, &id).await;
            assert_eq!(
                state.runtime_status,
                OrchestrationStatus::Completed,
                "ParallelActivityWithRetry: failure={:?}, execution_map={:?}",
                state.failure_details,
                execution_map.lock().unwrap()
            );
            assert_last_updated_after_created(&state, Duration::from_millis(2 * 10));
            let map = execution_map.lock().unwrap().clone();
            assert_eq!(map.len(), 2, "ParallelActivityWithRetry: {map:?}");
            for (k, v) in map {
                assert!(!k.is_empty());
                assert_eq!(v, 3);
            }
            guard.stop().await;
        }

        // ── t.Run("SingleActivityWithNoRetry") ──
        {
            setup!(env);
            let execution_id = Arc::new(Mutex::new(String::new()));
            let mut worker = env.new_worker();
            worker
                .registry_mut()
                .add_named_orchestrator("TaskExecutionID", |ctx| async move {
                    ctx.call_activity("Activity", ()).await?;
                    Ok(None)
                });
            {
                let execution_id = execution_id.clone();
                worker.registry_mut().add_named_activity(
                    "Activity",
                    move |ctx: ActivityContext, _input: Option<String>| {
                        let execution_id = execution_id.clone();
                        async move {
                            *execution_id.lock().unwrap() = ctx.task_execution_id().to_string();
                            Ok(None)
                        }
                    },
                );
            }
            let guard = WorkerGuard::start(worker);
            let mut client = env.new_client().await;

            let id = client
                .schedule_new_orchestration("TaskExecutionID", None, None, None)
                .await
                .unwrap();
            let state = wait_done(&mut client, &id).await;
            assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
            let execution_id = execution_id.lock().unwrap().clone();
            assert!(!execution_id.is_empty(), "SingleActivityWithNoRetry");
            uuid::Uuid::parse_str(&execution_id)
                .unwrap_or_else(|e| panic!("execution id {execution_id:?} is not a UUID: {e}"));
            guard.stop().await;
        }
    }

    #[cfg_attr(
        not(feature = "opentelemetry"),
        ignore = "requires the opentelemetry feature (activity trace context)"
    )]
    #[tokio::test]
    async fn test_activity_trace_context() {
        // Go reads `ctx.GetTraceContext().GetTraceParent()`: the trace context of
        // the activity span, which Go's backend attaches to the activity request.
        // The Rust `ActivityContext` has no trace-context accessor; instead the
        // worker runs the activity with its `activity||` span as the current OTel
        // context, so the traceparent is read from `Context::current()` with the
        // SDK's own `otel::trace_context_from_span_context`.
        init_tracing();
        setup!(env);
        let trace_parents = Arc::new(Mutex::new(HashMap::<String, String>::new()));
        let execution_id = Arc::new(Mutex::new(String::new()));
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("TraceContextWorkflow", |ctx| async move {
                ctx.call_activity_with_options("ActivityWithContext", (), retry_3x_10ms())
                    .await?;
                Ok(None)
            });
        {
            let (trace_parents, execution_id) = (trace_parents.clone(), execution_id.clone());
            worker.registry_mut().add_named_activity(
                "ActivityWithContext",
                move |ctx: ActivityContext, _input: Option<String>| {
                    let (trace_parents, execution_id) =
                        (trace_parents.clone(), execution_id.clone());
                    async move {
                        let exec_id = ctx.task_execution_id().to_string();
                        trace_parents
                            .lock()
                            .unwrap()
                            .insert(exec_id.clone(), current_trace_parent());
                        *execution_id.lock().unwrap() = exec_id;
                        // Go then extracts the traceparent into a fresh context
                        // with the W3C propagator (result unused) and starts an
                        // unparented span from context.Background().
                        #[cfg(feature = "opentelemetry")]
                        user_span("ActivityWith1Context", &opentelemetry::Context::new());
                        Ok(None)
                    }
                },
            );
        }
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("TraceContextWorkflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        let execution_id = execution_id.lock().unwrap().clone();
        assert!(!execution_id.is_empty());
        let trace_parent = trace_parents
            .lock()
            .unwrap()
            .get(&execution_id)
            .cloned()
            .unwrap_or_default();
        assert!(
            !trace_parent.is_empty(),
            "no traceparent for execution {execution_id}"
        );
        // Beyond Go's NotEmpty: it must be the activity span's own context, as
        // Go's GetTraceContext() is.
        assert_eq!(
            trace_parent,
            activity_trace_parent("ActivityWithContext", &id)
        );

        assert_span_sequence(&[
            E::Created("TraceContextWorkflow", &id),
            E::Named("ActivityWith1Context"),
            E::Activity("ActivityWithContext", &id, 0),
            E::Executed("TraceContextWorkflow", &id, "COMPLETED"),
        ]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_patching_default_to_patched() {
        setup!(env);
        let patches_found = Arc::new(Mutex::new(Vec::<bool>::new()));
        let mut worker = env.new_worker();
        {
            let patches_found = patches_found.clone();
            worker
                .registry_mut()
                .add_named_orchestrator("Workflow", move |ctx| {
                    let patches_found = patches_found.clone();
                    async move {
                        patches_found.lock().unwrap().push(ctx.is_patched("patch1"));
                        Ok(None)
                    }
                });
        }
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Workflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(*patches_found.lock().unwrap(), vec![true]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_patching_run_unpatched_version() {
        setup!(env);
        let run_number = Arc::new(AtomicU32::new(0));
        let patches_found = Arc::new(Mutex::new(Vec::<bool>::new()));
        let mut worker = env.new_worker();
        {
            let (run_number, patches_found) = (run_number.clone(), patches_found.clone());
            worker
                .registry_mut()
                .add_named_orchestrator("Workflow", move |ctx| {
                    let (run_number, patches_found) = (run_number.clone(), patches_found.clone());
                    async move {
                        let current_run = run_number.fetch_add(1, Ordering::SeqCst) + 1;
                        // Simulate a version upgrade across runs: the patch check only exists from run 2.
                        if current_run > 1 {
                            patches_found.lock().unwrap().push(ctx.is_patched("patch1"));
                        }
                        let _ = ctx.call_activity("SayHello", ()).await;
                        Ok(None)
                    }
                });
        }
        register_say_hello_plain(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Workflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(run_number.load(Ordering::SeqCst), 2);
        assert_eq!(*patches_found.lock().unwrap(), vec![false]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_patching_multiple_patches() {
        setup!(env);
        let run_number = Arc::new(AtomicU32::new(0));
        let patches1 = Arc::new(Mutex::new(Vec::<bool>::new()));
        let patches2 = Arc::new(Mutex::new(Vec::<bool>::new()));
        let mut worker = env.new_worker();
        {
            let (run_number, patches1, patches2) =
                (run_number.clone(), patches1.clone(), patches2.clone());
            worker
                .registry_mut()
                .add_named_orchestrator("Workflow", move |ctx| {
                    let (run_number, patches1, patches2) =
                        (run_number.clone(), patches1.clone(), patches2.clone());
                    async move {
                        let current_run = run_number.fetch_add(1, Ordering::SeqCst) + 1;
                        if current_run > 1 {
                            patches1.lock().unwrap().push(ctx.is_patched("patch1"));
                        }
                        let _ = ctx.call_activity("SayHello", ()).await;
                        patches2.lock().unwrap().push(ctx.is_patched("patch2"));
                        let _ = ctx.call_activity("SayHello", ()).await;
                        Ok(None)
                    }
                });
        }
        register_say_hello_plain(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Workflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(run_number.load(Ordering::SeqCst), 3);
        assert_eq!(*patches1.lock().unwrap(), vec![false, false]);
        assert_eq!(*patches2.lock().unwrap(), vec![true, true]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_patching_continue_as_new_do_not_carry_over_choices() {
        setup!(env);
        let run_number = Arc::new(AtomicU32::new(0));
        let ran_continue_as_new = Arc::new(AtomicBool::new(false));
        let patches_found = Arc::new(Mutex::new(Vec::<bool>::new()));
        let mut worker = env.new_worker();
        {
            let (run_number, ran_can, patches_found) = (
                run_number.clone(),
                ran_continue_as_new.clone(),
                patches_found.clone(),
            );
            worker
                .registry_mut()
                .add_named_orchestrator("Workflow", move |ctx| {
                    let (run_number, ran_can, patches_found) =
                        (run_number.clone(), ran_can.clone(), patches_found.clone());
                    async move {
                        let current_run = run_number.fetch_add(1, Ordering::SeqCst) + 1;
                        // The patch is checked from the 2nd rerun, so it should be false for the
                        // in-flight run, but true for the continue-as-new run.
                        if current_run > 1 {
                            patches_found.lock().unwrap().push(ctx.is_patched("patch1"));
                        }
                        let _ = ctx.call_activity("SayHello", ()).await;
                        if !ran_can.swap(true, Ordering::SeqCst) {
                            ctx.continue_as_new((), false);
                        }
                        Ok(None)
                    }
                });
        }
        register_say_hello_plain(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Workflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(*patches_found.lock().unwrap(), vec![false, true, true]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_patching_patch_persists_across_replays() {
        setup!(env);
        let run_number = Arc::new(AtomicU32::new(0));
        let patch_results = Arc::new(Mutex::new(Vec::<bool>::new()));
        let mut worker = env.new_worker();
        {
            let (run_number, patch_results) = (run_number.clone(), patch_results.clone());
            worker
                .registry_mut()
                .add_named_orchestrator("Workflow", move |ctx| {
                    let (run_number, patch_results) = (run_number.clone(), patch_results.clone());
                    async move {
                        run_number.fetch_add(1, Ordering::SeqCst);
                        for _ in 0..3 {
                            patch_results.lock().unwrap().push(ctx.is_patched("patch1"));
                            let _ = ctx.call_activity("SayHello", ()).await;
                        }
                        Ok(None)
                    }
                });
        }
        register_say_hello_plain(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Workflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

        // Workflow runs 4 times: initial + 3 activity completions
        assert_eq!(run_number.load(Ordering::SeqCst), 4);
        // Run 1: 1 check, Run 2: 2, Run 3: 3, Run 4: 3 → 9 checks, all true.
        let results = patch_results.lock().unwrap().clone();
        assert_eq!(results.len(), 9, "{results:?}");
        for (i, r) in results.iter().enumerate() {
            assert!(*r, "patch check {i} should be true");
        }
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_patching_patch_remembers_to_stay_false() {
        setup!(env);
        let run_number = Arc::new(AtomicU32::new(0));
        let patch_results = Arc::new(Mutex::new(Vec::<bool>::new()));
        let mut worker = env.new_worker();
        {
            let (run_number, patch_results) = (run_number.clone(), patch_results.clone());
            worker
                .registry_mut()
                .add_named_orchestrator("Workflow", move |ctx| {
                    let (run_number, patch_results) = (run_number.clone(), patch_results.clone());
                    async move {
                        let current_run = run_number.fetch_add(1, Ordering::SeqCst) + 1;
                        // Simulate code upgrade: patch check is only present from run 2 onwards.
                        if current_run >= 2 {
                            ctx.is_patched("patch1");
                        }
                        let _ = ctx.call_activity("SayHello", ()).await;
                        // It should return false because it was already seen as false earlier in this turn.
                        patch_results.lock().unwrap().push(ctx.is_patched("patch1"));
                        let _ = ctx.call_activity("SayHello", ()).await;
                        Ok(None)
                    }
                });
        }
        register_say_hello_plain(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("Workflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(run_number.load(Ordering::SeqCst), 3);
        assert_eq!(*patch_results.lock().unwrap(), vec![false, false]);
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_workflow_patching_tracing_spans() {
        init_tracing();
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("PatchTracingWorkflow", |ctx| async move {
                ctx.is_patched("patch1");
                ctx.call_activity("SayHello", ()).await?;
                ctx.is_patched("patch2");
                ctx.call_activity("SayHello", ()).await?;
                ctx.is_patched("patch3");
                Ok(None)
            });
        register_say_hello_plain(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("PatchTracingWorkflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

        // Go: created, activity(0), activity(1), executed(COMPLETED) with
        // `applied_patches = ["patch1","patch2","patch3"]`. That attribute is set
        // by the backend from the patches the SDK reports in its response, which
        // the backend also stores on the final turn's WorkflowStarted event; the
        // SDK's own span has no such attribute. Checked on that event.
        assert_span_sequence(&[
            E::Created("PatchTracingWorkflow", &id),
            E::Activity("SayHello", &id, 0),
            E::Activity("SayHello", &id, 1),
            E::Executed("PatchTracingWorkflow", &id, "COMPLETED"),
        ]);
        let events = history::fetch(&env.address, &id).await;
        assert_eq!(
            history::last_applied_patches(&events),
            Some(vec![
                "patch1".to_string(),
                "patch2".to_string(),
                "patch3".to_string()
            ])
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_started_at_after_execution() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("StartedAtAfterExec", |_ctx| async move { Ok(None) });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let before_schedule = chrono::Utc::now();
        let id = client
            .schedule_new_orchestration("StartedAtAfterExec", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        let after_completion = chrono::Utc::now();
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        guard.stop().await;
        let started_at = state
            .started_at
            .expect("started_at must be populated once execution has begun");
        assert!(
            started_at >= before_schedule,
            "started_at {started_at} should be >= scheduling time {before_schedule}"
        );
        assert!(
            started_at <= after_completion,
            "started_at {started_at} should be <= now {after_completion}"
        );
        let created_at = state.created_at.expect("created_at missing");
        assert!(
            started_at >= created_at,
            "started_at {started_at} should be >= created_at {created_at}"
        );
    }

    #[tokio::test]
    async fn test_started_at_with_schedule_time() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("StartedAtAfterExec", |_ctx| async move { Ok(None) });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let before_schedule = chrono::Utc::now();
        let start_time = before_schedule + chrono::Duration::seconds(1);
        let id = client
            .schedule_new_orchestration("StartedAtAfterExec", None, None, Some(start_time))
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        let after_completion = chrono::Utc::now();
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        guard.stop().await;
        let started_at = state
            .started_at
            .expect("started_at must be populated once execution has begun");
        assert!(
            started_at >= start_time,
            "started_at {started_at} should be >= start time {start_time}"
        );
        let created_at = state.created_at.expect("created_at missing");
        assert!(
            started_at >= created_at,
            "started_at {started_at} should be >= created_at {created_at}"
        );
        assert!(
            started_at <= after_completion,
            "started_at {started_at} should be <= now {after_completion}"
        );
    }

    #[tokio::test]
    async fn test_started_at_nil_before_execution() {
        // Go: no worker; schedule "NeverRun"; metadata is PENDING and StartedAt is
        // nil. A future start time keeps it deterministically pending here.
        setup!(env);
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                "NeverRun",
                None,
                None,
                Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            )
            .await
            .unwrap();
        let state = fetch(&mut client, &id).await.expect("state missing");
        assert_eq!(state.runtime_status, OrchestrationStatus::Pending);
        assert!(
            state.started_at.is_none(),
            "started_at must be None while the workflow is pending, got {:?}",
            state.started_at
        );
    }

    #[tokio::test]
    async fn test_started_at_after_continue_as_new() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("StartedAtCAN", |ctx| async move {
                let input: i32 = ctx.input()?;
                if input < 2 {
                    ctx.create_timer(Duration::ZERO).await?;
                    ctx.continue_as_new(input + 1, false);
                }
                Ok(json(input))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let before_schedule = chrono::Utc::now();
        let id = client
            .schedule_new_orchestration("StartedAtCAN", json(0), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        let after_completion = chrono::Utc::now();
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        guard.stop().await;
        let started_at = state.started_at.expect("started_at missing");
        assert!(
            started_at >= before_schedule,
            "started_at {started_at} should be >= scheduling time {before_schedule}"
        );
        assert!(
            started_at <= after_completion,
            "started_at {started_at} should be <= now {after_completion}"
        );
    }

    // ── tests/external_event_stale_timer_test.go ─────────────────────────────────

    #[tokio::test]
    async fn test_external_event_raised_before_timeout_survives_stale_timer_fired() {
        const EVENT_TIMEOUT: Duration = Duration::from_secs(2);
        const UNRELATED_TIMER: Duration = Duration::from_secs(4);

        setup!(env);
        let mut worker = env.new_worker();
        worker
        .registry_mut()
        .add_named_orchestrator("EventThenStaleTimer", |ctx| async move {
            // Go's WaitForSingleEvent registers the wait and its timeout timer
            // immediately. The Rust API is a lazy future, so poll it once to
            // register both before scheduling the unrelated timer.
            let mut w = Box::pin(ctx.wait_for_external_event_with_timeout("A", EVENT_TIMEOUT));
            let early = match futures::poll!(w.as_mut()) {
                std::task::Poll::Ready(r) => Some(r),
                std::task::Poll::Pending => None,
            };

            // Keep the workflow alive past the event-timeout window so the stale
            // TimerFired for the satisfied wait is delivered while it still runs.
            ctx.create_timer(UNRELATED_TIMER)
                .await
                .map_err(|e| err(&format!("unrelated timer failed: {e}")))?;

            let res = match early {
                Some(r) => r,
                None => w.await,
            }?;
            match res {
                ExternalEventResult::Received(raw) => {
                    let value: i32 = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
                    Ok(json(value))
                }
                ExternalEventResult::TimedOut => Err(err(
                    "event await failed even though the event was raised in time: the task was canceled",
                )),
            }
        });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("EventThenStaleTimer", None, None, None)
            .await
            .unwrap();
        // Raise the event immediately, well within the timeout window.
        client
            .raise_orchestration_event(&id, "A", json(42))
            .await
            .unwrap();

        let state = wait_done(&mut client, &id).await;
        assert_eq!(
            state.runtime_status,
            OrchestrationStatus::Completed,
            "workflow should complete successfully; failure details: {:?}",
            state.failure_details
        );
        assert_eq!(state.serialized_output.as_deref(), Some("42"));

        // Beyond Go: prove the scenario of the Go doc comment happened rather
        // than relying on timing: EventRaised(A) precedes the stale TimerFired of
        // the event-wait timer, which precedes the unrelated TimerFired.
        let events = history::fetch(&env.address, &id).await;
        let timers = history::created_timer_ids(&events);
        assert_eq!(timers.len(), 2, "event-wait timer + unrelated timer");
        let raised = history::event_raised_indices(&events, "A");
        let stale =
            history::timer_fired_index(&events, timers[0]).expect("stale TimerFired missing");
        let unrelated =
            history::timer_fired_index(&events, timers[1]).expect("unrelated TimerFired missing");
        assert_eq!(raised.len(), 1);
        assert!(
            raised[0] < stale && stale < unrelated,
            "expected EventRaised < stale TimerFired < unrelated TimerFired, got {} / {stale} / {unrelated}",
            raised[0]
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn test_external_event_receive_loop_second_event_after_stale_timer_fired() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("EventReceiveLoop", |ctx| async move {
                let v1: i32 = match ctx
                    .wait_for_external_event_with_timeout("A", Duration::from_secs(2))
                    .await?
                {
                    ExternalEventResult::Received(raw) => {
                        serde_json::from_str(raw.as_deref().unwrap_or("null"))?
                    }
                    ExternalEventResult::TimedOut => {
                        return Err(err("first wait failed: the task was canceled"));
                    }
                };
                let v2: i32 = match ctx
                    .wait_for_external_event_with_timeout("A", Duration::from_secs(10))
                    .await?
                {
                    ExternalEventResult::Received(raw) => {
                        serde_json::from_str(raw.as_deref().unwrap_or("null"))?
                    }
                    ExternalEventResult::TimedOut => {
                        return Err(err("second wait failed: the task was canceled"));
                    }
                };
                Ok(json(vec![v1, v2]))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("EventReceiveLoop", None, None, None)
            .await
            .unwrap();

        // First event: raised immediately, well within the first wait's 2s window.
        client
            .raise_orchestration_event(&id, "A", json(1))
            .await
            .unwrap();

        // Wait until the first wait's timeout timer (due at ~2s) has fired and been
        // processed, so the second EventRaised lands after the stale TimerFired.
        // 5s is well past the 2s due time while still well within the second
        // wait's 10s window.
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Second event: must complete the second wait, which is still pending.
        client
            .raise_orchestration_event(&id, "A", json(2))
            .await
            .unwrap();

        let state = wait_done(&mut client, &id).await;
        assert_eq!(
            state.runtime_status,
            OrchestrationStatus::Completed,
            "workflow should complete successfully; failure details: {:?}",
            state.failure_details
        );
        assert_eq!(state.serialized_output.as_deref(), Some("[1,2]"));

        // Beyond Go: prove the second EventRaised landed after the stale
        // TimerFired of the first wait (the scenario under test) instead of
        // trusting the sleep.
        let events = history::fetch(&env.address, &id).await;
        let timers = history::created_timer_ids(&events);
        assert_eq!(timers.len(), 2, "one timeout timer per wait");
        let raised = history::event_raised_indices(&events, "A");
        let stale =
            history::timer_fired_index(&events, timers[0]).expect("stale TimerFired missing");
        assert_eq!(raised.len(), 2);
        assert!(
            raised[0] < stale && stale < raised[1],
            "expected EventRaised(1) < stale TimerFired < EventRaised(2), got {} / {stale} / {}",
            raised[0],
            raised[1]
        );
        guard.stop().await;
    }
}

mod grpc_client_worker {

    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use dapr_durabletask::api::{
        DurableTaskError, FailureDetails, OrchestrationStatus, RetryPolicy,
    };
    use dapr_durabletask::client::TaskHubGrpcClient;
    use dapr_durabletask::task::{
        ActivityContext, ActivityOptions, ExternalEventResult, SubOrchestratorOptions,
    };
    use dapr_durabletask::worker::{TaskHubGrpcWorker, WorkerOptions};
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    use crate::harness::{self, WorkerGuard};
    use crate::setup;

    const TIMEOUT: Duration = Duration::from_secs(30);

    // backend_test.go constants
    const DEFAULT_NAME: &str = "testing";
    const DEFAULT_INPUT: &str = "Hello, 世界!";

    // ── Helpers ───────────────────────────────────────────────────────────────────

    fn js<T: serde::Serialize>(v: T) -> String {
        serde_json::to_string(&v).unwrap()
    }

    /// Poll `f` every 10 ms until it returns true or `timeout` elapses.
    async fn eventually(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if f() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Spawn a worker and hand back its shutdown token and join handle, so the
    /// test can observe exactly when `start()` returns (drain completion).
    fn spawn_worker(
        worker: TaskHubGrpcWorker,
    ) -> (
        CancellationToken,
        tokio::task::JoinHandle<dapr_durabletask::api::Result<()>>,
    ) {
        let token = CancellationToken::new();
        let t = token.clone();
        let handle = tokio::spawn(async move { worker.start(t).await });
        (token, handle)
    }

    /// A worker running on its own tokio runtime so that it can be killed
    /// abruptly (connections dropped, in-flight tasks discarded) — the Rust
    /// equivalent of a Go work item being *abandoned*: the sidecar sees the work
    /// item stream disconnect and re-queues the in-flight work item.
    struct DetachedWorker {
        rt: Option<tokio::runtime::Runtime>,
    }

    impl DetachedWorker {
        fn start(worker: TaskHubGrpcWorker) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.spawn(async move {
                let _ = worker.start(CancellationToken::new()).await;
            });
            Self { rt: Some(rt) }
        }

        fn kill(&mut self) {
            if let Some(rt) = self.rt.take() {
                rt.shutdown_background();
            }
        }
    }

    impl Drop for DetachedWorker {
        fn drop(&mut self) {
            self.kill();
        }
    }

    /// Stop a worker guard and fail the test if `start()` does not return within
    /// 10 s of cancellation (every caller has no blocked in-flight work at this
    /// point, so a hang is a shutdown regression, not something to tolerate).
    async fn stop(guard: WorkerGuard) {
        tokio::time::timeout(Duration::from_secs(10), guard.stop())
            .await
            .expect("worker did not stop within 10s after cancellation");
    }

    async fn get_state(
        client: &mut TaskHubGrpcClient,
        id: &str,
    ) -> dapr_durabletask::api::OrchestrationState {
        client
            .get_orchestration_state(id, true)
            .await
            .unwrap()
            .expect("no state returned")
    }

    async fn wait_done(
        client: &mut TaskHubGrpcClient,
        id: &str,
    ) -> dapr_durabletask::api::OrchestrationState {
        client
            .wait_for_orchestration_completion(id, true, Some(TIMEOUT))
            .await
            .unwrap()
            .expect("no state returned")
    }

    fn say_hello_activity(worker: &mut TaskHubGrpcWorker) {
        worker.registry_mut().add_named_activity(
            "SayHello",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let name: String = serde_json::from_str(input.as_deref().unwrap_or("null"))?;
                Ok(Some(js(format!("Hello, {name}!"))))
            },
        );
    }

    // ══════════════════════════════════════════════════════════════════════════════
    // tests/grpc/grpc_test.go
    // ══════════════════════════════════════════════════════════════════════════════

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_grpc_wait_for_instance_start_timeout() {
        setup!(env);
        let mut worker = env.new_worker();
        worker.registry_mut().add_named_orchestrator(
            "WaitForInstanceStartThrowsException",
            |_ctx| async move {
                // Go: time.Sleep(5 * time.Second) — a blocking sleep in the orchestrator.
                std::thread::sleep(Duration::from_secs(5));
                Ok(Some(js(42)))
            },
        );
        let guard = WorkerGuard::start(worker);

        // Go: `go grpcClient.ScheduleNewWorkflow(...)` (the sidecar blocks the
        // create call until the instance starts).
        let mut bg_client = env.new_client().await;
        let bg = tokio::spawn(async move {
            let _ = bg_client
                .schedule_new_orchestration(
                    "WaitForInstanceStartThrowsException",
                    Some(js("世界")),
                    Some("helloworld".to_string()),
                    None,
                )
                .await;
        });

        let mut client = env.new_client().await;
        let res = client
            .wait_for_orchestration_start("helloworld", true, Some(Duration::from_secs(1)))
            .await;
        // Go: error contains "context deadline exceeded"; the Rust equivalent of a
        // deadline expiry is DurableTaskError::Timeout.
        match res {
            Err(DurableTaskError::Timeout) => {}
            other => panic!("expected Timeout error, got {other:?}"),
        }

        let _ = bg.await;
        stop(guard).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_grpc_wait_for_instance_start_connection_resume() {
        setup!(env);
        let invocations = Arc::new(AtomicUsize::new(0));
        let make_worker = || {
            let mut worker = env.new_worker();
            let inv = invocations.clone();
            worker.registry_mut().add_named_orchestrator(
                "WaitForInstanceStartThrowsException",
                move |_ctx| {
                    let inv = inv.clone();
                    async move {
                        inv.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_secs(5));
                        Ok(Some(js(42)))
                    }
                },
            );
            worker
        };
        // Go's cancelListener() tears the stream down while the orchestrator is
        // still sleeping, so its completion is never delivered and the sidecar
        // abandons the work item. A graceful Rust shutdown would instead drain and
        // deliver it (no retry would happen), so the first listener is a worker
        // that can be dropped abruptly.
        let mut listener = DetachedWorker::start(make_worker());

        let mut bg_client = env.new_client().await;
        let bg = tokio::spawn(async move {
            let _ = bg_client
                .schedule_new_orchestration(
                    "WaitForInstanceStartThrowsException",
                    Some(js("世界")),
                    Some("worldhello".to_string()),
                    None,
                )
                .await;
        });

        let mut client = env.new_client().await;
        let res = client
            .wait_for_orchestration_start("worldhello", true, Some(Duration::from_secs(1)))
            .await;
        match res {
            Err(DurableTaskError::Timeout) => {}
            other => panic!("expected Timeout error, got {other:?}"),
        }

        // cancelListener(); time.Sleep(2s) — with the work item in flight on it.
        assert!(
            eventually(Duration::from_secs(3), || invocations
                .load(Ordering::SeqCst)
                == 1)
            .await
        );
        listener.kill();
        tokio::time::sleep(Duration::from_secs(2)).await;

        // reconnect
        let guard2 = WorkerGuard::start(make_worker());

        // workitem should be retried and completed.
        let state = wait_done(&mut client, "worldhello").await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.serialized_output.as_deref(), Some("42"));
        // The completed result came from the retry on the new listener, not from
        // the dropped one.
        assert_eq!(invocations.load(Ordering::SeqCst), 2);

        let _ = bg.await;
        stop(guard2).await;
    }

    #[tokio::test]
    async fn test_grpc_hello_workflow() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("SingleActivity", |ctx| async move {
                let input: String = ctx.input()?;
                let output = ctx.call_activity("SayHello", input).await;
                ctx.set_custom_status("hello-test");
                output
            });
        say_hello_activity(&mut worker);
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("SingleActivity", Some(js("世界")), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );
        assert_eq!(
            state.serialized_custom_status.as_deref(),
            Some("hello-test")
        );
        tokio::time::sleep(Duration::from_secs(1)).await;

        let purged = client.purge_orchestration(&id, false).await;
        assert!(purged.is_ok(), "purge failed: {purged:?}");

        stop(guard).await;
    }

    #[tokio::test]
    async fn test_grpc_continue_as_new() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ContinueAsNewGrpc", |ctx| async move {
                let input: i32 = ctx.input()?;
                if input < 10 {
                    ctx.continue_as_new(input + 1, false);
                }
                Ok(Some(js(input)))
            });
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("ContinueAsNewGrpc", Some(js(0)), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.serialized_output.as_deref(), Some("10"));

        client.purge_orchestration(&id, false).await.unwrap();
        stop(guard).await;
    }

    #[tokio::test]
    async fn test_grpc_suspend_resume() {
        const EVENT_COUNT: i32 = 10;
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("SuspendResumeWorkflow", |ctx| async move {
                for i in 0..EVENT_COUNT {
                    // Go: WaitForSingleEvent("MyEvent", 5s).Await(&value) — a
                    // timeout leaves value at its zero value.
                    let value: i32 = match ctx
                        .wait_for_external_event_with_timeout("MyEvent", Duration::from_secs(5))
                        .await?
                    {
                        ExternalEventResult::Received(Some(s)) => {
                            serde_json::from_str(&s).unwrap_or(0)
                        }
                        _ => 0,
                    };
                    if value != i {
                        return Err(DurableTaskError::Other("Unexpected value".into()));
                    }
                }
                Ok(Some(js(true)))
            });
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        // Run the workflow, which will block waiting for external events
        let id = client
            .schedule_new_orchestration("SuspendResumeWorkflow", Some(js(0)), None, None)
            .await
            .unwrap();

        // Suspend the workflow
        client.suspend_orchestration(&id, None).await.unwrap();

        // Raise a bunch of events (they should get buffered but not consumed)
        for i in 0..EVENT_COUNT {
            client
                .raise_orchestration_event(&id, "MyEvent", Some(js(i)))
                .await
                .unwrap();
        }

        // Make sure the workflow *doesn't* complete
        let res = client
            .wait_for_orchestration_completion(&id, false, Some(Duration::from_secs(3)))
            .await;
        assert!(
            matches!(res, Err(DurableTaskError::Timeout)),
            "expected timeout while suspended, got {res:?}"
        );

        let state = get_state(&mut client, &id).await;
        // Go: WorkflowMetadataIsRunning == not in a terminal state.
        assert!(!state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Suspended);

        // Resume the workflow and wait for it to complete
        client.resume_orchestration(&id, None).await.unwrap();
        let res = client
            .wait_for_orchestration_completion(&id, false, Some(Duration::from_secs(3)))
            .await;
        assert!(res.is_ok(), "expected completion after resume, got {res:?}");
        tokio::time::sleep(Duration::from_secs(1)).await;

        stop(guard).await;
    }

    #[tokio::test]
    async fn test_grpc_terminate_recursive() {
        let delay_time = Duration::from_secs(4);
        let executed_activity = Arc::new(AtomicBool::new(false));

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Root", |ctx| async move {
                let tasks: Vec<_> = (0..5)
                    .map(|_| ctx.call_sub_orchestrator("L1", (), None))
                    .collect();
                for t in tasks {
                    let _ = t.await;
                }
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L1", |ctx| async move {
                let _ = ctx.call_sub_orchestrator("L2", (), None).await;
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("L2", move |ctx| async move {
                let _ = ctx.create_timer(delay_time).await;
                let _ = ctx.call_activity("Fail", ()).await;
                Ok(None)
            });
        let flag = executed_activity.clone();
        worker.registry_mut().add_named_activity(
            "Fail",
            move |_ctx: ActivityContext, _input: Option<String>| {
                let flag = flag.clone();
                async move {
                    flag.store(true, Ordering::SeqCst);
                    Err(DurableTaskError::Other(
                        "Failed: Should not have executed the activity".into(),
                    ))
                }
            },
        );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        // Test terminating with and without recursion
        for recurse in [true, false] {
            let id = client
                .schedule_new_orchestration("Root", None, None, None)
                .await
                .unwrap();

            // Wait long enough to ensure all workflows have started (but not longer than the timer delay)
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Terminate the root workflow and mark whether a recursive termination
            let output = format!("Recursive termination = {recurse}");
            client
                .terminate_orchestration(&id, Some(js(&output)), recurse)
                .await
                .unwrap();

            // Wait for the root workflow to complete and verify its terminated status
            let state = wait_done(&mut client, &id).await;
            assert_eq!(
                state.runtime_status,
                OrchestrationStatus::Terminated,
                "recurse={recurse}"
            );
            assert_eq!(
                state.serialized_output,
                Some(format!("\"{output}\"")),
                "recurse={recurse}"
            );

            // Wait longer to ensure that none of the child workflows continued to
            // the next step of executing the activity function.
            tokio::time::sleep(delay_time).await;
            // Note (as in Go): executedActivity is not reset between sub-tests.
            assert_ne!(
                recurse,
                executed_activity.load(Ordering::SeqCst),
                "recurse={recurse}"
            );
        }

        stop(guard).await;
    }

    #[tokio::test]
    async fn test_grpc_reuse_instance_id_error() {
        let delay_time = Duration::from_secs(4);
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("SingleActivity", move |ctx| async move {
                let input: String = ctx.input()?;
                let _ = ctx.create_timer(delay_time).await;
                ctx.call_activity("SayHello", input).await
            });
        say_hello_activity(&mut worker);
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let instance_id = "THROW_IF_RUNNING_OR_COMPLETED".to_string();
        let id = client
            .schedule_new_orchestration("SingleActivity", Some(js("世界")), Some(instance_id), None)
            .await
            .unwrap();
        let res = client
            .schedule_new_orchestration("SingleActivity", Some(js("World")), Some(id), None)
            .await;
        match res {
            Err(e) => assert!(
                e.to_string()
                    .contains("orchestration instance already exists"),
                "unexpected error: {e}"
            ),
            Ok(id) => panic!("expected duplicate-instance error, got Ok({id})"),
        }

        stop(guard).await;
    }

    #[tokio::test]
    async fn test_grpc_enforce_unique_instance_id() {
        use dapr_durabletask::client::NewOrchestrationOptions;

        fn assert_already_exists(res: dapr_durabletask::api::Result<String>) {
            match res {
                Err(DurableTaskError::GrpcError(status)) => {
                    assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status}");
                }
                Err(e) => panic!("expected AlreadyExists gRPC error, got {e}"),
                Ok(id) => panic!("expected AlreadyExists error, got Ok({id})"),
            }
        }

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("SingleActivity", |ctx| async move {
                let input: String = ctx.input()?;
                ctx.call_activity("SayHello", input).await
            });
        say_hello_activity(&mut worker);
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                "SingleActivity",
                Some(js("世界")),
                Some("GRPC_ENFORCE_UNIQUE_INSTANCE_ID".to_string()),
                None,
            )
            .await
            .unwrap();
        let again = || {
            NewOrchestrationOptions::new()
                .with_input(js("World"))
                .with_instance_id(id.clone())
                .with_enforce_unique_instance_id()
        };

        // While the instance is active: ALREADY_EXISTS.
        assert_already_exists(
            client
                .schedule_new_orchestration_with_options("SingleActivity", again())
                .await,
        );
        let state = wait_done(&mut client, &id).await;
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );

        // After completion: ALREADY_EXISTS too, and the instance is not restarted.
        assert_already_exists(
            client
                .schedule_new_orchestration_with_options("SingleActivity", again())
                .await,
        );
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );

        stop(guard).await;
    }

    #[tokio::test]
    async fn test_grpc_activity_retries() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("ActivityRetries", |ctx| async move {
                ctx.call_activity_with_options(
                    "FailActivity",
                    (),
                    ActivityOptions::new()
                        .with_retry_policy(RetryPolicy::new(3, Duration::from_millis(10))),
                )
                .await?;
                Ok(None)
            });
        worker.registry_mut().add_named_activity(
            "FailActivity",
            |_ctx: ActivityContext, _input: Option<String>| async move {
                Err(DurableTaskError::Other("activity failure".into()))
            },
        );
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                "ActivityRetries",
                None,
                Some("activity_retries".to_string()),
                None,
            )
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Failed);
        // With 3 max attempts there will be two retries with 10 millis delay before each
        let created = state.created_at.expect("created_at");
        let updated = state.last_updated_at.expect("last_updated_at");
        assert!(
            updated >= created + chrono::Duration::milliseconds(20),
            "last_updated_at {updated} < created_at {created} + 20ms"
        );

        stop(guard).await;
    }

    #[tokio::test]
    async fn test_grpc_child_workflow_retries() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Parent", |ctx| async move {
                let child_id = format!("{}_child", ctx.instance_id());
                ctx.call_sub_orchestrator_with_options(
                    "Child",
                    (),
                    SubOrchestratorOptions::new()
                        .with_instance_id(child_id)
                        .with_retry_policy(
                            RetryPolicy::new(3, Duration::from_millis(10))
                                .with_backoff_coefficient(2.0),
                        ),
                )
                .await?;
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("Child", |_ctx| async move {
                Err(DurableTaskError::Other("Child failed".into()))
            });
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("Parent", None, Some("workflow_retries".to_string()), None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Failed);
        // With 3 max attempts there will be two retries with 10 millis delay before each
        let created = state.created_at.expect("created_at");
        let updated = state.last_updated_at.expect("last_updated_at");
        assert!(
            updated >= created + chrono::Duration::milliseconds(20),
            "last_updated_at {updated} < created_at {created} + 20ms"
        );

        stop(guard).await;
    }

    #[cfg(feature = "opentelemetry")]
    fn init_tracing() -> opentelemetry_sdk::trace::InMemorySpanExporter {
        crate::span_exporter()
    }

    #[cfg(feature = "opentelemetry")]
    fn span_attr(
        span: &opentelemetry_sdk::trace::SpanData,
        key: &str,
    ) -> Option<opentelemetry::Value> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.clone())
    }

    #[cfg(feature = "opentelemetry")]
    #[tokio::test]
    async fn test_single_activity_task_span() {
        use opentelemetry::Value;
        use opentelemetry::trace::{Span as _, Tracer as _};

        let exporter = init_tracing();

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("SingleActivity_TestSpan", |ctx| async move {
                let input: String = ctx.input()?;
                ctx.call_activity("SayHello", input).await
            });
        worker.registry_mut().add_named_activity(
            "SayHello",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let name: String = serde_json::from_str(input.as_deref().unwrap_or("null"))?;
                // Go: tracer.Start(ctx.Context(), "activityChild_TestSpan"). The Rust
                // ActivityContext carries no OTel context; instead the SDK attaches
                // the activity span as the current OTel context while the activity
                // runs, so a span started from the current context is its child.
                let mut child =
                    opentelemetry::global::tracer("grpc-test").start("activityChild_TestSpan");
                child.end();
                Ok(Some(js(format!("Hello, {name}!"))))
            },
        );
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                "SingleActivity_TestSpan",
                Some(js("世界")),
                None,
                Some(chrono::Utc::now()),
            )
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            state.serialized_output.as_deref(),
            Some(r#""Hello, 世界!""#)
        );
        stop(guard).await;

        // Validate the exported OTel traces. The provider is process-global, so
        // restrict to spans belonging to this instance (plus the user child span).
        let iid = Value::from(id.clone());
        let all = exporter.get_finished_spans().unwrap();
        let spans: Vec<_> = all
            .into_iter()
            .filter(|s| {
                s.name == "activityChild_TestSpan"
                    || span_attr(s, "durabletask.task.instance_id").as_ref() == Some(&iid)
            })
            .collect();
        let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();

        // AssertWorkflowCreated("SingleActivity_TestSpan", id)
        let created_idx = spans
            .iter()
            .position(|s| s.name == "create_orchestration||SingleActivity_TestSpan")
            .unwrap_or_else(|| panic!("no create_orchestration span in {names:?}"));
        let created = &spans[created_idx];
        assert_eq!(
            span_attr(created, "durabletask.type"),
            Some(Value::from("orchestration"))
        );
        assert_eq!(
            span_attr(created, "durabletask.task.name"),
            Some(Value::from("SingleActivity_TestSpan"))
        );

        // AssertSpan("activityChild_TestSpan")
        let child_idx = spans
            .iter()
            .position(|s| s.name == "activityChild_TestSpan")
            .unwrap_or_else(|| panic!("no activityChild_TestSpan span in {names:?}"));

        // AssertActivity("SayHello", id, 0)
        let act_idx = spans
            .iter()
            .position(|s| s.name == "activity||SayHello")
            .unwrap_or_else(|| panic!("no activity||SayHello span in {names:?}"));
        let act = &spans[act_idx];
        assert_eq!(
            span_attr(act, "durabletask.type"),
            Some(Value::from("activity"))
        );
        assert_eq!(
            span_attr(act, "durabletask.task.name"),
            Some(Value::from("SayHello"))
        );
        assert_eq!(
            span_attr(act, "durabletask.task.task_id"),
            Some(Value::I64(0))
        );

        // AssertWorkflowExecuted("SingleActivity_TestSpan", id, "COMPLETED").
        // Go emits a single backend-side orchestration span; the Rust SDK emits one
        // per orchestration episode, so select the one carrying the final status.
        let exec_idx = spans
            .iter()
            .position(|s| {
                s.name == "orchestration||SingleActivity_TestSpan"
                    && span_attr(s, "durabletask.runtime_status") == Some(Value::from("COMPLETED"))
            })
            .unwrap_or_else(|| panic!("no COMPLETED orchestration span in {names:?}"));
        assert_eq!(
            span_attr(&spans[exec_idx], "durabletask.type"),
            Some(Value::from("orchestration"))
        );
        assert_eq!(
            span_attr(&spans[exec_idx], "durabletask.task.name"),
            Some(Value::from("SingleActivity_TestSpan"))
        );

        // Sequence (Go): created, activityChild, activity, workflowExecuted. Spans
        // are exported in end order; the Rust SDK's extra per-episode orchestration
        // spans (e.g. the first, activity-scheduling episode) may interleave, so
        // assert the relative order of the four spans Go asserts.
        assert!(
            created_idx < child_idx && child_idx < act_idx && act_idx < exec_idx,
            "unexpected span order: {names:?}"
        );

        // assert child-parent relationship
        assert_eq!(
            spans[child_idx].parent_span_id,
            act.span_context.span_id(),
            "activityChild_TestSpan must be parented by the activity span"
        );
    }

    #[tokio::test]
    async fn test_grpc_list_instance_ids() {
        use dapr_durabletask::client::ListInstanceIdsOptions;

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("foo", |_ctx| async move { Ok(Some(js(42))) });
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        for i in 0..5 {
            client
                .schedule_new_orchestration("foo", None, Some(i.to_string()), None)
                .await
                .unwrap();
        }
        stop(guard).await;

        let page = client
            .list_instance_ids(ListInstanceIdsOptions::new())
            .await
            .unwrap();
        for id in ["0", "1", "2", "3", "4"] {
            assert!(
                page.instance_ids.iter().any(|i| i == id),
                "{id} missing from {:?}",
                page.instance_ids
            );
        }

        // The sqlite sidecar ignores paging and returns everything in one page.
        assert!(page.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_grpc_get_instance_history() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("foo", |ctx| async move {
                ctx.call_activity("bar", ()).await?;
                ctx.call_activity("bar", ()).await?;
                ctx.call_activity("bar", ()).await?;
                Ok(Some(js(42)))
            });
        worker.registry_mut().add_named_activity(
            "bar",
            |_ctx: ActivityContext, _input: Option<String>| async move { Ok(Some(js(42))) },
        );
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("foo", None, None, None)
            .await
            .unwrap();
        // Go reads the history straight after scheduling; waiting for
        // completion first makes the full 12-event history deterministic.
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        stop(guard).await;

        let events = client.get_instance_history(&id).await.unwrap();
        assert_eq!(events.len(), 12, "unexpected history: {events:#?}");
    }

    #[tokio::test]
    async fn test_grpc_stateful_history_multi_turn() {
        const ACTIVITY_COUNT: i32 = 8;
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("AccumulateSequential", |ctx| async move {
                let mut total = 0i32;
                for _ in 0..ACTIVITY_COUNT {
                    let got = ctx.call_activity("AddOne", total).await?;
                    total = serde_json::from_str(got.as_deref().unwrap_or("0"))?;
                }
                Ok(Some(js(total)))
            });
        worker.registry_mut().add_named_activity(
            "AddOne",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let n: i32 = serde_json::from_str(input.as_deref().unwrap_or("0"))?;
                Ok(Some(js(n + 1)))
            },
        );
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("AccumulateSequential", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(
            state.serialized_output,
            Some(ACTIVITY_COUNT.to_string()),
            "sequential accumulation must be correct even though most turns were served as deltas"
        );
        stop(guard).await;
        // The worker advertises WORKER_CAPABILITY_STATEFUL_HISTORY by default
        // (pinned by the grpc_worker unit test
        // test_work_items_request_advertises_stateful_history), so most of the
        // turns above were delta sends rebuilt from the worker's cache.
        assert!(WorkerOptions::default().stateful_history);
        let hist = client.get_instance_history(&id).await.unwrap();
        assert!(!hist.is_empty());
        client.purge_orchestration(&id, false).await.unwrap();
        assert!(
            client
                .get_orchestration_state(&id, false)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_grpc_patched_workflow() {
        let patches1_found = Arc::new(Mutex::new(Vec::<bool>::new()));
        let patches2_found = Arc::new(Mutex::new(Vec::<bool>::new()));
        let run_number = Arc::new(AtomicU32::new(0));

        setup!(env);
        let mut worker = env.new_worker();
        let (p1, p2, rn) = (
            patches1_found.clone(),
            patches2_found.clone(),
            run_number.clone(),
        );
        worker
            .registry_mut()
            .add_named_orchestrator("Workflow", move |ctx| {
                let (p1, p2, rn) = (p1.clone(), p2.clone(), rn.clone());
                async move {
                    let current_run = rn.fetch_add(1, Ordering::SeqCst) + 1;
                    if current_run > 1 {
                        p1.lock().unwrap().push(ctx.is_patched("patch1"));
                    }
                    let _ = ctx.call_activity("SayHello", ()).await;
                    p2.lock().unwrap().push(ctx.is_patched("patch2"));
                    Ok(None)
                }
            });
        worker.registry_mut().add_named_activity(
            "SayHello",
            |_ctx: ActivityContext, _input: Option<String>| async move { Ok(Some(js("Hello"))) },
        );
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("Workflow", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(*patches1_found.lock().unwrap(), vec![false]);
        assert_eq!(*patches2_found.lock().unwrap(), vec![true]);

        stop(guard).await;
    }

    // ══════════════════════════════════════════════════════════════════════════════
    // tests/worker_async_test.go — concurrency / shutdown of the task worker,
    // ported to TaskHubGrpcWorker (WorkerOptions::max_concurrent_work_items).
    // ══════════════════════════════════════════════════════════════════════════════

    /// Gated activity bookkeeping shared between the test and the activity fn.
    struct Gate {
        permits: Arc<Semaphore>,
        /// Activities currently blocked waiting for a permit (Go: `inFlight()`).
        in_flight: AtomicUsize,
        /// Activities currently executing (from entry until they return).
        running: AtomicUsize,
        /// Highest `running` value ever observed.
        max_running: AtomicUsize,
        started: Mutex<Vec<String>>,
        completed: Mutex<Vec<String>>,
        failed: Mutex<Vec<String>>,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                permits: Arc::new(Semaphore::new(0)),
                in_flight: AtomicUsize::new(0),
                running: AtomicUsize::new(0),
                max_running: AtomicUsize::new(0),
                started: Mutex::default(),
                completed: Mutex::default(),
                failed: Mutex::default(),
            })
        }
        fn in_flight(&self) -> usize {
            self.in_flight.load(Ordering::SeqCst)
        }
        fn max_running(&self) -> usize {
            self.max_running.load(Ordering::SeqCst)
        }
        fn started(&self) -> Vec<String> {
            self.started.lock().unwrap().clone()
        }
        fn completed(&self) -> Vec<String> {
            self.completed.lock().unwrap().clone()
        }
        fn failed(&self) -> Vec<String> {
            self.failed.lock().unwrap().clone()
        }
    }

    /// Register activity "Gated": records its input, blocks until the test adds a
    /// permit, runs for a further 50 ms, then succeeds — or fails when
    /// `fail_first` and it is the first invocation.
    fn register_gated_activity(worker: &mut TaskHubGrpcWorker, gate: Arc<Gate>, fail_first: bool) {
        let invocations = Arc::new(AtomicUsize::new(0));
        worker.registry_mut().add_named_activity(
            "Gated",
            move |_ctx: ActivityContext, input: Option<String>| {
                let gate = gate.clone();
                let invocations = invocations.clone();
                async move {
                    let running = gate.running.fetch_add(1, Ordering::SeqCst) + 1;
                    gate.max_running.fetch_max(running, Ordering::SeqCst);
                    let n = invocations.fetch_add(1, Ordering::SeqCst);
                    let input = input.unwrap_or_default();
                    gate.started.lock().unwrap().push(input.clone());
                    gate.in_flight.fetch_add(1, Ordering::SeqCst);
                    gate.permits.acquire().await.unwrap().forget();
                    gate.in_flight.fetch_sub(1, Ordering::SeqCst);
                    // Stay "running" across a suspension point so that items
                    // executing concurrently would overlap and show in max_running.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let result = if fail_first && n == 0 {
                        gate.failed.lock().unwrap().push(input);
                        Err(DurableTaskError::Other("dummy processing error".into()))
                    } else {
                        gate.completed.lock().unwrap().push(input);
                        Ok(None)
                    };
                    gate.running.fetch_sub(1, Ordering::SeqCst);
                    result
                }
            },
        );
    }

    /// Register orchestrator "FanOutGated": schedules `n` "Gated" activities in
    /// parallel with inputs 1..=n and returns, per activity, whether it succeeded.
    fn register_fan_out_gated(worker: &mut TaskHubGrpcWorker) {
        worker
            .registry_mut()
            .add_named_orchestrator("FanOutGated", |ctx| async move {
                let n: i32 = ctx.input()?;
                let tasks: Vec<_> = (1..=n).map(|i| ctx.call_activity("Gated", i)).collect();
                let mut ok = Vec::new();
                for t in tasks {
                    ok.push(t.await.is_ok());
                }
                Ok(Some(js(ok)))
            });
    }

    #[tokio::test]
    async fn test_task_worker_async_concurrent_completions() {
        setup!(env);
        let gate = Gate::new();
        let mut worker = TaskHubGrpcWorker::with_options(
            &env.address,
            WorkerOptions::new().with_max_concurrent_work_items(4),
        );
        register_gated_activity(&mut worker, gate.clone(), false);
        register_fan_out_gated(&mut worker);
        let (token, handle) = spawn_worker(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("FanOutGated", Some(js(8)), None, None)
            .await
            .unwrap();

        // All four slots fill, and no fifth item is handed out while they are held.
        assert!(eventually(Duration::from_secs(2), || gate.in_flight() == 4).await);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(gate.in_flight(), 4);
        // Go: PendingWorkItems() has 4 left — i.e. only 4 of 8 were started.
        assert_eq!(gate.started().len(), 4);

        // Deliver the four completions concurrently.
        gate.permits.add_permits(4);

        // The released slots admit the remaining four items.
        assert!(
            eventually(Duration::from_secs(2), || gate.started().len() == 8
                && gate.in_flight() == 4)
            .await
        );
        gate.permits.add_permits(4);

        assert!(eventually(Duration::from_secs(2), || gate.completed().len() == 8).await);

        let state = wait_done(&mut client, &id).await;
        token.cancel();
        handle.await.unwrap().unwrap();

        assert_eq!(gate.completed().len(), 8);
        // Nothing abandoned / re-run, nothing left pending.
        assert_eq!(gate.started().len(), 8);
        // The four slots were never exceeded.
        assert_eq!(gate.max_running(), 4);
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output, Some(js(vec![true; 8])));
    }

    #[tokio::test]
    async fn test_task_worker_async_semaphore_released_on_error() {
        setup!(env);
        let gate = Gate::new();
        let mut worker = TaskHubGrpcWorker::with_options(
            &env.address,
            WorkerOptions::new().with_max_concurrent_work_items(1),
        );
        register_gated_activity(&mut worker, gate.clone(), true);
        register_fan_out_gated(&mut worker);
        let (token, handle) = spawn_worker(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("FanOutGated", Some(js(2)), None, None)
            .await
            .unwrap();

        assert!(eventually(Duration::from_secs(2), || gate.in_flight() == 1).await);

        // Failing the first item must release its slot, or the second item can
        // never start. (Go abandons the failed item; a failing Rust activity is
        // reported as a task failure — the slot-release behaviour is the same.)
        gate.permits.add_permits(1);

        assert!(
            eventually(Duration::from_secs(2), || gate.failed().len() == 1
                && gate.in_flight() == 1)
            .await
        );
        // Go: the failed item is `first`, the first item handed to the worker.
        // The sidecar dispatches the two fanned-out activities from separate
        // goroutines (backend/worker.go), so which input arrives first is not
        // fixed — compare against the worker's own receive order instead.
        let started = gate.started();
        assert_eq!(started.len(), 2, "{started:?}");
        assert_eq!(gate.failed(), vec![started[0].clone()]);

        gate.permits.add_permits(1);
        assert!(eventually(Duration::from_secs(2), || gate.completed().len() == 1).await);
        assert_eq!(gate.completed(), vec![started[1].clone()]);
        assert_eq!(gate.max_running(), 1);

        let state = wait_done(&mut client, &id).await;
        let failed_input: usize = started[0].parse().unwrap();
        let expected: Vec<bool> = (1..=2).map(|i| i != failed_input).collect();
        assert_eq!(state.serialized_output, Some(js(expected)));

        token.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_task_worker_async_shutdown_drains_in_flight() {
        setup!(env);
        let gate = Gate::new();
        let make_worker = |gate: Arc<Gate>| {
            let mut worker = TaskHubGrpcWorker::with_options(
                &env.address,
                WorkerOptions::new().with_max_concurrent_work_items(1),
            );
            register_gated_activity(&mut worker, gate, false);
            register_fan_out_gated(&mut worker);
            worker
        };
        let (token, handle) = spawn_worker(make_worker(gate.clone()));

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("FanOutGated", Some(js(1)), None, None)
            .await
            .unwrap();

        assert!(eventually(Duration::from_secs(2), || gate.in_flight() == 1).await);

        token.cancel();
        let mut handle = handle;
        // StopAndDrain must not return while a work item is still in flight.
        let early = tokio::time::timeout(Duration::from_millis(200), &mut handle).await;
        assert!(
            early.is_err(),
            "worker returned while a work item was still in flight"
        );

        gate.permits.add_permits(1);

        let drained = tokio::time::timeout(Duration::from_secs(2), &mut handle).await;
        assert!(
            drained.is_ok(),
            "worker did not finish draining after the in-flight item completed"
        );
        drained.unwrap().unwrap().unwrap();

        assert_eq!(gate.completed().len(), 1);

        // Go: AbandonedWorkItems() is empty. Rust equivalent: the drained result
        // reached the sidecar, so a fresh worker completes the orchestration
        // without re-running the activity.
        let guard = WorkerGuard::start(make_worker(gate.clone()));
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(gate.started().len(), 1, "activity must not be re-executed");
        stop(guard).await;
    }

    /// Register activity "Gated" whose processor watches the context: it blocks
    /// until the test adds a permit or the worker's shutdown cancels
    /// `ActivityContext`, in which case it gives up with an error (Go: settles
    /// with the context error) and bumps `cancelled`.
    fn register_cancellable_gated_activity(
        worker: &mut TaskHubGrpcWorker,
        gate: Arc<Gate>,
        cancelled: Arc<AtomicUsize>,
    ) {
        worker.registry_mut().add_named_activity(
            "Gated",
            move |ctx: ActivityContext, input: Option<String>| {
                let gate = gate.clone();
                let cancelled = cancelled.clone();
                async move {
                    let input = input.unwrap_or_default();
                    gate.started.lock().unwrap().push(input.clone());
                    gate.in_flight.fetch_add(1, Ordering::SeqCst);
                    let outcome = tokio::select! {
                        _ = ctx.cancelled() => None,
                        p = gate.permits.acquire() => Some(p),
                    };
                    gate.in_flight.fetch_sub(1, Ordering::SeqCst);
                    match outcome {
                        None => {
                            cancelled.fetch_add(1, Ordering::SeqCst);
                            Err(DurableTaskError::Other("context canceled".into()))
                        }
                        Some(p) => {
                            p.unwrap().forget();
                            gate.completed.lock().unwrap().push(input);
                            Ok(None)
                        }
                    }
                }
            },
        );
    }

    #[tokio::test]
    async fn test_task_worker_async_shutdown_cancels_via_context() {
        // Go: an in-flight item whose processor watches ctx.Done() is settled with
        // the context error when StopAndDrain is called, so the drain completes
        // without an external delivery and the item is abandoned
        // (AbandonedWorkItems()==1, CompletedWorkItems()==0).
        setup!(env);
        let gate = Gate::new();
        let cancelled = Arc::new(AtomicUsize::new(0));
        let make_worker = |gate: Arc<Gate>| {
            let mut worker = TaskHubGrpcWorker::with_options(
                &env.address,
                WorkerOptions::new().with_max_concurrent_work_items(1),
            );
            register_cancellable_gated_activity(&mut worker, gate, cancelled.clone());
            register_fan_out_gated(&mut worker);
            worker
        };
        let (token, handle) = spawn_worker(make_worker(gate.clone()));

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("FanOutGated", Some(js(1)), None, None)
            .await
            .unwrap();
        assert!(eventually(Duration::from_secs(5), || gate.in_flight() == 1).await);

        // StopAndDrain completes without any external delivery: the context
        // watcher settles the in-flight item.
        token.cancel();
        let drained = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(drained.is_ok(), "drain must not wait for a cancelled item");
        drained.unwrap().unwrap().unwrap();

        // CompletedWorkItems()==0; the item settled through the context.
        assert!(gate.completed().is_empty());
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(gate.started().len(), 1);

        // AbandonedWorkItems()==1: the cancellation was not recorded as an
        // activity failure; the sidecar redelivers the item to the next worker,
        // which completes it, and the orchestration sees a success.
        gate.permits.add_permits(Semaphore::MAX_PERMITS);
        let guard = WorkerGuard::start(make_worker(gate.clone()));
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output, Some(js(vec![true])));
        assert_eq!(gate.started().len(), 2, "the abandoned item is redelivered");
        assert_eq!(gate.completed().len(), 1);
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        stop(guard).await;
    }

    // ══════════════════════════════════════════════════════════════════════════════
    // tests/worker_test.go
    // ══════════════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_try_process_single_workflow_work_item_basic_flow() {
        setup!(env);
        let invocations = Arc::new(Mutex::new(Vec::<(String, String, bool)>::new()));
        let mut worker = env.new_worker();
        let inv = invocations.clone();
        worker
            .registry_mut()
            .add_named_orchestrator("MyOrch", move |ctx| {
                let inv = inv.clone();
                async move {
                    inv.lock().unwrap().push((
                        ctx.instance_id().to_string(),
                        ctx.name().to_string(),
                        ctx.is_replaying(),
                    ));
                    // Go: the executor returns an empty WorkflowResponse (no actions).
                    let _ = ctx.wait_for_external_event("never").await;
                    Ok(None)
                }
            });
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("MyOrch", None, Some("test123".to_string()), None)
            .await
            .unwrap();

        assert!(
            eventually(Duration::from_secs(1), || !invocations
                .lock()
                .unwrap()
                .is_empty())
            .await
        );
        tokio::time::sleep(Duration::from_millis(300)).await;

        // ExecuteWorkflow called exactly once, with no old events (non-replay).
        // Go's NewEvents layout (WorkflowStarted, ExecutionStarted) is not visible
        // to a Rust orchestrator.
        let inv = invocations.lock().unwrap().clone();
        assert_eq!(inv.len(), 1, "{inv:?}");
        assert_eq!(inv[0], ("test123".to_string(), "MyOrch".to_string(), false));
        // CompleteWorkflowWorkItem: the empty response was committed (a work item
        // that was never completed would leave the instance Pending).
        let state = get_state(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Running);

        stop(guard).await;
    }

    /// Orchestrator whose first invocation blocks its thread until `release` is
    /// set (so the worker can be killed with the work item in flight) and whose
    /// later invocations run `then`.
    fn register_blocking_first_attempt<F, Fut>(
        worker: &mut TaskHubGrpcWorker,
        name: &str,
        attempts: Arc<Mutex<Vec<bool>>>,
        release: Arc<AtomicBool>,
        then: F,
    ) where
        F: Fn(dapr_durabletask::task::OrchestrationContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = dapr_durabletask::api::Result<Option<String>>>
            + Send
            + 'static,
    {
        let then = Arc::new(then);
        worker
            .registry_mut()
            .add_named_orchestrator(name, move |ctx| {
                let attempts = attempts.clone();
                let release = release.clone();
                let then = then.clone();
                async move {
                    let n = {
                        let mut a = attempts.lock().unwrap();
                        a.push(ctx.is_replaying());
                        a.len()
                    };
                    if n == 1 {
                        while !release.load(Ordering::SeqCst) {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        return Err(DurableTaskError::Other("dummy error".into()));
                    }
                    then(ctx).await
                }
            });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_try_process_single_workflow_work_item_idempotency() {
        setup!(env);
        let attempts = Arc::new(Mutex::new(Vec::<bool>::new()));
        let release = Arc::new(AtomicBool::new(false));
        let make_worker = || {
            let mut worker = env.new_worker();
            register_blocking_first_attempt(
                &mut worker,
                "MyOrch",
                attempts.clone(),
                release.clone(),
                |ctx| async move {
                    // Go: second ExecuteWorkflow returns an empty response (no actions).
                    let _ = ctx.wait_for_external_event("never").await;
                    Ok(None)
                },
            );
            worker
        };

        // First attempt: the "executor" fails — in the Rust topology the worker
        // dies with the work item in flight, so the sidecar abandons it.
        let mut w1 = DetachedWorker::start(make_worker());
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                "MyOrch",
                None,
                Some("test123".to_string()),
                Some(chrono::Utc::now()),
            )
            .await
            .unwrap();
        assert!(
            eventually(Duration::from_secs(5), || attempts.lock().unwrap().len()
                == 1)
            .await
        );
        w1.kill();

        // Second attempt succeeds on a fresh worker.
        let guard = WorkerGuard::start(make_worker());
        assert!(
            eventually(Duration::from_secs(10), || attempts.lock().unwrap().len()
                == 2)
            .await
        );
        tokio::time::sleep(Duration::from_millis(300)).await;

        // ExecuteWorkflow called exactly twice, both times for the same first turn
        // (history WorkflowStarted, ExecutionStarted, WorkflowStarted: nothing was
        // committed by the failed attempt).
        assert_eq!(*attempts.lock().unwrap(), vec![false, false]);
        let state = get_state(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Running);

        release.store(true, Ordering::SeqCst);
        stop(guard).await;
    }

    #[tokio::test]
    async fn test_try_process_single_workflow_work_item_execution_started_and_completed() {
        setup!(env);
        let invocations = Arc::new(Mutex::new(Vec::<bool>::new()));
        let mut worker = env.new_worker();
        let inv = invocations.clone();
        worker
            .registry_mut()
            .add_named_orchestrator("MyWorkflow", move |ctx| {
                let inv = inv.clone();
                async move {
                    inv.lock().unwrap().push(ctx.is_replaying());
                    Ok(Some("done".to_string()))
                }
            });
        let guard = WorkerGuard::start(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("MyWorkflow", None, Some("test123".to_string()), None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;

        // ExecuteWorkflow called once, with an empty oldEvents list (non-replay).
        // The resulting ExecutionCompleted is observed as the Completed status and
        // output (Go's NewEvents layout is not visible from the SDK).
        assert_eq!(*invocations.lock().unwrap(), vec![false]);
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output.as_deref(), Some("done"));

        stop(guard).await;
    }

    #[tokio::test]
    async fn test_task_worker() {
        setup!(env);
        let gate = Gate::new();
        gate.permits.add_permits(Semaphore::MAX_PERMITS); // tp.UnblockProcessing()
        let mut worker = TaskHubGrpcWorker::with_options(
            &env.address,
            WorkerOptions::new().with_max_concurrent_work_items(1),
        );
        register_gated_activity(&mut worker, gate.clone(), false);
        register_fan_out_gated(&mut worker);
        let (token, handle) = spawn_worker(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("FanOutGated", Some(js(2)), None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;

        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        // Nothing abandoned (no failure, no re-delivery), nothing left pending.
        assert!(gate.failed().is_empty());
        let started = gate.started();
        let mut sorted = started.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["1".to_string(), "2".to_string()]);
        // Go: CompletedWorkItems() == [first, second] — with parallelism 1 the
        // items complete one at a time, in the order the worker received them.
        // The sidecar hands the two fanned-out activities to the stream from
        // separate goroutines (backend/worker.go), so the receive order itself is
        // not fixed; it is compared against the worker's own start order.
        assert_eq!(gate.completed(), started);
        assert_eq!(gate.max_running(), 1);

        token.cancel();
        let drained = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(
            drained.is_ok(),
            "worker stop and drain not finished within timeout"
        );
        drained.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_start_and_stop() {
        setup!(env);
        // tp.BlockProcessing(): the item blocks until its context is cancelled.
        let gate = Gate::new();
        let cancelled = Arc::new(AtomicUsize::new(0));
        let mut worker = TaskHubGrpcWorker::with_options(
            &env.address,
            WorkerOptions::new().with_max_concurrent_work_items(1),
        );
        register_cancellable_gated_activity(&mut worker, gate.clone(), cancelled.clone());
        register_fan_out_gated(&mut worker);
        let (token, handle) = spawn_worker(worker);

        let mut client = env.new_client().await;
        let _id = client
            .schedule_new_orchestration("FanOutGated", Some(js(2)), None, None)
            .await
            .unwrap();
        // Go: PendingWorkItems() has 1 left — one item in flight, one pending.
        assert!(eventually(Duration::from_secs(5), || gate.in_flight() == 1).await);
        assert_eq!(gate.started().len(), 1);

        // The blocked item is released by context cancellation, so StopAndDrain
        // returns within 1s.
        token.cancel();
        let drained = tokio::time::timeout(Duration::from_secs(1), handle).await;
        gate.permits.add_permits(Semaphore::MAX_PERMITS);
        assert!(
            drained.is_ok(),
            "worker stop and drain not finished within timeout"
        );
        drained.unwrap().unwrap().unwrap();

        // Go (unreachable after its early return, asserted here anyway): the
        // first item is abandoned (settled through its context), the second
        // stays pending (never started by this worker), nothing completed.
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(gate.started().len(), 1, "the pending item must not start");
        assert!(gate.completed().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_try_process_single_workflow_work_item_forces_terminate_when_executor_ignores_it()
    {
        // Go hands the runtime one work item carrying ExecutionStarted,
        // ExecutionTerminated and a later event (TaskCompleted), and an executor
        // that ignores the terminate and returns a CreateTimer action.
        //
        // Rust topology: the first delivery of ExecutionStarted is held by a worker
        // while the terminate and then an external event are queued, and that
        // worker is dropped so the sidecar abandons the work item. The retry then
        // carries ExecutionStarted, ExecutionTerminated, EventRaised in ONE batch
        // (terminate not last). The orchestrator code wants to consume the event
        // and schedule a timer, i.e. ignore the terminate.
        //
        // Not observable: whether the terminal state comes from the SDK's own
        // terminate handling or from the runtime forcing it (Go asserts the
        // latter); the end state and the absence of pending timers/tasks are.
        setup!(env);
        let attempts = Arc::new(Mutex::new(Vec::<bool>::new()));
        let release = Arc::new(AtomicBool::new(false));
        let saw_event = Arc::new(AtomicBool::new(false));
        let make_worker = || {
            let mut worker = env.new_worker();
            let saw = saw_event.clone();
            register_blocking_first_attempt(
                &mut worker,
                "MyOrch",
                attempts.clone(),
                release.clone(),
                move |ctx| {
                    let saw = saw.clone();
                    async move {
                        ctx.wait_for_external_event("after").await?;
                        saw.store(true, Ordering::SeqCst);
                        ctx.create_timer(Duration::from_secs(1)).await?;
                        Ok(None)
                    }
                },
            );
            worker
        };

        let mut w1 = DetachedWorker::start(make_worker());
        let mut client = env.new_client().await;
        // Scheduled start: skip the sidecar's wait-for-start (the first turn blocks).
        let id = client
            .schedule_new_orchestration(
                "MyOrch",
                None,
                Some("test123".to_string()),
                Some(chrono::Utc::now()),
            )
            .await
            .unwrap();
        assert!(
            eventually(Duration::from_secs(10), || attempts.lock().unwrap().len()
                == 1)
            .await
        );

        // The sidecar's TerminateInstance blocks until the instance completes, so
        // issue it in the background and give it time to enqueue the
        // ExecutionTerminated event before queueing the next event.
        let mut term_client = env.new_client().await;
        let term_id = id.clone();
        let terminate = tokio::spawn(async move {
            term_client
                .terminate_orchestration(&term_id, Some(r#""reason""#.to_string()), false)
                .await
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        client
            .raise_orchestration_event(&id, "after", None)
            .await
            .unwrap();

        // Abandon the in-flight first delivery; the retry goes to a fresh worker.
        w1.kill();
        let guard = WorkerGuard::start(make_worker());

        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Terminated);
        assert_eq!(state.serialized_output.as_deref(), Some(r#""reason""#));
        let term = tokio::time::timeout(TIMEOUT, terminate).await;
        assert!(
            matches!(term, Ok(Ok(Ok(())))),
            "terminate call failed: {term:?}"
        );
        // Both deliveries were fresh executions (nothing committed before), so
        // the terminating turn was the one that also carried ExecutionStarted.
        assert_eq!(*attempts.lock().unwrap(), vec![false, false]);
        // SDK side: the event queued after the terminate was not delivered to the
        // orchestrator, so it never reached its timer.
        assert!(!saw_event.load(Ordering::SeqCst));

        // No pending tasks/timers: nothing fires afterwards and re-runs the workflow.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(attempts.lock().unwrap().len(), 2);
        assert_eq!(
            get_state(&mut client, &id).await.runtime_status,
            OrchestrationStatus::Terminated
        );

        release.store(true, Ordering::SeqCst);
        stop(guard).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_try_process_single_workflow_work_item_terminate_beats_continue_as_new_from_executor()
     {
        setup!(env);
        let generations = Arc::new(Mutex::new(Vec::<i32>::new()));
        let mut worker = env.new_worker();
        let gens = generations.clone();
        worker
            .registry_mut()
            .add_named_orchestrator("MyOrch", move |ctx| {
                let gens = gens.clone();
                async move {
                    let generation: i32 = ctx.input()?;
                    let n = {
                        let mut g = gens.lock().unwrap();
                        g.push(generation);
                        g.len()
                    };
                    // Committed history will contain a child workflow.
                    let _child = ctx.call_sub_orchestrator("MyChild", (), Some("child1"));
                    if n == 1 {
                        // Hold the first turn's work-item lock so that the event
                        // and the terminate raised meanwhile are delivered to the
                        // next turn in ONE batch. (The sidecar pre-fetches a work
                        // item as soon as any event arrives, so queueing them with
                        // no worker connected would split the batch.)
                        std::thread::sleep(Duration::from_secs(2));
                    }
                    ctx.wait_for_external_event("go").await?;
                    // The "executor" returns ContinueAsNew in the same batch as
                    // the terminate.
                    ctx.continue_as_new(generation + 1, false);
                    Ok(None)
                }
            });
        worker
            .registry_mut()
            .add_named_orchestrator("MyChild", |ctx| async move {
                ctx.wait_for_external_event("never").await?;
                Ok(None)
            });
        // Run the worker on its own runtime: the first turn blocks a thread, which
        // must not stall the test's own client calls.
        let mut detached = DetachedWorker::start(worker);

        let mut client = env.new_client().await;
        // Scheduled start: skip the sidecar's wait-for-start (the first turn blocks).
        let id = client
            .schedule_new_orchestration(
                "MyOrch",
                Some(js(0)),
                Some("test123".to_string()),
                Some(chrono::Utc::now()),
            )
            .await
            .unwrap();
        assert!(eventually(TIMEOUT, || generations.lock().unwrap().len() == 1).await);

        // While the first turn is in flight, queue the event that triggers
        // ContinueAsNew and a recursive terminate.
        client
            .raise_orchestration_event(&id, "go", None)
            .await
            .unwrap();
        // The sidecar's TerminateInstance blocks until the instance completes, so
        // issue it in the background.
        let mut term_client = env.new_client().await;
        let term_id = id.clone();
        let terminate = tokio::spawn(async move {
            term_client
                .terminate_orchestration(&term_id, Some(r#""reason""#.to_string()), true)
                .await
        });

        let state = wait_done(&mut client, &id).await;
        assert!(state.runtime_status.is_terminal());
        assert_eq!(state.runtime_status, OrchestrationStatus::Terminated);
        assert_eq!(state.serialized_output.as_deref(), Some(r#""reason""#));
        let term = tokio::time::timeout(TIMEOUT, terminate).await;
        assert!(
            matches!(term, Ok(Ok(Ok(())))),
            "terminate call failed: {term:?}"
        );
        // The ContinueAsNew must not have started a new generation.
        assert!(
            !generations.lock().unwrap().contains(&1),
            "the ContinueAsNew must not have started a new generation: {:?}",
            generations.lock().unwrap()
        );
        // The recursive terminate must cascade to the child.
        let child = wait_done(&mut client, "child1").await;
        assert_eq!(child.runtime_status, OrchestrationStatus::Terminated);

        detached.kill();
    }

    // ══════════════════════════════════════════════════════════════════════════════
    // tests/taskhub_test.go
    // ══════════════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_task_hub_worker_starts_dependencies() {
        // Go: TaskHubWorker.Start starts the backend, the orchestration worker and
        // the activity worker. Rust equivalent: a started TaskHubGrpcWorker
        // processes both orchestration and activity work items.
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator(
                "Orch",
                |ctx| async move { ctx.call_activity("Act", 1).await },
            );
        worker.registry_mut().add_named_activity(
            "Act",
            |_ctx: ActivityContext, input: Option<String>| async move { Ok(input) },
        );
        let (token, handle) = spawn_worker(worker);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("Orch", None, None, None)
            .await
            .unwrap();
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output.as_deref(), Some("1"));

        token.cancel();
        let res = tokio::time::timeout(TIMEOUT, handle)
            .await
            .unwrap()
            .unwrap();
        assert!(res.is_ok(), "{res:?}");
    }

    #[tokio::test]
    async fn test_task_hub_worker_stops_dependencies() {
        // Go: TaskHubWorker.Shutdown stops the backend and drains both workers,
        // returning no error. Rust equivalent: cancelling the worker makes start()
        // return Ok(()), after which no more work items are processed.
        setup!(env);
        let invoked = Arc::new(AtomicUsize::new(0));
        let mut worker = env.new_worker();
        let inv = invoked.clone();
        worker
            .registry_mut()
            .add_named_orchestrator("Orch", move |_ctx| {
                let inv = inv.clone();
                async move {
                    inv.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                }
            });
        let (token, handle) = spawn_worker(worker);
        // Give the worker time to connect.
        tokio::time::sleep(Duration::from_millis(500)).await;

        token.cancel();
        let res = tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("idle worker: start() did not return within 10s after cancellation")
            .unwrap();
        assert!(res.is_ok(), "{res:?}");

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("Orch", None, None, Some(chrono::Utc::now()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert_eq!(
            get_state(&mut client, &id).await.runtime_status,
            OrchestrationStatus::Pending
        );
    }

    // ══════════════════════════════════════════════════════════════════════════════
    // tests/backend_test.go — backend state transitions, observed through the
    // client API. Inputs are JSON-encoded because the Rust SDK treats payloads as
    // JSON (Go stores the raw DEFAULT_INPUT string).
    // ══════════════════════════════════════════════════════════════════════════════

    /// Common validations from Go's workItemProcessingTestLogic.
    fn validate_common_metadata(
        state: &dapr_durabletask::api::OrchestrationState,
        start_time: chrono::DateTime<chrono::Utc>,
    ) {
        let created = state.created_at.expect("created_at");
        assert!(
            created >= start_time,
            "created {created} < start {start_time}"
        );
        assert_eq!(state.name, DEFAULT_NAME);
        assert_eq!(state.serialized_input, Some(js(DEFAULT_INPUT)));
    }

    fn start_time_now() -> chrono::DateTime<chrono::Utc> {
        use chrono::SubsecRound;
        chrono::Utc::now().trunc_subsecs(6)
    }

    #[tokio::test]
    async fn backend_test_new_workflow_work_item_single() {
        setup!(env);
        let expected_id = "myinstance";
        let mut client = env.new_client().await;
        client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some(expected_id.to_string()),
                Some(chrono::Utc::now()),
            )
            .await
            .unwrap();

        // Initial state: not started yet (Go: Name/Input -> ErrNotStarted, no events).
        let state = get_state(&mut client, expected_id).await;
        assert_eq!(state.instance_id, expected_id);
        assert_eq!(state.runtime_status, OrchestrationStatus::Pending);

        let mut worker = env.new_worker();
        let seen = register_recording_orchestrator(&mut worker);
        let guard = WorkerGuard::start(worker);
        wait_done(&mut client, expected_id).await;

        // One work item whose ExecutionStarted carries the instance ID, name and
        // input, delivered with an empty runtime state (no old events).
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen, vec![fresh_start(expected_id)]);
        stop(guard).await;
    }

    /// (instance ID, name, input, is_replaying) as seen by each invocation.
    type Seen = Arc<Mutex<Vec<(String, String, String, bool)>>>;

    /// Register a `DEFAULT_NAME` orchestrator that records what it was started with.
    fn register_recording_orchestrator(worker: &mut TaskHubGrpcWorker) -> Seen {
        let seen = Seen::default();
        let s = seen.clone();
        worker
            .registry_mut()
            .add_named_orchestrator(DEFAULT_NAME, move |ctx| {
                let s = s.clone();
                async move {
                    let input: String = ctx.input()?;
                    s.lock().unwrap().push((
                        ctx.instance_id().to_string(),
                        ctx.name().to_string(),
                        input,
                        ctx.is_replaying(),
                    ));
                    Ok(None)
                }
            });
        seen
    }

    fn fresh_start(id: &str) -> (String, String, String, bool) {
        (
            id.to_string(),
            DEFAULT_NAME.to_string(),
            DEFAULT_INPUT.to_string(),
            false,
        )
    }

    #[tokio::test]
    async fn backend_test_new_workflow_work_item_multiple() {
        const WORK_ITEMS: usize = 4;
        setup!(env);
        let mut client = env.new_client().await;

        // Create multiple work items up front
        for j in 0..WORK_ITEMS {
            client
                .schedule_new_orchestration(
                    DEFAULT_NAME,
                    Some(js(DEFAULT_INPUT)),
                    Some(format!("instance_{j}")),
                    Some(chrono::Utc::now()),
                )
                .await
                .unwrap();
        }
        for j in 0..WORK_ITEMS {
            let state = get_state(&mut client, &format!("instance_{j}")).await;
            assert_eq!(state.runtime_status, OrchestrationStatus::Pending);
        }

        let mut worker = TaskHubGrpcWorker::with_options(
            &env.address,
            WorkerOptions::new().with_max_concurrent_work_items(1),
        );
        let seen = register_recording_orchestrator(&mut worker);
        let guard = WorkerGuard::start(worker);
        for j in 0..WORK_ITEMS {
            wait_done(&mut client, &format!("instance_{j}")).await;
        }

        // Each instance yields exactly one fresh work item (ID, name, input, no
        // old events). Go additionally asserts FIFO fetch order from its sqlite
        // store; not observable here: the sidecar fetches in order but hands each
        // work item to the stream from its own goroutine (backend/worker.go), so
        // the delivery order is not fixed.
        let mut seen = seen.lock().unwrap().clone();
        seen.sort();
        let expected: Vec<_> = (0..WORK_ITEMS)
            .map(|j| fresh_start(&format!("instance_{j}")))
            .collect();
        assert_eq!(seen, expected);
        stop(guard).await;
    }

    #[tokio::test]
    async fn backend_test_complete_workflow() {
        const EXPECTED_RESULT: &str = "done!";
        const STACK: &str = "at backend_test_complete_workflow (e2e.rs)";
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator(DEFAULT_NAME, |ctx| async move {
                let id = ctx.instance_id().to_string();
                if id.ends_with("Failed") {
                    Err(DurableTaskError::TaskFailed {
                        message: "Kah-BOOOM!!".into(),
                        failure_details: Some(FailureDetails {
                            message: "Kah-BOOOM!!".into(),
                            error_type: "MyError".into(),
                            stack_trace: Some(STACK.into()),
                        }),
                    })
                } else if id.ends_with("Terminated") {
                    // Terminated is driven by the client (below).
                    ctx.wait_for_external_event("never").await?;
                    Ok(None)
                } else {
                    Ok(Some(EXPECTED_RESULT.to_string()))
                }
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        for expected_status in [
            OrchestrationStatus::Completed,
            OrchestrationStatus::Terminated,
            OrchestrationStatus::Failed,
        ] {
            let start_time = start_time_now();
            let id = format!("myinstance_{expected_status:?}");
            client
                .schedule_new_orchestration(
                    DEFAULT_NAME,
                    Some(js(DEFAULT_INPUT)),
                    Some(id.clone()),
                    None,
                )
                .await
                .unwrap();
            if expected_status == OrchestrationStatus::Terminated {
                client
                    .terminate_orchestration(&id, Some(EXPECTED_RESULT.to_string()), false)
                    .await
                    .unwrap();
            }
            let state = wait_done(&mut client, &id).await;

            validate_common_metadata(&state, start_time);
            assert_eq!(state.runtime_status, expected_status);
            assert!(state.runtime_status.is_terminal());
            assert!(!state.runtime_status.is_running());
            if expected_status == OrchestrationStatus::Failed {
                let fd = state.failure_details.expect("failure details");
                assert_eq!(fd.error_type, "MyError");
                assert_eq!(fd.message, "Kah-BOOOM!!");
                assert_eq!(fd.stack_trace.as_deref(), Some(STACK));
            } else {
                assert_eq!(state.serialized_output.as_deref(), Some(EXPECTED_RESULT));
            }
        }
        stop(guard).await;
    }

    #[tokio::test]
    async fn backend_test_schedule_activity_tasks() {
        const EXPECTED_INPUT: &str = "Hello, activity!";
        const EXPECTED_NAME: &str = "MyActivity";
        const EXPECTED_RESULT: &str = "42";

        setup!(env);
        let gate = Arc::new(Semaphore::new(0));
        let received = Arc::new(Mutex::new(Vec::<(i32, Option<String>)>::new()));
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator(DEFAULT_NAME, |ctx| async move {
                ctx.call_activity(EXPECTED_NAME, EXPECTED_INPUT).await
            });
        let (g, r) = (gate.clone(), received.clone());
        worker.registry_mut().add_named_activity(
            EXPECTED_NAME,
            move |ctx: ActivityContext, input: Option<String>| {
                let (g, r) = (g.clone(), r.clone());
                async move {
                    r.lock().unwrap().push((ctx.task_id(), input));
                    g.acquire().await.unwrap().forget();
                    Ok(Some(EXPECTED_RESULT.to_string()))
                }
            },
        );
        let guard = WorkerGuard::start(worker);

        let start_time = start_time_now();
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some("myinstance".to_string()),
                None,
            )
            .await
            .unwrap();

        // There should be an activity work item with the scheduled name/input.
        assert!(
            eventually(Duration::from_secs(10), || !received
                .lock()
                .unwrap()
                .is_empty())
            .await
        );
        let state = get_state(&mut client, &id).await;
        validate_common_metadata(&state, start_time);
        // Make sure the metadata reflects that the workflow is running
        assert!(state.runtime_status.is_running());
        let (task_id, input) = received.lock().unwrap()[0].clone();
        assert_eq!(input, Some(js(EXPECTED_INPUT)));

        // Complete the activity: a TaskCompleted event for the scheduled task id
        // (Go: 7, chosen by the test; Rust assigns sequential ids — first is 0)
        // carrying the result is delivered to the orchestration.
        gate.add_permits(1);
        let state = wait_done(&mut client, &id).await;
        assert_eq!(task_id, 0);
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(state.serialized_output.as_deref(), Some(EXPECTED_RESULT));

        stop(guard).await;
    }

    #[tokio::test]
    async fn backend_test_schedule_timer_tasks() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator(DEFAULT_NAME, |ctx| async move {
                let timer_duration = Duration::from_secs(1);
                let expected_fire_at = ctx.current_utc_datetime()
                    + chrono::Duration::from_std(timer_duration).unwrap();
                ctx.create_timer(timer_duration).await?;
                let fired_turn = ctx.current_utc_datetime();
                Ok(Some(js((
                    expected_fire_at.timestamp_micros(),
                    fired_turn.timestamp_micros(),
                ))))
            });
        let guard = WorkerGuard::start(worker);

        let start_time = start_time_now();
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some("myinstance".to_string()),
                None,
            )
            .await
            .unwrap();

        // Make sure the metadata reflects that the workflow is running
        let state = get_state(&mut client, &id).await;
        validate_common_metadata(&state, start_time);
        assert!(state.runtime_status.is_running());

        // The timer work item becomes visible once the fire-at time passes.
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        let (fire_at, fired_turn): (i64, i64) =
            serde_json::from_str(state.serialized_output.as_deref().unwrap()).unwrap();
        // Go asserts TimerFired.FireAt == expectedFireAt exactly; the fire-at time
        // is not exposed to Rust orchestrators, so assert the turn that observed
        // the fired timer is not earlier than the requested fire-at time.
        assert!(
            fired_turn >= fire_at,
            "timer observed at {fired_turn} before fire_at {fire_at}"
        );
        assert!(chrono::Utc::now() >= start_time + chrono::Duration::seconds(1));

        stop(guard).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn backend_test_abandon_workflow_work_item() {
        let iid = "abc";
        setup!(env);
        let attempts = Arc::new(Mutex::new(Vec::<bool>::new()));
        let release = Arc::new(AtomicBool::new(false));
        let make_worker = || {
            let mut worker = env.new_worker();
            register_blocking_first_attempt(
                &mut worker,
                DEFAULT_NAME,
                attempts.clone(),
                release.clone(),
                |_ctx| async move { Ok(None) },
            );
            worker
        };

        let mut client = env.new_client().await;
        client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some(iid.to_string()),
                Some(chrono::Utc::now()),
            )
            .await
            .unwrap();

        let mut w1 = DetachedWorker::start(make_worker());
        assert!(
            eventually(Duration::from_secs(5), || attempts.lock().unwrap().len()
                == 1)
            .await
        );
        // Abandon the work item (worker drops its stream with the item in flight).
        w1.kill();

        // Make sure it can be fetched again immediately after abandoning.
        let guard = WorkerGuard::start(make_worker());
        assert!(
            eventually(Duration::from_secs(5), || attempts.lock().unwrap().len()
                == 2)
            .await,
            "abandoned workflow work item was not re-delivered"
        );
        let state = wait_done(&mut client, iid).await;
        assert_eq!(state.instance_id, iid);
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

        release.store(true, Ordering::SeqCst);
        stop(guard).await;
    }

    #[tokio::test]
    async fn backend_test_abandon_activity_work_item() {
        setup!(env);
        let deliveries = Arc::new(Mutex::new(Vec::<(i32, Option<String>)>::new()));
        let make_worker = || {
            let mut worker = env.new_worker();
            worker
                .registry_mut()
                .add_named_orchestrator(DEFAULT_NAME, |ctx| async move {
                    ctx.call_activity("MyActivity", ()).await
                });
            let d = deliveries.clone();
            worker.registry_mut().add_named_activity(
                "MyActivity",
                move |ctx: ActivityContext, input: Option<String>| {
                    let d = d.clone();
                    async move {
                        let n = {
                            let mut d = d.lock().unwrap();
                            d.push((ctx.task_id(), input));
                            d.len()
                        };
                        if n == 1 {
                            // Hold the first delivery until the worker is killed.
                            std::future::pending::<()>().await;
                        }
                        Ok(None)
                    }
                },
            );
            worker
        };

        let mut w1 = DetachedWorker::start(make_worker());
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some("myinstance".to_string()),
                None,
            )
            .await
            .unwrap();

        assert!(
            eventually(Duration::from_secs(10), || deliveries.lock().unwrap().len()
                == 1)
            .await
        );
        // Make sure the metadata reflects that the workflow is running
        assert!(
            get_state(&mut client, &id)
                .await
                .runtime_status
                .is_running()
        );
        // Abandon the activity work item.
        w1.kill();

        // Re-fetch the abandoned activity work item.
        let guard = WorkerGuard::start(make_worker());
        assert!(
            eventually(Duration::from_secs(10), || deliveries.lock().unwrap().len()
                == 2)
            .await,
            "abandoned activity work item was not re-delivered"
        );
        let d = deliveries.lock().unwrap().clone();
        // Go: name "MyActivity", EventId 123 (test-chosen), nil input. Rust: same
        // task id as the original delivery, and no input.
        assert_eq!(d[1].0, d[0].0);
        assert_eq!(d[1].1, None);
        let state = wait_done(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);

        stop(guard).await;
    }

    #[tokio::test]
    async fn backend_test_get_non_existing_metadata() {
        setup!(env);
        let mut client = env.new_client().await;
        // Go: GetWorkflowMetadata("bogus") -> api.ErrInstanceNotFound. The Rust
        // client maps "not found" to Ok(None).
        let res = client.get_orchestration_state("bogus", false).await;
        assert!(
            matches!(res, Ok(None)),
            "expected Ok(None) for a missing instance, got {res:?}"
        );
    }

    #[tokio::test]
    async fn backend_test_get_workflow_metadata_parent_app_id() {
        // Go creates the child directly with ParentInstance{parent-instance,
        // AppID: parent-app}. Through the SDK the child is a real
        // sub-orchestration; the sidecar stamps the parent's app ID with its own
        // app ID, which the durabletask-go sidecar hard-codes to "example".
        const INSTANCE_ID: &str = "child-instance";
        const PARENT_ID: &str = "parent-instance";
        const PARENT_APP_ID: &str = "example";

        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("child", |_ctx| async move { Ok(None) });
        worker
            .registry_mut()
            .add_named_orchestrator("parent", |ctx| async move {
                ctx.call_sub_orchestrator("child", (), Some(INSTANCE_ID))
                    .await
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        client
            .schedule_new_orchestration("parent", None, Some(PARENT_ID.into()), None)
            .await
            .unwrap();
        let parent = wait_done(&mut client, PARENT_ID).await;
        assert_eq!(parent.runtime_status, OrchestrationStatus::Completed);

        let child = get_state(&mut client, INSTANCE_ID).await;
        assert_eq!(child.parent_instance_id.as_deref(), Some(PARENT_ID));
        assert_eq!(child.parent_app_id.as_deref(), Some(PARENT_APP_ID));
        stop(guard).await;
    }

    #[tokio::test]
    async fn backend_test_get_workflow_metadata_no_parent() {
        setup!(env);
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some("top-level-instance".into()),
                // With no worker connected, an instance that is due immediately
                // blocks the sidecar's metadata reads; a future start time keeps
                // it pending and readable.
                Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            )
            .await
            .unwrap();
        let state = get_state(&mut client, &id).await;
        // Go: empty ParentInstanceId and nil ParentAppId.
        assert!(state.parent_instance_id.is_none(), "{state:?}");
        assert!(state.parent_app_id.is_none(), "{state:?}");
    }

    #[tokio::test]
    async fn backend_test_get_workflow_metadata_started_at() {
        use dapr_durabletask_proto::history_event::EventType;

        setup!(env);
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some("startedat-instance".into()),
                // A short future start time keeps the instance pending (and its
                // metadata readable) until the worker below is connected.
                Some(chrono::Utc::now() + chrono::Duration::seconds(2)),
            )
            .await
            .unwrap();

        // Pre-execution (no worker yet): StartedAt stays unset.
        let state = get_state(&mut client, &id).await;
        assert_eq!(state.runtime_status, OrchestrationStatus::Pending);
        assert!(
            state.started_at.is_none(),
            "started_at should be None before the first work item is processed"
        );

        let before_process = chrono::Utc::now();
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator(DEFAULT_NAME, |_ctx| async move { Ok(None) });
        let guard = WorkerGuard::start(worker);
        let state = wait_done(&mut client, &id).await;
        let after_process = chrono::Utc::now();
        stop(guard).await;

        let started_at = state
            .started_at
            .expect("started_at missing after execution");
        assert!(
            started_at >= before_process && started_at <= after_process,
            "started_at {started_at} not within [{before_process}, {after_process}]"
        );

        // StartedAt is never earlier than the ExecutionStarted event.
        let history = client.get_instance_history(&id).await.unwrap();
        let exec_started = history
            .iter()
            .find(|e| matches!(e.event_type, Some(EventType::ExecutionStarted(_))))
            .and_then(|e| e.timestamp.as_ref())
            .expect("no ExecutionStarted event in history");
        let exec_started =
            chrono::DateTime::from_timestamp(exec_started.seconds, exec_started.nanos as u32)
                .unwrap();
        assert!(
            started_at >= exec_started,
            "started_at {started_at} should be >= ExecutionStarted {exec_started}"
        );
    }

    #[tokio::test]
    async fn backend_test_purge_workflow_state() {
        const EXPECTED_RESULT: &str = "done!";
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator(DEFAULT_NAME, |_ctx| async move {
                Ok(Some(EXPECTED_RESULT.to_string()))
            });
        let guard = WorkerGuard::start(worker);

        let start_time = start_time_now();
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration(
                DEFAULT_NAME,
                Some(js(DEFAULT_INPUT)),
                Some("myinstance".to_string()),
                None,
            )
            .await
            .unwrap();

        // Make sure the workflow actually completed
        let state = wait_done(&mut client, &id).await;
        validate_common_metadata(&state, start_time);
        assert!(state.runtime_status.is_terminal());
        assert!(!state.runtime_status.is_running());

        // Purge the workflow state
        let res = client.purge_orchestration(&id, false).await;
        assert!(res.is_ok(), "purge failed: {res:?}");

        // The metadata should be gone
        assert!(
            client
                .get_orchestration_state(&id, true)
                .await
                .unwrap()
                .is_none(),
            "state still present after purge"
        );

        // Attempting to purge again should fail with api.ErrInstanceNotFound
        match client.purge_orchestration(&id, false).await {
            Err(e) => assert!(
                e.to_string().contains("no such instance exists"),
                "unexpected error: {e}"
            ),
            Ok(n) => panic!("second purge should fail with instance-not-found, got Ok({n})"),
        }

        stop(guard).await;
    }
}

mod backend {
    //! Runtime-side behaviour (the durabletask-go backend runs as the sidecar)
    //! that constrains what the SDK must emit, how it must replay, or what a
    //! client observes. Each case is checked as an `OrchestrationExecutor` unit
    //! test (the SDK's half of the contract) and/or as an end-to-end test
    //! against the sidecar (the observable outcome); the two share helpers, so
    //! they live together.

    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use dapr_durabletask::api::{
        HistoryPropagationScope, OrchestrationState, OrchestrationStatus, PropagatedHistory,
    };
    use dapr_durabletask::api::{Result as DtResult, RetryPolicy};
    use dapr_durabletask::client::TaskHubGrpcClient;
    use dapr_durabletask::task::{
        ActivityContext, ActivityOptions, OrchestrationContext, SubOrchestratorOptions, when_all,
    };
    use dapr_durabletask::worker::{
        OrchestrationExecutor, OrchestratorFn, TaskHubGrpcWorker, WorkerOptions,
    };
    use dapr_durabletask_proto as proto;
    use dapr_durabletask_proto::create_timer_action::Origin;
    use dapr_durabletask_proto::history_event::EventType;
    use dapr_durabletask_proto::workflow_action::WorkflowActionType as Wat;

    use crate::harness::{self, WorkerGuard};
    use crate::setup;

    const TIMEOUT: Duration = Duration::from_secs(30);

    // ===========================================================================
    // Executor helpers
    // ===========================================================================

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    fn ts(dt: chrono::DateTime<chrono::Utc>) -> proto::prost_types::Timestamp {
        proto::prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }
    }

    fn ev_at(
        event_id: i32,
        at: chrono::DateTime<chrono::Utc>,
        et: EventType,
    ) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id,
            timestamp: Some(ts(at)),
            router: None,
            event_type: Some(et),
        }
    }

    fn ev(event_id: i32, et: EventType) -> proto::HistoryEvent {
        ev_at(event_id, now(), et)
    }

    fn ws() -> proto::HistoryEvent {
        ev(-1, EventType::WorkflowStarted(Default::default()))
    }

    fn es(name: &str, input: Option<&str>) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: name.to_string(),
                input: input.map(str::to_string),
                workflow_instance: Some(proto::WorkflowInstance {
                    instance_id: "test-instance".to_string(),
                    execution_id: Some(uuid::Uuid::new_v4().to_string()),
                }),
                ..Default::default()
            }),
        )
    }

    fn es_child(name: &str, parent_task_id: i32) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: name.to_string(),
                workflow_instance: Some(proto::WorkflowInstance {
                    instance_id: "child_id".to_string(),
                    execution_id: Some(uuid::Uuid::new_v4().to_string()),
                }),
                parent_instance: Some(proto::ParentInstanceInfo {
                    task_scheduled_id: parent_task_id,
                    name: Some("Parent".to_string()),
                    workflow_instance: Some(proto::WorkflowInstance {
                        instance_id: "parent_id".to_string(),
                        execution_id: None,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
    }

    fn sched(id: i32, name: &str) -> proto::HistoryEvent {
        sched_exec(id, name, "")
    }

    fn sched_exec(id: i32, name: &str, exec_id: &str) -> proto::HistoryEvent {
        ev(
            id,
            EventType::TaskScheduled(proto::TaskScheduledEvent {
                name: name.to_string(),
                task_execution_id: exec_id.to_string(),
                ..Default::default()
            }),
        )
    }

    fn completed(task_id: i32, result: Option<&str>) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::TaskCompleted(proto::TaskCompletedEvent {
                task_scheduled_id: task_id,
                result: result.map(str::to_string),
                ..Default::default()
            }),
        )
    }

    fn failure(msg: &str) -> Option<proto::TaskFailureDetails> {
        Some(proto::TaskFailureDetails {
            error_type: "Error".to_string(),
            error_message: msg.to_string(),
            ..Default::default()
        })
    }

    fn failed(task_id: i32) -> proto::HistoryEvent {
        failed_exec(task_id, "")
    }

    fn failed_exec(task_id: i32, exec_id: &str) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::TaskFailed(proto::TaskFailedEvent {
                task_scheduled_id: task_id,
                failure_details: failure("task failed"),
                task_execution_id: exec_id.to_string(),
                ..Default::default()
            }),
        )
    }

    fn timer_created(id: i32) -> proto::HistoryEvent {
        ev(
            id,
            EventType::TimerCreated(proto::TimerCreatedEvent {
                fire_at: Some(ts(now())),
                ..Default::default()
            }),
        )
    }

    fn timer_fired(timer_id: i32) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::TimerFired(proto::TimerFiredEvent {
                fire_at: Some(ts(now())),
                timer_id,
            }),
        )
    }

    fn child_created(id: i32, name: &str, instance_id: &str) -> proto::HistoryEvent {
        ev(
            id,
            EventType::ChildWorkflowInstanceCreated(proto::ChildWorkflowInstanceCreatedEvent {
                instance_id: instance_id.to_string(),
                name: name.to_string(),
                ..Default::default()
            }),
        )
    }

    fn child_completed(task_id: i32, result: Option<&str>) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::ChildWorkflowInstanceCompleted(proto::ChildWorkflowInstanceCompletedEvent {
                task_scheduled_id: task_id,
                result: result.map(str::to_string),
                ..Default::default()
            }),
        )
    }

    fn child_failed(task_id: i32) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::ChildWorkflowInstanceFailed(proto::ChildWorkflowInstanceFailedEvent {
                task_scheduled_id: task_id,
                failure_details: failure("child failed"),
                ..Default::default()
            }),
        )
    }

    fn raised(name: &str, input: Option<&str>) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::EventRaised(proto::EventRaisedEvent {
                name: name.to_string(),
                input: input.map(str::to_string),
            }),
        )
    }

    fn suspended() -> proto::HistoryEvent {
        ev(-1, EventType::ExecutionSuspended(Default::default()))
    }

    fn resumed() -> proto::HistoryEvent {
        ev(-1, EventType::ExecutionResumed(Default::default()))
    }

    fn terminated(output: &str) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::ExecutionTerminated(proto::ExecutionTerminatedEvent {
                input: Some(output.to_string()),
                recurse: false,
            }),
        )
    }

    fn stalled(reason: proto::StalledReason, description: &str) -> proto::HistoryEvent {
        ev(
            -1,
            EventType::ExecutionStalled(proto::ExecutionStalledEvent {
                reason: reason as i32,
                description: Some(description.to_string()),
            }),
        )
    }

    fn orch<F, Fut>(f: F) -> OrchestratorFn
    where
        F: Fn(OrchestrationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = DtResult<Option<String>>> + Send + 'static,
    {
        Arc::new(move |ctx| Box::pin(f(ctx)))
    }

    async fn run(
        orch_fn: &OrchestratorFn,
        old: Vec<proto::HistoryEvent>,
        new: Vec<proto::HistoryEvent>,
    ) -> proto::WorkflowResponse {
        OrchestrationExecutor::execute(
            orch_fn,
            "test-instance",
            old,
            new,
            String::new(),
            &WorkerOptions::default(),
            None,
        )
        .await
        .expect("executor returned an error")
    }

    fn completes(r: &proto::WorkflowResponse) -> Vec<&proto::CompleteWorkflowAction> {
        r.actions
            .iter()
            .filter_map(|a| match &a.workflow_action_type {
                Some(Wat::CompleteWorkflow(c)) => Some(c),
                _ => None,
            })
            .collect()
    }

    fn single_complete(r: &proto::WorkflowResponse) -> &proto::CompleteWorkflowAction {
        let c = completes(r);
        assert_eq!(
            c.len(),
            1,
            "expected exactly one CompleteWorkflow action: {r:?}"
        );
        c[0]
    }

    fn schedules(r: &proto::WorkflowResponse) -> Vec<(i32, &proto::ScheduleTaskAction)> {
        r.actions
            .iter()
            .filter_map(|a| match &a.workflow_action_type {
                Some(Wat::ScheduleTask(s)) => Some((a.id, s)),
                _ => None,
            })
            .collect()
    }

    fn timers(r: &proto::WorkflowResponse) -> Vec<(i32, &proto::CreateTimerAction)> {
        r.actions
            .iter()
            .filter_map(|a| match &a.workflow_action_type {
                Some(Wat::CreateTimer(t)) => Some((a.id, t)),
                _ => None,
            })
            .collect()
    }

    fn children(r: &proto::WorkflowResponse) -> Vec<(i32, &proto::CreateChildWorkflowAction)> {
        r.actions
            .iter()
            .filter_map(|a| match &a.workflow_action_type {
                Some(Wat::CreateChildWorkflow(c)) => Some((a.id, c)),
                _ => None,
            })
            .collect()
    }

    /// The instance ID the runtime's applier gives a child: the action's own ID,
    /// or, when the SDK leaves it empty (as durabletask-go's SDK also does), the
    /// ID `runtimestate.Applier` derives with
    /// `helpers.GenerateChildWorkflowInstanceID`: `<parent>:` plus the last four
    /// hex digits of `0x10000 + action id`. The Go tests read the child's ID back
    /// after the applier filled it in; the Rust tests have no applier, so they
    /// derive it the same way.
    fn applied_instance_id((id, action): (i32, &proto::CreateChildWorkflowAction)) -> String {
        if action.instance_id.is_empty() {
            let hex = format!("{:x}", 0x10000_i64 + i64::from(id));
            format!("test-instance:{}", &hex[hex.len() - 4..])
        } else {
            action.instance_id.clone()
        }
    }

    fn status(s: proto::OrchestrationStatus) -> i32 {
        s as i32
    }

    /// Kinds of durable work used by table-driven dedup tests.
    #[derive(Clone, Copy, Debug)]
    enum Kind {
        Task,
        Timer,
        Child,
    }

    fn schedule_kind(
        ctx: &OrchestrationContext,
        kind: Kind,
    ) -> dapr_durabletask::task::CompletableTask {
        match kind {
            Kind::Task => ctx.call_activity("act", ()),
            Kind::Timer => ctx.create_timer(Duration::from_secs(3600)),
            Kind::Child => ctx.call_sub_orchestrator("child", (), Some("child-instance")),
        }
    }

    fn scheduled_event(kind: Kind, id: i32) -> proto::HistoryEvent {
        match kind {
            Kind::Task => sched(id, "act"),
            Kind::Timer => timer_created(id),
            Kind::Child => child_created(id, "child", "child-instance"),
        }
    }

    fn resolution(kind: Kind, id: i32) -> proto::HistoryEvent {
        match kind {
            Kind::Task => completed(id, None),
            Kind::Timer => timer_fired(id),
            Kind::Child => child_completed(id, None),
        }
    }

    fn scheduled_ids(r: &proto::WorkflowResponse, kind: Kind) -> Vec<i32> {
        match kind {
            Kind::Task => schedules(r).into_iter().map(|(id, _)| id).collect(),
            Kind::Timer => timers(r).into_iter().map(|(id, _)| id).collect(),
            Kind::Child => children(r).into_iter().map(|(id, _)| id).collect(),
        }
    }

    // ===========================================================================
    // E2E helpers
    // ===========================================================================

    fn always() -> bool {
        std::hint::black_box(true)
    }

    async fn wait_status(
        client: &mut TaskHubGrpcClient,
        id: &str,
        want: OrchestrationStatus,
    ) -> OrchestrationState {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            if let Ok(Some(s)) = client.get_orchestration_state(id, true).await
                && s.runtime_status == want
            {
                return s;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {id} to reach {want:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn complete(client: &mut TaskHubGrpcClient, id: &str) -> OrchestrationState {
        client
            .wait_for_orchestration_completion(id, true, Some(TIMEOUT))
            .await
            .expect("wait_for_orchestration_completion failed")
            .expect("no state returned")
    }

    /// Spawns a worker that can be killed abruptly (dropping its work-item
    /// stream) by aborting the returned handle.
    fn spawn_abortable(worker: TaskHubGrpcWorker) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let _ = worker
                .start(tokio_util::sync::CancellationToken::new())
                .await;
        })
    }

    /// Registers the orchestrators shared by the recursive terminate/purge tests.
    fn register_tree(worker: &mut TaskHubGrpcWorker) {
        let reg = worker.registry_mut();
        reg.add_named_orchestrator("tree_waiter", |ctx| async move {
            ctx.wait_for_external_event("never").await
        });
        reg.add_named_orchestrator("tree_quick", |_ctx| async move { Ok(None) });
        reg.add_named_orchestrator("tree_parent_waiting", |ctx| async move {
            let child = format!("{}-child", ctx.instance_id());
            ctx.call_sub_orchestrator("tree_waiter", (), Some(&child))
                .await
        });
        reg.add_named_orchestrator("tree_parent_quick", |ctx| async move {
            let child = format!("{}-child", ctx.instance_id());
            ctx.call_sub_orchestrator("tree_quick", (), Some(&child))
                .await
        });
        reg.add_named_orchestrator("tree_parent_two", |ctx| async move {
            let id = ctx.instance_id();
            // Each child is itself a parent waiting on a grandchild, so a cascade
            // that drops the `recurse` flag stops one level short.
            let old = ctx.call_sub_orchestrator(
                "tree_parent_waiting",
                (),
                Some(&format!("{id}-child-old")),
            );
            // Turn boundary: the two ChildWorkflowInstanceCreated events are
            // committed by different turns.
            ctx.call_activity("tree_noop", ()).await?;
            let new = ctx.call_sub_orchestrator(
                "tree_parent_waiting",
                (),
                Some(&format!("{id}-child-new")),
            );
            when_all(vec![old, new]).await?;
            Ok(None)
        });
        reg.add_named_activity(
            "tree_noop",
            |_ctx: ActivityContext, _in| async move { Ok(None) },
        );
    }

    // ===========================================================================
    // backend/backend_test.go
    // ===========================================================================

    #[tokio::test]
    async fn backend_get_child_workflow_instances_no_router_treated_as_local() {
        // Go feeds getChildWorkflowInstances a ChildWorkflowInstanceCreated event
        // with a nil router (legacy history) and asserts it is enumerated as a
        // local child with a nil router. Through the SDK the nil-router event
        // itself cannot be produced: the applier stamps every action's router
        // with the parent's SourceAppID before recording it. What is portable:
        // (1) the SDK schedules a same-app child with no router at all, and
        // (2) the backend treats that child as local, so a recursive terminate
        // of the parent reaches it.
        let f = orch(|ctx| async move {
            ctx.call_sub_orchestrator("tree_waiter", (), Some("no-router-child"))
                .await
        });
        let resp = run(&f, vec![], vec![ws(), es("tree_parent", None)]).await;
        assert_eq!(resp.actions.len(), 1, "{resp:?}");
        assert!(
            resp.actions[0].router.is_none(),
            "a same-app child must be scheduled without a router: {resp:?}"
        );
        assert_eq!(children(&resp)[0].1.instance_id, "no-router-child");

        setup!(env);
        let mut worker = env.new_worker();
        register_tree(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = "no-router-parent";
        client
            .schedule_new_orchestration("tree_parent_waiting", None, Some(id.into()), None)
            .await
            .unwrap();
        let child = format!("{id}-child");
        wait_status(&mut client, &child, OrchestrationStatus::Running).await;

        // A child scheduled without a router is a local child: a recursive
        // terminate of the parent must reach it.
        client
            .terminate_orchestration(id, None, true)
            .await
            .unwrap();
        wait_status(&mut client, id, OrchestrationStatus::Terminated).await;
        wait_status(&mut client, &child, OrchestrationStatus::Terminated).await;
        guard.stop().await;
    }

    #[tokio::test]
    async fn backend_get_child_workflow_instances_router_without_namespace_is_local() {
        // SDK side: a cross-app child carries a router with a target app ID and
        // no namespace; the backend preserves exactly this router.
        let f = orch(|ctx| async move {
            ctx.call_sub_orchestrator_with_app_id("child", (), Some("cross-app-child"), "other-app")
                .await
        });
        let resp = run(&f, vec![], vec![ws(), es("parent", None)]).await;
        assert_eq!(resp.actions.len(), 1);
        let action = &resp.actions[0];
        let router = action.router.as_ref().expect("router must be set");
        assert_eq!(router.target_app_id.as_deref(), Some("other-app"));
        assert_eq!(router.target_app_namespace, None);
        let c = children(&resp);
        assert_eq!(c[0].1.instance_id, "cross-app-child");
    }

    #[tokio::test]
    async fn backend_purge_workflow_state_recursive_skips_missing_same_app_child() {
        setup!(env);
        let mut worker = env.new_worker();
        register_tree(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = "missing-child-parent";
        client
            .schedule_new_orchestration("tree_parent_quick", None, Some(id.into()), None)
            .await
            .unwrap();
        complete(&mut client, id).await;
        let child = format!("{id}-child");
        // Purge the child out-of-band first.
        assert_eq!(client.purge_orchestration(&child, false).await.unwrap(), 1);

        let count = client
            .purge_orchestration(id, true)
            .await
            .expect("recursive purge must not abort on an already-purged child");
        assert_eq!(
            count, 1,
            "parent must still be purged when child is already gone"
        );
        assert!(
            client
                .get_orchestration_state(id, false)
                .await
                .unwrap()
                .is_none()
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn backend_purge_workflow_state_recursive_requires_completed_without_force() {
        setup!(env);
        let mut worker = env.new_worker();
        register_tree(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = "running-root";
        client
            .schedule_new_orchestration("tree_waiter", None, Some(id.into()), None)
            .await
            .unwrap();
        wait_status(&mut client, id, OrchestrationStatus::Running).await;

        let res = client.purge_orchestration(id, true).await;
        // Go: require.ErrorIs(err, api.ErrNotCompleted) ("orchestration has not
        // yet completed"); the sidecar relays that error's text.
        let err = res.expect_err("purging an in-progress workflow without force must fail");
        assert!(
            err.to_string().contains("has not yet completed"),
            "expected ErrNotCompleted, got {err}"
        );
        let st = client.get_orchestration_state(id, false).await.unwrap();
        assert_eq!(
            st.expect("instance must not be purged").runtime_status,
            OrchestrationStatus::Running
        );
        client
            .terminate_orchestration(id, None, false)
            .await
            .unwrap();
        guard.stop().await;
    }

    #[tokio::test]
    async fn backend_append_cascade_terminate_messages_emits_pending_message_per_child() {
        // Go asserts one ExecutionTerminated pending message per child, each
        // carrying the parent's reason and `recurse = true`, and that a child's
        // cross-namespace router is copied onto its message. Observable here:
        // every child is terminated with the parent's reason, and the recurse
        // flag travels with the message (each child's own child, a grandchild of
        // the root, is terminated too).
        //
        // Not portable: Go places "child-new" in NewEvents (created in the same
        // turn as the terminate); through a well-behaved SDK a terminated turn
        // emits only the completion, so both creation events are already
        // committed when the terminate is processed. The "other-ns" router
        // assertion needs a runtime with more than one namespace; the
        // standalone sidecar has one. The SDK side (children scheduled with a
        // namespace carry it on their router) is covered by
        // backend_get_child_workflow_instances_preserves_cross_namespace_router.
        setup!(env);
        let mut worker = env.new_worker();
        register_tree(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = "cascade-parent";
        client
            .schedule_new_orchestration("tree_parent_two", None, Some(id.into()), None)
            .await
            .unwrap();
        let old = format!("{id}-child-old");
        let new = format!("{id}-child-new");
        let grandchildren = [format!("{old}-child"), format!("{new}-child")];
        for inst in [&old, &new].into_iter().chain(&grandchildren) {
            wait_status(&mut client, inst, OrchestrationStatus::Running).await;
        }

        client
            .terminate_orchestration(id, Some("\"reason\"".into()), true)
            .await
            .unwrap();
        let st = wait_status(&mut client, id, OrchestrationStatus::Terminated).await;
        assert_eq!(st.serialized_output.as_deref(), Some("\"reason\""));
        for inst in [&old, &new].into_iter().chain(&grandchildren) {
            let st = wait_status(&mut client, inst, OrchestrationStatus::Terminated).await;
            assert_eq!(
                st.serialized_output.as_deref(),
                Some("\"reason\""),
                "{inst}: the cascade must carry the parent's reason"
            );
        }
        guard.stop().await;
    }

    #[tokio::test]
    async fn backend_append_cascade_terminate_messages_non_recursive_is_no_op() {
        setup!(env);
        let mut worker = env.new_worker();
        register_tree(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = "nonrec-parent";
        client
            .schedule_new_orchestration("tree_parent_waiting", None, Some(id.into()), None)
            .await
            .unwrap();
        let child = format!("{id}-child");
        wait_status(&mut client, &child, OrchestrationStatus::Running).await;

        client
            .terminate_orchestration(id, None, false)
            .await
            .unwrap();
        wait_status(&mut client, id, OrchestrationStatus::Terminated).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let st = client
            .get_orchestration_state(&child, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.runtime_status, OrchestrationStatus::Running);
        client
            .terminate_orchestration(&child, None, false)
            .await
            .unwrap();
        guard.stop().await;
    }

    // ===========================================================================
    // backend/activity_panic_test.go, orchestration_panic_test.go
    // ===========================================================================

    #[tokio::test]
    async fn activity_panic_activity_processor_inline_executor_panic() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("panic_act_orch", |ctx| async move {
                ctx.call_activity("boom", ()).await
            });
        worker.registry_mut().add_named_activity(
            "boom",
            |_ctx: ActivityContext, _in: Option<String>| async move {
                if always() {
                    panic!("activity exploded");
                }
                Ok(Some("\"unreachable\"".to_string()))
            },
        );
        worker
            .registry_mut()
            .add_named_orchestrator("healthy_orch", |_ctx| async move {
                Ok(Some("\"ok\"".to_string()))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = client
            .schedule_new_orchestration("panic_act_orch", None, None, None)
            .await
            .unwrap();
        // A panicking activity must surface as an explicit failure, never
        // produce a result nor strand the orchestration.
        let st = client
            .wait_for_orchestration_completion(&id, true, Some(Duration::from_secs(15)))
            .await;
        let st = st
            .expect("orchestration never completed: activity panic was not reported")
            .expect("no state");
        assert_eq!(st.runtime_status, OrchestrationStatus::Failed);
        // The sidecar reports "no output" as an empty string.
        assert!(
            st.serialized_output
                .as_deref()
                .unwrap_or_default()
                .is_empty(),
            "a panicked execution must not produce a result: {st:?}"
        );
        let failure = st.failure_details.expect("failure details");
        assert_eq!(failure.error_type, "TaskActivityPanic");
        assert_eq!(failure.message, "panic: activity exploded");

        // The worker survives.
        let id2 = client
            .schedule_new_orchestration("healthy_orch", None, None, None)
            .await
            .unwrap();
        assert_eq!(
            complete(&mut client, &id2).await.runtime_status,
            OrchestrationStatus::Completed
        );
        guard.stop().await;
    }

    #[tokio::test]
    async fn orchestration_panic_workflow_turn_execute_inline_executor_panic() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("panic_orch", |_ctx| async move {
                if always() {
                    panic!("workflow exploded");
                }
                Ok(None)
            });
        worker
            .registry_mut()
            .add_named_orchestrator("healthy_orch", |_ctx| async move {
                Ok(Some("\"ok\"".to_string()))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        // Go: the turn's done callback receives an error containing "workflow
        // executor panicked" instead of the panic escaping. In the SDK the
        // executor is the component that must recover: the panicked turn is
        // reported back as an explicit failure naming the panic, never as a
        // success and never left unreported (which would wedge the instance).
        let bad = client
            .schedule_new_orchestration("panic_orch", None, None, None)
            .await
            .unwrap();
        let st = client
            .wait_for_orchestration_completion(&bad, true, Some(Duration::from_secs(15)))
            .await
            .expect("panicked orchestrator turn was never reported")
            .expect("no state");
        assert_eq!(st.runtime_status, OrchestrationStatus::Failed, "{st:?}");
        let failure = st.failure_details.expect("failure details");
        assert_eq!(failure.error_type, "OrchestratorPanic");
        assert_eq!(failure.message, "panic: workflow exploded");

        // The panic must not crash the worker: other work still completes.
        let good = client
            .schedule_new_orchestration("healthy_orch", None, None, None)
            .await
            .unwrap();
        let st = complete(&mut client, &good).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(st.serialized_output.as_deref(), Some("\"ok\""));
        guard.stop().await;
    }

    // ===========================================================================
    // backend/orchestration_terminate_test.go
    // ===========================================================================

    #[tokio::test]
    async fn orchestration_terminate_workflow_turn_apply_response_forces_terminate_when_executor_ignores_it()
     {
        let f = orch(|ctx| async move {
            ctx.create_timer(Duration::from_secs(1)).await?;
            Ok(None)
        });
        // Go: the backend forces the termination when the executor ignores the
        // terminate and returns a CreateTimer. The SDK half of that contract is
        // not to ignore it: the turn carries only the Terminated completion, so
        // nothing is left pending (no tasks, timers or messages).
        let resp = run(
            &f,
            vec![],
            vec![ws(), es("MyOrch", None), terminated("\"reason\"")],
        )
        .await;
        assert_eq!(resp.actions.len(), 1, "only the completion: {resp:?}");
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Terminated)
        );
        assert_eq!(c.result.as_deref(), Some("\"reason\""));
    }

    #[tokio::test]
    async fn orchestration_terminate_workflow_turn_apply_response_terminate_beats_continue_as_new()
    {
        // Go strips the executor's ContinuedAsNew completion so no new
        // generation starts. The SDK must not emit it in the first place.
        let f = orch(|ctx| async move {
            ctx.continue_as_new("restart", false);
            Ok(None)
        });
        let resp = run(
            &f,
            vec![],
            vec![ws(), es("MyOrch", None), terminated("\"reason\"")],
        )
        .await;
        assert_eq!(resp.actions.len(), 1, "only the completion: {resp:?}");
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Terminated)
        );
        assert_eq!(c.result.as_deref(), Some("\"reason\""));
    }

    // ===========================================================================
    // backend/executor_stateful_test.go
    // ===========================================================================

    #[tokio::test]
    async fn executor_stateful_apply_stateful_history_non_capable_stream_unchanged() {
        // The Rust worker advertises no WORKER_CAPABILITY_STATEFUL_HISTORY, so
        // every turn must carry the full history; a multi-turn orchestration
        // only replays correctly if it does.
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("full_hist_orch", |ctx| async move {
                let mut acc = Vec::new();
                for i in 0..5 {
                    let r = ctx.call_activity("echo", i).await?;
                    acc.push(r.unwrap_or_default());
                }
                Ok(Some(serde_json::to_string(&acc).unwrap()))
            });
        worker.registry_mut().add_named_activity(
            "echo",
            |_ctx: ActivityContext, input: Option<String>| async move { Ok(input) },
        );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("full_hist_orch", None, None, None)
            .await
            .unwrap();
        let st = complete(&mut client, &id).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            st.serialized_output.as_deref(),
            Some(r#"["0","1","2","3","4"]"#)
        );
        guard.stop().await;
    }

    // ===========================================================================
    // backend/executor_stream_test.go
    // ===========================================================================

    #[tokio::test]
    async fn executor_stream_get_work_items_per_instance_order_across_streams() {
        const INSTANCES: usize = 6;
        const TURNS: i64 = 5;
        setup!(env);
        let seen: Arc<Mutex<HashMap<String, Vec<i64>>>> = Arc::default();
        let mut guards = Vec::new();
        for _ in 0..3 {
            let mut worker = env.new_worker();
            worker
                .registry_mut()
                .add_named_orchestrator("order_orch", |ctx| async move {
                    let mut out = Vec::new();
                    for turn in 0..TURNS {
                        let r = ctx.call_activity("order_record", turn).await?;
                        out.push(serde_json::from_str::<i64>(r.as_deref().unwrap()).unwrap());
                    }
                    Ok(Some(serde_json::to_string(&out).unwrap()))
                });
            let seen = seen.clone();
            worker.registry_mut().add_named_activity(
                "order_record",
                move |ctx: ActivityContext, input: Option<String>| {
                    let seen = seen.clone();
                    async move {
                        let turn: i64 = serde_json::from_str(input.as_deref().unwrap()).unwrap();
                        seen.lock()
                            .unwrap()
                            .entry(ctx.orchestration_id().to_string())
                            .or_default()
                            .push(turn);
                        Ok(input)
                    }
                },
            );
            guards.push(WorkerGuard::start(worker));
        }
        let mut client = env.new_client().await;
        let mut ids = Vec::new();
        for _ in 0..INSTANCES {
            ids.push(
                client
                    .schedule_new_orchestration("order_orch", None, None, None)
                    .await
                    .unwrap(),
            );
        }
        let expected: Vec<i64> = (0..TURNS).collect();
        for id in &ids {
            let st = complete(&mut client, id).await;
            assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
            assert_eq!(
                st.serialized_output.as_deref(),
                Some(serde_json::to_string(&expected).unwrap().as_str())
            );
            assert_eq!(
                seen.lock().unwrap().get(id),
                Some(&expected),
                "turn order for {id}"
            );
        }
        for g in guards {
            g.stop().await;
        }
    }

    fn register_hang_once(
        worker: &mut TaskHubGrpcWorker,
        worker_id: usize,
        first: Arc<AtomicBool>,
        tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<usize>>>>,
    ) {
        worker
            .registry_mut()
            .add_named_orchestrator("hang_once_orch", |ctx| async move {
                ctx.call_activity("hang_once", ()).await
            });
        worker.registry_mut().add_named_activity(
            "hang_once",
            move |_ctx: ActivityContext, _in: Option<String>| {
                let first = first.clone();
                let tx = tx.clone();
                async move {
                    if !first.swap(true, Ordering::SeqCst) {
                        if let Some(tx) = tx.lock().unwrap().take() {
                            let _ = tx.send(worker_id);
                        }
                        std::future::pending::<()>().await;
                    }
                    Ok(Some(format!("\"worker-{worker_id}\"")))
                }
            },
        );
    }

    #[tokio::test]
    async fn executor_stream_get_work_items_send_failure_recovers_work_item() {
        setup!(env);
        let first = Arc::new(AtomicBool::new(false));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));

        let mut worker_a = env.new_worker();
        register_hang_once(&mut worker_a, 1, first.clone(), tx.clone());
        let handle_a = spawn_abortable(worker_a);

        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("hang_once_orch", None, None, None)
            .await
            .unwrap();
        let got = tokio::time::timeout(TIMEOUT, rx).await.unwrap().unwrap();
        assert_eq!(got, 1);

        // The stream holding the in-flight item dies; the item must be
        // recovered and delivered to the next worker instead of being dropped.
        handle_a.abort();
        let _ = handle_a.await;

        let mut worker_b = env.new_worker();
        register_hang_once(&mut worker_b, 2, first, tx);
        let guard_b = WorkerGuard::start(worker_b);
        let st = complete(&mut client, &id).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(st.serialized_output.as_deref(), Some("\"worker-2\""));
        guard_b.stop().await;
    }

    #[tokio::test]
    async fn executor_stream_get_work_items_dispatch_not_blocked_during_slow_send() {
        setup!(env);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("slow_orch", |ctx| async move {
                ctx.call_activity("slow_act", ()).await
            });
        worker
            .registry_mut()
            .add_named_orchestrator("fast_orch", |ctx| async move {
                ctx.call_activity("fast_act", ()).await
            });
        let g = gate.clone();
        worker.registry_mut().add_named_activity(
            "slow_act",
            move |_ctx: ActivityContext, _in: Option<String>| {
                let g = g.clone();
                async move {
                    let _permit = g.acquire().await.unwrap();
                    Ok(Some("\"slow\"".to_string()))
                }
            },
        );
        worker.registry_mut().add_named_activity(
        "fast_act",
        |_ctx: ActivityContext, _in: Option<String>| async move { Ok(Some("\"fast\"".to_string())) },
    );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let slow = client
            .schedule_new_orchestration("slow_orch", None, None, None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut fast = Vec::new();
        for _ in 0..20 {
            fast.push(
                client
                    .schedule_new_orchestration("fast_orch", None, None, None)
                    .await
                    .unwrap(),
            );
        }
        for id in &fast {
            let st = complete(&mut client, id).await;
            assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        }
        let st = client
            .get_orchestration_state(&slow, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            st.runtime_status,
            OrchestrationStatus::Running,
            "slow item still in flight"
        );
        gate.add_permits(1);
        let st = complete(&mut client, &slow).await;
        assert_eq!(st.serialized_output.as_deref(), Some("\"slow\""));
        guard.stop().await;
    }

    #[tokio::test]
    async fn executor_stream_get_work_items_disconnect_redelivers_buffered_item() {
        setup!(env);
        let first = Arc::new(AtomicBool::new(false));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));

        // Two streams connected at once; whichever takes the item dies, the
        // surviving stream must deliver it.
        let mut handles = Vec::new();
        for worker_id in 1..=2 {
            let mut w = env.new_worker();
            register_hang_once(&mut w, worker_id, first.clone(), tx.clone());
            handles.push(Some(spawn_abortable(w)));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("hang_once_orch", None, None, None)
            .await
            .unwrap();
        let dying = tokio::time::timeout(TIMEOUT, rx).await.unwrap().unwrap();
        let h = handles[dying - 1].take().unwrap();
        h.abort();
        let _ = h.await;

        let survivor = 3 - dying;
        let st = complete(&mut client, &id).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(
            st.serialized_output.as_deref(),
            Some(format!("\"worker-{survivor}\"").as_str())
        );
        for h in handles.into_iter().flatten() {
            h.abort();
            let _ = h.await;
        }
    }

    // ===========================================================================
    // backend/runtimestate/runtimestate_test.go
    // ===========================================================================

    #[tokio::test]
    async fn runtimestate_add_event_stalled_cleared_on_new_event() {
        // Go asserts the backend's Stalled marker / STALLED status is cleared by
        // the next event. The status half needs an instance that can actually
        // stall, which the Rust SDK cannot produce (it never emits
        // WorkflowVersionNotAvailableAction; see
        // runtimestate_add_event_stalled_set_from_old_events). The SDK half: a
        // replayed ExecutionStalled marker does not block the workflow, so each
        // of Go's clearing events lets execution continue past it.
        //
        // Go's fourth case, ExecutionCompleted, is not an SDK input: the backend
        // never dispatches a completed instance, and durabletask-go's SDK rejects
        // that event ("don't know how to handle event").
        let f = orch(|ctx| async move { ctx.call_activity("test-activity", ()).await });
        let old = || {
            vec![
                ws(),
                es("test-workflow", None),
                stalled(proto::StalledReason::PatchMismatch, "test stall"),
            ]
        };
        // ExecutionSuspended: the workflow is not resumed while suspended.
        let resp = run(&f, old(), vec![suspended()]).await;
        assert!(resp.actions.is_empty(), "suspended after stall: {resp:?}");
        // ExecutionResumed: execution continues past the stall.
        let resp = run(&f, old(), vec![resumed()]).await;
        let s = schedules(&resp);
        assert_eq!(resp.actions.len(), 1, "{resp:?}");
        assert_eq!(s[0].1.name, "test-activity");
        // TaskScheduled: the recorded schedule retires the replayed action...
        let resp = run(&f, old(), vec![sched(0, "test-activity")]).await;
        assert!(resp.actions.is_empty(), "{resp:?}");
        // ...and its completion is delivered: the workflow is running, not stuck.
        let resp = run(
            &f,
            old(),
            vec![sched(0, "test-activity"), completed(0, Some("\"ok\""))],
        )
        .await;
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(c.result.as_deref(), Some("\"ok\""));
    }

    #[tokio::test]
    async fn runtimestate_add_event_stalled_cleared_by_subsequent_old_event() {
        let f = orch(|ctx| async move { ctx.call_activity("test-activity", ()).await });
        let old = vec![
            ws(),
            es("test-workflow", None),
            stalled(proto::StalledReason::PatchMismatch, "stalled"),
            sched(0, "test-activity"),
        ];
        // Still running: the scheduled activity is not re-emitted.
        let resp = run(&f, old.clone(), vec![]).await;
        assert!(resp.actions.is_empty(), "{resp:?}");
        let resp = run(&f, old, vec![completed(0, Some("\"ok\""))]).await;
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(c.result.as_deref(), Some("\"ok\""));
    }

    /// Orchestrator awaiting a single piece of work of `kind` at seq 0 and
    /// returning its outcome as a label.
    fn single_work_orch(kind: Kind) -> OrchestratorFn {
        orch(move |ctx| async move {
            match schedule_kind(&ctx, kind).await {
                Ok(v) => Ok(Some(v.unwrap_or_else(|| "\"fired\"".to_string()))),
                Err(_) => Ok(Some("\"failed\"".to_string())),
            }
        })
    }

    #[tokio::test]
    async fn runtimestate_add_event_duplicate_task_completed() {
        let cases: Vec<(&str, Kind, proto::HistoryEvent, proto::HistoryEvent, &str)> = vec![
            (
                "completed/completed",
                Kind::Task,
                completed(0, Some("\"first\"")),
                completed(0, Some("\"dup\"")),
                "\"first\"",
            ),
            (
                "completed/failed",
                Kind::Task,
                completed(0, Some("\"first\"")),
                failed(0),
                "\"first\"",
            ),
            (
                "failed/completed",
                Kind::Task,
                failed(0),
                completed(0, Some("\"dup\"")),
                "\"failed\"",
            ),
            (
                "timer/timer",
                Kind::Timer,
                timer_fired(0),
                timer_fired(0),
                "\"fired\"",
            ),
            (
                "child/child",
                Kind::Child,
                child_completed(0, Some("\"first\"")),
                child_completed(0, Some("\"dup\"")),
                "\"first\"",
            ),
            (
                "child/child-failed",
                Kind::Child,
                child_completed(0, Some("\"first\"")),
                child_failed(0),
                "\"first\"",
            ),
        ];
        for (name, kind, first, dup, want) in cases {
            let f = single_work_orch(kind);
            let old = vec![ws(), es("wf", None), scheduled_event(kind, 0)];
            let resp = run(&f, old, vec![first, dup]).await;
            let c = single_complete(&resp);
            assert_eq!(
                c.workflow_status,
                status(proto::OrchestrationStatus::Completed),
                "{name}"
            );
            assert_eq!(
                c.result.as_deref(),
                Some(want),
                "{name}: duplicate must not override first"
            );
            assert_eq!(resp.actions.len(), 1, "{name}");
        }
    }

    #[tokio::test]
    async fn runtimestate_add_event_distinct_ids_and_kinds_are_not_duplicates() {
        let f = orch(|ctx| async move {
            let a = ctx.call_activity("a", ());
            let b = ctx.call_activity("b", ());
            let t1 = ctx.create_timer(Duration::from_secs(1));
            let t2 = ctx.create_timer(Duration::from_secs(2));
            let c1 = ctx.call_sub_orchestrator("c", (), Some("c1"));
            let c2 = ctx.call_sub_orchestrator("c", (), Some("c2"));
            let a = a.await?;
            let b = b.await?;
            t1.await?;
            t2.await?;
            let c1 = c1.await?;
            let c2 = c2.await?;
            Ok(Some(serde_json::to_string(&vec![a, b, c1, c2]).unwrap()))
        });
        let old = vec![
            ws(),
            es("wf", None),
            sched(0, "a"),
            sched(1, "b"),
            timer_created(2),
            timer_created(3),
            child_created(4, "c", "c1"),
            child_created(5, "c", "c2"),
        ];
        let new = vec![
            completed(0, Some("1")),
            completed(1, Some("2")),
            timer_fired(2),
            timer_fired(3),
            child_completed(4, Some("3")),
            child_completed(5, Some("4")),
        ];
        let resp = run(&f, old, new).await;
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(c.result.as_deref(), Some(r#"["1","2","3","4"]"#));

        // Go also reuses the SAME ids across kinds (task 1/2, timer 1/2, child
        // 1/2) and accepts all of them: the task, timer and child namespaces are
        // independent. In the SDK an id belongs to one kind, so the same property
        // reads: a resolution of another kind carrying a pending id neither
        // resolves nor shadows the work at that id.
        let f = orch(|ctx| async move {
            let a = ctx.call_activity("a", ());
            let t = ctx.create_timer(Duration::from_secs(1));
            let c = ctx.call_sub_orchestrator("c", (), Some("c1"));
            let a = a.await?;
            t.await?;
            let c = c.await?;
            Ok(Some(serde_json::to_string(&vec![a, c]).unwrap()))
        });
        let old = vec![
            ws(),
            es("wf", None),
            sched(0, "a"),
            timer_created(1),
            child_created(2, "c", "c1"),
        ];
        let cross_kind = || {
            vec![
                timer_fired(0),
                child_completed(0, Some("\"child@0\"")),
                completed(1, Some("\"task@1\"")),
                child_completed(1, Some("\"child@1\"")),
                timer_fired(2),
                completed(2, Some("\"task@2\"")),
            ]
        };
        let resp = run(&f, old.clone(), cross_kind()).await;
        assert!(
            resp.actions.is_empty(),
            "resolutions of another kind must not resolve pending work: {resp:?}"
        );
        let mut new = cross_kind();
        new.extend([
            completed(0, Some("\"a\"")),
            timer_fired(1),
            child_completed(2, Some("\"c\"")),
        ]);
        let resp = run(&f, old, new).await;
        let c = single_complete(&resp);
        assert_eq!(c.result.as_deref(), Some(r#"["\"a\"","\"c\""]"#));
    }

    #[tokio::test]
    async fn runtimestate_add_event_duplicate_against_old_events() {
        let cases: Vec<(&str, Kind, proto::HistoryEvent, proto::HistoryEvent, &str)> = vec![
            (
                "completed/completed",
                Kind::Task,
                completed(0, None),
                completed(0, None),
                "ok",
            ),
            (
                "completed/failed",
                Kind::Task,
                completed(0, None),
                failed(0),
                "ok",
            ),
            (
                "failed/completed",
                Kind::Task,
                failed(0),
                completed(0, None),
                "failed",
            ),
            (
                "timer/timer",
                Kind::Timer,
                timer_fired(0),
                timer_fired(0),
                "ok",
            ),
            (
                "child/child-failed",
                Kind::Child,
                child_completed(0, None),
                child_failed(0),
                "ok",
            ),
        ];
        for (name, kind, committed, dup, want) in cases {
            let f = orch(move |ctx| async move {
                let label = match schedule_kind(&ctx, kind).await {
                    Ok(_) => "ok",
                    Err(_) => "failed",
                };
                ctx.call_activity("next", label).await
            });
            let old = vec![ws(), es("wf", None), scheduled_event(kind, 0), committed];
            let resp = run(&f, old, vec![dup]).await;
            let s = schedules(&resp);
            assert_eq!(resp.actions.len(), 1, "{name}: {resp:?}");
            assert_eq!(s.len(), 1, "{name}");
            assert_eq!(s[0].0, 1, "{name}");
            assert_eq!(
                s[0].1.input.as_deref(),
                Some(format!("\"{want}\"").as_str()),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn runtimestate_add_event_new_orchestration_runtime_state_drops_history_duplicates() {
        let f = orch(|ctx| async move {
            let a = ctx.call_activity("a", ()).await?;
            let b = ctx.call_activity("b", ()).await?;
            Ok(Some(serde_json::to_string(&vec![a, b]).unwrap()))
        });
        let old = vec![
            ws(),
            es("wf", None),
            sched(0, "a"),
            completed(0, Some("\"a\"")),
            completed(0, Some("\"dup\"")),
            sched(1, "b"),
        ];
        let resp = run(&f, old, vec![completed(1, Some("\"b\""))]).await;
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(c.result.as_deref(), Some(r#"["\"a\"","\"b\""]"#));
    }

    fn time_orch() -> OrchestratorFn {
        orch(|ctx| async move {
            Ok(Some(
                serde_json::to_string(&ctx.current_utc_datetime().timestamp_micros()).unwrap(),
            ))
        })
    }

    #[tokio::test]
    async fn runtimestate_get_started_time() {
        let first_run = now() - chrono::Duration::seconds(30);
        let creation = first_run - chrono::Duration::seconds(1);
        let started = ev_at(
            -1,
            creation,
            EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: "wf".into(),
                ..Default::default()
            }),
        );
        let old = vec![
            ev_at(
                -1,
                first_run,
                EventType::WorkflowStarted(Default::default()),
            ),
            started,
        ];
        let resp = run(&time_orch(), old, vec![]).await;
        let c = single_complete(&resp);
        assert_eq!(
            c.result.as_deref(),
            Some(first_run.timestamp_micros().to_string().as_str()),
            "must use the WorkflowStarted timestamp, not ExecutionStarted's"
        );
    }

    #[tokio::test]
    async fn runtimestate_get_started_time_new_events_only() {
        let t = now() - chrono::Duration::seconds(10);
        let new = vec![
            ev_at(-1, t, EventType::WorkflowStarted(Default::default())),
            es("wf", None),
        ];
        let resp = run(&time_orch(), vec![], new).await;
        let c = single_complete(&resp);
        assert_eq!(
            c.result.as_deref(),
            Some(t.timestamp_micros().to_string().as_str())
        );
    }

    // ===========================================================================
    // backend/runtimestate/applier_test.go
    // ===========================================================================

    #[tokio::test]
    async fn applier_actions_resolved_schedule_is_recorded_not_dispatched() {
        // Go (applier): with the resolution for id 1 already in the state, both
        // scheduling events are recorded, only id 2 is dispatched, and the early
        // resolution is retained. The dispatch decision is the backend's; the
        // SDK half is that the early resolution is delivered to the work it
        // matches while both scheduling actions are still emitted, so the
        // backend can record them and the history replays.
        for kind in [Kind::Task, Kind::Timer, Kind::Child] {
            let f = orch(move |ctx| async move {
                let first = schedule_kind(&ctx, kind);
                let second = schedule_kind(&ctx, kind);
                first.await?;
                ctx.set_custom_status("first resolved");
                second.await?;
                Ok(Some("\"done\"".to_string()))
            });
            // The resolution for id 0 reached the state before its scheduling
            // action was committed.
            let resp = run(
                &f,
                vec![],
                vec![ws(), es("parent", None), resolution(kind, 0)],
            )
            .await;
            assert_eq!(
                scheduled_ids(&resp, kind),
                vec![0, 1],
                "{kind:?}: both scheduling actions must be emitted so the backend records them: {resp:?}"
            );
            assert_eq!(resp.actions.len(), 2, "{kind:?}: {resp:?}");
            assert_eq!(
                resp.custom_status.as_deref(),
                Some("first resolved"),
                "{kind:?}: the early resolution must be retained and delivered to id 0"
            );

            // Next turn, as the applier recorded it: the retained resolution
            // stays matched to id 0 and only id 1 is outstanding.
            let old = vec![
                ws(),
                es("parent", None),
                resolution(kind, 0),
                scheduled_event(kind, 0),
                scheduled_event(kind, 1),
            ];
            let resp = run(&f, old.clone(), vec![]).await;
            assert!(resp.actions.is_empty(), "{kind:?}: {resp:?}");
            let resp = run(&f, old, vec![resolution(kind, 1)]).await;
            let c = single_complete(&resp);
            assert_eq!(c.result.as_deref(), Some("\"done\""), "{kind:?}");
        }
    }

    // ===========================================================================
    // backend/runtimestate/propagation_test.go
    //
    // AssembleProtoPropagatedHistory / canForwardScope are backend code: the
    // applier builds the chunks from the caller's runtime state. The standalone
    // sidecar's sqlite backend does not consume the applier's OutgoingHistory
    // (only Dapr's actor backend does; see the comments in
    // backend/runtimestate/applier.go), so the assembled history never reaches a
    // worker end-to-end and the assembly assertions themselves (which events,
    // which chunks, ancestor kept or dropped) cannot be observed from the SDK.
    //
    // What each port pins instead is the SDK's half of the same contract:
    // (1) the scope the SDK stamps on the action that asks for propagation, and
    // (2) that a history shaped exactly as Go's assembler produces for the
    //     test's state is decoded and exposed verbatim (chunk metadata, typed
    //     events in order, flat event stream) to the workflow or activity.
    // ===========================================================================

    /// (app_id, instance_id, event count) of a propagated chunk.
    type ChunkSummary = (String, String, usize);

    fn raw_chunk(
        app: &str,
        inst: &str,
        name: &str,
        events: &[proto::HistoryEvent],
    ) -> proto::PropagatedHistoryChunk {
        use dapr_durabletask_proto::prost::Message as _;
        proto::PropagatedHistoryChunk {
            raw_events: events.iter().map(|e| e.encode_to_vec()).collect(),
            app_id: app.to_string(),
            instance_id: inst.to_string(),
            workflow_name: name.to_string(),
            ..Default::default()
        }
    }

    fn prop(
        scope: proto::HistoryPropagationScope,
        chunks: Vec<proto::PropagatedHistoryChunk>,
    ) -> proto::PropagatedHistory {
        proto::PropagatedHistory {
            scope: scope as i32,
            chunks,
        }
    }

    /// What an activity would observe for the given wire-level history.
    fn activity_view(p: proto::PropagatedHistory) -> Option<PropagatedHistory> {
        let ctx = ActivityContext::new("wf".to_string(), 0, String::new())
            .with_propagated_history(PropagatedHistory::from_proto(p));
        ctx.propagated_history().cloned()
    }

    fn chunk_labels(c: &dapr_durabletask::api::PropagatedHistoryChunk) -> Vec<String> {
        c.events.iter().map(event_label).collect()
    }

    fn event_label(e: &proto::HistoryEvent) -> String {
        match &e.event_type {
            Some(EventType::ExecutionStarted(x)) => format!("ExecutionStarted:{}", x.name),
            Some(EventType::TaskScheduled(x)) => format!("TaskScheduled:{}", x.name),
            _ => "other".to_string(),
        }
    }

    async fn run_with_history(
        orch_fn: &OrchestratorFn,
        new: Vec<proto::HistoryEvent>,
        incoming: Option<proto::PropagatedHistory>,
    ) -> proto::WorkflowResponse {
        OrchestrationExecutor::execute(
            orch_fn,
            "wf-child",
            vec![],
            new,
            String::new(),
            &WorkerOptions::default(),
            incoming.and_then(PropagatedHistory::from_proto),
        )
        .await
        .expect("executor returned an error")
    }

    fn propagating_orch(scope: HistoryPropagationScope) -> OrchestratorFn {
        orch(move |ctx| async move {
            ctx.call_activity_with_options(
                "inspect",
                (),
                ActivityOptions::new().with_history_propagation(scope),
            )
            .await
        })
    }

    fn sched_scope(r: &proto::WorkflowResponse) -> Option<i32> {
        let s = schedules(r);
        assert_eq!(s.len(), 1, "{r:?}");
        s[0].1.history_propagation_scope
    }

    #[tokio::test]
    async fn runtimestate_propagation_assemble_proto_propagated_history_own_history_single_app() {
        let resp = run_with_history(
            &propagating_orch(HistoryPropagationScope::OwnHistory),
            vec![ws(), es("MyWorkflow", None)],
            None,
        )
        .await;
        assert_eq!(
            sched_scope(&resp),
            Some(proto::HistoryPropagationScope::OwnHistory as i32)
        );

        // Go's assembly for this state: one chunk (appA, wf-001, MyWorkflow)
        // whose rawEvents are the 2 old + 1 new events. The receiving side must
        // expose it verbatim, decoded in order.
        let events = [es("MyWorkflow", None), sched(1, "act1"), sched(2, "act2")];
        let ph = activity_view(prop(
            proto::HistoryPropagationScope::OwnHistory,
            vec![raw_chunk("appA", "wf-001", "MyWorkflow", &events)],
        ))
        .expect("propagated history expected");
        assert_eq!(ph.scope, HistoryPropagationScope::OwnHistory);
        assert_eq!(ph.chunks.len(), 1);
        let c = &ph.chunks[0];
        assert_eq!(c.app_id, "appA");
        assert_eq!(c.instance_id, "wf-001");
        assert_eq!(c.workflow_name, "MyWorkflow");
        assert_eq!(c.event_count, 3);
        assert_eq!(c.start_event_index, 0);
        assert_eq!(
            chunk_labels(c),
            [
                "ExecutionStarted:MyWorkflow",
                "TaskScheduled:act1",
                "TaskScheduled:act2"
            ],
            "all 3 raw events decoded in order"
        );
        assert_eq!(ph.events.len(), 3);
    }

    #[tokio::test]
    async fn runtimestate_propagation_assemble_proto_propagated_history_lineage_no_ancestor() {
        let resp = run_with_history(
            &propagating_orch(HistoryPropagationScope::Lineage),
            vec![ws(), es("MyWorkflow", None)],
            None,
        )
        .await;
        assert_eq!(
            sched_scope(&resp),
            Some(proto::HistoryPropagationScope::Lineage as i32)
        );
        // Go's assembly for a root with no received history: LINEAGE with only
        // the caller's own chunk. The receiver sees exactly that chunk.
        let ph = activity_view(prop(
            proto::HistoryPropagationScope::Lineage,
            vec![raw_chunk(
                "appA",
                "wf-001",
                "MyWorkflow",
                &[es("MyWorkflow", None)],
            )],
        ))
        .expect("propagated history expected");
        assert_eq!(ph.scope, HistoryPropagationScope::Lineage);
        assert_eq!(ph.chunks.len(), 1);
        assert_eq!(ph.chunks[0].app_id, "appA");
        assert_eq!(chunk_labels(&ph.chunks[0]), ["ExecutionStarted:MyWorkflow"]);
        assert_eq!(ph.app_ids(), ["appA"]);
    }

    #[tokio::test]
    async fn runtimestate_propagation_assemble_proto_propagated_history_lineage_with_ancestor() {
        let parent_events = [es("ParentWf", None), sched(1, "parentAct")];
        let parent_chunk = raw_chunk("appA", "wf-parent", "ParentWf", &parent_events);
        let received = prop(
            proto::HistoryPropagationScope::Lineage,
            vec![parent_chunk.clone()],
        );

        // The child workflow sees its ancestor chunk verbatim and stamps LINEAGE
        // on the work it propagates to.
        let seen: Arc<Mutex<Option<Vec<ChunkSummary>>>> = Arc::default();
        let s = seen.clone();
        let f = orch(move |ctx| {
            let s = s.clone();
            async move {
                *s.lock().unwrap() = ctx.propagated_history().map(|ph| {
                    ph.chunks
                        .iter()
                        .map(|c| (c.app_id.clone(), c.instance_id.clone(), c.events.len()))
                        .collect()
                });
                ctx.call_activity("childAct1", ()).await?;
                ctx.call_activity("childAct2", ()).await?;
                ctx.call_activity_with_options(
                    "inspect",
                    (),
                    ActivityOptions::new()
                        .with_history_propagation(HistoryPropagationScope::Lineage),
                )
                .await
            }
        });
        let resp = run_with_history(&f, vec![ws(), es("ChildWf", None)], Some(received)).await;
        assert_eq!(schedules(&resp).len(), 1);
        assert_eq!(
            seen.lock().unwrap().clone(),
            Some(vec![("appA".to_string(), "wf-parent".to_string(), 2)])
        );
        let old = vec![
            ws(),
            es("ChildWf", None),
            sched(0, "childAct1"),
            completed(0, None),
            sched(1, "childAct2"),
        ];
        let resp = OrchestrationExecutor::execute(
            &f,
            "wf-child",
            old,
            vec![completed(1, None)],
            String::new(),
            &WorkerOptions::default(),
            PropagatedHistory::from_proto(prop(
                proto::HistoryPropagationScope::Lineage,
                vec![parent_chunk.clone()],
            )),
        )
        .await
        .unwrap();
        assert_eq!(
            sched_scope(&resp),
            Some(proto::HistoryPropagationScope::Lineage as i32)
        );

        // Downstream view: Go's assembly is the ancestor chunk verbatim followed
        // by the caller's own chunk (appB). The receiver keeps that order, both
        // per chunk and in the flat event stream.
        let own = [
            es("ChildWf", None),
            sched(1, "childAct1"),
            sched(2, "childAct2"),
        ];
        let ph = activity_view(prop(
            proto::HistoryPropagationScope::Lineage,
            vec![parent_chunk, raw_chunk("appB", "wf-child", "ChildWf", &own)],
        ))
        .expect("propagated history expected");
        assert_eq!(ph.chunks.len(), 2);
        assert_eq!(ph.chunks[0].app_id, "appA");
        assert_eq!(ph.chunks[0].instance_id, "wf-parent");
        assert_eq!(
            chunk_labels(&ph.chunks[0]),
            ["ExecutionStarted:ParentWf", "TaskScheduled:parentAct"]
        );
        assert_eq!(ph.chunks[1].app_id, "appB");
        assert_eq!(ph.chunks[1].instance_id, "wf-child");
        assert_eq!(ph.chunks[1].workflow_name, "ChildWf");
        assert_eq!(ph.chunks[1].start_event_index, 2);
        assert_eq!(
            chunk_labels(&ph.chunks[1]),
            [
                "ExecutionStarted:ChildWf",
                "TaskScheduled:childAct1",
                "TaskScheduled:childAct2"
            ]
        );
        assert_eq!(
            ph.events.iter().map(event_label).collect::<Vec<_>>(),
            [
                "ExecutionStarted:ParentWf",
                "TaskScheduled:parentAct",
                "ExecutionStarted:ChildWf",
                "TaskScheduled:childAct1",
                "TaskScheduled:childAct2"
            ]
        );
    }

    #[tokio::test]
    async fn runtimestate_propagation_assemble_proto_propagated_history_own_history_ignores_ancestor()
     {
        let received = prop(
            proto::HistoryPropagationScope::Lineage,
            vec![raw_chunk(
                "appA",
                "wf-parent",
                "ParentWf",
                &[es("ParentWf", None)],
            )],
        );
        // A workflow that received lineage but asks for OWN_HISTORY must stamp
        // OWN_HISTORY, not inherit LINEAGE: the backend drops the ancestor
        // chunk only because the action says OWN_HISTORY.
        let resp = run_with_history(
            &propagating_orch(HistoryPropagationScope::OwnHistory),
            vec![ws(), es("MyWorkflow", None)],
            Some(received),
        )
        .await;
        assert_eq!(
            sched_scope(&resp),
            Some(proto::HistoryPropagationScope::OwnHistory as i32)
        );
        // Go's assembly: only the caller's own chunk (appB). The receiver
        // exposes exactly that; the SDK never re-attaches ancestry itself.
        let ph = activity_view(prop(
            proto::HistoryPropagationScope::OwnHistory,
            vec![raw_chunk(
                "appB",
                "wf-001",
                "MyWorkflow",
                &[es("MyWorkflow", None)],
            )],
        ))
        .expect("propagated history expected");
        assert_eq!(ph.scope, HistoryPropagationScope::OwnHistory);
        assert_eq!(ph.chunks.len(), 1);
        assert_eq!(ph.chunks[0].app_id, "appB");
    }

    #[tokio::test]
    async fn runtimestate_propagation_can_forward_scope_no_defaults() {
        // Go: at a ContinueAsNew boundary the backend forwards only the scope the
        // workflow itself received; there is no implicit default and NONE is not
        // promoted. The SDK side of "what did this workflow receive" is
        // ctx.propagated_history(): nothing is fabricated for a root, a
        // NONE-scoped history is not surfaced, and OWN_HISTORY / LINEAGE pass
        // through unchanged.
        let f = orch(|ctx| async move {
            let scope = ctx.propagated_history().map(|p| format!("{:?}", p.scope));
            Ok(Some(serde_json::to_string(&scope).unwrap()))
        });
        let result = |r: proto::WorkflowResponse| single_complete(&r).result.clone();
        // No incoming history: nothing is seeded.
        let r = run_with_history(&f, vec![ws(), es("wf", None)], None).await;
        assert_eq!(result(r).as_deref(), Some("null"));
        // A NONE-scoped incoming history is not promoted.
        let r = run_with_history(
            &f,
            vec![ws(), es("wf", None)],
            Some(prop(proto::HistoryPropagationScope::None, vec![])),
        )
        .await;
        assert_eq!(result(r).as_deref(), Some("null"));
        // OWN_HISTORY and LINEAGE incoming scopes pass through unchanged.
        for (scope, want) in [
            (proto::HistoryPropagationScope::OwnHistory, "\"OwnHistory\""),
            (proto::HistoryPropagationScope::Lineage, "\"Lineage\""),
        ] {
            let r = run_with_history(
                &f,
                vec![ws(), es("wf", None)],
                Some(prop(
                    scope,
                    vec![raw_chunk("appA", "p", "P", &[es("P", None)])],
                )),
            )
            .await;
            assert_eq!(result(r).as_deref(), Some(want));
        }
    }

    // ===========================================================================
    // backend/runtimestate/dedup
    // ===========================================================================

    #[tokio::test]
    async fn dedup_of() {
        // dedup.Of maps TaskCompleted and TaskFailed to (KindTask,
        // task_scheduled_id), TimerFired to (KindTimer, timer_id), and
        // ExecutionStarted to no resolution at all. durabletask-go's SDK keys its
        // pending work by the same (dedup.Kind, id) pairs, so the SDK-side
        // property is which pending slot each event resolves. Every resolution
        // here carries event_id -1, so it can only match via the id field.
        //
        // ExecutionStarted appears once, as in every real history: it starts the
        // workflow and resolves no slot. A second ExecutionStarted is not an SDK
        // input (the backend rejects it with ErrDuplicateEvent) and would restart
        // the workflow in both SDKs.
        let f = orch(|ctx| async move {
            let t0 = ctx.call_activity("a", ());
            let t1 = ctx.call_activity("b", ());
            let t2 = ctx.create_timer(Duration::from_secs(1));
            let r0 = if t0.await.is_err() { "failed" } else { "ok" };
            let r1 = t1.await?.unwrap_or_default();
            t2.await?;
            Ok(Some(serde_json::to_string(&(r0, r1)).unwrap()))
        });
        let old = vec![
            ws(),
            es("wf", None),
            sched(0, "a"),
            sched(1, "b"),
            timer_created(2),
        ];
        let new = vec![failed(0), completed(1, Some("7")), timer_fired(2)];
        let resp = run(&f, old, new).await;
        let c = single_complete(&resp);
        assert_eq!(c.result.as_deref(), Some(r#"["failed","7"]"#));
    }

    #[tokio::test]
    async fn dedup_new_for_state_pre_populates() {
        let f = orch(|ctx| async move {
            let a = ctx.call_activity("a", ()).await?;
            ctx.create_timer(Duration::from_secs(1)).await?;
            ctx.call_activity("next", a).await
        });
        let old = vec![
            ws(),
            es("wf", None),
            sched(0, "a"),
            completed(0, Some("\"a\"")),
            timer_created(1),
            timer_fired(1),
        ];
        // Redelivered resolutions already in committed history are ignored.
        let resp = run(&f, old, vec![completed(0, Some("\"dup\"")), timer_fired(1)]).await;
        let s = schedules(&resp);
        assert_eq!(resp.actions.len(), 1, "{resp:?}");
        assert_eq!(s[0].0, 2);
        assert_eq!(s[0].1.input.as_deref(), Some("\"\\\"a\\\"\""));
    }

    // ===========================================================================
    // tests/runtimestate_test.go
    // ===========================================================================

    #[tokio::test]
    async fn tests_runtimestate_new_workflow() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("myworkflow", |ctx| async move {
                ctx.wait_for_external_event("go").await
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;
        let before = now() - chrono::Duration::seconds(2);
        let id = client
            .schedule_new_orchestration("myworkflow", None, Some("abc".into()), None)
            .await
            .unwrap();
        let st = wait_status(&mut client, &id, OrchestrationStatus::Running).await;
        let after = now() + chrono::Duration::seconds(2);
        assert_eq!(st.instance_id, "abc");
        assert_eq!(st.name, "myworkflow");
        let created = st.created_at.expect("created_at");
        assert!(created >= before && created <= after, "{created}");
        assert!(st.serialized_output.is_none());
        assert!(st.failure_details.is_none());
        client
            .raise_orchestration_event(&id, "go", None)
            .await
            .unwrap();
        complete(&mut client, &id).await;
        guard.stop().await;
    }

    #[tokio::test]
    async fn tests_runtimestate_completed_workflow() {
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("myworkflow", |_ctx| async move { Ok(None) });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("myworkflow", None, Some("abc".into()), None)
            .await
            .unwrap();
        let st = complete(&mut client, &id).await;
        assert_eq!(st.instance_id, "abc");
        assert_eq!(st.name, "myworkflow");
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        let created = st.created_at.expect("created_at");
        let updated = st.last_updated_at.expect("last_updated_at");
        assert!(created <= updated);
        guard.stop().await;
    }

    #[tokio::test]
    async fn tests_runtimestate_completed_child_workflow() {
        // Executor side: a child emits exactly one Completed action with output.
        let f = orch(|_ctx| async move { Ok(Some("\"done!\"".to_string())) });
        let resp = run(&f, vec![], vec![ws(), es_child("Child", 3)]).await;
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(c.result.as_deref(), Some("\"done!\""));
        assert!(c.failure_details.is_none());

        // Backend side: the parent is notified with the child's output.
        setup!(env);
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("Parent", |ctx| async move {
                ctx.call_sub_orchestrator("Child", (), None).await
            });
        worker
            .registry_mut()
            .add_named_orchestrator(
                "Child",
                |_ctx| async move { Ok(Some("\"done!\"".to_string())) },
            );
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;
        let id = client
            .schedule_new_orchestration("Parent", None, None, None)
            .await
            .unwrap();
        let st = complete(&mut client, &id).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(st.serialized_output.as_deref(), Some("\"done!\""));
        guard.stop().await;
    }

    #[tokio::test]
    async fn tests_runtimestate_runtime_state_continue_as_new() {
        let f = orch(|ctx| async move {
            ctx.continue_as_new("done!", true);
            Ok(None)
        });
        let new = vec![
            ws(),
            es("MyWorkflow", None),
            raised("MyRaisedEvent", Some("MyEventPayload")),
        ];
        let resp = run(&f, vec![], new).await;
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::ContinuedAsNew)
        );
        assert_eq!(c.result.as_deref(), Some("\"done!\""));
        assert_eq!(c.carryover_events.len(), 1);
        match &c.carryover_events[0].event_type {
            Some(EventType::EventRaised(er)) => {
                assert_eq!(er.name, "MyRaisedEvent");
                assert_eq!(er.input.as_deref(), Some("MyEventPayload"));
            }
            other => panic!("expected EventRaised carryover, got {other:?}"),
        }
        assert!(
            schedules(&resp).is_empty() && timers(&resp).is_empty() && children(&resp).is_empty()
        );
    }

    #[tokio::test]
    async fn tests_runtimestate_create_timer() {
        // Go applies three CreateTimer actions named "foo" and checks fire_at,
        // name and the CreateTimer origin on each recorded event. The SDK must
        // produce those actions. Not portable: the name. durabletask-go's SDK
        // sets it with task.WithTimerName; the Rust create_timer takes no name
        // and always sends `name: None` (feature gap: user-named timers).
        let f = orch(|ctx| async move {
            let a = ctx.create_timer(Duration::from_secs(72 * 3600));
            let b = ctx.create_timer(Duration::from_secs(72 * 3600));
            let c = ctx.create_timer(Duration::from_secs(72 * 3600));
            when_all(vec![a, b, c]).await?;
            Ok(None)
        });
        let start = now();
        let resp = run(
            &f,
            vec![],
            vec![
                ev_at(-1, start, EventType::WorkflowStarted(Default::default())),
                es("MyWorkflow", None),
            ],
        )
        .await;
        let t = timers(&resp);
        assert_eq!(t.len(), 3);
        let expected = ts(start + chrono::Duration::hours(72));
        for (i, (id, timer)) in t.iter().enumerate() {
            assert_eq!(*id, i as i32);
            assert_eq!(timer.fire_at, Some(expected));
            assert!(
                matches!(timer.origin, Some(Origin::CreateTimer(_))),
                "timer must carry CreateTimer origin, got {:?}",
                timer.origin
            );
        }
    }

    #[tokio::test]
    async fn tests_runtimestate_create_timer_external_event_origin() {
        let f = orch(|ctx| async move {
            ctx.wait_for_external_event_with_timeout("myEvent", Duration::from_secs(1800))
                .await?;
            Ok(None)
        });
        let start = now();
        let resp = run(
            &f,
            vec![],
            vec![
                ev_at(-1, start, EventType::WorkflowStarted(Default::default())),
                es("MyOrchestration", None),
            ],
        )
        .await;
        let t = timers(&resp);
        assert_eq!(t.len(), 1);
        let timer = t[0].1;
        assert_eq!(
            timer.fire_at,
            Some(ts(start + chrono::Duration::minutes(30)))
        );
        match &timer.origin {
            Some(Origin::ExternalEvent(e)) => assert_eq!(e.name, "myEvent"),
            other => panic!("expected ExternalEvent origin, got {other:?}"),
        }
        assert_eq!(
            timer.name.as_deref(),
            Some("myEvent"),
            "timer name must be the event name"
        );
    }

    #[tokio::test]
    async fn tests_runtimestate_create_timer_activity_retry_origin() {
        let f = orch(|ctx| async move {
            ctx.call_activity_with_options(
                "myActivity",
                (),
                ActivityOptions::new()
                    .with_retry_policy(RetryPolicy::new(3, Duration::from_secs(10))),
            )
            .await
        });
        // Go applies a retry CreateTimer named "myActivity-retry" whose
        // ActivityRetry origin carries "task-exec-123", firing 10s out. Here the
        // SDK has to build that action itself from the failure it replays.
        let turn = now();
        let old = vec![
            ev_at(-1, turn, EventType::WorkflowStarted(Default::default())),
            es("MyOrchestration", None),
            sched_exec(0, "myActivity", "task-exec-123"),
        ];
        let resp = run(&f, old, vec![failed_exec(0, "task-exec-123")]).await;
        let t = timers(&resp);
        assert_eq!(resp.actions.len(), 1, "{resp:?}");
        assert_eq!(t.len(), 1, "{resp:?}");
        let timer = t[0].1;
        assert_eq!(timer.name.as_deref(), Some("myActivity-retry"));
        assert_eq!(
            timer.fire_at,
            Some(ts(turn + chrono::Duration::seconds(10)))
        );
        match &timer.origin {
            Some(Origin::ActivityRetry(r)) => assert_eq!(r.task_execution_id, "task-exec-123"),
            other => panic!("expected ActivityRetry origin, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tests_runtimestate_create_timer_child_workflow_retry_origin() {
        let f = orch(|ctx| async move {
            ctx.call_sub_orchestrator_with_options(
                "myWorkflow",
                (),
                SubOrchestratorOptions::new()
                    .with_instance_id("child-instance-456")
                    .with_retry_policy(RetryPolicy::new(3, Duration::from_secs(30))),
            )
            .await
        });
        // Go applies a retry CreateTimer named "myWorkflow-retry" whose
        // ChildWorkflowRetry origin carries "child-instance-456", firing 30s out.
        let turn = now();
        let old = vec![
            ev_at(-1, turn, EventType::WorkflowStarted(Default::default())),
            es("MyOrchestration", None),
            child_created(0, "myWorkflow", "child-instance-456"),
        ];
        let resp = run(&f, old, vec![child_failed(0)]).await;
        let t = timers(&resp);
        assert_eq!(resp.actions.len(), 1, "{resp:?}");
        assert_eq!(t.len(), 1, "{resp:?}");
        let timer = t[0].1;
        assert_eq!(timer.name.as_deref(), Some("myWorkflow-retry"));
        assert_eq!(
            timer.fire_at,
            Some(ts(turn + chrono::Duration::seconds(30)))
        );
        match &timer.origin {
            Some(Origin::ChildWorkflowRetry(r)) => assert_eq!(r.instance_id, "child-instance-456"),
            other => panic!("expected ChildWorkflowRetry origin, got {other:?}"),
        }
    }

    fn retrying_parent() -> OrchestratorFn {
        orch(|ctx| async move {
            ctx.call_sub_orchestrator_with_options(
                "Child",
                (),
                SubOrchestratorOptions::new()
                    .with_retry_policy(RetryPolicy::new(4, Duration::from_secs(1))),
            )
            .await
        })
    }

    /// Drives the four rounds of a failing child with retries. Returns the
    /// responses of each round and the first child's instance ID.
    async fn child_retry_rounds() -> (Vec<proto::WorkflowResponse>, String) {
        let f = retrying_parent();
        let start = es("Parent", None);
        let mut responses = Vec::new();

        // Round 1: CreateChildWorkflow #0.
        let r1 = run(&f, vec![], vec![ws(), start.clone()]).await;
        let c = children(&r1);
        assert_eq!(r1.actions.len(), 1);
        let first = applied_instance_id(c[0]);
        responses.push(r1);

        // Round 2: child fails -> retry timer #1.
        let mut old = vec![ws(), start, child_created(0, "Child", &first)];
        let r2 = run(&f, old.clone(), vec![child_failed(0)]).await;
        assert_eq!(r2.actions.len(), 1, "{r2:?}");
        assert_eq!(timers(&r2).len(), 1);
        responses.push(r2);

        // Round 3: timer fires -> CreateChildWorkflow #2.
        old.extend([child_failed(0), timer_created(1)]);
        let r3 = run(&f, old.clone(), vec![timer_fired(1)]).await;
        assert_eq!(r3.actions.len(), 1, "{r3:?}");
        let c = children(&r3);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].0, 2);
        let second = applied_instance_id(c[0]);
        responses.push(r3);

        // Round 4: second child fails -> retry timer #3.
        old.extend([timer_fired(1), child_created(2, "Child", &second)]);
        let r4 = run(&f, old, vec![child_failed(2)]).await;
        assert_eq!(r4.actions.len(), 1, "{r4:?}");
        assert_eq!(timers(&r4).len(), 1);
        responses.push(r4);
        (responses, first)
    }

    #[tokio::test]
    async fn tests_runtimestate_child_workflow_retry_timer_origin_points_to_first_child() {
        let (r, first) = child_retry_rounds().await;
        let second = applied_instance_id(children(&r[2])[0]);
        assert_ne!(
            first, second,
            "each retry should get a different auto-generated instance ID"
        );
        for (round, resp) in [(2, &r[1]), (4, &r[3])] {
            match &timers(resp)[0].1.origin {
                Some(Origin::ChildWorkflowRetry(o)) => assert_eq!(
                    o.instance_id, first,
                    "round {round}: retry timer origin should point to the first child"
                ),
                other => panic!("round {round}: expected ChildWorkflowRetry origin, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn tests_runtimestate_child_workflow_retry_retry_parent_instance_info_links_to_first_child()
     {
        let (r, first) = child_retry_rounds().await;
        assert!(
            children(&r[0])[0].1.retry_parent_instance_info.is_none(),
            "the first attempt action must not carry a RetryParentInstanceInfo"
        );
        let retry = children(&r[2])[0].1;
        assert_ne!(applied_instance_id(children(&r[2])[0]), first);
        let info = retry
            .retry_parent_instance_info
            .as_ref()
            .expect("the retry re-creation action must carry a RetryParentInstanceInfo");
        assert_eq!(info.instance_id, first);
    }

    #[tokio::test]
    async fn tests_runtimestate_child_workflow_retry_applier_copies_retry_parent_per_action() {
        // Two concurrent retry chains with identical name/input, plus a fresh
        // non-retry child: each re-creation is stamped with its own chain's
        // first-attempt ID and the fresh creation carries nothing.
        let f = orch(|ctx| async move {
            let opts = || {
                SubOrchestratorOptions::new()
                    .with_retry_policy(RetryPolicy::new(3, Duration::from_secs(1)))
            };
            let a = ctx.call_sub_orchestrator_with_options("worker", "same", opts());
            let b = ctx.call_sub_orchestrator_with_options("worker", "same", opts());
            let (ra, rb) = futures::join!(a, b);
            ra?;
            rb?;
            ctx.call_sub_orchestrator("worker", "same", Some("worker-c-attempt1"))
                .await
        });
        let start = es("Parent", None);
        let r1 = run(&f, vec![], vec![ws(), start.clone()]).await;
        let c = children(&r1);
        assert_eq!(c.len(), 2);
        assert!(
            c.iter()
                .all(|(_, a)| a.retry_parent_instance_info.is_none())
        );
        let (first_a, first_b) = (applied_instance_id(c[0]), applied_instance_id(c[1]));

        let mut old = vec![
            ws(),
            start,
            child_created(0, "worker", &first_a),
            child_created(1, "worker", &first_b),
        ];
        let r2 = run(&f, old.clone(), vec![child_failed(0), child_failed(1)]).await;
        let t: Vec<i32> = timers(&r2).into_iter().map(|(id, _)| id).collect();
        assert_eq!(t, vec![2, 3], "{r2:?}");

        old.extend([
            child_failed(0),
            child_failed(1),
            timer_created(2),
            timer_created(3),
        ]);
        let r3 = run(&f, old.clone(), vec![timer_fired(2), timer_fired(3)]).await;
        let c = children(&r3);
        assert_eq!(c.len(), 2, "{r3:?}");
        assert_eq!(c.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [4, 5]);
        assert!(
            c.iter()
                .all(|(_, a)| a.name == "worker" && a.input.as_deref() == Some("\"same\"")),
            "the re-creations are otherwise indistinguishable: {r3:?}"
        );
        let parents: Vec<Option<String>> = c
            .iter()
            .map(|(_, a)| {
                a.retry_parent_instance_info
                    .as_ref()
                    .map(|i| i.instance_id.clone())
            })
            .collect();
        assert_eq!(parents, vec![Some(first_a), Some(first_b)]);

        // Both retries succeed; the fresh, identical creation that follows
        // ("worker-c-attempt1") must not carry a RetryParentInstanceInfo.
        let (second_a, second_b) = (applied_instance_id(c[0]), applied_instance_id(c[1]));
        old.extend([
            timer_fired(2),
            timer_fired(3),
            child_created(4, "worker", &second_a),
            child_created(5, "worker", &second_b),
        ]);
        let r4 = run(
            &f,
            old,
            vec![child_completed(4, None), child_completed(5, None)],
        )
        .await;
        assert_eq!(r4.actions.len(), 1, "{r4:?}");
        let c = children(&r4);
        assert_eq!(c[0].0, 6);
        assert_eq!(c[0].1.instance_id, "worker-c-attempt1");
        assert_eq!(c[0].1.input.as_deref(), Some("\"same\""));
        assert!(
            c[0].1.retry_parent_instance_info.is_none(),
            "a fresh (non-retry) creation must not carry a RetryParentInstanceInfo: {r4:?}"
        );
    }

    #[tokio::test]
    async fn tests_runtimestate_activity_retry_timer_origin_matches_task_execution_id() {
        let f = orch(|ctx| async move {
            ctx.call_activity_with_options(
                "flaky",
                (),
                ActivityOptions::new()
                    .with_retry_policy(RetryPolicy::new(4, Duration::from_secs(1))),
            )
            .await
        });
        let start = es("Parent", None);
        let r1 = run(&f, vec![], vec![ws(), start.clone()]).await;
        let s = schedules(&r1);
        assert_eq!(r1.actions.len(), 1);
        let exec = s[0].1.task_execution_id.clone();
        assert!(
            !exec.is_empty(),
            "ScheduleTask must carry a TaskExecutionId"
        );

        let mut old = vec![ws(), start, sched_exec(0, "flaky", &exec)];
        let r2 = run(&f, old.clone(), vec![failed_exec(0, &exec)]).await;
        assert_eq!(r2.actions.len(), 1);
        match &timers(&r2)[0].1.origin {
            Some(Origin::ActivityRetry(o)) => assert_eq!(o.task_execution_id, exec),
            other => panic!("expected ActivityRetry origin, got {other:?}"),
        }

        old.extend([failed_exec(0, &exec), timer_created(1)]);
        let r3 = run(&f, old.clone(), vec![timer_fired(1)]).await;
        assert_eq!(r3.actions.len(), 1);
        let s = schedules(&r3);
        assert_eq!(
            s[0].1.task_execution_id, exec,
            "retries keep the same TaskExecutionId"
        );

        old.extend([timer_fired(1), sched_exec(2, "flaky", &exec)]);
        let r4 = run(&f, old, vec![failed_exec(2, &exec)]).await;
        assert_eq!(r4.actions.len(), 1);
        match &timers(&r4)[0].1.origin {
            Some(Origin::ActivityRetry(o)) => assert_eq!(o.task_execution_id, exec),
            other => panic!("expected ActivityRetry origin, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tests_runtimestate_schedule_task() {
        let f = orch(|ctx| async move { ctx.call_activity("MyActivity", "foo").await });
        let resp = run(&f, vec![], vec![ws(), es("MyWorkflow", None)]).await;
        let s = schedules(&resp);
        assert_eq!(resp.actions.len(), 1);
        assert_eq!(s[0].0, 0);
        assert_eq!(s[0].1.name, "MyActivity");
        assert_eq!(s[0].1.input.as_deref(), Some("\"foo\""));
    }

    #[tokio::test]
    async fn tests_runtimestate_create_child_workflow() {
        let f = orch(|ctx| async move {
            ctx.call_sub_orchestrator("MyChild", "foo", Some("xyz"))
                .await
        });
        let resp = run(&f, vec![], vec![ws(), es("Parent", None)]).await;
        let c = children(&resp);
        assert_eq!(resp.actions.len(), 1);
        assert_eq!(c[0].0, 0);
        assert_eq!(c[0].1.instance_id, "xyz");
        assert_eq!(c[0].1.name, "MyChild");
        assert_eq!(c[0].1.input.as_deref(), Some("\"foo\""));
        assert!(resp.actions[0].router.is_none());
    }

    #[tokio::test]
    async fn tests_runtimestate_runtime_state_terminated_then_continue_as_new() {
        // Go: a Terminated completion followed by a ContinuedAsNew completion in
        // one apply pass yields one completion, TERMINATED, no new generation.
        // SDK side: the workflow already asked to continue as new when the
        // terminate is replayed; the terminate wins and is the only completion.
        let f = orch(|ctx| async move {
            ctx.continue_as_new("again", false);
            Ok(None)
        });
        let resp = run(
            &f,
            vec![ws(), es("MyWorkflow", None)],
            vec![terminated("\"stop\"")],
        )
        .await;
        assert_eq!(resp.actions.len(), 1, "{resp:?}");
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Terminated)
        );
        assert_eq!(c.result.as_deref(), Some("\"stop\""));
    }

    #[tokio::test]
    async fn tests_runtimestate_runtime_state_duplicate_completion_single_parent_notification() {
        // Go: a child's Completed("done") followed by a Terminated("stop")
        // completion in one apply pass keeps COMPLETED/"done" and queues exactly
        // one parent notification. SDK side: the child completes, then a
        // terminate for it is replayed in the same turn; the earlier completion
        // wins and exactly one completion (hence one parent notification) is
        // emitted.
        let f = orch(|_ctx| async move { Ok(Some("\"done\"".to_string())) });
        let resp = run(
            &f,
            vec![],
            vec![ws(), es_child("Child", 3), terminated("\"stop\"")],
        )
        .await;
        assert_eq!(resp.actions.len(), 1, "{resp:?}");
        let c = single_complete(&resp);
        assert_eq!(
            c.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(c.result.as_deref(), Some("\"done\""));
        assert!(c.failure_details.is_none());
    }

    // ===========================================================================
    // Feature gaps: the SDK half of these contracts is missing from the Rust SDK
    //
    // Each body records what the Go test asserts, so the port can be written
    // once the feature exists.
    // ===========================================================================

    // --- backend/backend_test.go ---

    #[tokio::test]
    async fn backend_get_child_workflow_instances_preserves_cross_namespace_router() {
        // Go: getChildWorkflowInstances(old=[local-old, xns-old], new=[local-new,
        // xns-new]) enumerates all 4 children, and the routers of xns-old / xns-new
        // keep TargetAppNamespace = "other-ns" so the recursive terminate is sent to
        // the right sidecar.
        // SDK half: a child scheduled into another namespace carries
        // router.target_app_namespace on its CreateChildWorkflow action, and a
        // local child carries no router. The standalone sidecar has no second
        // namespace, so the backend half is not observable end to end.
        let f = orch(|ctx| async move {
            let xns = || {
                SubOrchestratorOptions::new()
                    .with_app_id("other-app")
                    .with_app_namespace("other-ns")
            };
            let local = SubOrchestratorOptions::new;
            let _ = futures::join!(
                ctx.call_sub_orchestrator_with_options(
                    "child",
                    (),
                    local().with_instance_id("local-old")
                ),
                ctx.call_sub_orchestrator_with_options(
                    "child",
                    (),
                    xns().with_instance_id("xns-old")
                ),
                ctx.call_sub_orchestrator_with_options(
                    "child",
                    (),
                    local().with_instance_id("local-new")
                ),
                ctx.call_sub_orchestrator_with_options(
                    "child",
                    (),
                    xns().with_instance_id("xns-new")
                ),
            );
            Ok(None)
        });
        let resp = run(&f, vec![], vec![ws(), es("parent", None)]).await;
        let routed: Vec<(&str, Option<&proto::TaskRouter>)> = resp
            .actions
            .iter()
            .filter_map(|a| match &a.workflow_action_type {
                Some(Wat::CreateChildWorkflow(c)) => {
                    Some((c.instance_id.as_str(), a.router.as_ref()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(routed.len(), 4, "{:?}", resp.actions);
        for (id, router) in routed {
            if id.starts_with("xns-") {
                let r = router.unwrap_or_else(|| panic!("{id} must carry a router"));
                assert_eq!(r.target_app_id.as_deref(), Some("other-app"), "{id}");
                assert_eq!(r.target_app_namespace.as_deref(), Some("other-ns"), "{id}");
            } else {
                assert!(router.is_none(), "{id} is local: {router:?}");
            }
        }
    }

    #[tokio::test]
    async fn backend_purge_workflow_state_recursive_force_bypasses_is_completed() {
        // Go: purgeWorkflowState(recursive=true, force=true) on a RUNNING parent (no
        // CompletedEvent) with one child succeeds, returns 2 and purges child and
        // parent. Without force the same call fails with ErrNotCompleted (see
        // backend_purge_workflow_state_recursive_requires_completed_without_force).
        //
        // Observable through the SDK + sidecar: `force` reaches the sidecar and
        // bypasses purgeWorkflowState's root IsCompleted check, so the walk moves
        // on to the child. The Go test's fake backend then purges both; the
        // sqlite backend's own local purge ignores `force` and still requires a
        // completed instance, so the "returns 2 / both gone" half is not
        // observable here — nothing is purged and the error moves from the root
        // check to the child's backend purge.
        use dapr_durabletask::client::PurgeOptions;

        setup!(env);
        let mut worker = env.new_worker();
        register_tree(&mut worker);
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;

        let id = "force-root";
        let child = format!("{id}-child");
        client
            .schedule_new_orchestration("tree_parent_waiting", None, Some(id.into()), None)
            .await
            .unwrap();
        wait_status(&mut client, &child, OrchestrationStatus::Running).await;
        wait_status(&mut client, id, OrchestrationStatus::Running).await;
        guard.stop().await;

        // Without force: rejected by the root IsCompleted check.
        let err = client
            .purge_orchestration_with_options(id, PurgeOptions::new().with_recursive(true))
            .await
            .expect_err("unforced recursive purge of a running tree must fail");
        let msg = err.to_string();
        assert!(msg.contains("has not yet completed"), "{msg}");
        assert!(
            !msg.contains("child workflow"),
            "root check expected: {msg}"
        );

        // With force: the root check is bypassed and the walk reaches the child.
        let err = client
            .purge_orchestration_with_options(
                id,
                PurgeOptions::new().with_recursive(true).with_force(true),
            )
            .await
            .expect_err("sqlite backend's local purge ignores force");
        let msg = err.to_string();
        assert!(
            msg.contains("failed to purge child workflow"),
            "force must bypass the root IsCompleted check: {msg}"
        );

        for iid in [id, child.as_str()] {
            let st = client.get_orchestration_state(iid, false).await.unwrap();
            assert!(
                st.is_some(),
                "{iid} must not be purged by the sqlite backend"
            );
        }
    }

    #[tokio::test]
    async fn backend_purge_workflow_state_remote_router_delegates_without_local_walk() {
        // Go: for recursive in {true, false}, a purge whose router targets
        // "other-app" is one delegated backend call that reads no local state,
        // returns the remote count as-is and keeps the recursive flag.
        //
        // Observable through the SDK + sidecar: the router reaches the sidecar,
        // which delegates the whole purge to the backend's cross-app dispatch
        // (the sqlite backend rejects it as unsupported) instead of walking the
        // local instance, which does not exist and would report "not found".
        // The fake backend's remote count and the recursive flag on the
        // delegated backend call are internal to the Go backend.
        use dapr_durabletask::client::PurgeOptions;

        setup!(env);
        let mut client = env.new_client().await;
        let root = "remote-root";

        for recursive in [true, false] {
            let err = client
                .purge_orchestration_with_options(
                    root,
                    PurgeOptions::new()
                        .with_recursive(recursive)
                        .with_app_id("other-app"),
                )
                .await
                .expect_err("sqlite sidecar cannot dispatch a cross-app purge");
            assert!(
                err.to_string().contains("cross-app purge"),
                "recursive={recursive}: expected delegated cross-app purge, got {err}"
            );

            // Without the router the same purge walks local state instead.
            let local = client
                .purge_orchestration_with_options(
                    root,
                    PurgeOptions::new().with_recursive(recursive),
                )
                .await;
            if let Err(e) = &local {
                assert!(
                    !e.to_string().contains("cross-app"),
                    "recursive={recursive}: local purge must not be delegated, got {e}"
                );
            }
            assert!(
                !matches!(local, Ok(n) if n > 0),
                "nothing exists locally to purge: {local:?}"
            );
        }
    }

    // --- backend/executor_stateful_test.go ---

    //
    // The sidecar half is driven through a raw work-item stream that advertises
    // WORKER_CAPABILITY_STATEFUL_HISTORY and records exactly what the sidecar
    // sends. The worker half (rebuilding cached prefix + delta, fetching on a
    // miss, dropping the prefix on completion / continue-as-new) is covered by
    // the grpc_worker `history_cache` unit tests and end to end by running a
    // real (capable, by default) TaskHubGrpcWorker over multi-turn workloads.

    /// A raw capable worker on the sidecar's work-item stream.
    struct RawCapableStream {
        client: proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient<
            tonic::transport::Channel,
        >,
        stream: Option<tonic::Streaming<proto::WorkItem>>,
        /// The sidecar may withhold the stream's response headers until the
        /// first work item exists, so opening completes in the background.
        opening: Option<tokio::task::JoinHandle<tonic::Streaming<proto::WorkItem>>>,
    }

    impl RawCapableStream {
        async fn open(env: &harness::TestEnv) -> Self {
            let client =
                proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient::connect(
                    env.address.clone(),
                )
                .await
                .expect("connect raw stream");
            let mut opener = client.clone();
            let opening = tokio::spawn(async move {
                opener
                    .get_work_items(proto::GetWorkItemsRequest {
                        capabilities: vec![proto::WorkerCapability::StatefulHistory as i32],
                    })
                    .await
                    .expect("open work item stream")
                    .into_inner()
            });
            // Let the stream register with the sidecar before work is scheduled.
            tokio::time::sleep(Duration::from_millis(200)).await;
            Self {
                client,
                stream: None,
                opening: Some(opening),
            }
        }

        async fn stream(&mut self) -> &mut tonic::Streaming<proto::WorkItem> {
            if let Some(opening) = self.opening.take() {
                let stream = tokio::time::timeout(TIMEOUT, opening)
                    .await
                    .expect("timed out opening the work item stream")
                    .unwrap();
                self.stream = Some(stream);
            }
            self.stream.as_mut().unwrap()
        }

        /// Receive the next workflow work item, completing any activity work
        /// items on the way with the result `"ok"`.
        async fn next_workflow(&mut self) -> (proto::WorkflowRequest, String) {
            loop {
                let item = tokio::time::timeout(TIMEOUT, self.stream().await.message())
                    .await
                    .expect("timed out waiting for a work item")
                    .expect("stream error")
                    .expect("stream closed");
                match item.request {
                    Some(proto::work_item::Request::WorkflowRequest(req)) => {
                        return (req, item.completion_token);
                    }
                    Some(proto::work_item::Request::ActivityRequest(act)) => {
                        self.client
                            .complete_activity_task(proto::ActivityResponse {
                                instance_id: act
                                    .workflow_instance
                                    .map(|i| i.instance_id)
                                    .unwrap_or_default(),
                                task_id: act.task_id,
                                result: Some("\"ok\"".into()),
                                failure_details: None,
                                completion_token: item.completion_token,
                            })
                            .await
                            .expect("complete activity");
                    }
                    None => {}
                }
            }
        }

        /// Answer a workflow turn with `actions`.
        async fn respond(
            &mut self,
            req: &proto::WorkflowRequest,
            token: String,
            actions: Vec<proto::WorkflowAction>,
        ) {
            #[allow(deprecated)]
            self.client
                .complete_orchestrator_task(proto::WorkflowResponse {
                    instance_id: req.instance_id.clone(),
                    actions,
                    custom_status: None,
                    completion_token: token,
                    num_events_processed: None,
                    version: None,
                })
                .await
                .expect("complete workflow turn");
        }

        /// Receive the next turn, which must belong to `instance_id`.
        async fn turn(&mut self, instance_id: &str) -> (proto::WorkflowRequest, String) {
            let (req, token) = self.next_workflow().await;
            assert_eq!(req.instance_id, instance_id, "unexpected turn: {req:?}");
            (req, token)
        }
    }

    fn raw_schedule(id: i32) -> proto::WorkflowAction {
        proto::WorkflowAction {
            id,
            router: None,
            workflow_action_type: Some(Wat::ScheduleTask(proto::ScheduleTaskAction {
                name: "raw_act".into(),
                ..Default::default()
            })),
        }
    }

    fn raw_finish(id: i32, status: proto::OrchestrationStatus) -> proto::WorkflowAction {
        proto::WorkflowAction {
            id,
            router: None,
            workflow_action_type: Some(Wat::CompleteWorkflow(proto::CompleteWorkflowAction {
                workflow_status: status as i32,
                ..Default::default()
            })),
        }
    }

    fn event_kinds(events: &[proto::HistoryEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| {
                let dbg = format!("{:?}", e.event_type);
                dbg.split('(').nth(1).unwrap_or("").to_string()
            })
            .collect()
    }

    /// Drive a fresh instance through three activity turns on the raw stream,
    /// returning its turns 2 and 3 (turn 1 is the ExecutionStarted turn).
    async fn raw_warm_up(
        env: &harness::TestEnv,
        raw: &mut RawCapableStream,
        client: &mut TaskHubGrpcClient,
        id: &str,
    ) -> (proto::WorkflowRequest, proto::WorkflowRequest, String) {
        // The sidecar answers StartInstance only once the first turn ran, so
        // schedule concurrently with serving that turn.
        let schedule = {
            let mut client = env.new_client().await;
            let id = id.to_string();
            tokio::spawn(async move {
                client
                    .schedule_new_orchestration("raw_wf", None, Some(id), None)
                    .await
                    .unwrap()
            })
        };
        let (t1, tok) = raw.turn(id).await;
        assert!(t1.cached_history.is_none(), "first turn is a full send");
        assert!(t1.past_events.is_empty(), "nothing committed yet: {t1:?}");
        raw.respond(&t1, tok, vec![raw_schedule(0)]).await;
        assert_eq!(
            tokio::time::timeout(TIMEOUT, schedule)
                .await
                .unwrap()
                .unwrap(),
            id
        );

        let (t2, tok) = raw.turn(id).await;
        assert!(
            t2.cached_history.is_none(),
            "the stream is not yet warm with a non-empty prefix: {t2:?}"
        );
        assert!(!t2.past_events.is_empty());
        raw.respond(&t2, tok, vec![raw_schedule(1)]).await;

        let (t3, tok) = raw.turn(id).await;
        // prefix + delta is exactly the committed history.
        let committed = client.get_instance_history(id).await.unwrap();
        let mut rebuilt = t2.past_events.clone();
        rebuilt.extend(t3.past_events.iter().cloned());
        assert_eq!(event_kinds(&rebuilt), event_kinds(&committed));
        (t2, t3, tok)
    }

    #[tokio::test]
    async fn executor_stateful_apply_stateful_history_first_turn_sends_full_then_warms() {
        // Go: on a capable stream the first turn of an instance (5 past, 2 new) is
        // sent in full (5 past_events, cached_history unset), and the instance is
        // then warm at 5.
        setup!(env);
        let mut client = env.new_client().await;
        let mut raw = RawCapableStream::open(&env).await;
        let id = String::from("warm-a");
        let (t2, t3, tok) = raw_warm_up(&env, &mut raw, &mut client, &id).await;
        // The first turn with committed history (t2) was a full send; the
        // stream is then warm at exactly that many events.
        let warm = t3.cached_history.expect("warm stream sends a delta");
        assert_eq!(warm.event_count as usize, t2.past_events.len());
        raw.respond(
            &t3,
            tok,
            vec![raw_finish(2, proto::OrchestrationStatus::Completed)],
        )
        .await;
        let st = complete(&mut client, &id).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
    }

    #[tokio::test]
    async fn executor_stateful_apply_stateful_history_subsequent_turn_sends_delta() {
        // Go: after a first turn with 5 committed events, a turn with 8 committed and
        // 1 new event is sent as cached_history.event_count = 5, past_events = the 3
        // events 5..8, new_events = the 1 new event (always in full); warm becomes 8.
        setup!(env);
        let mut client = env.new_client().await;
        let mut raw = RawCapableStream::open(&env).await;
        let id = String::from("delta-a");
        let (t2, t3, tok) = raw_warm_up(&env, &mut raw, &mut client, &id).await;
        assert_eq!(
            t3.cached_history.map(|c| c.event_count as usize),
            Some(t2.past_events.len())
        );
        // The delta is exactly what was committed after t2's prefix: t2's new
        // events plus the TaskScheduled recorded for its action.
        assert_eq!(t3.past_events.len(), t2.new_events.len() + 1);
        assert_eq!(
            event_kinds(&t3.past_events[..t2.new_events.len()]),
            event_kinds(&t2.new_events)
        );
        assert_eq!(
            event_kinds(&t3.past_events)[t2.new_events.len()],
            "TaskScheduled"
        );
        // New events are always sent in full (the TaskCompleted of task 1).
        assert!(event_kinds(&t3.new_events).contains(&"TaskCompleted".to_string()));
        let committed_at_t3 = t2.past_events.len() + t3.past_events.len();
        raw.respond(&t3, tok, vec![raw_schedule(2)]).await;

        // Warm becomes the full committed length at t3.
        let (t4, tok) = raw.turn(&id).await;
        assert_eq!(
            t4.cached_history.map(|c| c.event_count as usize),
            Some(committed_at_t3)
        );
        assert_eq!(t4.past_events.len(), t3.new_events.len() + 1);
        raw.respond(
            &t4,
            tok,
            vec![raw_finish(3, proto::OrchestrationStatus::Completed)],
        )
        .await;
        let st = complete(&mut client, &id).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
    }

    #[tokio::test]
    async fn executor_stateful_apply_stateful_history_per_instance_isolation() {
        // Go: warming instance "a" (3 events) does not warm "b": b's first turn is a
        // full send of its 4 events without cached_history; warm a = 3, b = 4.
        setup!(env);
        let mut client = env.new_client().await;
        let mut raw = RawCapableStream::open(&env).await;
        let a = String::from("iso-a");
        let (a2, a3, a_tok) = raw_warm_up(&env, &mut raw, &mut client, &a).await;
        assert!(a3.cached_history.is_some(), "a is warm");
        let warm_a = a2.past_events.len();

        // b's turns on the same stream start cold, independent of a.
        let b = String::from("iso-b");
        let (b2, b3, b_tok) = raw_warm_up(&env, &mut raw, &mut client, &b).await;
        let warm_b = b3.cached_history.expect("b warms on its own").event_count as usize;
        assert_eq!(warm_b, b2.past_events.len());

        // a's delta is still based on a's own warm count.
        raw.respond(&a3, a_tok, vec![raw_schedule(2)]).await;
        let (a4, tok) = raw.turn(&a).await;
        assert_eq!(
            a4.cached_history.map(|c| c.event_count as usize),
            Some(warm_a + a3.past_events.len())
        );
        raw.respond(
            &a4,
            tok,
            vec![raw_finish(3, proto::OrchestrationStatus::Completed)],
        )
        .await;
        raw.respond(
            &b3,
            b_tok,
            vec![raw_finish(2, proto::OrchestrationStatus::Completed)],
        )
        .await;
        assert_eq!(
            complete(&mut client, &a).await.runtime_status,
            OrchestrationStatus::Completed
        );
        assert_eq!(
            complete(&mut client, &b).await.runtime_status,
            OrchestrationStatus::Completed
        );
    }

    #[tokio::test]
    async fn executor_stateful_apply_stateful_history_shrinking_history_falls_back_to_full() {
        // Go: after continue-as-new the committed history shrinks (10 -> 2): the turn
        // is a full send (2 past_events, no cached_history) and warm is re-based to 2.
        // The worker side must drop its cached prefix when the instance completes or
        // continues as new.
        setup!(env);
        let mut client = env.new_client().await;
        {
            let mut raw = RawCapableStream::open(&env).await;
            let id = String::from("shrink-a");
            let (t2, t3, tok) = raw_warm_up(&env, &mut raw, &mut client, &id).await;
            let warm = t2.past_events.len() + t3.past_events.len();
            raw.respond(
                &t3,
                tok,
                vec![raw_finish(2, proto::OrchestrationStatus::ContinuedAsNew)],
            )
            .await;

            // The new execution's history is shorter than the warm count: a
            // full send, no cached_history.
            let (n1, tok) = raw.turn(&id).await;
            assert!(n1.cached_history.is_none(), "{n1:?}");
            assert!(n1.past_events.len() < warm);
            assert!(event_kinds(&n1.new_events).contains(&"ExecutionStarted".to_string()));
            raw.respond(&n1, tok, vec![raw_schedule(0)]).await;

            // Warm was re-based to the new execution's (empty) committed
            // history, so the next turn is full again, and the one after is a
            // delta counted from the new execution only.
            let (n2, tok) = raw.turn(&id).await;
            assert!(n2.cached_history.is_none(), "{n2:?}");
            raw.respond(&n2, tok, vec![raw_schedule(1)]).await;
            let (n3, tok) = raw.turn(&id).await;
            assert_eq!(
                n3.cached_history.map(|c| c.event_count as usize),
                Some(n2.past_events.len())
            );
            raw.respond(
                &n3,
                tok,
                vec![raw_finish(2, proto::OrchestrationStatus::Completed)],
            )
            .await;
            let st = complete(&mut client, &id).await;
            assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        }

        // Worker half: a capable TaskHubGrpcWorker drops its cached prefix on
        // continue-as-new, so every generation replays correctly.
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_named_orchestrator("shrink_orch", |ctx| async move {
                let generation: i32 = ctx.input()?;
                let mut total = generation;
                for _ in 0..3 {
                    let r = ctx.call_activity("shrink_add", total).await?;
                    total = serde_json::from_str(r.as_deref().unwrap())?;
                }
                if generation < 3 {
                    ctx.continue_as_new(generation + 1, false);
                    return Ok(None);
                }
                Ok(Some(total.to_string()))
            });
        worker.registry_mut().add_named_activity(
            "shrink_add",
            |_ctx: ActivityContext, input: Option<String>| async move {
                let n: i32 = serde_json::from_str(input.as_deref().unwrap_or("0"))?;
                Ok(Some((n + 1).to_string()))
            },
        );
        let guard = WorkerGuard::start(worker);
        let id = client
            .schedule_new_orchestration("shrink_orch", Some("0".into()), None, None)
            .await
            .unwrap();
        let st = complete(&mut client, &id).await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(st.serialized_output.as_deref(), Some("6"));
        guard.stop().await;
    }

    // --- backend/runtimestate/runtimestate_test.go ---

    /// A worker whose only "stall_wf" is `version` (marked latest). The
    /// workflow waits for a "go" event and returns `"<version>:<data>"`.
    fn stall_worker(env: &harness::TestEnv, version: &'static str) -> TaskHubGrpcWorker {
        let mut worker = env.new_worker();
        worker
            .registry_mut()
            .add_latest_orchestrator("stall_wf", version, move |ctx| async move {
                let data = ctx.wait_for_external_event("go").await?;
                let data: String = serde_json::from_str(data.as_deref().unwrap_or("\"\""))?;
                Ok(Some(
                    serde_json::to_string(&format!("{version}:{data}")).unwrap(),
                ))
            });
        worker
    }

    fn stall_events(events: &[proto::HistoryEvent]) -> Vec<proto::ExecutionStalledEvent> {
        events
            .iter()
            .filter_map(|e| match &e.event_type {
                Some(EventType::ExecutionStalled(s)) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }

    fn count_raised(events: &[proto::HistoryEvent]) -> usize {
        events
            .iter()
            .filter(|e| matches!(e.event_type, Some(EventType::EventRaised(_))))
            .count()
    }

    /// Poll the history until `n` "go" events have been committed by a turn.
    async fn wait_raised(
        client: &mut TaskHubGrpcClient,
        id: &str,
        n: usize,
    ) -> Vec<proto::HistoryEvent> {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let events = client.get_instance_history(id).await.unwrap_or_default();
            if count_raised(&events) >= n {
                return events;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "event {n} never processed"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Assert the state of an instance whose v1-pinned turn ran on a worker
    /// without v1: it stalled instead of failing.
    ///
    /// The Rust worker answers such a turn with WorkflowVersionNotAvailableAction
    /// (pinned by grpc_worker's `pinned_version_not_registered_stalls_turn`),
    /// which the applier turns into an ExecutionStalled(VERSION_NOT_AVAILABLE,
    /// "Version not available: v1") message. The upstream durabletask-go
    /// standalone sidecar used here queues that message without a target
    /// instance, so it is never recorded and the status reads RUNNING; backends
    /// that route it (daprd) record the event and report STALLED. Both are
    /// accepted, and when recorded the stall's details are checked exactly.
    fn assert_stalled_not_failed(
        st: &OrchestrationState,
        events: &[proto::HistoryEvent],
        expected_stalls: usize,
    ) {
        assert!(
            !st.runtime_status.is_terminal(),
            "the turn must not fail: {st:?}"
        );
        assert!(st.failure_details.is_none(), "{st:?}");
        assert!(
            st.serialized_output.is_none(),
            "v2 must not run the v1 instance"
        );
        // The instance stays pinned to v1.
        assert!(events.iter().all(|e| match &e.event_type {
            Some(EventType::WorkflowStarted(ws)) =>
                ws.version.as_ref().and_then(|v| v.name.as_deref()) == Some("v1"),
            _ => true,
        }));
        assert!(!events.iter().any(|e| matches!(
            e.event_type,
            Some(EventType::ExecutionCompleted(_)) | Some(EventType::TaskScheduled(_))
        )));
        let stalls = stall_events(events);
        if stalls.is_empty() {
            assert_eq!(st.runtime_status, OrchestrationStatus::Running);
        } else {
            assert_eq!(stalls.len(), expected_stalls, "{events:#?}");
            assert_eq!(st.runtime_status, OrchestrationStatus::Stalled);
            for s in &stalls {
                assert_eq!(s.reason, proto::StalledReason::VersionNotAvailable as i32);
                assert_eq!(s.description.as_deref(), Some("Version not available: v1"));
            }
            assert!(matches!(
                events.last().and_then(|e| e.event_type.as_ref()),
                Some(EventType::ExecutionStalled(_))
            ));
        }
    }

    /// Start "stall_wf" on a v1 worker (which pins v1 in history), then hand
    /// it to a worker that only has v2 and deliver an event: the v1-pinned turn
    /// cannot run there and must stall. Returns the running v2 worker.
    async fn stall_on_missing_version(
        env: &harness::TestEnv,
        client: &mut TaskHubGrpcClient,
        id: &str,
    ) -> WorkerGuard {
        let v1 = WorkerGuard::start(stall_worker(env, "v1"));
        client
            .schedule_new_orchestration("stall_wf", None, Some(id.to_string()), None)
            .await
            .unwrap();
        // The first turn is committed once its WorkflowStarted carries the
        // version the worker reported.
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let events = client.get_instance_history(id).await.unwrap_or_default();
            let pinned = events.iter().any(|e| {
                matches!(&e.event_type, Some(EventType::WorkflowStarted(ws))
                    if ws.version.as_ref().and_then(|v| v.name.as_deref()) == Some("v1"))
            });
            if pinned {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "v1 never pinned");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        v1.stop().await;

        let v2 = WorkerGuard::start(stall_worker(env, "v2"));
        client
            .raise_orchestration_event(id, "go", Some("\"one\"".into()))
            .await
            .unwrap();
        wait_raised(client, id, 1).await;
        v2
    }

    #[tokio::test]
    async fn runtimestate_add_event_stalled_set_from_old_events() {
        // Go: a runtime state loaded from [ExecutionStarted,
        // ExecutionStalled(VERSION_NOT_AVAILABLE, "old stall")] has Stalled set with
        // that reason and description, and RuntimeStatus is STALLED.
        //
        // End to end: the Rust worker stalls (WorkflowVersionNotAvailableAction)
        // a turn pinned to a version it does not have, and the state reloaded
        // from the stored events, with no worker running, is the stalled one
        // (see assert_stalled_not_failed for what this sidecar records).
        setup!(env);
        let mut client = env.new_client().await;
        let v2 = stall_on_missing_version(&env, &mut client, "stall-old").await;
        v2.stop().await;

        let mut fresh = env.new_client().await;
        let st = fresh
            .get_orchestration_state("stall-old", true)
            .await
            .unwrap()
            .expect("instance exists");
        let events = fresh.get_instance_history("stall-old").await.unwrap();
        assert_stalled_not_failed(&st, &events, 1);
    }

    #[tokio::test]
    async fn runtimestate_add_event_stalled_replaced_by_new_stalled() {
        // Go: a second ExecutionStalled(VERSION_NOT_AVAILABLE, "second stall")
        // replaces the first (PATCH_MISMATCH, "first stall"): reason and description
        // are the new ones.
        //
        // End to end: a stalled instance that receives another event gets a new
        // turn, stalls again on the still-missing version (the newest stall is
        // the one in effect), and a worker that has the pinned version later
        // resumes it from where it stalled.
        setup!(env);
        let mut client = env.new_client().await;
        let v2 = stall_on_missing_version(&env, &mut client, "stall-twice").await;

        client
            .raise_orchestration_event("stall-twice", "go", Some("\"two\"".into()))
            .await
            .unwrap();
        let events = wait_raised(&mut client, "stall-twice", 2).await;
        let st = client
            .get_orchestration_state("stall-twice", true)
            .await
            .unwrap()
            .expect("instance exists");
        assert_stalled_not_failed(&st, &events, 2);
        v2.stop().await;

        // A worker with the pinned version resumes the instance on its next
        // event and replays with v1.
        let v1 = WorkerGuard::start(stall_worker(&env, "v1"));
        client
            .raise_orchestration_event("stall-twice", "go", Some("\"three\"".into()))
            .await
            .unwrap();
        let st = complete(&mut client, "stall-twice").await;
        assert_eq!(st.runtime_status, OrchestrationStatus::Completed);
        assert_eq!(st.serialized_output.as_deref(), Some("\"v1:one\""));
        v1.stop().await;
    }

    // --- tests/runtimestate_test.go ---

    // --- detached workflows (tests/runtimestate_test.go), observed end to end ---
    //
    // Go applies CreateDetachedWorkflowAction to a runtime state directly and
    // inspects the recorded events and pending messages. Here the Rust SDK
    // emits the action and the sidecar applies it; the caller's persisted
    // history shows the recorded events and the spawned instance's persisted
    // ExecutionStarted shows what the pending message carried.

    async fn sidecar_history(address: &str, id: &str) -> Vec<proto::HistoryEvent> {
        proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient::connect(
            address.to_string(),
        )
        .await
        .expect("connect to sidecar")
        .get_instance_history(proto::GetInstanceHistoryRequest {
            instance_id: id.to_string(),
        })
        .await
        .unwrap_or_else(|e| panic!("GetInstanceHistory({id}) failed: {e}"))
        .into_inner()
        .events
    }

    /// `(event id, instance id, router)` of every DetachedWorkflowInstanceCreated.
    fn detached_created_events(
        events: &[proto::HistoryEvent],
    ) -> Vec<(i32, String, Option<proto::TaskRouter>)> {
        events
            .iter()
            .filter_map(|e| match &e.event_type {
                Some(EventType::DetachedWorkflowInstanceCreated(d)) => {
                    Some((e.event_id, d.instance_id.clone(), e.router.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// The spawned instance's ExecutionStarted event and its envelope router.
    fn execution_started_of(
        events: &[proto::HistoryEvent],
    ) -> (proto::ExecutionStartedEvent, Option<proto::TaskRouter>) {
        events
            .iter()
            .find_map(|e| match &e.event_type {
                Some(EventType::ExecutionStarted(s)) => Some((s.clone(), e.router.clone())),
                _ => None,
            })
            .expect("ExecutionStarted missing")
    }

    /// Registers "Spawned" (echoes its input) and a "Caller" that runs `spawn`
    /// and completes; starts the caller as `caller_id` and waits for it.
    async fn run_detached_caller(
        env: &harness::TestEnv,
        caller_id: &str,
        spawn: impl Fn(&OrchestrationContext) -> DtResult<()> + Send + Sync + 'static,
    ) -> (WorkerGuard, TaskHubGrpcClient) {
        let mut worker = env.new_worker();
        let spawn = Arc::new(spawn);
        worker
            .registry_mut()
            .add_named_orchestrator("Caller", move |ctx| {
                let spawned = spawn(&ctx);
                async move {
                    spawned?;
                    Ok(None)
                }
            });
        worker
            .registry_mut()
            .add_named_orchestrator("Spawned", |ctx| async move {
                Ok(ctx.input::<Option<String>>()?.map(|s| format!("\"{s}\"")))
            });
        let guard = WorkerGuard::start(worker);
        let mut client = env.new_client().await;
        client
            .schedule_new_orchestration("Caller", None, Some(caller_id.to_string()), None)
            .await
            .unwrap();
        let state = complete(&mut client, caller_id).await;
        assert_eq!(
            state.runtime_status,
            OrchestrationStatus::Completed,
            "{:?}",
            state.failure_details
        );
        (guard, client)
    }

    #[tokio::test]
    async fn tests_runtimestate_create_detached_workflow() {
        // Go: applying CreateDetachedWorkflowAction (id 7, instance "spawned")
        // records exactly one DetachedWorkflowInstanceCreated (event id 7, no
        // ChildWorkflowInstanceCreated) and one pending message to "spawned" whose
        // ExecutionStarted carries the name, input, execution ID, tags and scheduled
        // start time, the caller's trace context, and no ParentInstance.
        //
        // Here the spawn is the caller's third action (id 2, after two timers).
        // Not portable: the SDK never sets an execution ID, tags or trace
        // context on the action (Go's SDK doesn't either), so the spawned
        // execution ID is only checked to be minted and tags to be empty.
        setup!(env);
        let start = chrono::DateTime::parse_from_rfc3339("2020-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let (guard, mut client) = run_detached_caller(&env, "caller", move |ctx| {
            drop(ctx.create_timer(Duration::ZERO));
            drop(ctx.create_timer(Duration::ZERO));
            ctx.schedule_new_detached_workflow(
                "Spawned",
                "payload",
                dapr_durabletask::task::DetachedWorkflowOptions::new()
                    .with_instance_id("spawned")
                    .with_start_time(start),
            )
            .map(drop)
        })
        .await;

        let caller = sidecar_history(&env.address, "caller").await;
        let created = detached_created_events(&caller);
        assert_eq!(created.len(), 1, "{created:?}");
        assert_eq!((created[0].0, created[0].1.as_str()), (2, "spawned"));
        assert!(
            !caller.iter().any(|e| matches!(
                e.event_type,
                Some(EventType::ChildWorkflowInstanceCreated(_))
            )),
            "a detached spawn must not record ChildWorkflowInstanceCreated"
        );

        let spawned = complete(&mut client, "spawned").await;
        assert_eq!(spawned.runtime_status, OrchestrationStatus::Completed);
        let (es, _) = execution_started_of(&sidecar_history(&env.address, "spawned").await);
        assert_eq!(es.name, "Spawned");
        assert_eq!(es.input.as_deref(), Some("\"payload\""));
        assert_eq!(es.scheduled_start_timestamp, Some(ts(start)));
        assert!(
            es.parent_instance.is_none(),
            "a detached spawn has no parent"
        );
        let wi = es.workflow_instance.expect("workflow instance");
        assert_eq!(wi.instance_id, "spawned");
        assert!(!wi.execution_id.unwrap_or_default().is_empty());
        assert!(es.tags.is_empty());
        guard.stop().await;
    }

    #[tokio::test]
    async fn tests_runtimestate_create_detached_workflow_mints_execution_id_when_absent() {
        // Go: when the action's execution ID is absent or empty, the spawned
        // ExecutionStarted still gets a non-empty execution ID. The Rust SDK
        // never sets one, so every spawn exercises the "absent" case.
        setup!(env);
        let (guard, mut client) = run_detached_caller(&env, "caller", |ctx| {
            ctx.schedule_new_detached_workflow(
                "Spawned",
                (),
                dapr_durabletask::task::DetachedWorkflowOptions::new(),
            )
            .map(drop)
        })
        .await;

        complete(&mut client, "caller-0").await;
        let (es, _) = execution_started_of(&sidecar_history(&env.address, "caller-0").await);
        let wi = es.workflow_instance.expect("workflow instance");
        assert!(!wi.execution_id.unwrap_or_default().is_empty());
        guard.stop().await;
    }

    #[tokio::test]
    async fn tests_runtimestate_create_detached_workflow_dispatcher_correlation_invariant() {
        // Go: three detached spawns in one batch produce three
        // DetachedWorkflowInstanceCreated events and three messages; every message's
        // target instance matches exactly one event, whose event ID equals the
        // originating action ID.
        setup!(env);
        let (guard, mut client) = run_detached_caller(&env, "caller", |ctx| {
            for id in ["spawn-a", "spawn-b", "spawn-c"] {
                ctx.schedule_new_detached_workflow(
                    "Spawned",
                    (),
                    dapr_durabletask::task::DetachedWorkflowOptions::new().with_instance_id(id),
                )?;
            }
            Ok(())
        })
        .await;

        let created = detached_created_events(&sidecar_history(&env.address, "caller").await);
        let pairs: Vec<(i32, &str)> = created.iter().map(|(i, s, _)| (*i, s.as_str())).collect();
        assert_eq!(pairs, [(0, "spawn-a"), (1, "spawn-b"), (2, "spawn-c")]);
        for (_, id) in pairs {
            let s = complete(&mut client, id).await;
            assert_eq!(s.runtime_status, OrchestrationStatus::Completed);
        }
        guard.stop().await;
    }

    #[tokio::test]
    async fn tests_runtimestate_create_detached_workflow_local_spawn_no_router() {
        // Go: a detached spawn without a router yields a pending message without a
        // router. Observable: the SDK emits the action without a router, and
        // neither the caller's creation event nor the spawned instance's
        // ExecutionStarted carries a routing target (the sidecar stamps its own
        // app ID as the source on persisted events, so only targets are checked).
        setup!(env);
        let (guard, mut client) = run_detached_caller(&env, "caller", |ctx| {
            ctx.schedule_new_detached_workflow(
                "Spawned",
                (),
                dapr_durabletask::task::DetachedWorkflowOptions::new().with_instance_id("local"),
            )
            .map(drop)
        })
        .await;

        let created = detached_created_events(&sidecar_history(&env.address, "caller").await);
        assert_eq!(created.len(), 1);
        let untargeted = |r: &Option<proto::TaskRouter>| {
            r.as_ref()
                .is_none_or(|r| r.target_app_id.is_none() && r.target_app_namespace.is_none())
        };
        assert!(untargeted(&created[0].2), "{:?}", created[0].2);
        complete(&mut client, "local").await;
        let (_, router) = execution_started_of(&sidecar_history(&env.address, "local").await);
        assert!(untargeted(&router), "{router:?}");
        guard.stop().await;
    }

    #[tokio::test]
    async fn tests_runtimestate_create_detached_workflow_missing_instance_id_errors() {
        // Go: the applier rejects a CreateDetachedWorkflowAction with an empty
        // instance ID (the SDK always fills in "<caller>-<n>" when none is given).
        // SDK half: an action with an empty instance ID is never emitted; an
        // explicit empty ID is rejected and an omitted one defaults.
        let f = orch(|ctx| async move {
            let opts = dapr_durabletask::task::DetachedWorkflowOptions::new;
            assert!(
                ctx.schedule_new_detached_workflow("Spawned", (), opts().with_instance_id(""))
                    .is_err()
            );
            ctx.schedule_new_detached_workflow("Spawned", (), opts())?;
            std::future::pending::<()>().await;
            Ok(None)
        });
        let resp = run(&f, vec![], vec![ws(), es("parent", None)]).await;
        let ids: Vec<&str> = resp
            .actions
            .iter()
            .filter_map(|a| match &a.workflow_action_type {
                Some(Wat::CreateDetachedWorkflow(d)) => Some(d.instance_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(ids.len(), 1, "{:?}", resp.actions);
        assert!(!ids[0].is_empty());
        assert!(ids[0].ends_with("-0"), "{}", ids[0]);
    }

    #[tokio::test]
    async fn tests_runtimestate_create_detached_workflow_router_propagated() {
        // Go: a cross-app detached spawn's pending message carries a router with
        // only the target app ID (no source app, no namespace), while the caller's
        // history event keeps the full action router, including source "caller-app".
        //
        // Observable: the caller's recorded event keeps the router the SDK put
        // on the action (target app ID). Not portable: the SDK leaves the source
        // app empty (the Dapr runtime fills it in), and the standalone sidecar
        // has no second app to deliver the spawned instance's message to.
        setup!(env);
        let (guard, _client) = run_detached_caller(&env, "caller", |ctx| {
            ctx.schedule_new_detached_workflow(
                "Spawned",
                (),
                dapr_durabletask::task::DetachedWorkflowOptions::new()
                    .with_instance_id("remote")
                    .with_app_id("target-app"),
            )
            .map(drop)
        })
        .await;

        let created = detached_created_events(&sidecar_history(&env.address, "caller").await);
        assert_eq!(created.len(), 1);
        let router = created[0]
            .2
            .as_ref()
            .expect("router kept on the history event");
        assert_eq!(router.target_app_id.as_deref(), Some("target-app"));
        assert_eq!(router.target_app_namespace, None);
        guard.stop().await;
    }
}
