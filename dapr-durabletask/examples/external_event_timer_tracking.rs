//! External Event Timer Tracking Example
//!
//! Demonstrates `wait_for_external_event_with_timeout`, which waits for an
//! external event while emitting a Durable Task timer tagged with
//! `origin: ExternalEvent { name: "approval" }`. Runtime history can use that
//! origin to distinguish event-wait timers from regular workflow timers.
//!
//! Requires a running Durable Task sidecar at localhost:4001.

use std::time::Duration;

use dapr_durabletask::api::Result;
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{ExternalEventResult, OrchestrationContext};
use dapr_durabletask::worker::{OrchestratorResult, TaskHubGrpcWorker};

const ORCHESTRATOR: &str = "approval_timer_tracking";
const APPROVAL_EVENT: &str = "approval";
const APPROVAL_TIMEOUT_SECS: u64 = 10;

async fn approval_timer_tracking(ctx: OrchestrationContext) -> OrchestratorResult {
    let request: String = ctx
        .input()
        .unwrap_or_else(|_| "approval-request".to_string());

    match ctx
        .wait_for_external_event_with_timeout(
            APPROVAL_EVENT,
            Duration::from_secs(APPROVAL_TIMEOUT_SECS),
        )
        .await?
    {
        ExternalEventResult::Received(payload) => {
            Ok(Some(serde_json::to_string(&serde_json::json!({
                "request": request,
                "status": "approved",
                "payload": payload,
            }))?))
        }
        ExternalEventResult::TimedOut => Ok(Some(serde_json::to_string(&serde_json::json!({
            "request": request,
            "status": "timed out",
        }))?)),
    }
}

async fn print_completion(
    client: &mut TaskHubGrpcClient,
    label: &str,
    instance_id: &str,
) -> Result<()> {
    let state = client
        .wait_for_orchestration_completion(instance_id, true, Some(Duration::from_secs(30)))
        .await?;

    match state {
        Some(state) => {
            println!(
                "[main] {label} completed: status={} output={:?}",
                state.runtime_status, state.serialized_output
            );
        }
        None => {
            println!("[main] {label} did not complete");
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let address = "http://localhost:4001";

    let mut worker = TaskHubGrpcWorker::new(address);
    worker
        .registry_mut()
        .add_named_orchestrator(ORCHESTRATOR, approval_timer_tracking);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_handle = tokio::spawn(async move { worker.start(worker_shutdown).await });

    let mut client = TaskHubGrpcClient::new(address).await?;

    let approved_id = client
        .schedule_new_orchestration(
            ORCHESTRATOR,
            Some(serde_json::to_string("request-with-approval")?),
            None,
            None,
        )
        .await?;
    println!("[main] Started approval path: {approved_id}");

    client
        .wait_for_orchestration_start(&approved_id, false, Some(Duration::from_secs(30)))
        .await?;

    let approval_payload = serde_json::to_string(&serde_json::json!({
        "approved": true,
        "approver": "admin",
    }))?;
    client
        .raise_orchestration_event(&approved_id, APPROVAL_EVENT, Some(approval_payload))
        .await?;
    print_completion(&mut client, "approval path", &approved_id).await?;

    let timed_out_id = client
        .schedule_new_orchestration(
            ORCHESTRATOR,
            Some(serde_json::to_string("request-without-approval")?),
            None,
            None,
        )
        .await?;
    println!("[main] Started timeout path: {timed_out_id}");
    print_completion(&mut client, "timeout path", &timed_out_id).await?;

    shutdown.cancel();
    let _ = worker_handle.await;

    Ok(())
}
