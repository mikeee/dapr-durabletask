//! Human Interaction Example
//!
//! Demonstrates the external event and timer pattern with three concurrent
//! orchestrations running in parallel:
//!
//! 1. Start orchestrations 1, 2, 3 (each waits for an "approval" event or times out).
//! 2. Print the status of all three.
//! 3. Send approvals in order: 1 → 2 → 3, waiting for each to complete before
//!    sending the next so the processing order is deterministic.
//!
//! Requires a running Durable Task sidecar at localhost:4001.

use std::time::Duration;

use dapr_durabletask::api::Result;
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{ActivityContext, OrchestrationContext, when_any};
use dapr_durabletask::worker::{ActivityResult, OrchestratorResult, TaskHubGrpcWorker};

/// Activity: sends an approval request (placeholder).
async fn send_approval_request(_ctx: ActivityContext, input: Option<String>) -> ActivityResult {
    let order: String = serde_json::from_str(input.as_deref().unwrap_or("\"\""))?;
    println!("[activity] Sending approval request for order: {}", order);
    Ok(None)
}

/// Activity: process an approved order.
async fn process_order(_ctx: ActivityContext, input: Option<String>) -> ActivityResult {
    let order: String = serde_json::from_str(input.as_deref().unwrap_or("\"\""))?;
    println!("[activity] Processing approved order: {}", order);
    Ok(Some(serde_json::to_string(&format!(
        "Processed: {}",
        order
    ))?))
}

/// Orchestrator: waits for an "approval" external event or a 1-hour timeout.
async fn approval_workflow(ctx: OrchestrationContext) -> OrchestratorResult {
    let order: String = ctx.get_input().unwrap_or_else(|_| "default-order".into());

    ctx.call_activity("send_approval_request", &order).await?;

    let approval_task = ctx.wait_for_external_event("approval");
    let timeout_task = ctx.create_timer(Duration::from_secs(60 * 60));

    let winner = when_any(vec![approval_task, timeout_task]).await?;

    if winner == 1 {
        Ok(Some(serde_json::to_string("Order timed out — cancelled")?))
    } else {
        let result = ctx.call_activity("process_order", &order).await?;
        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let address = "http://localhost:4001";

    // ── Start worker ──────────────────────────────────────────────────────────
    let mut worker = TaskHubGrpcWorker::new(address);
    worker
        .registry_mut()
        .add_named_orchestrator("approval_workflow", approval_workflow);
    worker
        .registry_mut()
        .add_named_activity("send_approval_request", send_approval_request);
    worker
        .registry_mut()
        .add_named_activity("process_order", process_order);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_handle = tokio::spawn(async move { worker.start(worker_shutdown).await });

    let mut client = TaskHubGrpcClient::new(address).await?;

    // ── 1. Start orchestrations 1, 2, 3 ──────────────────────────────────────
    let orders = ["order-1", "order-2", "order-3"];
    let mut instance_ids: Vec<String> = Vec::with_capacity(orders.len());

    for (i, order) in orders.iter().enumerate() {
        let input = serde_json::to_string(order)?;
        let id = client
            .schedule_new_orchestration("approval_workflow", Some(input), None, None)
            .await?;
        println!(
            "[main] Started orchestration {} → instance_id={}",
            i + 1,
            id
        );
        instance_ids.push(id);
    }

    // Give the worker a moment to pick up the orchestrations so they show
    // "Running" when we print status below.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── 2. Print status of all three ─────────────────────────────────────────
    println!("\n[main] === Current status ===");
    for (i, id) in instance_ids.iter().enumerate() {
        let state = client.get_orchestration_state(id, false).await?;
        match state {
            Some(s) => println!(
                "[main]   Orchestration {} ({}): {}",
                i + 1,
                id,
                s.runtime_status
            ),
            None => println!("[main]   Orchestration {} ({}): not found", i + 1, id),
        }
    }
    println!();

    // ── 3. Approve 1 → 2 → 3, waiting for each to complete before the next ──
    for (i, id) in instance_ids.iter().enumerate() {
        let approval_payload =
            serde_json::to_string(&serde_json::json!({"approved": true, "approver": "admin"}))?;

        println!(
            "[main] Sending approval for orchestration {} ({})",
            i + 1,
            id
        );
        client
            .raise_orchestration_event(id, "approval", Some(approval_payload))
            .await?;

        // Wait for this orchestration to complete before approving the next,
        // ensuring processing order matches approval order: 1 → 2 → 3.
        let state = client
            .wait_for_orchestration_completion(id, true, Some(Duration::from_secs(30)))
            .await?;

        match state {
            Some(s) => println!(
                "[main] Orchestration {} completed — status={} output={:?}\n",
                i + 1,
                s.runtime_status,
                s.serialized_output
            ),
            None => println!(
                "[main] Orchestration {} did not complete within timeout\n",
                i + 1
            ),
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────
    shutdown.cancel();
    let _ = worker_handle.await;

    Ok(())
}
