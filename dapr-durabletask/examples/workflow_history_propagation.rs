//! # Workflow History Propagation Example
//!
//! Mirrors the Go SDK `examples/workflow-history-propagation` demo. A
//! 3-tier payment workflow shows both propagation scopes:
//!
//!   PaymentParent
//!     ├─ "verify_funds" activity      — Lineage   (gets parent + ancestors)
//!     └─ FraudCheckChild              — Lineage   (gets parent + ancestors)
//!          └─ "audit_log" activity    — OwnHistory (only sees the child's own
//!                                                   events; parent is dropped)
//!
//! Each child / activity logs `ctx.propagated_history()` so the chain of
//! custody is visible in the output.
//!
//! Run against a Dapr sidecar that exposes the workflow gRPC API (default
//! `localhost:4001`) — for example `dapr run --app-id rust-app
//! --dapr-grpc-port 4001 -- cargo run --example workflow_history_propagation`.

use dapr_durabletask::api::{HistoryPropagationScope, Result};
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{
    ActivityContext, ActivityOptions, OrchestrationContext, SubOrchestratorOptions,
};
use dapr_durabletask::worker::{ActivityResult, OrchestratorResult, TaskHubGrpcWorker};

async fn verify_funds(ctx: ActivityContext, input: Option<String>) -> ActivityResult {
    if let Some(history) = ctx.propagated_history() {
        println!(
            "[verify_funds] received history scope={:?}, app_ids={:?}, total_events={}",
            history.scope,
            history.app_ids(),
            history.events.len(),
        );
    } else {
        println!("[verify_funds] no propagated history attached");
    }
    let amount: f64 = serde_json::from_str(input.as_deref().unwrap_or("0"))?;
    let approved = amount < 10_000.0;
    Ok(Some(serde_json::to_string(&approved)?))
}

async fn audit_log(ctx: ActivityContext, input: Option<String>) -> ActivityResult {
    if let Some(history) = ctx.propagated_history() {
        println!(
            "[audit_log] OwnHistory scope={:?}, app_ids={:?} (ancestors must be dropped)",
            history.scope,
            history.app_ids(),
        );
        for chunk in &history.chunks {
            println!(
                "  chunk: app={} workflow={} instance={} events={}",
                chunk.app_id, chunk.workflow_name, chunk.instance_id, chunk.event_count
            );
        }
    } else {
        println!("[audit_log] no propagated history attached");
    }
    let entry: String = serde_json::from_str(input.as_deref().unwrap_or("\"\""))?;
    println!("[audit_log] {entry}");
    Ok(Some(serde_json::to_string(&"logged")?))
}

async fn fraud_check_child(ctx: OrchestrationContext) -> OrchestratorResult {
    if let Some(history) = ctx.propagated_history() {
        println!(
            "[FraudCheckChild] received Lineage scope={:?} with {} chunk(s) covering apps {:?}",
            history.scope,
            history.chunks.len(),
            history.app_ids(),
        );
    } else {
        println!("[FraudCheckChild] no propagated history (parent did not opt in)");
    }

    // Forward only this child's own history to the audit activity — the
    // parent's events must not be visible from inside audit_log.
    let _ = ctx
        .call_activity_with_options(
            "audit_log",
            "fraud check completed",
            ActivityOptions::new().with_history_propagation(HistoryPropagationScope::OwnHistory),
        )
        .await?;

    Ok(Some(serde_json::to_string(&"clean")?))
}

async fn payment_parent(ctx: OrchestrationContext) -> OrchestratorResult {
    let amount: f64 = ctx
        .get_input::<Option<f64>>()
        .ok()
        .flatten()
        .unwrap_or(250.0);

    // Activity call with Lineage — verify_funds gets parent + any ancestors.
    let _ = ctx
        .call_activity_with_options(
            "verify_funds",
            amount,
            ActivityOptions::new().with_history_propagation(HistoryPropagationScope::Lineage),
        )
        .await?;

    // Child workflow with Lineage — the child receives the parent's events
    // and forwards them again under OwnHistory to its audit activity.
    let _ = ctx
        .call_sub_orchestrator_with_options(
            "FraudCheckChild",
            serde_json::Value::Null,
            SubOrchestratorOptions::new()
                .with_history_propagation(HistoryPropagationScope::Lineage),
        )
        .await?;

    Ok(Some(serde_json::to_string(&format!(
        "payment {amount:.2} processed"
    ))?))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let address = "http://localhost:4001";

    let mut worker = TaskHubGrpcWorker::new(address);
    worker
        .registry_mut()
        .add_named_orchestrator("PaymentParent", payment_parent);
    worker
        .registry_mut()
        .add_named_orchestrator("FraudCheckChild", fraud_check_child);
    worker
        .registry_mut()
        .add_named_activity("verify_funds", verify_funds);
    worker
        .registry_mut()
        .add_named_activity("audit_log", audit_log);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_handle = tokio::spawn(async move { worker.start(worker_shutdown).await });

    let mut client = TaskHubGrpcClient::new(address).await?;
    let instance_id = client
        .schedule_new_orchestration("PaymentParent", None, Some("250.0".to_string()), None)
        .await?;
    println!("Started PaymentParent: {instance_id}");

    if let Some(state) = client
        .wait_for_orchestration_completion(&instance_id, true, None)
        .await?
    {
        println!("Status: {}", state.runtime_status);
        println!("Output: {:?}", state.serialized_output);
    }

    shutdown.cancel();
    let _ = worker_handle.await;
    Ok(())
}
