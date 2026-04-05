//! Activity Sequence Example
//!
//! Demonstrates the function chaining pattern: multiple activities are called
//! sequentially, with each result feeding into the next.
//!
//! Requires a running Durable Task sidecar at localhost:4001.

use dapr_durabletask::api::Result;
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{ActivityContext, OrchestrationContext};
use dapr_durabletask::worker::{ActivityResult, OrchestratorResult, TaskHubGrpcWorker};

/// Activity: greets a city by name.
async fn say_hello(_ctx: ActivityContext, input: Option<String>) -> ActivityResult {
    let name: String = serde_json::from_str(input.as_deref().unwrap_or("\"World\""))?;
    let greeting = format!("Hello, {}!", name);
    Ok(Some(serde_json::to_string(&greeting)?))
}

/// Orchestrator: calls `say_hello` for several cities in sequence.
async fn sequence(ctx: OrchestrationContext) -> OrchestratorResult {
    let r1: String = serde_json::from_str(
        ctx.call_activity("say_hello", "Tokyo")
            .await?
            .as_deref()
            .unwrap_or("null"),
    )?;
    let r2: String = serde_json::from_str(
        ctx.call_activity("say_hello", "Seattle")
            .await?
            .as_deref()
            .unwrap_or("null"),
    )?;
    let r3: String = serde_json::from_str(
        ctx.call_activity("say_hello", "London")
            .await?
            .as_deref()
            .unwrap_or("null"),
    )?;

    let results = vec![r1, r2, r3];
    Ok(Some(serde_json::to_string(&results)?))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let address = "http://localhost:4001";

    // Register orchestrator and activities with the worker
    let mut worker = TaskHubGrpcWorker::new(address);
    worker
        .registry_mut()
        .add_named_orchestrator("sequence", sequence);
    worker
        .registry_mut()
        .add_named_activity("say_hello", say_hello);

    // Start the worker in the background
    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_handle = tokio::spawn(async move { worker.start(worker_shutdown).await });

    // Use the client to schedule the orchestration
    let mut client = TaskHubGrpcClient::new(address).await?;
    let instance_id = client
        .schedule_new_orchestration("sequence", None, None, None)
        .await?;
    println!("Started orchestration: {}", instance_id);

    // Wait for completion
    let state = client
        .wait_for_orchestration_completion(&instance_id, true, None)
        .await?;
    if let Some(state) = state {
        println!("Status: {}", state.runtime_status);
        println!("Output: {:?}", state.serialized_output);
    }

    // Shutdown
    shutdown.cancel();
    let _ = worker_handle.await;

    Ok(())
}
