//! Fan-out / Fan-in Example
//!
//! Demonstrates the parallel execution pattern: multiple activities are
//! scheduled concurrently (fan-out) and the results are aggregated (fan-in).
//!
//! Requires a running Durable Task sidecar at localhost:4001.

use dapr_durabletask::api::Result;
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{ActivityContext, OrchestrationContext, when_all};
use dapr_durabletask::worker::{ActivityResult, OrchestratorResult, TaskHubGrpcWorker};

/// Activity: returns a list of work items to process.
async fn get_work_items(_ctx: ActivityContext, _input: Option<String>) -> ActivityResult {
    let items = vec!["item-1", "item-2", "item-3", "item-4", "item-5"];
    Ok(Some(serde_json::to_string(&items)?))
}

/// Activity: "processes" a work item and returns a numeric result.
async fn process_item(_ctx: ActivityContext, input: Option<String>) -> ActivityResult {
    let item: String = serde_json::from_str(input.as_deref().unwrap_or("\"\""))?;
    let result = item.len() as i32;
    Ok(Some(serde_json::to_string(&result)?))
}

/// Orchestrator: fans out work items to parallel activities, then aggregates.
async fn fanout_fanin(ctx: OrchestrationContext) -> OrchestratorResult {
    // Step 1: get work items
    let items_json = ctx.call_activity("get_work_items", ()).await?;
    let items: Vec<String> = serde_json::from_str(items_json.as_deref().unwrap_or("[]"))?;

    // Step 2: fan-out — schedule all activities in parallel
    let tasks: Vec<_> = items
        .iter()
        .map(|item| ctx.call_activity("process_item", item))
        .collect();

    // Step 3: fan-in — wait for all to complete
    let results = when_all(tasks).await?;

    // Step 4: aggregate results
    let total: i32 = results
        .iter()
        .map(|r| {
            r.as_deref()
                .and_then(|s| serde_json::from_str::<i32>(s).ok())
                .unwrap_or(0)
        })
        .sum();

    Ok(Some(serde_json::to_string(&total)?))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let address = "http://localhost:4001";

    let mut worker = TaskHubGrpcWorker::new(address);
    worker
        .registry_mut()
        .add_named_orchestrator("fanout_fanin", fanout_fanin);
    worker
        .registry_mut()
        .add_named_activity("get_work_items", get_work_items);
    worker
        .registry_mut()
        .add_named_activity("process_item", process_item);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_handle = tokio::spawn(async move { worker.start(worker_shutdown).await });

    let mut client = TaskHubGrpcClient::new(address).await?;
    let instance_id = client
        .schedule_new_orchestration("fanout_fanin", None, None, None)
        .await?;
    println!("Started orchestration: {}", instance_id);

    let state = client
        .wait_for_orchestration_completion(&instance_id, true, None)
        .await?;
    if let Some(state) = state {
        println!("Status: {}", state.runtime_status);
        println!("Total: {:?}", state.serialized_output);
    }

    shutdown.cancel();
    let _ = worker_handle.await;

    Ok(())
}
