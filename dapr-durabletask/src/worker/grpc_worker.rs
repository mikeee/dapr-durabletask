use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use tokio::sync::Semaphore;

use crate::api::DurableTaskError;
use crate::internal::validate_identifier;
use crate::proto;
use crate::proto::history_event::EventType;
use crate::proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient;
use crate::proto::work_item::Request;

use super::activity_executor::ActivityExecutor;
use super::options::{HistoryCacheOptions, WorkerOptions};
use super::orchestration_executor::OrchestrationExecutor;
use super::reconnect_policy::BackoffIter;
use super::registry::{OrchestratorResolution, Registry};

/// Worker that connects to a Durable Task sidecar and processes work items.
///
/// The worker opens a streaming gRPC connection to receive orchestrator and
/// activity work items, dispatches them to registered handler functions, and
/// returns results to the sidecar.
///
/// # Example
///
/// ```rust,no_run
/// use dapr_durabletask::worker::TaskHubGrpcWorker;
/// use dapr_durabletask::task::OrchestrationContext;
///
/// # async fn example() {
/// let mut worker = TaskHubGrpcWorker::new("http://localhost:4001");
/// worker.registry_mut().add_named_orchestrator("my_orch", |ctx: OrchestrationContext| async move {
///     let result = ctx.call_activity("greet", "world").await?;
///     Ok(result)
/// });
/// worker.registry_mut().add_named_activity("greet", |_ctx, input| async move {
///     Ok(input)
/// });
///
/// let shutdown = tokio_util::sync::CancellationToken::new();
/// // worker.start(shutdown).await.unwrap();
/// # }
/// ```
pub struct TaskHubGrpcWorker {
    host_address: String,
    registry: Arc<Registry>,
    options: Arc<WorkerOptions>,
}

impl TaskHubGrpcWorker {
    /// Create a new worker that will connect to the given sidecar address.
    pub fn new(host_address: &str) -> Self {
        Self {
            host_address: host_address.to_string(),
            registry: Arc::new(Registry::new()),
            options: Arc::new(WorkerOptions::default()),
        }
    }

    /// Create a new worker with custom options.
    pub fn with_options(host_address: &str, options: WorkerOptions) -> Self {
        Self {
            host_address: host_address.to_string(),
            registry: Arc::new(Registry::new()),
            options: Arc::new(options),
        }
    }

    /// Get a mutable reference to the registry for adding orchestrators and activities.
    ///
    /// # Panics
    ///
    /// Panics if called after the registry has been shared (i.e., after `start()`
    /// has begun processing).
    pub fn registry_mut(&mut self) -> &mut Registry {
        Arc::get_mut(&mut self.registry).expect("Cannot modify registry after worker has started")
    }

    /// Start the worker. Runs until the cancellation token is triggered or the
    /// reconnect policy's `max_attempts` is exhausted.
    ///
    /// ## Shutdown behaviour
    ///
    /// When the cancellation token is fired:
    /// 1. The worker **stops reading new work items** from the sidecar stream.
    /// 2. It **waits for all in-flight tasks** (orchestrations and activities
    ///    already dispatched) to complete and send their results to the sidecar
    ///    before returning.
    ///
    /// This guarantees that no already-accepted work item is abandoned at
    /// shutdown unless it opts in to cancellation. Work items still queued
    /// inside the sidecar but not yet dispatched to this worker are unaffected
    /// — the sidecar will re-dispatch them to the next available worker.
    ///
    /// Firing the token also cancels every in-flight activity's
    /// [`ActivityContext::cancelled`](crate::task::ActivityContext::cancelled)
    /// signal. An activity that reacts by returning an error is *abandoned*:
    /// its failure is not reported, so the drain does not wait on it beyond
    /// its own return and the sidecar redelivers the work item (like
    /// durabletask-go, whose processors settle on `ctx.Done()`). Activities
    /// that ignore the signal are drained and reported as described above. A
    /// work item received while waiting for a free concurrency slot at
    /// shutdown is not started.
    ///
    /// ## Stateful history
    ///
    /// Unless disabled with [`WorkerOptions::with_stateful_history_disabled`],
    /// the worker advertises `WORKER_CAPABILITY_STATEFUL_HISTORY` and keeps a
    /// bounded per-instance history cache for the lifetime of each work-item
    /// stream, so the sidecar can send only the new part of an instance's
    /// history on each turn (see [`HistoryCacheOptions`]).
    ///
    /// [`ReconnectPolicy`]: super::reconnect_policy::ReconnectPolicy
    pub async fn start(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> crate::api::Result<()> {
        let mut backoff = BackoffIter::new(&self.options.reconnect_policy);

        // The history cache lives as long as this listener; it is reset
        // whenever a new work-item stream is opened. A janitor reclaims idle
        // entries until `start()` returns.
        let history_cache = Arc::new(WorkflowHistoryCache::new(&self.options.history_cache));
        let janitor_stop = CancellationToken::new();
        let _janitor_guard = janitor_stop.clone().drop_guard();
        if self.options.stateful_history {
            tokio::spawn(history_cache.clone().run_janitor(janitor_stop));
        }

        loop {
            if shutdown.is_cancelled() {
                tracing::info!("Worker shutdown before connecting");
                return Ok(());
            }

            tracing::info!(address = %self.host_address, "Worker connecting to sidecar");

            match Self::connect(&self.host_address).await {
                Ok(channel) => {
                    tracing::info!(address = %self.host_address, "Worker connected, starting work loop");
                    backoff.reset();

                    let mut client = TaskHubSidecarServiceClient::new(channel);

                    match Self::run_work_loop(
                        &mut client,
                        &self.registry,
                        &self.options,
                        &history_cache,
                        &shutdown,
                    )
                    .await
                    {
                        Ok(()) => {
                            if shutdown.is_cancelled() {
                                tracing::info!(
                                    "Worker shut down gracefully after draining in-flight tasks"
                                );
                            } else {
                                tracing::info!("Work item stream closed cleanly; shutting down");
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Work loop error");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Connection to sidecar failed");
                }
            }

            // Connection failed or stream dropped — apply backoff.
            match backoff.next_delay() {
                None => {
                    let msg = format!(
                        "Worker exceeded maximum reconnect attempts ({}); giving up",
                        self.options.reconnect_policy.max_attempts.unwrap_or(0)
                    );
                    tracing::error!("{}", msg);
                    return Err(DurableTaskError::ConnectionFailed(msg));
                }
                Some(delay) => {
                    tracing::info!(
                        delay_ms = delay.as_millis(),
                        address = %self.host_address,
                        "Waiting before reconnect"
                    );
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!("Worker shutdown during reconnect wait");
                            return Ok(());
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    async fn connect(address: &str) -> crate::api::Result<Channel> {
        const USER_AGENT: &str = concat!("dapr-durabletask/rust/", env!("CARGO_PKG_VERSION"));

        Channel::from_shared(address.to_string())
            .map_err(|e| DurableTaskError::InvalidAddress(format!("Invalid address: {e}")))?
            .user_agent(USER_AGENT)
            .map_err(|e| DurableTaskError::InvalidAddress(format!("Invalid user agent: {e}")))?
            .connect()
            .await
            .map_err(|e| DurableTaskError::ConnectionFailed(format!("Connection failed: {e}")))
    }

    async fn run_work_loop(
        client: &mut TaskHubSidecarServiceClient<Channel>,
        registry: &Arc<Registry>,
        options: &Arc<WorkerOptions>,
        history_cache: &Arc<WorkflowHistoryCache>,
        shutdown: &tokio_util::sync::CancellationToken,
    ) -> crate::api::Result<()> {
        let request = work_items_request(options);
        // The sidecar may withhold response headers until the first work item,
        // so opening the stream must not block shutdown.
        let mut stream = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("Shutdown while opening the work item stream");
                return Ok(());
            }
            response = client.get_work_items(request) => response?.into_inner(),
        };
        // On a new stream the sidecar holds no warm state for this worker, so
        // start cold to stay in sync with it.
        history_cache.reset();
        // Cancelled by a work item that cannot reconstruct its history, to
        // force a reconnect (the sidecar then redelivers the work item with
        // the full history on the new, cold stream).
        let stream_teardown = CancellationToken::new();
        let dispatch = DispatchContext {
            registry,
            options,
            semaphore: Arc::new(Semaphore::new(options.max_concurrent_work_items)),
            history_cache,
            shutdown,
            stream_teardown: &stream_teardown,
        };
        let mut tasks: JoinSet<()> = JoinSet::new();
        tracing::info!("Work item stream established");

        // `shutdown_triggered` tracks whether we exited the intake loop because
        // of a cancellation (true) or because the stream closed (false/error).
        let shutdown_triggered = loop {
            prune_finished_tasks(&mut tasks);
            tokio::select! {
                biased; // check shutdown first so we don't accept more items
                _ = shutdown.cancelled() => {
                    tracing::info!(
                        in_flight = tasks.len(),
                        "Shutdown: stopping intake, draining in-flight work items"
                    );
                    break true;
                }
                _ = stream_teardown.cancelled() => {
                    tracing::warn!("Resetting the work item stream after a history resolution failure");
                    break false;
                }
                Some(outcome) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(e) = outcome {
                        tracing::error!(error = ?e, "Work item task panicked");
                    }
                }
                item = stream.next() => {
                    match item {
                        None => {
                            // Sidecar closed the stream — treat as a transient
                            // error so the caller will reconnect.
                            tracing::info!("Work item stream closed by sidecar");
                            break false;
                        }
                        Some(Err(e)) => {
                            return Err(DurableTaskError::ConnectionFailed(format!("Stream error: {e}")));
                        }
                        Some(Ok(work_item)) => {
                            Self::dispatch_work_item(
                                work_item,
                                client.clone(),
                                &dispatch,
                                &mut tasks,
                            ).await?;
                        }
                    }
                }
            }
        };

        if stream_teardown.is_cancelled() {
            // Close the stream now rather than after the drain, so the sidecar
            // promptly cancels and redelivers the turn that could not resolve
            // its history (durabletask-go cancels the stream's context).
            drop(stream);
        }

        // Drain all in-flight tasks before returning, regardless of why we stopped.
        if !tasks.is_empty() {
            tracing::info!(count = tasks.len(), "Draining in-flight work items");
            while let Some(outcome) = tasks.join_next().await {
                if let Err(e) = outcome {
                    tracing::error!(error = ?e, "In-flight task panicked during drain");
                }
            }
            tracing::info!("All in-flight work items drained");
        }

        if shutdown_triggered {
            // Caller checks shutdown.is_cancelled() to know this was intentional.
            Ok(())
        } else {
            // Stream closed by sidecar — signal the caller to reconnect.
            Err(DurableTaskError::ConnectionFailed(
                "Work item stream closed by sidecar".into(),
            ))
        }
    }

    /// Validate and dispatch a single work item into the `JoinSet`.
    async fn dispatch_work_item(
        work_item: proto::WorkItem,
        client: TaskHubSidecarServiceClient<Channel>,
        ctx: &DispatchContext<'_>,
        tasks: &mut JoinSet<()>,
    ) -> crate::api::Result<()> {
        let DispatchContext {
            registry, options, ..
        } = ctx;
        match work_item.request {
            Some(Request::WorkflowRequest(req)) => {
                let instance_id = req.instance_id.clone();
                if let Err(e) =
                    validate_identifier(&instance_id, "instance ID", options.max_identifier_length)
                {
                    tracing::warn!(
                        instance_id = %instance_id,
                        error = %e,
                        "Rejected work item: invalid instance ID"
                    );
                    return Ok(());
                }
                tracing::debug!(
                    instance_id = %instance_id,
                    past_events = req.past_events.len(),
                    new_events = req.new_events.len(),
                    "Received orchestrator work item"
                );

                let registry = (*registry).clone();
                let options = (*options).clone();
                let mut stub = client;
                let completion_token = work_item.completion_token.clone();
                let Some(permit) = ctx.acquire_permit().await? else {
                    return Ok(());
                };
                let history_cache = ctx.history_cache.clone();
                let stream_teardown = ctx.stream_teardown.clone();
                if options.stateful_history {
                    history_cache.note_dispatch(&instance_id, &completion_token);
                }

                tasks.spawn(async move {
                    let _permit = permit;
                    let mut req = req;
                    // Rebuild the full committed history: the cached prefix
                    // plus the delta, or a GetInstanceHistory fetch on a miss.
                    let fetch_client = stub.clone();
                    if let Err(e) = resolve_workflow_history(&history_cache, &mut req, |iid| {
                        fetch_instance_history(fetch_client, iid)
                    })
                    .await
                    {
                        // No per-item NACK exists: tear down the stream so the
                        // sidecar cancels and promptly redelivers this turn.
                        tracing::error!(
                            instance_id = %instance_id,
                            error = %e,
                            "Failed to resolve workflow history; resetting the work item stream"
                        );
                        stream_teardown.cancel();
                        return;
                    }
                    let committed = options.stateful_history.then(|| req.past_events.clone());
                    let response = Self::handle_orchestrator_request(
                        &registry,
                        req,
                        completion_token.clone(),
                        &options,
                    )
                    .await;
                    // Refresh the cache with the committed history just
                    // replayed, unless a newer dispatch superseded this one;
                    // drop it once the execution ended.
                    if let Some(committed) = committed
                        && history_cache.is_latest_dispatch(&instance_id, &completion_token)
                    {
                        if workflow_history_reset(&response) {
                            history_cache.delete(&instance_id);
                            history_cache.forget_dispatch(&instance_id);
                        } else {
                            history_cache.put(&instance_id, committed);
                        }
                    }
                    // TODO: migrate to complete_work_item once sidecar supports it.
                    #[allow(deprecated)]
                    if let Err(e) = stub.complete_orchestrator_task(response).await {
                        tracing::error!(
                            instance_id = %instance_id,
                            error = %e,
                            "Failed to complete orchestrator task"
                        );
                    }
                });
            }
            Some(Request::ActivityRequest(req)) => {
                let instance_id = req
                    .workflow_instance
                    .as_ref()
                    .map(|i| i.instance_id.clone())
                    .unwrap_or_default();
                tracing::debug!(
                    instance_id = %instance_id,
                    activity = %req.name,
                    task_id = req.task_id,
                    "Received activity work item"
                );

                let registry = (*registry).clone();
                let options = (*options).clone();
                let mut stub = client;
                let completion_token = work_item.completion_token.clone();
                let activity_name = req.name.clone();
                let Some(permit) = ctx.acquire_permit().await? else {
                    return Ok(());
                };
                let cancellation = ctx.shutdown.child_token();

                tasks.spawn(async move {
                    let _permit = permit;
                    let response = Self::handle_activity_request_with_cancellation(
                        &registry,
                        req,
                        completion_token,
                        &options,
                        cancellation.clone(),
                    )
                    .await;
                    if cancellation.is_cancelled() && response.failure_details.is_some() {
                        // The activity gave up because the worker is shutting
                        // down: abandon it rather than record a failure, so the
                        // sidecar redelivers it to the next worker.
                        tracing::info!(
                            instance_id = %instance_id,
                            activity = %activity_name,
                            "Abandoning activity cancelled by worker shutdown"
                        );
                        return;
                    }
                    if let Err(e) = stub.complete_activity_task(response).await {
                        tracing::error!(
                            instance_id = %instance_id,
                            activity = %activity_name,
                            error = %e,
                            "Failed to complete activity task"
                        );
                    }
                });
            }
            None => {
                tracing::warn!("Received work item with no request payload");
            }
        }
        Ok(())
    }

    async fn handle_orchestrator_request(
        registry: &Registry,
        request: proto::WorkflowRequest,
        completion_token: String,
        options: &WorkerOptions,
    ) -> proto::WorkflowResponse {
        let instance_id = request.instance_id.clone();

        // Single-pass extraction of orchestrator name and version from history.
        let (name, version) = request
            .past_events
            .iter()
            .chain(request.new_events.iter())
            .find_map(|e| {
                if let Some(EventType::ExecutionStarted(es)) = &e.event_type {
                    Some((es.name.clone(), es.version.clone()))
                } else {
                    None
                }
            })
            .unwrap_or_default();

        if let Err(e) =
            validate_identifier(&name, "orchestrator name", options.max_identifier_length)
        {
            tracing::warn!(
                instance_id = %instance_id,
                orchestrator = %name,
                error = %e,
                "Rejected orchestrator request: invalid name"
            );
            return build_error_response(&instance_id, &e.to_string(), completion_token);
        }

        // The version a previous turn ran with, recorded by the runtime on the
        // turn's WorkflowStarted event. Replays must use exactly that version.
        let pinned_version = request
            .past_events
            .iter()
            .chain(request.new_events.iter())
            .find_map(|e| match &e.event_type {
                Some(EventType::WorkflowStarted(ws)) => {
                    ws.version.as_ref().and_then(|v| v.name.clone())
                }
                _ => None,
            });

        let (orchestrator_fn, resolved_version) = match registry.resolve_orchestrator(
            &name,
            pinned_version.as_deref(),
            version.as_deref(),
        ) {
            OrchestratorResolution::Found { f, version } => (f, version.map(str::to_string)),
            OrchestratorResolution::VersionNotAvailable => {
                let wanted = pinned_version.or(version);
                tracing::warn!(
                    instance_id = %instance_id,
                    orchestrator = %name,
                    version = ?wanted,
                    "Orchestrator version not available on this worker; stalling"
                );
                return build_version_not_available_response(
                    &instance_id,
                    wanted,
                    completion_token,
                );
            }
            OrchestratorResolution::NotRegistered => {
                tracing::warn!(
                    instance_id = %instance_id,
                    orchestrator = %name,
                    "Unregistered orchestrator requested"
                );
                return build_error_response(
                    &instance_id,
                    &format!("Orchestrator '{name}' not registered"),
                    completion_token,
                );
            }
        };
        // Report the version that ran so the runtime pins it for later turns.
        let version_name = resolved_version.or(pinned_version);

        // Malformed propagated history fails the turn, as in durabletask-go.
        let propagated_history = match request
            .propagated_history
            .map(crate::api::PropagatedHistory::try_from_proto)
            .transpose()
        {
            Ok(history) => history.flatten(),
            Err(e) => {
                tracing::warn!(
                    instance_id = %instance_id,
                    orchestrator = %name,
                    error = %e,
                    "Rejected orchestrator request: invalid propagated history"
                );
                return build_error_response(
                    &instance_id,
                    &format!("invalid propagated history: {e}"),
                    completion_token,
                );
            }
        };

        match OrchestrationExecutor::execute(
            orchestrator_fn,
            &instance_id,
            request.past_events,
            request.new_events,
            completion_token.clone(),
            options,
            propagated_history,
        )
        .await
        {
            Ok(mut response) => {
                if let Some(version_name) = version_name {
                    let version = response.version.get_or_insert_with(Default::default);
                    if version.name.is_none() {
                        version.name = Some(version_name);
                    }
                }
                response
            }
            Err(e) => {
                tracing::error!(
                    instance_id = %instance_id,
                    orchestrator = %name,
                    error = %e,
                    "Orchestrator execution failed"
                );
                build_error_response(&instance_id, &e.to_string(), completion_token)
            }
        }
    }

    #[cfg(test)]
    async fn handle_activity_request(
        registry: &Registry,
        request: proto::ActivityRequest,
        completion_token: String,
        options: &WorkerOptions,
    ) -> proto::ActivityResponse {
        Self::handle_activity_request_with_cancellation(
            registry,
            request,
            completion_token,
            options,
            CancellationToken::new(),
        )
        .await
    }

    async fn handle_activity_request_with_cancellation(
        registry: &Registry,
        request: proto::ActivityRequest,
        completion_token: String,
        options: &WorkerOptions,
        cancellation: CancellationToken,
    ) -> proto::ActivityResponse {
        let instance_id = request
            .workflow_instance
            .as_ref()
            .map(|i| i.instance_id.as_str())
            .unwrap_or("");

        let build_activity_error =
            |error_type: &str, error_message: String| proto::ActivityResponse {
                instance_id: instance_id.to_string(),
                task_id: request.task_id,
                result: None,
                failure_details: Some(proto::TaskFailureDetails {
                    error_type: error_type.to_string(),
                    error_message,
                    stack_trace: None,
                    inner_failure: None,
                    is_non_retriable: true,
                }),
                completion_token: completion_token.clone(),
            };

        if let Err(e) = validate_identifier(
            &request.name,
            "activity name",
            options.max_identifier_length,
        ) {
            tracing::warn!(
                instance_id = %instance_id,
                activity = %request.name,
                error = %e,
                "Rejected activity request: invalid name"
            );
            return build_activity_error("InvalidActivityName", e.to_string());
        }

        let activity_fn = match registry.get_activity(&request.name) {
            Some(f) => f,
            None => {
                tracing::warn!(
                    instance_id = %instance_id,
                    activity = %request.name,
                    "Unregistered activity requested"
                );
                return build_activity_error(
                    "ActivityNotRegistered",
                    format!("Activity '{}' not registered", request.name),
                );
            }
        };

        // Malformed propagated history fails the activity, as in durabletask-go.
        let propagated_history = match request
            .propagated_history
            .map(crate::api::PropagatedHistory::try_from_proto)
            .transpose()
        {
            Ok(history) => history.flatten(),
            Err(e) => {
                tracing::warn!(
                    instance_id = %instance_id,
                    activity = %request.name,
                    error = %e,
                    "Rejected activity request: invalid propagated history"
                );
                let mut response = build_activity_error("InvalidPropagatedHistory", e.to_string());
                if let Some(details) = response.failure_details.as_mut() {
                    details.is_non_retriable = false;
                }
                return response;
            }
        };

        ActivityExecutor::execute_with_cancellation(
            activity_fn,
            &request.name,
            instance_id,
            request.task_id,
            request.task_execution_id,
            request.input,
            request.parent_trace_context.as_ref(),
            completion_token,
            propagated_history,
            cancellation,
        )
        .await
    }
}

fn prune_finished_tasks(tasks: &mut JoinSet<()>) {
    while let Some(outcome) = tasks.try_join_next() {
        if let Err(e) = outcome {
            tracing::error!(error = ?e, "Work item task panicked");
        }
    }
}

/// Per-stream state shared by every work item dispatched from one work-item
/// stream.
struct DispatchContext<'a> {
    registry: &'a Arc<Registry>,
    options: &'a Arc<WorkerOptions>,
    semaphore: Arc<Semaphore>,
    history_cache: &'a Arc<WorkflowHistoryCache>,
    shutdown: &'a CancellationToken,
    stream_teardown: &'a CancellationToken,
}

impl DispatchContext<'_> {
    /// Wait for a free concurrency slot. Returns `None` if shutdown fires
    /// first: the work item is then not started (the sidecar redelivers it).
    async fn acquire_permit(
        &self,
    ) -> crate::api::Result<Option<tokio::sync::OwnedSemaphorePermit>> {
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => {
                tracing::info!("Shutdown while waiting for a free slot; not starting the work item");
                Ok(None)
            }
            permit = self.semaphore.clone().acquire_owned() => permit
                .map(Some)
                .map_err(|_| DurableTaskError::Internal("Semaphore closed".to_string())),
        }
    }
}

/// Build the `GetWorkItems` request, advertising the stateful-history
/// capability when enabled.
fn work_items_request(options: &WorkerOptions) -> proto::GetWorkItemsRequest {
    let mut capabilities = Vec::new();
    if options.stateful_history {
        capabilities.push(proto::WorkerCapability::StatefulHistory as i32);
    }
    proto::GetWorkItemsRequest { capabilities }
}

/// Fetch an instance's full committed history from the sidecar.
async fn fetch_instance_history(
    mut client: TaskHubSidecarServiceClient<Channel>,
    instance_id: String,
) -> crate::api::Result<Vec<proto::HistoryEvent>> {
    let response = client
        .get_instance_history(proto::GetInstanceHistoryRequest { instance_id })
        .await?;
    Ok(response.into_inner().events)
}

/// Replace a delta work item's `past_events` with the full committed history
/// (durabletask-go `resolveWorkflowHistory`).
///
/// A full send (no `cached_history`) is left untouched. For a delta send the
/// cached prefix is prepended when it holds exactly `cached_history.event_count`
/// events; on any other outcome (miss or length mismatch) the full history is
/// fetched with `fetch`. `new_events` is applied on top by the executor, so only
/// the committed past is resolved.
async fn resolve_workflow_history<F, Fut>(
    cache: &WorkflowHistoryCache,
    request: &mut proto::WorkflowRequest,
    fetch: F,
) -> crate::api::Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = crate::api::Result<Vec<proto::HistoryEvent>>>,
{
    let Some(cached_history) = request.cached_history.take() else {
        return Ok(());
    };
    let expected = usize::try_from(cached_history.event_count).ok();
    if let Some(mut full) = cache.get(&request.instance_id)
        && Some(full.len()) == expected
    {
        full.append(&mut request.past_events);
        request.past_events = full;
        return Ok(());
    }
    tracing::debug!(
        instance_id = %request.instance_id,
        event_count = cached_history.event_count,
        "History cache miss; fetching the full history"
    );
    request.past_events = fetch(request.instance_id.clone()).await?;
    Ok(())
}

/// Whether this turn ended the instance's current execution (a
/// `CompleteWorkflow` action, whatever its status, including continue-as-new),
/// after which the cached history no longer extends and must be dropped. A
/// `TerminateWorkflow` action targets another instance and is not a reset.
fn workflow_history_reset(response: &proto::WorkflowResponse) -> bool {
    response.actions.iter().any(|a| {
        matches!(
            a.workflow_action_type,
            Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
        )
    })
}

/// Serialized size of a history, the cache's proxy for its memory footprint.
fn history_bytes(events: &[proto::HistoryEvent]) -> u64 {
    events
        .iter()
        .map(|e| crate::proto::prost::Message::encoded_len(e) as u64)
        .sum()
}

type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

struct CachedWorkflowHistory {
    events: Vec<proto::HistoryEvent>,
    last_access: Instant,
    bytes: u64,
}

#[derive(Default)]
struct HistoryCacheEntries {
    entries: HashMap<String, CachedWorkflowHistory>,
    total_bytes: u64,
}

impl HistoryCacheEntries {
    fn remove(&mut self, instance_id: &str) {
        if let Some(e) = self.entries.remove(instance_id) {
            self.total_bytes -= e.bytes;
        }
    }

    fn lru_except(&self, keep: &str) -> Option<String> {
        self.entries
            .iter()
            .filter(|(id, _)| id.as_str() != keep)
            .min_by_key(|(_, e)| e.last_access)
            .map(|(id, _)| id.clone())
    }
}

/// Each instance's committed history, kept for the lifetime of one work-item
/// stream so the sidecar can send deltas (durabletask-go
/// `workflowHistoryCache`). Entries are reclaimed by a TTL janitor, on
/// completion, and by least-recently-used eviction once the instance cap or
/// byte budget is exceeded. Eviction is always safe: the next delta becomes a
/// cache miss recovered with `GetInstanceHistory`.
pub(crate) struct WorkflowHistoryCache {
    state: Mutex<HistoryCacheEntries>,
    /// Completion token of each instance's newest dispatch, and when it was
    /// recorded. Cache writes are gated on it so a superseded handler cannot
    /// overwrite a newer prefix. Deliberately not cleared by `reset()`, to
    /// fence handlers that outlive a reconnect; markers older than the TTL
    /// with no cached history are swept so the map stays bounded.
    latest_tokens: Mutex<HashMap<String, (String, Instant)>>,
    ttl: Duration,
    sweep_interval: Duration,
    max_instances: usize,
    /// `0` means unlimited.
    max_bytes: u64,
    now: Clock,
}

impl WorkflowHistoryCache {
    pub(crate) fn new(options: &HistoryCacheOptions) -> Self {
        Self {
            state: Mutex::default(),
            latest_tokens: Mutex::default(),
            ttl: options.effective_ttl(),
            sweep_interval: options.effective_sweep_interval(),
            max_instances: options.effective_max_instances(),
            max_bytes: options.effective_max_bytes(),
            now: Arc::new(Instant::now),
        }
    }

    #[cfg(test)]
    fn with_clock(mut self, now: Clock) -> Self {
        self.now = now;
        self
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HistoryCacheEntries> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn tokens(&self) -> std::sync::MutexGuard<'_, HashMap<String, (String, Instant)>> {
        self.latest_tokens.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record `token` as the newest dispatch for `instance_id`.
    pub(crate) fn note_dispatch(&self, instance_id: &str, token: &str) {
        let now = (self.now)();
        self.tokens()
            .insert(instance_id.to_string(), (token.to_string(), now));
    }

    /// Whether `token` still belongs to the newest dispatch for `instance_id`.
    /// An instance with no recorded dispatch accepts any token.
    pub(crate) fn is_latest_dispatch(&self, instance_id: &str, token: &str) -> bool {
        self.tokens()
            .get(instance_id)
            .is_none_or(|(latest, _)| latest == token)
    }

    /// Drop the newest-dispatch marker of an instance whose execution ended.
    pub(crate) fn forget_dispatch(&self, instance_id: &str) {
        self.tokens().remove(instance_id);
    }

    /// A copy of the cached history, refreshing the entry's sliding TTL.
    pub(crate) fn get(&self, instance_id: &str) -> Option<Vec<proto::HistoryEvent>> {
        let now = (self.now)();
        let mut state = self.lock();
        let entry = state.entries.get_mut(instance_id)?;
        entry.last_access = now;
        Some(entry.events.clone())
    }

    /// Store an instance's committed history, then evict least-recently-used
    /// entries (never this one) until within the instance cap and byte budget.
    pub(crate) fn put(&self, instance_id: &str, events: Vec<proto::HistoryEvent>) {
        // Only size the history when a byte budget is configured.
        let bytes = if self.max_bytes > 0 {
            history_bytes(&events)
        } else {
            0
        };
        let now = (self.now)();
        let mut state = self.lock();
        state.remove(instance_id);
        state.entries.insert(
            instance_id.to_string(),
            CachedWorkflowHistory {
                events,
                last_access: now,
                bytes,
            },
        );
        state.total_bytes += bytes;

        // A single entry larger than the budget is kept (soft overage).
        while state.entries.len() > 1 {
            let over_count = state.entries.len() > self.max_instances;
            let over_bytes = self.max_bytes > 0 && state.total_bytes > self.max_bytes;
            if !over_count && !over_bytes {
                break;
            }
            let Some(victim) = state.lru_except(instance_id) else {
                break;
            };
            state.remove(&victim);
        }
    }

    pub(crate) fn delete(&self, instance_id: &str) {
        self.lock().remove(instance_id);
    }

    /// Drop every cached history (the dispatch markers are kept).
    pub(crate) fn reset(&self) {
        *self.lock() = HistoryCacheEntries::default();
    }

    /// Drop entries whose last turn was longer ago than the TTL, and the
    /// dispatch markers of instances that have neither a cached history nor
    /// a dispatch within the TTL (they moved to another worker, were purged,
    /// or ended without this worker running their final turn).
    pub(crate) fn sweep_expired(&self) {
        let now = (self.now)();
        let ttl = self.ttl;
        let mut state = self.lock();
        let expired: Vec<String> = state
            .entries
            .iter()
            .filter(|(_, e)| now.saturating_duration_since(e.last_access) > ttl)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            state.remove(&id);
        }
        self.tokens().retain(|id, (_, dispatched)| {
            state.entries.contains_key(id) || now.saturating_duration_since(*dispatched) <= ttl
        });
    }

    /// Periodically reclaim expired entries until `stop` fires.
    async fn run_janitor(self: Arc<Self>, stop: CancellationToken) {
        let mut ticker = tokio::time::interval(self.sweep_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticker.tick() => self.sweep_expired(),
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().entries.len()
    }

    #[cfg(test)]
    fn total_bytes(&self) -> u64 {
        self.lock().total_bytes
    }
}

/// Build the stall response for a turn whose orchestrator version is not
/// registered on this worker (durabletask-go `setVersionNotRegistered`). The
/// runtime records `ExecutionStalled(VERSION_NOT_AVAILABLE)` and the instance
/// waits in the `Stalled` status for a worker that has the version.
fn build_version_not_available_response(
    instance_id: &str,
    version_name: Option<String>,
    completion_token: String,
) -> proto::WorkflowResponse {
    proto::WorkflowResponse {
        instance_id: instance_id.to_string(),
        actions: vec![proto::WorkflowAction {
            id: 0,
            router: None,
            workflow_action_type: Some(
                proto::workflow_action::WorkflowActionType::WorkflowVersionNotAvailable(
                    proto::WorkflowVersionNotAvailableAction {},
                ),
            ),
        }],
        custom_status: None,
        completion_token,
        num_events_processed: None,
        version: version_name.map(|name| proto::WorkflowVersion {
            patches: Vec::new(),
            name: Some(name),
        }),
    }
}

fn build_error_response(
    instance_id: &str,
    message: &str,
    completion_token: String,
) -> proto::WorkflowResponse {
    proto::WorkflowResponse {
        instance_id: instance_id.to_string(),
        actions: vec![proto::WorkflowAction {
            id: -1,
            router: None,
            workflow_action_type: Some(
                proto::workflow_action::WorkflowActionType::CompleteWorkflow(
                    proto::CompleteWorkflowAction {
                        workflow_status: proto::OrchestrationStatus::Failed as i32,
                        result: None,
                        details: None,
                        new_version: None,
                        carryover_events: vec![],
                        failure_details: Some(proto::TaskFailureDetails {
                            error_type: "WorkerError".to_string(),
                            error_message: message.to_string(),
                            stack_trace: None,
                            inner_failure: None,
                            is_non_retriable: false,
                        }),
                    },
                ),
            ),
        }],
        custom_status: None,
        completion_token,
        num_events_processed: None,
        version: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;

    const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

    async fn prune_until_empty(tasks: &mut JoinSet<()>) {
        timeout(WAIT_TIMEOUT, async {
            while !tasks.is_empty() {
                prune_finished_tasks(tasks);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for prune_finished_tasks to drain the JoinSet");
    }

    async fn spawn_completed_tasks(tasks: &mut JoinSet<()>, count: usize) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        for _ in 0..count {
            let tx = tx.clone();
            tasks.spawn(async move {
                let _ = tx.send(());
            });
        }
        drop(tx);

        timeout(WAIT_TIMEOUT, async {
            for _ in 0..count {
                rx.recv()
                    .await
                    .expect("completed task signal channel closed early");
            }
        })
        .await
        .expect("timed out waiting for spawned tasks to complete");

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn prune_finished_tasks_drains_all_completed_tasks() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        for _ in 0..16 {
            tasks.spawn(async {});
        }

        prune_until_empty(&mut tasks).await;

        assert!(tasks.is_empty());
        assert_eq!(tasks.len(), 0);
    }

    #[tokio::test]
    async fn prune_finished_tasks_keeps_in_flight_tasks() {
        let mut tasks: JoinSet<()> = JoinSet::new();

        for _ in 0..8 {
            tasks.spawn(async {});
        }

        let mut senders = Vec::new();
        for _ in 0..4 {
            let (tx, rx) = oneshot::channel::<()>();
            senders.push(tx);
            tasks.spawn(async move {
                let _ = rx.await;
            });
        }

        timeout(WAIT_TIMEOUT, async {
            while tasks.len() > 4 {
                prune_finished_tasks(&mut tasks);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for completed tasks to be pruned");
        assert_eq!(tasks.len(), 4);

        for tx in senders {
            let _ = tx.send(());
        }
        prune_until_empty(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn prune_finished_tasks_handles_panicked_tasks() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(async {
            panic!("intentional test panic");
        });

        prune_until_empty(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    /// Simulates repeated waves of short-lived work items arriving while a few
    /// long-running tasks remain in flight. After each wave, prune must remove
    /// all completed tasks so the JoinSet never grows unbounded.
    #[tokio::test]
    async fn repeated_waves_do_not_accumulate_completed_tasks() {
        let mut tasks: JoinSet<()> = JoinSet::new();

        let mut long_running_senders = Vec::new();
        const LONG_RUNNING: usize = 3;
        for _ in 0..LONG_RUNNING {
            let (tx, rx) = oneshot::channel::<()>();
            long_running_senders.push(tx);
            tasks.spawn(async move {
                let _ = rx.await;
            });
        }

        const WAVES: usize = 10;
        const TASKS_PER_WAVE: usize = 50;

        for _ in 0..WAVES {
            spawn_completed_tasks(&mut tasks, TASKS_PER_WAVE).await;

            prune_finished_tasks(&mut tasks);

            assert_eq!(
                tasks.len(),
                LONG_RUNNING,
                "single prune pass must remove completed tasks after each wave"
            );
        }

        for tx in long_running_senders {
            let _ = tx.send(());
        }
        prune_until_empty(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    /// High-volume burst: spawn a large number of tasks (simulating a
    /// busy worker) while some remain in-flight, then verify a single prune
    /// pass brings the set back to only in-flight tasks.
    #[tokio::test]
    async fn high_volume_completed_tasks_pruned_with_in_flight_remaining() {
        let mut tasks: JoinSet<()> = JoinSet::new();

        const IN_FLIGHT: usize = 5;
        let mut senders = Vec::new();
        for _ in 0..IN_FLIGHT {
            let (tx, rx) = oneshot::channel::<()>();
            senders.push(tx);
            tasks.spawn(async move {
                let _ = rx.await;
            });
        }

        const BURST_SIZE: usize = 500;
        spawn_completed_tasks(&mut tasks, BURST_SIZE).await;

        prune_finished_tasks(&mut tasks);

        assert_eq!(tasks.len(), IN_FLIGHT);

        for tx in senders {
            let _ = tx.send(());
        }
        prune_until_empty(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    /// Verifies that prune_finished_tasks is idempotent: calling it multiple
    /// times on an already-pruned JoinSet with only in-flight tasks does not
    /// corrupt state or cause spurious removals.
    #[tokio::test]
    async fn prune_is_idempotent_on_only_in_flight_tasks() {
        let mut tasks: JoinSet<()> = JoinSet::new();

        const IN_FLIGHT: usize = 4;
        let mut senders = Vec::new();
        for _ in 0..IN_FLIGHT {
            let (tx, rx) = oneshot::channel::<()>();
            senders.push(tx);
            tasks.spawn(async move {
                let _ = rx.await;
            });
        }

        tokio::task::yield_now().await;

        for _ in 0..10 {
            prune_finished_tasks(&mut tasks);
            assert_eq!(tasks.len(), IN_FLIGHT);
        }

        for tx in senders {
            let _ = tx.send(());
        }
        prune_until_empty(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    /// Simulates a shutdown/drain scenario: all tasks (both short and
    /// long-running) complete, and repeated prune calls fully drain the set
    /// without leaking handles.
    #[tokio::test]
    async fn shutdown_drain_fully_empties_joinset() {
        let mut tasks: JoinSet<()> = JoinSet::new();

        let mut senders = Vec::new();
        for _ in 0..8 {
            let (tx, rx) = oneshot::channel::<()>();
            senders.push(tx);
            tasks.spawn(async move {
                let _ = rx.await;
            });
        }

        for _ in 0..20 {
            tasks.spawn(async {});
        }

        for tx in senders {
            let _ = tx.send(());
        }

        prune_until_empty(&mut tasks).await;
        assert!(tasks.is_empty());
        assert_eq!(tasks.len(), 0);
    }

    /// Propagated history whose only chunk has an undecodable raw event.
    fn malformed_propagated_history() -> proto::PropagatedHistory {
        proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::Lineage as i32,
            chunks: vec![proto::PropagatedHistoryChunk {
                raw_events: vec![vec![0xFF, 0xFF, 0xFF]],
                app_id: "app".into(),
                instance_id: "parent".into(),
                workflow_name: "wf".into(),
                raw_signatures: vec![],
                signing_cert_chains: vec![],
            }],
        }
    }

    #[tokio::test]
    async fn malformed_propagated_history_fails_activity() {
        let mut registry = Registry::new();
        registry.add_named_activity("act", |_ctx, _input| async {
            panic!("activity must not run with invalid propagated history")
        });
        let request = proto::ActivityRequest {
            name: "act".into(),
            task_id: 3,
            propagated_history: Some(malformed_propagated_history()),
            ..Default::default()
        };

        let response = TaskHubGrpcWorker::handle_activity_request(
            &registry,
            request,
            "token".into(),
            &WorkerOptions::default(),
        )
        .await;

        assert_eq!(response.task_id, 3);
        assert!(response.result.is_none());
        let failure = response.failure_details.expect("failure details");
        assert_eq!(failure.error_type, "InvalidPropagatedHistory");
        assert!(
            failure
                .error_message
                .contains("failed to decode rawEvent 0")
        );
    }

    #[tokio::test]
    async fn malformed_propagated_history_fails_orchestrator_turn() {
        let mut registry = Registry::new();
        registry.add_named_orchestrator("wf", |_ctx| async {
            panic!("orchestrator must not run with invalid propagated history")
        });
        let request = proto::WorkflowRequest {
            instance_id: "inst".into(),
            new_events: vec![proto::HistoryEvent {
                event_id: -1,
                timestamp: None,
                router: None,
                event_type: Some(EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                    name: "wf".into(),
                    ..Default::default()
                })),
            }],
            propagated_history: Some(malformed_propagated_history()),
            ..Default::default()
        };

        let response = TaskHubGrpcWorker::handle_orchestrator_request(
            &registry,
            request,
            "token".into(),
            &WorkerOptions::default(),
        )
        .await;

        assert_eq!(response.actions.len(), 1);
        match &response.actions[0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(c)) => {
                assert_eq!(c.workflow_status, proto::OrchestrationStatus::Failed as i32);
                let failure = c.failure_details.as_ref().expect("failure details");
                assert!(
                    failure
                        .error_message
                        .starts_with("invalid propagated history:"),
                    "{}",
                    failure.error_message
                );
            }
            other => panic!("expected CompleteWorkflow, got {other:?}"),
        }
    }

    fn ws_event(version_name: Option<&str>) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id: -1,
            timestamp: None,
            router: None,
            event_type: Some(EventType::WorkflowStarted(proto::WorkflowStartedEvent {
                version: version_name.map(|n| proto::WorkflowVersion {
                    patches: vec![],
                    name: Some(n.to_string()),
                }),
            })),
        }
    }

    fn es_event(name: &str) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id: -1,
            timestamp: None,
            router: None,
            event_type: Some(EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: name.into(),
                ..Default::default()
            })),
        }
    }

    #[tokio::test]
    async fn pinned_version_not_registered_stalls_turn() {
        let mut registry = Registry::new();
        registry.add_latest_orchestrator("wf", "v2", |_ctx| async {
            panic!("a turn pinned to v1 must not run v2")
        });
        let request = proto::WorkflowRequest {
            instance_id: "inst".into(),
            past_events: vec![ws_event(Some("v1")), es_event("wf")],
            new_events: vec![ws_event(None)],
            ..Default::default()
        };
        let response = TaskHubGrpcWorker::handle_orchestrator_request(
            &registry,
            request,
            "token".into(),
            &WorkerOptions::default(),
        )
        .await;
        assert_eq!(response.completion_token, "token");
        assert_eq!(response.actions.len(), 1);
        assert!(matches!(
            response.actions[0].workflow_action_type,
            Some(proto::workflow_action::WorkflowActionType::WorkflowVersionNotAvailable(_))
        ));
        assert_eq!(response.version.and_then(|v| v.name).as_deref(), Some("v1"));
    }

    #[tokio::test]
    async fn resolved_version_is_reported_and_pinned() {
        let mut registry = Registry::new();
        registry.add_versioned_orchestrator("wf", "v1", |_ctx| async { Ok(Some("1".into())) });
        registry.add_latest_orchestrator("wf", "v2", |_ctx| async { Ok(Some("2".into())) });
        let options = WorkerOptions::default();
        let run = |past: Vec<proto::HistoryEvent>| {
            let request = proto::WorkflowRequest {
                instance_id: "inst".into(),
                past_events: past,
                new_events: vec![ws_event(None), es_event("wf")],
                ..Default::default()
            };
            TaskHubGrpcWorker::handle_orchestrator_request(
                &registry,
                request,
                "token".into(),
                &options,
            )
        };
        let complete_result = |r: &proto::WorkflowResponse| match &r.actions[0].workflow_action_type
        {
            Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(c)) => {
                c.result.clone()
            }
            other => panic!("expected CompleteWorkflow, got {other:?}"),
        };

        // First turn: the latest version runs and is reported for pinning.
        let response = run(vec![]).await;
        assert_eq!(complete_result(&response).as_deref(), Some("2"));
        assert_eq!(response.version.and_then(|v| v.name).as_deref(), Some("v2"));

        // A turn pinned to v1 keeps running v1 even though v2 is latest.
        let response = run(vec![ws_event(Some("v1"))]).await;
        assert_eq!(complete_result(&response).as_deref(), Some("1"));
        assert_eq!(response.version.and_then(|v| v.name).as_deref(), Some("v1"));
    }

    mod history_cache {

        use super::super::*;
        use super::{WAIT_TIMEOUT, timeout};

        use std::sync::atomic::{AtomicUsize, Ordering};

        fn events(n: usize) -> Vec<proto::HistoryEvent> {
            (0..n)
                .map(|i| proto::HistoryEvent {
                    event_id: i as i32,
                    timestamp: None,
                    router: None,
                    event_type: Some(EventType::WorkflowStarted(Default::default())),
                })
                .collect()
        }

        /// Events with a non-trivial serialized size.
        fn sized_events(n: usize) -> Vec<proto::HistoryEvent> {
            (0..n)
                .map(|i| proto::HistoryEvent {
                    event_id: i as i32,
                    timestamp: None,
                    router: None,
                    event_type: Some(EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                        name: "workflow-with-a-reasonably-long-name".into(),
                        input: Some("x".repeat(64)),
                        ..Default::default()
                    })),
                })
                .collect()
        }

        fn cache(options: HistoryCacheOptions) -> WorkflowHistoryCache {
            WorkflowHistoryCache::new(&options)
        }

        /// A controllable clock: returns the cache and a handle to advance it.
        fn fake_clock_cache(
            options: HistoryCacheOptions,
        ) -> (WorkflowHistoryCache, Arc<Mutex<Instant>>) {
            let now = Arc::new(Mutex::new(Instant::now()));
            let n = now.clone();
            let cache = cache(options).with_clock(Arc::new(move || *n.lock().unwrap()));
            (cache, now)
        }

        fn advance(clock: &Arc<Mutex<Instant>>, by: Duration) {
            *clock.lock().unwrap() += by;
        }

        fn request(past: usize, new: usize, cached_count: Option<i32>) -> proto::WorkflowRequest {
            proto::WorkflowRequest {
                instance_id: "a".into(),
                past_events: events(past),
                new_events: events(new),
                cached_history: cached_count
                    .map(|event_count| proto::CachedHistory { event_count }),
                ..Default::default()
            }
        }

        /// A fetcher standing in for the sidecar's GetInstanceHistory RPC.
        fn fake_fetch(
            calls: &AtomicUsize,
            n: usize,
        ) -> impl FnOnce(String) -> std::future::Ready<crate::api::Result<Vec<proto::HistoryEvent>>> + '_
        {
            move |iid| {
                assert_eq!(iid, "a");
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(events(n)))
            }
        }

        fn action(t: proto::workflow_action::WorkflowActionType) -> proto::WorkflowResponse {
            proto::WorkflowResponse {
                actions: vec![proto::WorkflowAction {
                    id: 0,
                    router: None,
                    workflow_action_type: Some(t),
                }],
                ..Default::default()
            }
        }

        #[tokio::test]
        async fn test_resolve_workflow_history_full_send() {
            let c = cache(HistoryCacheOptions::default());
            let calls = AtomicUsize::new(0);
            let mut req = request(4, 1, None);
            resolve_workflow_history(&c, &mut req, fake_fetch(&calls, 99))
                .await
                .unwrap();
            assert_eq!(req.past_events.len(), 4);
            assert_eq!(req.new_events.len(), 1);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn test_resolve_workflow_history_cache_hit_reconstructs() {
            let c = cache(HistoryCacheOptions::default());
            c.put("a", events(5));
            let calls = AtomicUsize::new(0);
            let mut req = request(3, 1, Some(5));
            resolve_workflow_history(&c, &mut req, fake_fetch(&calls, 99))
                .await
                .unwrap();
            assert_eq!(req.past_events.len(), 8, "cached prefix 5 + delta 3");
            assert!(req.cached_history.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            // Prefix first, then the delta.
            let ids: Vec<i32> = req.past_events.iter().map(|e| e.event_id).collect();
            assert_eq!(ids, vec![0, 1, 2, 3, 4, 0, 1, 2]);
        }

        #[tokio::test]
        async fn test_resolve_workflow_history_cache_miss_fetches_from_server() {
            let c = cache(HistoryCacheOptions::default());
            let calls = AtomicUsize::new(0);
            let mut req = request(3, 1, Some(5));
            resolve_workflow_history(&c, &mut req, fake_fetch(&calls, 9))
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(req.past_events.len(), 9);
        }

        #[tokio::test]
        async fn test_resolve_workflow_history_length_mismatch_is_miss() {
            let c = cache(HistoryCacheOptions::default());
            c.put("a", events(4));
            let calls = AtomicUsize::new(0);
            let mut req = request(3, 1, Some(5));
            resolve_workflow_history(&c, &mut req, fake_fetch(&calls, 9))
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                req.past_events.len(),
                9,
                "not a 4 + 3 reconstruction from the stale cache"
            );
        }

        #[tokio::test]
        async fn test_resolve_workflow_history_fetch_error_propagates() {
            let c = cache(HistoryCacheOptions::default());
            let mut req = request(3, 1, Some(5));
            let err = resolve_workflow_history(&c, &mut req, |_| {
                std::future::ready(Err(DurableTaskError::ConnectionFailed("gone".into())))
            })
            .await;
            assert!(err.is_err());
        }

        #[test]
        fn test_workflow_history_reset() {
            use proto::workflow_action::WorkflowActionType as Wat;
            assert!(!workflow_history_reset(&action(Wat::ScheduleTask(
                Default::default()
            ))));
            assert!(workflow_history_reset(&action(Wat::CompleteWorkflow(
                Default::default()
            ))));
            // Terminating another instance does not end this one.
            assert!(!workflow_history_reset(&action(Wat::TerminateWorkflow(
                Default::default()
            ))));
        }

        #[test]
        fn test_workflow_history_cache() {
            let c = cache(HistoryCacheOptions::default());
            assert!(c.get("a").is_none());
            c.put("a", events(3));
            assert_eq!(c.get("a").map(|e| e.len()), Some(3));
            c.delete("a");
            assert!(c.get("a").is_none());
            c.put("b", events(1));
            c.reset();
            assert!(c.get("b").is_none());
        }

        #[test]
        fn test_workflow_history_cache_bounded() {
            let c = cache(HistoryCacheOptions::default());
            let max = crate::worker::DEFAULT_HISTORY_CACHE_MAX_INSTANCES;
            for i in 0..max + 5 {
                c.put(&format!("wf-{i}"), Vec::new());
            }
            assert!(c.len() <= max);
            // The most recently written entry is never evicted.
            assert!(c.get(&format!("wf-{}", max + 4)).is_some());
        }

        #[test]
        fn test_workflow_history_cache_configurable_max_instances() {
            let (c, clock) = fake_clock_cache(HistoryCacheOptions {
                max_instances: Some(2),
                ..Default::default()
            });
            for id in ["a", "b", "c"] {
                c.put(id, events(1));
                advance(&clock, Duration::from_secs(1));
            }
            assert!(c.len() <= 2);
            assert!(c.get("a").is_none(), "least recently used is evicted");
            assert!(c.get("c").is_some());
        }

        #[test]
        fn test_workflow_history_cache_config_defaults() {
            let c = cache(HistoryCacheOptions::default());
            assert_eq!(c.ttl, crate::worker::DEFAULT_HISTORY_CACHE_TTL);
            assert_eq!(c.ttl, Duration::from_secs(3600));
            assert_eq!(
                c.sweep_interval,
                crate::worker::DEFAULT_HISTORY_CACHE_SWEEP_INTERVAL
            );
            assert_eq!(c.sweep_interval, Duration::from_secs(60));
            assert_eq!(
                c.max_instances,
                crate::worker::DEFAULT_HISTORY_CACHE_MAX_INSTANCES
            );
            assert_eq!(c.max_instances, 100_000);
            assert_eq!(c.max_bytes, 0, "no byte limit by default");

            // WorkerOptions carries the same defaults to the worker.
            let c = WorkflowHistoryCache::new(&WorkerOptions::default().history_cache);
            assert_eq!(c.max_bytes, 0);
            assert_eq!(c.max_instances, 100_000);
        }

        #[test]
        fn test_workflow_history_cache_max_bytes_evicts_lru() {
            let entry_size = history_bytes(&sized_events(4));
            assert!(entry_size > 0);
            let (c, clock) = fake_clock_cache(HistoryCacheOptions {
                max_bytes: Some(entry_size + 1),
                ..Default::default()
            });
            c.put("a", sized_events(4));
            advance(&clock, Duration::from_secs(1));
            c.put("b", sized_events(4));
            assert!(
                c.get("a").is_none(),
                "LRU entry evicted over the byte budget"
            );
            assert!(c.get("b").is_some());
            assert!(c.total_bytes() <= entry_size + 1);
        }

        #[test]
        fn test_workflow_history_cache_single_oversized_entry_kept() {
            let c = cache(HistoryCacheOptions {
                max_bytes: Some(1),
                ..Default::default()
            });
            c.put("big", sized_events(5));
            assert!(
                c.get("big").is_some(),
                "a lone oversized entry is kept as a soft overage"
            );
        }

        #[test]
        fn test_workflow_history_cache_byte_accounting() {
            let c = cache(HistoryCacheOptions {
                max_bytes: Some(1 << 30),
                ..Default::default()
            });
            let bytes = |n| history_bytes(&sized_events(n));
            c.put("a", sized_events(3));
            c.put("b", sized_events(2));
            assert_eq!(c.total_bytes(), bytes(3) + bytes(2));
            c.put("a", sized_events(6));
            assert_eq!(c.total_bytes(), bytes(6) + bytes(2));
            c.delete("a");
            assert_eq!(c.total_bytes(), bytes(2));
            c.reset();
            assert_eq!(c.total_bytes(), 0);
        }

        #[test]
        fn test_workflow_history_cache_ttl_sweep_updates_bytes() {
            let (c, clock) = fake_clock_cache(HistoryCacheOptions {
                ttl: Some(Duration::from_secs(60)),
                max_bytes: Some(1 << 30),
                ..Default::default()
            });
            c.put("a", sized_events(3));
            assert!(c.total_bytes() > 0);
            advance(&clock, Duration::from_secs(120));
            c.sweep_expired();
            assert_eq!(c.total_bytes(), 0);
            assert_eq!(c.len(), 0);
        }

        #[test]
        fn test_workflow_history_cache_ttl_sweep() {
            let (c, clock) = fake_clock_cache(HistoryCacheOptions {
                ttl: Some(Duration::from_secs(60)),
                ..Default::default()
            });
            c.put("idle", events(1));
            c.put("active", events(1));
            advance(&clock, Duration::from_secs(120));
            // A turn refreshes the sliding TTL.
            assert!(c.get("active").is_some());
            c.sweep_expired();
            assert!(c.get("idle").is_none());
            assert!(c.get("active").is_some());
        }

        #[test]
        fn test_workflow_history_cache_ttl_sweep_prunes_stale_dispatch_markers() {
            // Markers of instances whose final turn never ran here would
            // otherwise accumulate for the worker's lifetime.
            let (c, clock) = fake_clock_cache(HistoryCacheOptions {
                ttl: Some(Duration::from_secs(60)),
                ..Default::default()
            });
            c.note_dispatch("moved", "t1");
            c.note_dispatch("cached", "t2");
            c.put("cached", events(1));
            advance(&clock, Duration::from_secs(30));
            c.note_dispatch("recent", "t3");
            advance(&clock, Duration::from_secs(45));
            assert!(c.get("cached").is_some());
            c.sweep_expired();

            let tokens = c.tokens();
            assert!(!tokens.contains_key("moved"), "stale marker kept");
            assert!(
                tokens.contains_key("cached"),
                "marker of a cached instance dropped"
            );
            assert!(
                tokens.contains_key("recent"),
                "marker within the TTL dropped"
            );
        }

        #[test]
        fn test_workflow_history_cache_ttl_sliding_on_put() {
            let (c, clock) = fake_clock_cache(HistoryCacheOptions {
                ttl: Some(Duration::from_secs(60)),
                ..Default::default()
            });
            c.put("a", events(1));
            advance(&clock, Duration::from_secs(90));
            c.put("a", events(3));
            c.sweep_expired();
            assert_eq!(c.get("a").map(|e| e.len()), Some(3));
        }

        #[tokio::test]
        async fn test_workflow_history_cache_janitor_sweeps() {
            let (c, clock) = fake_clock_cache(HistoryCacheOptions {
                ttl: Some(Duration::from_secs(60)),
                sweep_interval: Some(Duration::from_millis(5)),
                ..Default::default()
            });
            let c = Arc::new(c);
            let stop = CancellationToken::new();
            let janitor = tokio::spawn(c.clone().run_janitor(stop.clone()));
            c.put("a", events(1));
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(c.len(), 1, "not expired yet");
            advance(&clock, Duration::from_secs(120));
            timeout(WAIT_TIMEOUT, async {
                while c.len() != 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the janitor reclaims expired entries");
            stop.cancel();
            janitor.await.unwrap();
        }

        #[test]
        fn test_history_cache_superseded_dispatch_cannot_write() {
            let c = cache(HistoryCacheOptions::default());
            c.note_dispatch("wf1", "token-1");
            assert!(c.is_latest_dispatch("wf1", "token-1"));
            // The markers survive a reset (reconnect) to fence stale handlers.
            c.reset();
            c.note_dispatch("wf1", "token-2");
            assert!(!c.is_latest_dispatch("wf1", "token-1"));
            assert!(c.is_latest_dispatch("wf1", "token-2"));
            c.forget_dispatch("wf1");
            assert!(c.is_latest_dispatch("wf1", "token-1"));
            assert!(c.is_latest_dispatch("other", "any"));
        }

        #[test]
        fn test_work_items_request_advertises_stateful_history() {
            let req = work_items_request(&WorkerOptions::default());
            assert_eq!(
                req.capabilities,
                vec![proto::WorkerCapability::StatefulHistory as i32]
            );
            let req = work_items_request(&WorkerOptions::new().with_stateful_history_disabled());
            assert!(req.capabilities.is_empty());
        }
    }
}
