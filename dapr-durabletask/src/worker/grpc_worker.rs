use std::sync::Arc;

use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use tokio::sync::Semaphore;

use crate::api::DurableTaskError;
use crate::internal::validate_identifier;
use crate::proto;
use crate::proto::history_event::EventType;
use crate::proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient;
use crate::proto::work_item::Request;

use super::activity_executor::ActivityExecutor;
use super::options::WorkerOptions;
use super::orchestration_executor::OrchestrationExecutor;
use super::reconnect_policy::BackoffIter;
use super::registry::Registry;

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
    /// shutdown. Work items still queued inside the sidecar but not yet
    /// dispatched to this worker are unaffected — the sidecar will re-dispatch
    /// them to the next available worker.
    ///
    /// [`ReconnectPolicy`]: super::reconnect_policy::ReconnectPolicy
    pub async fn start(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> crate::api::Result<()> {
        let mut backoff = BackoffIter::new(&self.options.reconnect_policy);

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

                    match Self::run_work_loop(&mut client, &self.registry, &self.options, &shutdown)
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
                    return Err(DurableTaskError::Other(msg));
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
        Channel::from_shared(address.to_string())
            .map_err(|e| DurableTaskError::Other(format!("Invalid address: {}", e)))?
            .connect()
            .await
            .map_err(|e| DurableTaskError::Other(format!("Connection failed: {}", e)))
    }

    async fn run_work_loop(
        client: &mut TaskHubSidecarServiceClient<Channel>,
        registry: &Arc<Registry>,
        options: &Arc<WorkerOptions>,
        shutdown: &tokio_util::sync::CancellationToken,
    ) -> crate::api::Result<()> {
        let request = proto::GetWorkItemsRequest {};
        let mut stream = client.get_work_items(request).await?.into_inner();
        let semaphore = Arc::new(Semaphore::new(options.max_concurrent_work_items));
        let mut tasks: JoinSet<()> = JoinSet::new();
        tracing::info!("Work item stream established");

        // `shutdown_triggered` tracks whether we exited the intake loop because
        // of a cancellation (true) or because the stream closed (false/error).
        let shutdown_triggered = loop {
            tokio::select! {
                biased; // check shutdown first so we don't accept more items
                _ = shutdown.cancelled() => {
                    tracing::info!(
                        in_flight = tasks.len(),
                        "Shutdown: stopping intake, draining in-flight work items"
                    );
                    break true;
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
                            return Err(DurableTaskError::Other(format!("Stream error: {e}")));
                        }
                        Some(Ok(work_item)) => {
                            Self::dispatch_work_item(
                                work_item,
                                client,
                                registry,
                                options,
                                &semaphore,
                                &mut tasks,
                            ).await?;
                        }
                    }
                }
            }
        };

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
            Err(DurableTaskError::Other(
                "Work item stream closed by sidecar".into(),
            ))
        }
    }

    /// Validate and dispatch a single work item into the `JoinSet`.
    async fn dispatch_work_item(
        work_item: proto::WorkItem,
        client: &TaskHubSidecarServiceClient<Channel>,
        registry: &Arc<Registry>,
        options: &Arc<WorkerOptions>,
        semaphore: &Arc<Semaphore>,
        tasks: &mut JoinSet<()>,
    ) -> crate::api::Result<()> {
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

                let registry = registry.clone();
                let options = options.clone();
                let mut stub = client.clone();
                let completion_token = work_item.completion_token.clone();
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| DurableTaskError::Other("Semaphore closed".to_string()))?;

                tasks.spawn(async move {
                    let _permit = permit;
                    let response = Self::handle_orchestrator_request(
                        &registry,
                        req,
                        completion_token,
                        &options,
                    )
                    .await;
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

                let registry = registry.clone();
                let options = options.clone();
                let mut stub = client.clone();
                let completion_token = work_item.completion_token.clone();
                let activity_name = req.name.clone();
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| DurableTaskError::Other("Semaphore closed".to_string()))?;

                tasks.spawn(async move {
                    let _permit = permit;
                    let response =
                        Self::handle_activity_request(&registry, req, completion_token, &options)
                            .await;
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

        let name = request
            .past_events
            .iter()
            .chain(request.new_events.iter())
            .find_map(|e| {
                if let Some(EventType::ExecutionStarted(es)) = &e.event_type {
                    Some(es.name.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let version = request
            .past_events
            .iter()
            .chain(request.new_events.iter())
            .find_map(|e| {
                if let Some(EventType::ExecutionStarted(es)) = &e.event_type {
                    es.version.clone()
                } else {
                    None
                }
            });

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

        let orchestrator_fn = match registry.get_orchestrator_version(&name, version.as_deref()) {
            Some(f) => f,
            None => {
                tracing::warn!(
                    instance_id = %instance_id,
                    orchestrator = %name,
                    "Unregistered orchestrator requested"
                );
                return build_error_response(
                    &instance_id,
                    &format!("Orchestrator '{}' not registered", name),
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
        )
        .await
        {
            Ok(response) => response,
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

    async fn handle_activity_request(
        registry: &Registry,
        request: proto::ActivityRequest,
        completion_token: String,
        options: &WorkerOptions,
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

        ActivityExecutor::execute(
            activity_fn,
            &request.name,
            instance_id,
            request.task_id,
            request.task_execution_id,
            request.input,
            request.parent_trace_context.as_ref(),
            completion_token,
        )
        .await
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
