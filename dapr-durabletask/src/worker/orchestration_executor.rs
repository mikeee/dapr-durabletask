use std::task::Poll;

use futures::FutureExt;

use crate::api::{DurableTaskError, FailureDetails, OrchestrationStatus};
use crate::internal::from_timestamp;
use crate::proto;
use crate::proto::history_event::EventType;
use crate::task::OrchestrationContext;
use crate::task::orchestration_context::OrchestrationContextInner;

use super::options::WorkerOptions;
use super::registry::OrchestratorFn;

/// Executes orchestrator functions by replaying history and processing new events.
///
/// The executor follows the durable task replay model:
/// 1. Process old (past) events to pre-populate completed tasks
/// 2. Process new events to deliver results and external events
/// 3. Run the orchestrator function (unless suspended, terminated, or already complete)
/// 4. Collect pending actions and build the response
pub struct OrchestrationExecutor;

impl OrchestrationExecutor {
    /// Execute an orchestrator function by replaying history and processing new events.
    ///
    /// Returns a `WorkflowResponse` with the actions to take.
    pub async fn execute(
        orchestrator_fn: &OrchestratorFn,
        instance_id: &str,
        old_events: Vec<proto::HistoryEvent>,
        new_events: Vec<proto::HistoryEvent>,
        completion_token: String,
        options: &WorkerOptions,
    ) -> crate::api::Result<proto::WorkflowResponse> {
        tracing::info!(
            instance_id = %instance_id,
            past_events = old_events.len(),
            new_events = new_events.len(),
            "Starting orchestration execution"
        );

        let ctx = OrchestrationContext::new(
            instance_id.to_string(),
            String::new(),
            None,
            chrono::Utc::now(),
            true,
            options,
            old_events.len() + new_events.len(),
        );

        // Process all history events under a single lock acquisition.
        // Previously each event locked/unlocked individually — O(N) lock ops
        // for N events. This reduces it to O(1).
        {
            let mut inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.is_replaying = true;
            tracing::debug!(
                instance_id = %instance_id,
                count = old_events.len(),
                "Replaying old events"
            );
            for event in &old_events {
                Self::process_event(
                    &mut inner,
                    event,
                    instance_id,
                    options.max_identifier_length,
                );
            }

            inner.is_replaying = false;
            tracing::debug!(
                instance_id = %instance_id,
                count = new_events.len(),
                "Processing new events"
            );
            for event in &new_events {
                Self::process_event(
                    &mut inner,
                    event,
                    instance_id,
                    options.max_identifier_length,
                );
            }
        }

        // Start an OTel orchestration span
        #[cfg(feature = "opentelemetry")]
        let otel_ctx = {
            let inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
            let parent_tc = Self::find_parent_trace_context(&old_events, &new_events);
            let parent_ctx = crate::internal::otel::context_from_trace_context(parent_tc);
            crate::internal::otel::start_orchestration_span(&parent_ctx, &inner.name, instance_id)
        };

        let should_run = {
            let inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
            !inner.is_suspended && !inner.is_complete
        };

        if should_run {
            tracing::debug!(instance_id = %instance_id, "Polling orchestrator function");

            // Poll the orchestrator future once — if all awaited tasks are already
            // completed from replay, the future runs to completion in a single poll.
            let mut future = (orchestrator_fn)(ctx.clone()).boxed();
            let poll_result = futures::poll!(future.as_mut());

            match poll_result {
                Poll::Ready(Ok(output)) => {
                    let mut inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
                    if inner.continue_as_new_input.is_some() {
                        tracing::info!(
                            instance_id = %instance_id,
                            orchestrator = %inner.name,
                            "Orchestration continuing as new"
                        );
                        #[cfg(feature = "opentelemetry")]
                        crate::internal::otel::set_span_status_attribute(
                            &otel_ctx,
                            "CONTINUED_AS_NEW",
                        );
                        inner.is_complete = true;
                        inner.completion_status = Some(OrchestrationStatus::ContinuedAsNew);
                    } else if !inner.is_complete {
                        tracing::info!(
                            instance_id = %instance_id,
                            orchestrator = %inner.name,
                            "Orchestration completed successfully"
                        );
                        #[cfg(feature = "opentelemetry")]
                        crate::internal::otel::set_span_status_attribute(&otel_ctx, "COMPLETED");
                        inner.is_complete = true;
                        inner.completion_status = Some(OrchestrationStatus::Completed);
                        inner.completion_result = output;
                    }
                }
                Poll::Ready(Err(DurableTaskError::TaskFailed {
                    message,
                    failure_details,
                })) => {
                    let mut inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
                    tracing::warn!(
                        instance_id = %instance_id,
                        orchestrator = %inner.name,
                        error = %message,
                        "Orchestration failed due to task failure"
                    );
                    #[cfg(feature = "opentelemetry")]
                    {
                        crate::internal::otel::set_span_status_attribute(&otel_ctx, "FAILED");
                        crate::internal::otel::set_span_error(&otel_ctx, &message);
                    }
                    inner.is_complete = true;
                    inner.completion_status = Some(OrchestrationStatus::Failed);
                    inner.completion_failure =
                        Some(failure_details.unwrap_or_else(|| FailureDetails {
                            message: message.clone(),
                            error_type: "TaskFailed".to_string(),
                            stack_trace: None,
                        }));
                }
                Poll::Ready(Err(e)) => {
                    let mut inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
                    tracing::error!(
                        instance_id = %instance_id,
                        orchestrator = %inner.name,
                        error = %e,
                        "Orchestration failed with error"
                    );
                    #[cfg(feature = "opentelemetry")]
                    {
                        crate::internal::otel::set_span_status_attribute(&otel_ctx, "FAILED");
                        crate::internal::otel::set_span_error(&otel_ctx, &e.to_string());
                    }
                    inner.is_complete = true;
                    inner.completion_status = Some(OrchestrationStatus::Failed);
                    inner.completion_failure = Some(FailureDetails {
                        message: e.to_string(),
                        error_type: "OrchestratorError".to_string(),
                        stack_trace: None,
                    });
                }
                Poll::Pending => {
                    let inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
                    tracing::debug!(
                        instance_id = %instance_id,
                        orchestrator = %inner.name,
                        pending_actions = inner.pending_actions.len(),
                        "Orchestrator yielded, waiting for tasks"
                    );
                }
            }
        } else {
            let inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());
            tracing::debug!(
                instance_id = %instance_id,
                is_suspended = inner.is_suspended,
                is_complete = inner.is_complete,
                "Skipping orchestrator execution"
            );
            #[cfg(feature = "opentelemetry")]
            if inner.is_complete {
                crate::internal::otel::set_span_status_attribute(&otel_ctx, "TERMINATED");
            }
        }

        // End the OTel span
        #[cfg(feature = "opentelemetry")]
        crate::internal::otel::end_span(&otel_ctx);

        let response = Self::build_response(&ctx, instance_id, completion_token);
        tracing::debug!(
            instance_id = %instance_id,
            actions = response.actions.len(),
            "Built orchestration response"
        );
        Ok(response)
    }

    /// Find the parent trace context from ExecutionStarted events in history.
    #[cfg(feature = "opentelemetry")]
    fn find_parent_trace_context<'a>(
        old_events: &'a [proto::HistoryEvent],
        new_events: &'a [proto::HistoryEvent],
    ) -> Option<&'a proto::TraceContext> {
        old_events.iter().chain(new_events.iter()).find_map(|e| {
            if let Some(EventType::ExecutionStarted(es)) = &e.event_type {
                es.parent_trace_context.as_ref()
            } else {
                None
            }
        })
    }

    /// Process a single history event, updating the orchestration context state.
    ///
    /// Operates directly on the inner state to avoid per-event mutex acquisition.
    /// The caller must hold the lock.
    fn process_event(
        inner: &mut OrchestrationContextInner,
        event: &proto::HistoryEvent,
        instance_id: &str,
        max_identifier_length: usize,
    ) {
        let event_type = match &event.event_type {
            Some(et) => et,
            None => return,
        };

        match event_type {
            EventType::WorkflowStarted(ws) => {
                if let Some(ts) = &event.timestamp {
                    if let Some(dt) = from_timestamp(ts) {
                        inner.current_utc_datetime = dt;
                    }
                }
                if let Some(version) = &ws.version {
                    for patch in &version.patches {
                        inner.history_patches.insert(patch.clone());
                    }
                }
            }
            EventType::ExecutionStarted(e) => {
                tracing::debug!(
                    instance_id = %instance_id,
                    orchestrator = %e.name,
                    "Execution started event"
                );
                inner.name = e.name.clone();
                inner.input = e.input.clone();
            }
            EventType::TaskCompleted(e) => {
                let seq = e.task_scheduled_id;
                tracing::debug!(
                    instance_id = %instance_id,
                    task_id = seq,
                    "Task completed"
                );
                let task = inner.pending_tasks.entry(seq).or_default();
                if task.is_complete() {
                    tracing::debug!(
                        instance_id = %instance_id,
                        task_id = seq,
                        "Skipping duplicate task completion"
                    );
                    return;
                }
                task.complete(e.result.clone());
            }
            EventType::TaskFailed(e) => {
                let seq = e.task_scheduled_id;
                let details = e
                    .failure_details
                    .as_ref()
                    .map(FailureDetails::from)
                    .unwrap_or_else(|| FailureDetails {
                        message: "Task failed".to_string(),
                        error_type: "Unknown".to_string(),
                        stack_trace: None,
                    });
                tracing::debug!(
                    instance_id = %instance_id,
                    task_id = seq,
                    error = %details.message,
                    "Task failed"
                );
                let task = inner.pending_tasks.entry(seq).or_default();
                if task.is_complete() {
                    tracing::debug!(
                        instance_id = %instance_id,
                        task_id = seq,
                        "Skipping duplicate task completion"
                    );
                    return;
                }
                task.fail(details);
            }
            EventType::TaskScheduled(_)
            | EventType::TimerCreated(_)
            | EventType::ChildWorkflowInstanceCreated(_) => {
                inner.history_scheduled_count += 1;
            }
            EventType::TimerFired(e) => {
                let seq = e.timer_id;
                tracing::debug!(instance_id = %instance_id, timer_id = seq, "Timer fired");
                let task = inner.pending_tasks.entry(seq).or_default();
                if task.is_complete() {
                    tracing::debug!(
                        instance_id = %instance_id,
                        task_id = seq,
                        "Skipping duplicate task completion"
                    );
                    return;
                }
                task.complete(None);
            }
            EventType::ChildWorkflowInstanceCompleted(e) => {
                let seq = e.task_scheduled_id;
                tracing::debug!(
                    instance_id = %instance_id,
                    task_id = seq,
                    "Child workflow completed"
                );
                let task = inner.pending_tasks.entry(seq).or_default();
                if task.is_complete() {
                    tracing::debug!(
                        instance_id = %instance_id,
                        task_id = seq,
                        "Skipping duplicate task completion"
                    );
                    return;
                }
                task.complete(e.result.clone());
            }
            EventType::ChildWorkflowInstanceFailed(e) => {
                let seq = e.task_scheduled_id;
                let details = e
                    .failure_details
                    .as_ref()
                    .map(FailureDetails::from)
                    .unwrap_or_else(|| FailureDetails {
                        message: "Sub-orchestration failed".to_string(),
                        error_type: "Unknown".to_string(),
                        stack_trace: None,
                    });
                tracing::debug!(
                    instance_id = %instance_id,
                    task_id = seq,
                    error = %details.message,
                    "Child workflow failed"
                );
                let task = inner.pending_tasks.entry(seq).or_default();
                if task.is_complete() {
                    tracing::debug!(
                        instance_id = %instance_id,
                        task_id = seq,
                        "Skipping duplicate task completion"
                    );
                    return;
                }
                task.fail(details);
            }
            EventType::EventRaised(e) => {
                if let Err(err) = crate::internal::validate_identifier(
                    &e.name,
                    "event name",
                    max_identifier_length,
                ) {
                    tracing::warn!(
                        instance_id = %instance_id,
                        event_name = %e.name,
                        error = %err,
                        "Rejected event: invalid event name"
                    );
                    return;
                }
                let event_name = e.name.to_lowercase();
                tracing::debug!(
                    instance_id = %instance_id,
                    event_name = %e.name,
                    "External event raised"
                );

                if let Some(tasks) = inner.pending_event_tasks.get_mut(&event_name) {
                    if !tasks.is_empty() {
                        let task = tasks.remove(0);
                        if task.is_complete() {
                            tracing::debug!(
                                instance_id = %instance_id,
                                event_name = %e.name,
                                "Skipping duplicate task completion"
                            );
                            return;
                        }
                        task.complete(e.input.clone());
                        return;
                    }
                }

                if inner.buffered_events.len() >= inner.max_event_names
                    && !inner.buffered_events.contains_key(&event_name)
                {
                    tracing::warn!(
                        instance_id = %instance_id,
                        event_name = %e.name,
                        "Event name limit reached, discarding event"
                    );
                    return;
                }

                let max_events = inner.max_events_per_name;
                let events = inner.buffered_events.entry(event_name).or_default();
                if events.len() >= max_events {
                    tracing::warn!(
                        instance_id = %instance_id,
                        event_name = %e.name,
                        "Event buffer limit reached, discarding event"
                    );
                    return;
                }
                events.push(e.input.clone());
            }
            EventType::ExecutionSuspended(_) => {
                tracing::info!(instance_id = %instance_id, "Orchestration suspended");
                inner.is_suspended = true;
            }
            EventType::ExecutionResumed(_) => {
                tracing::info!(instance_id = %instance_id, "Orchestration resumed");
                inner.is_suspended = false;
            }
            EventType::ExecutionTerminated(e) => {
                tracing::info!(instance_id = %instance_id, "Orchestration terminated");
                inner.is_complete = true;
                inner.completion_status = Some(OrchestrationStatus::Terminated);
                inner.completion_result = e.input.clone();
                inner.pending_actions.clear();
            }
            EventType::ExecutionCompleted(_)
            | EventType::WorkflowCompleted(_)
            | EventType::EventSent(_)
            | EventType::ContinueAsNew(_)
            | EventType::ExecutionStalled(_) => {}
        }
    }

    fn build_response(
        ctx: &OrchestrationContext,
        instance_id: &str,
        completion_token: String,
    ) -> proto::WorkflowResponse {
        let mut inner = ctx.inner.lock().unwrap_or_else(|e| e.into_inner());

        // Move actions out instead of cloning — the context is consumed after
        // this response is built, so the original Vec is no longer needed.
        let mut actions = std::mem::take(&mut inner.pending_actions);

        if let Some(new_input) = inner.continue_as_new_input.take() {
            let mut carryover_events = Vec::new();
            if inner.save_events_on_continue {
                for (name, events) in &inner.buffered_events {
                    for input in events {
                        carryover_events.push(proto::HistoryEvent {
                            event_id: -1,
                            timestamp: None,
                            router: None,
                            event_type: Some(EventType::EventRaised(proto::EventRaisedEvent {
                                name: name.clone(),
                                input: input.clone(),
                            })),
                        });
                    }
                }
            }

            actions.push(proto::WorkflowAction {
                id: actions.len() as i32,
                router: None,
                workflow_action_type: Some(
                    proto::workflow_action::WorkflowActionType::CompleteWorkflow(
                        proto::CompleteWorkflowAction {
                            workflow_status: proto::OrchestrationStatus::ContinuedAsNew as i32,
                            result: Some(new_input),
                            details: None,
                            new_version: None,
                            carryover_events,
                            failure_details: None,
                        },
                    ),
                ),
            });
        } else if let Some(ref status) = inner.completion_status {
            match status {
                OrchestrationStatus::Completed => {
                    actions.push(proto::WorkflowAction {
                        id: actions.len() as i32,
                        router: None,
                        workflow_action_type: Some(
                            proto::workflow_action::WorkflowActionType::CompleteWorkflow(
                                proto::CompleteWorkflowAction {
                                    workflow_status: proto::OrchestrationStatus::Completed as i32,
                                    result: inner.completion_result.take(),
                                    details: None,
                                    new_version: None,
                                    carryover_events: vec![],
                                    failure_details: None,
                                },
                            ),
                        ),
                    });
                }
                OrchestrationStatus::Failed => {
                    let failure = inner.completion_failure.take();
                    actions.push(proto::WorkflowAction {
                        id: actions.len() as i32,
                        router: None,
                        workflow_action_type: Some(
                            proto::workflow_action::WorkflowActionType::CompleteWorkflow(
                                proto::CompleteWorkflowAction {
                                    workflow_status: proto::OrchestrationStatus::Failed as i32,
                                    result: None,
                                    details: None,
                                    new_version: None,
                                    carryover_events: vec![],
                                    failure_details: failure.map(|f| proto::TaskFailureDetails {
                                        error_type: f.error_type,
                                        error_message: f.message,
                                        stack_trace: f.stack_trace,
                                        inner_failure: None,
                                        is_non_retriable: false,
                                    }),
                                },
                            ),
                        ),
                    });
                }
                OrchestrationStatus::Terminated => {
                    actions.push(proto::WorkflowAction {
                        id: actions.len() as i32,
                        router: None,
                        workflow_action_type: Some(
                            proto::workflow_action::WorkflowActionType::CompleteWorkflow(
                                proto::CompleteWorkflowAction {
                                    workflow_status: proto::OrchestrationStatus::Terminated as i32,
                                    result: inner.completion_result.take(),
                                    details: None,
                                    new_version: None,
                                    carryover_events: vec![],
                                    failure_details: None,
                                },
                            ),
                        ),
                    });
                }
                _ => {
                    // Other statuses (Pending, Running, etc.) don't produce completion actions
                }
            }
        }

        proto::WorkflowResponse {
            instance_id: instance_id.to_string(),
            actions,
            custom_status: inner.custom_status.take(),
            completion_token,
            num_events_processed: None,
            version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::to_timestamp;
    use crate::proto::history_event::EventType;

    use std::sync::Arc;

    fn make_workflow_started(ts: chrono::DateTime<chrono::Utc>) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id: 1,
            timestamp: Some(to_timestamp(ts)),
            router: None,
            event_type: Some(EventType::WorkflowStarted(proto::WorkflowStartedEvent {
                version: None,
            })),
        }
    }

    fn make_execution_started(name: &str, input: Option<String>) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id: 2,
            timestamp: Some(to_timestamp(chrono::Utc::now())),
            router: None,
            event_type: Some(EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: name.to_string(),
                version: None,
                input,
                workflow_instance: None,
                parent_instance: None,
                scheduled_start_timestamp: None,
                parent_trace_context: None,
                workflow_span_id: None,
                tags: Default::default(),
            })),
        }
    }

    fn make_task_scheduled(event_id: i32, name: &str) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id,
            timestamp: Some(to_timestamp(chrono::Utc::now())),
            router: None,
            event_type: Some(EventType::TaskScheduled(proto::TaskScheduledEvent {
                name: name.to_string(),
                version: None,
                input: None,
                parent_trace_context: None,
                task_execution_id: String::new(),
                rerun_parent_instance_info: None,
            })),
        }
    }

    fn make_task_completed(
        event_id: i32,
        task_scheduled_id: i32,
        result: Option<String>,
    ) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id,
            timestamp: Some(to_timestamp(chrono::Utc::now())),
            router: None,
            event_type: Some(EventType::TaskCompleted(proto::TaskCompletedEvent {
                task_scheduled_id,
                result,
                task_execution_id: String::new(),
            })),
        }
    }

    fn make_task_failed(event_id: i32, task_scheduled_id: i32) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id,
            timestamp: Some(to_timestamp(chrono::Utc::now())),
            router: None,
            event_type: Some(EventType::TaskFailed(proto::TaskFailedEvent {
                task_scheduled_id,
                failure_details: Some(proto::TaskFailureDetails {
                    error_type: "TestError".to_string(),
                    error_message: "test failure".to_string(),
                    stack_trace: None,
                    inner_failure: None,
                    is_non_retriable: false,
                }),
                task_execution_id: String::new(),
            })),
        }
    }

    #[tokio::test]
    async fn test_simple_orchestrator_completes() {
        let orch_fn: OrchestratorFn =
            Arc::new(|_ctx| Box::pin(async { Ok(Some("\"done\"".to_string())) }));

        let ts = chrono::Utc::now();
        let old_events = vec![make_workflow_started(ts)];
        let new_events = vec![make_execution_started("test_orch", None)];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(resp.instance_id, "inst-1");
        let complete_action = resp.actions.iter().find(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
            )
        });
        assert!(complete_action.is_some());
        if let Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) =
            &complete_action.unwrap().workflow_action_type
        {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Completed as i32
            );
            assert_eq!(cw.result, Some("\"done\"".to_string()));
        }
    }

    #[tokio::test]
    async fn test_orchestrator_with_activity_replay() {
        let orch_fn: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let result = ctx.call_activity("greet", "world").await?;
                Ok(result)
            })
        });

        let ts = chrono::Utc::now();
        let old_events = vec![
            make_workflow_started(ts),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "greet"),
            make_task_completed(4, 0, Some("\"hello world\"".to_string())),
        ];
        let new_events = vec![];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        let complete_action = resp.actions.iter().find(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
            )
        });
        assert!(complete_action.is_some());
        if let Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) =
            &complete_action.unwrap().workflow_action_type
        {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Completed as i32
            );
            assert_eq!(cw.result, Some("\"hello world\"".to_string()));
        }
    }

    #[tokio::test]
    async fn test_orchestrator_pending_activity() {
        let orch_fn: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let result = ctx.call_activity("greet", "world").await?;
                Ok(result)
            })
        });

        let ts = chrono::Utc::now();
        let old_events = vec![make_workflow_started(ts)];
        let new_events = vec![make_execution_started("test_orch", None)];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        let has_schedule = resp.actions.iter().any(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::ScheduleTask(_))
            )
        });
        assert!(has_schedule);

        let has_complete = resp.actions.iter().any(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
            )
        });
        assert!(!has_complete);
    }

    #[tokio::test]
    async fn test_orchestrator_task_failure() {
        let orch_fn: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let result = ctx.call_activity("greet", "world").await?;
                Ok(result)
            })
        });

        let ts = chrono::Utc::now();
        let old_events = vec![
            make_workflow_started(ts),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "greet"),
            make_task_failed(4, 0),
        ];
        let new_events = vec![];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        let complete_action = resp.actions.iter().find(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
            )
        });
        assert!(complete_action.is_some());
        if let Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) =
            &complete_action.unwrap().workflow_action_type
        {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Failed as i32
            );
            assert!(cw.failure_details.is_some());
        }
    }

    #[tokio::test]
    async fn test_suspended_orchestration_not_run() {
        let orch_fn: OrchestratorFn = Arc::new(|_ctx| Box::pin(async { panic!("should not run") }));

        let ts = chrono::Utc::now();
        let old_events = vec![make_workflow_started(ts)];
        let new_events = vec![
            make_execution_started("test_orch", None),
            proto::HistoryEvent {
                event_id: 3,
                timestamp: Some(to_timestamp(chrono::Utc::now())),
                router: None,
                event_type: Some(EventType::ExecutionSuspended(
                    proto::ExecutionSuspendedEvent {
                        input: Some("paused".to_string()),
                    },
                )),
            },
        ];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        assert!(resp.actions.is_empty());
    }

    #[tokio::test]
    async fn test_terminated_orchestration_not_run() {
        let orch_fn: OrchestratorFn = Arc::new(|_ctx| Box::pin(async { panic!("should not run") }));

        let ts = chrono::Utc::now();
        let old_events = vec![make_workflow_started(ts)];
        let new_events = vec![
            make_execution_started("test_orch", None),
            proto::HistoryEvent {
                event_id: 3,
                timestamp: Some(to_timestamp(chrono::Utc::now())),
                router: None,
                event_type: Some(EventType::ExecutionTerminated(
                    proto::ExecutionTerminatedEvent {
                        input: None,
                        recurse: false,
                    },
                )),
            },
        ];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        // Terminated — CompleteWorkflow with Terminated status
        assert_eq!(resp.actions.len(), 1);
        match &resp.actions[0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) => {
                assert_eq!(
                    cw.workflow_status,
                    proto::OrchestrationStatus::Terminated as i32
                );
                assert!(cw.result.is_none());
            }
            other => panic!("expected CompleteWorkflow, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_continue_as_new() {
        let orch_fn: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.continue_as_new("new_input", false);
                Ok(None)
            })
        });

        let ts = chrono::Utc::now();
        let old_events = vec![make_workflow_started(ts)];
        let new_events = vec![make_execution_started("test_orch", None)];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        let complete_action = resp.actions.iter().find(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
            )
        });
        assert!(complete_action.is_some());
        if let Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) =
            &complete_action.unwrap().workflow_action_type
        {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::ContinuedAsNew as i32
            );
            assert_eq!(cw.result, Some("\"new_input\"".to_string()));
        }
    }

    #[tokio::test]
    async fn test_external_event_delivery() {
        let orch_fn: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let result = ctx.wait_for_external_event("approval").await?;
                Ok(result)
            })
        });

        let ts = chrono::Utc::now();
        let old_events = vec![make_workflow_started(ts)];
        let new_events = vec![
            make_execution_started("test_orch", None),
            proto::HistoryEvent {
                event_id: 3,
                timestamp: Some(to_timestamp(chrono::Utc::now())),
                router: None,
                event_type: Some(EventType::EventRaised(proto::EventRaisedEvent {
                    name: "approval".to_string(),
                    input: Some("\"yes\"".to_string()),
                })),
            },
        ];

        let resp = OrchestrationExecutor::execute(
            &orch_fn,
            "inst-1",
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
        )
        .await
        .unwrap();

        let complete_action = resp.actions.iter().find(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
            )
        });
        assert!(complete_action.is_some());
        if let Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) =
            &complete_action.unwrap().workflow_action_type
        {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Completed as i32
            );
            assert_eq!(cw.result, Some("\"yes\"".to_string()));
        }
    }
}
