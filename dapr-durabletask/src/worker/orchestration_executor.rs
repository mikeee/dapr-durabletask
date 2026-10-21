use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use futures::future::BoxFuture;

use crate::api::{DurableTaskError, FailureDetails};
use crate::internal::from_timestamp;
use crate::proto;
use crate::proto::history_event::EventType;
use crate::proto::workflow_action::WorkflowActionType;
use crate::task::OrchestrationContext;
use crate::task::orchestration_context::{
    BufferedEvent, BufferedResolution, FAR_FUTURE_TIMESTAMP, OrchestrationContextInner, Resolution,
    TaskKind, lock_inner,
};

use super::options::WorkerOptions;
use super::registry::OrchestratorFn;

type OrchestratorFuture = BoxFuture<'static, crate::api::Result<Option<String>>>;

/// Executes orchestrator functions by replaying history and processing new events.
///
/// The executor follows the durable task replay model, matching durabletask-go:
/// history events (past, then new) are applied one at a time, and the
/// orchestrator is resumed after every event that may unblock it. This keeps
/// `current_utc_datetime`, `is_replaying` and `is_patched` accurate at each
/// point of the replay, and makes the order in which the orchestrator observes
/// results identical between the original execution and every replay.
///
/// Scheduling events in history (`TaskScheduled`, `TimerCreated`,
/// `ChildWorkflowInstanceCreated`) retire the matching action produced by the
/// replayed orchestrator; a mismatch is a non-determinism failure. Only
/// actions not yet recorded in history are returned to the runtime.
pub struct OrchestrationExecutor;

/// Whether applying an event may have unblocked the orchestrator.
#[derive(PartialEq, Eq)]
enum Resume {
    No,
    Yes,
    Start,
}

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
        propagated_history: Option<crate::api::PropagatedHistory>,
    ) -> crate::api::Result<proto::WorkflowResponse> {
        tracing::info!(
            instance_id = %instance_id,
            past_events = old_events.len(),
            new_events = new_events.len(),
            "Starting orchestration execution"
        );

        // Start an OTel orchestration span covering the replay.
        #[cfg(feature = "opentelemetry")]
        let otel_ctx = {
            let (name, parent_tc) = old_events
                .iter()
                .chain(new_events.iter())
                .find_map(|e| match &e.event_type {
                    Some(EventType::ExecutionStarted(es)) => {
                        Some((es.name.as_str(), es.parent_trace_context.as_ref()))
                    }
                    _ => None,
                })
                .unwrap_or_default();
            let parent_ctx = crate::internal::otel::context_from_trace_context(parent_tc);
            crate::internal::otel::start_orchestration_span(&parent_ctx, name, instance_id)
        };

        let response = Self::replay(
            orchestrator_fn,
            instance_id,
            &old_events,
            &new_events,
            options,
            propagated_history,
            completion_token,
        );

        #[cfg(feature = "opentelemetry")]
        {
            let completion = response
                .actions
                .iter()
                .find_map(|a| match &a.workflow_action_type {
                    Some(WorkflowActionType::CompleteWorkflow(c)) => Some(c),
                    _ => None,
                });
            if let Some(completion) = completion {
                let status = proto::OrchestrationStatus::try_from(completion.workflow_status)
                    .unwrap_or(proto::OrchestrationStatus::Failed);
                let label = match status {
                    proto::OrchestrationStatus::Completed => "COMPLETED",
                    proto::OrchestrationStatus::ContinuedAsNew => "CONTINUED_AS_NEW",
                    proto::OrchestrationStatus::Terminated => "TERMINATED",
                    _ => "FAILED",
                };
                crate::internal::otel::set_span_status_attribute(&otel_ctx, label);
                if let Some(fd) = &completion.failure_details {
                    crate::internal::otel::set_span_error(&otel_ctx, &fd.error_message);
                }
            }
            crate::internal::otel::end_span(&otel_ctx);
        }

        tracing::debug!(
            instance_id = %instance_id,
            actions = response.actions.len(),
            "Built orchestration response"
        );
        Ok(response)
    }

    /// Replay the history and build the response.
    fn replay(
        orchestrator_fn: &OrchestratorFn,
        instance_id: &str,
        old_events: &[proto::HistoryEvent],
        new_events: &[proto::HistoryEvent],
        options: &WorkerOptions,
        propagated_history: Option<crate::api::PropagatedHistory>,
        completion_token: String,
    ) -> proto::WorkflowResponse {
        // Name and input start empty and are overwritten when ExecutionStarted is replayed.
        let ctx = OrchestrationContext::new(
            instance_id.to_string(),
            String::new(),
            None,
            chrono::Utc::now(),
            true,
            options,
            old_events.len() + new_events.len(),
        );
        {
            let mut inner = lock_inner(&ctx.inner);
            inner.history_len = old_events.len() + new_events.len();
            // Every recorded patch is reported back even if the replay stops
            // before reaching the turn that recorded it.
            for event in old_events.iter().chain(new_events) {
                if let Some(EventType::WorkflowStarted(ws)) = &event.event_type
                    && let Some(version) = &ws.version
                {
                    for patch in &version.patches {
                        if !inner.recorded_patches.contains(patch) {
                            inner.recorded_patches.push(patch.clone());
                        }
                    }
                }
            }
            // Stash the propagated history (if any) before running the function so
            // that ctx.propagated_history() is available during user code.
            inner.propagated_history = propagated_history.map(std::sync::Arc::new);
        }
        Self::replay_per_event(
            orchestrator_fn,
            &ctx,
            old_events,
            new_events,
            options,
            instance_id,
        );
        Self::build_response(&ctx, instance_id, completion_token)
    }

    /// Apply history events one at a time, resuming the orchestrator after
    /// each event that may unblock it.
    fn replay_per_event(
        orchestrator_fn: &OrchestratorFn,
        ctx: &OrchestrationContext,
        old_events: &[proto::HistoryEvent],
        new_events: &[proto::HistoryEvent],
        options: &WorkerOptions,
        instance_id: &str,
    ) {
        let mut future: Option<OrchestratorFuture> = None;
        let old_len = old_events.len();
        'history: for (index, event) in old_events.iter().chain(new_events.iter()).enumerate() {
            let during_replay = index < old_len;
            if !Self::step(
                orchestrator_fn,
                ctx,
                &mut future,
                event,
                index,
                during_replay,
                options,
                instance_id,
            ) {
                break;
            }
            // Events held while suspended are applied in their original order,
            // resuming the orchestrator after each, as if they arrived now.
            loop {
                let Some(held) = lock_inner(&ctx.inner).resumed_events.pop_front() else {
                    break;
                };
                if !Self::step(
                    orchestrator_fn,
                    ctx,
                    &mut future,
                    &held,
                    index,
                    during_replay,
                    options,
                    instance_id,
                ) {
                    break 'history;
                }
            }
        }
        drop(future);
    }

    /// Apply one history event and resume the orchestrator if it may have
    /// been unblocked. Returns `false` once the history cannot be applied.
    #[allow(clippy::too_many_arguments)]
    fn step(
        orchestrator_fn: &OrchestratorFn,
        ctx: &OrchestrationContext,
        future: &mut Option<OrchestratorFuture>,
        event: &proto::HistoryEvent,
        index: usize,
        during_replay: bool,
        options: &WorkerOptions,
        instance_id: &str,
    ) -> bool {
        let resume = {
            let mut inner = lock_inner(&ctx.inner);
            inner.history_index = index + 1;
            inner.is_replaying.store(during_replay, Ordering::Release);
            Self::apply_event(&mut inner, event, during_replay, options)
        };
        match resume {
            Ok(Resume::No) => {}
            Ok(Resume::Start) => {
                tracing::debug!(instance_id = %instance_id, "Starting orchestrator function");
                // The closure itself may panic before returning a future.
                let started =
                    std::panic::catch_unwind(AssertUnwindSafe(|| (orchestrator_fn)(ctx.clone())));
                match started {
                    Ok(f) => {
                        *future = Some(f);
                        Self::resume(ctx, future, instance_id);
                    }
                    Err(panic) => {
                        *future = None;
                        Self::record_outcome(ctx, Err(panic_message(panic.as_ref())), instance_id);
                    }
                }
            }
            Ok(Resume::Yes) => Self::resume(ctx, future, instance_id),
            Err(failure) => {
                tracing::error!(
                    instance_id = %instance_id,
                    error = %failure.message,
                    "Orchestration failed while applying history"
                );
                *future = None;
                lock_inner(&ctx.inner).fail_replay(failure);
                return false;
            }
        }
        true
    }

    /// Poll the orchestrator once, recording its completion if it returns.
    fn resume(
        ctx: &OrchestrationContext,
        future: &mut Option<OrchestratorFuture>,
        instance_id: &str,
    ) {
        {
            let inner = lock_inner(&ctx.inner);
            if inner.is_terminated || inner.is_complete {
                return;
            }
        }
        let Some(fut) = future.as_mut() else {
            return;
        };

        let mut cx = Context::from_waker(Waker::noop());
        let poll = std::panic::catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut cx)));
        let outcome = match poll {
            Ok(Poll::Pending) => {
                tracing::debug!(instance_id = %instance_id, "Orchestrator yielded, waiting for tasks");
                return;
            }
            Ok(Poll::Ready(result)) => Ok(result),
            Err(panic) => Err(panic_message(panic.as_ref())),
        };
        *future = None;
        Self::record_outcome(ctx, outcome, instance_id);
    }

    /// Queue the completion for the orchestrator's return value, or for the
    /// message of a panic it raised.
    fn record_outcome(
        ctx: &OrchestrationContext,
        outcome: Result<crate::api::Result<Option<String>>, String>,
        instance_id: &str,
    ) {
        let mut inner = lock_inner(&ctx.inner);
        match outcome {
            Ok(Ok(output)) => {
                if let Some(new_input) = inner.continue_as_new_input.clone() {
                    tracing::info!(
                        instance_id = %instance_id,
                        orchestrator = %inner.name,
                        "Orchestration continuing as new"
                    );
                    inner.set_complete(proto::OrchestrationStatus::ContinuedAsNew, new_input, None);
                } else {
                    tracing::info!(
                        instance_id = %instance_id,
                        orchestrator = %inner.name,
                        "Orchestration completed successfully"
                    );
                    inner.set_complete(proto::OrchestrationStatus::Completed, output, None);
                }
            }
            Ok(Err(DurableTaskError::TaskFailed {
                message,
                failure_details,
            })) => {
                tracing::warn!(
                    instance_id = %instance_id,
                    orchestrator = %inner.name,
                    error = %message,
                    "Orchestration failed due to task failure"
                );
                let failure = failure_details.unwrap_or(FailureDetails {
                    message,
                    error_type: "TaskFailed".to_string(),
                    stack_trace: None,
                });
                inner.set_complete(proto::OrchestrationStatus::Failed, None, Some(failure));
            }
            Ok(Err(e)) => {
                tracing::error!(
                    instance_id = %instance_id,
                    orchestrator = %inner.name,
                    error = %e,
                    "Orchestration failed with error"
                );
                let failure = FailureDetails {
                    message: e.to_string(),
                    error_type: "OrchestratorError".to_string(),
                    stack_trace: None,
                };
                inner.set_complete(proto::OrchestrationStatus::Failed, None, Some(failure));
            }
            Err(message) => {
                tracing::error!(
                    instance_id = %instance_id,
                    orchestrator = %inner.name,
                    error = %message,
                    "Orchestrator panicked"
                );
                let failure = FailureDetails {
                    message,
                    error_type: "OrchestratorPanic".to_string(),
                    stack_trace: None,
                };
                inner.set_complete(proto::OrchestrationStatus::Failed, None, Some(failure));
            }
        }
    }

    /// Apply a single history event to the orchestration state.
    ///
    /// Returns whether the orchestrator should be resumed, or the failure to
    /// report if the history does not match the orchestrator's actions.
    fn apply_event(
        inner: &mut OrchestrationContextInner,
        event: &proto::HistoryEvent,
        during_replay: bool,
        options: &WorkerOptions,
    ) -> Result<Resume, FailureDetails> {
        let Some(event_type) = &event.event_type else {
            return Ok(Resume::No);
        };
        let instance_id = inner.instance_id.clone();

        // A terminated workflow processes no further events.
        if inner.is_terminated {
            return Ok(Resume::No);
        }

        // While suspended, hold events back until resumed or terminated.
        // WorkflowStarted is applied straight away: the held events run on
        // the resume turn, so they must see that turn's clock and the patches
        // recorded on it.
        if inner.is_suspended
            && !matches!(
                event_type,
                EventType::ExecutionResumed(_)
                    | EventType::ExecutionTerminated(_)
                    | EventType::WorkflowStarted(_)
            )
        {
            inner.suspended_events.push(event.clone());
            return Ok(Resume::No);
        }

        let resolution = |kind: TaskKind, id: i32, what: &str, resolution: Resolution| {
            (
                kind,
                id,
                BufferedResolution {
                    resolution,
                    during_replay,
                    description: format!("{what} for id {id}"),
                    task_execution_id: None,
                },
            )
        };

        let resolved = match event_type {
            EventType::WorkflowStarted(ws) => {
                if let Some(ts) = &event.timestamp
                    && let Some(dt) = from_timestamp(ts)
                {
                    inner.current_utc_datetime = dt;
                }
                // daprd records only the patches new to each turn, the
                // durabletask-go backend the full list: keep each patch once,
                // in first-seen order.
                if let Some(version) = &ws.version {
                    for patch in &version.patches {
                        if !inner.history_patches.contains(patch) {
                            inner.history_patches.push(patch.clone());
                        }
                    }
                }
                return Ok(Resume::No);
            }
            EventType::ExecutionStarted(e) => {
                tracing::debug!(
                    instance_id = %instance_id,
                    orchestrator = %e.name,
                    "Execution started event"
                );
                inner.name = std::sync::Arc::<str>::from(e.name.clone());
                inner.input = e.input.clone();
                return Ok(Resume::Start);
            }
            EventType::TaskScheduled(e) => {
                return Self::retire_action(inner, event, TaskKind::Activity, &e.name);
            }
            EventType::TimerCreated(_) => {
                return Self::retire_action(inner, event, TaskKind::Timer, "");
            }
            EventType::ChildWorkflowInstanceCreated(e) => {
                return Self::retire_action(inner, event, TaskKind::ChildWorkflow, &e.name);
            }
            EventType::DetachedWorkflowInstanceCreated(e) => {
                return Self::retire_action(
                    inner,
                    event,
                    TaskKind::DetachedWorkflow,
                    &e.instance_id,
                );
            }
            EventType::TaskCompleted(e) => resolution(
                TaskKind::Activity,
                e.task_scheduled_id,
                "TaskCompleted",
                Resolution::Completed(e.result.clone()),
            ),
            EventType::TaskFailed(e) => {
                let (kind, id, mut buffered) = resolution(
                    TaskKind::Activity,
                    e.task_scheduled_id,
                    "TaskFailed",
                    Resolution::Failed(
                        e.failure_details
                            .as_ref()
                            .map(FailureDetails::from)
                            .unwrap_or_else(|| FailureDetails {
                                message: "Task failed".to_string(),
                                error_type: "Unknown".to_string(),
                                stack_trace: None,
                            }),
                    ),
                );
                buffered.task_execution_id = Some(e.task_execution_id.clone());
                (kind, id, buffered)
            }
            EventType::TimerFired(e) => resolution(
                TaskKind::Timer,
                e.timer_id,
                "TimerFired",
                Resolution::Completed(None),
            ),
            EventType::ChildWorkflowInstanceCompleted(e) => resolution(
                TaskKind::ChildWorkflow,
                e.task_scheduled_id,
                "ChildWorkflowInstanceCompleted",
                Resolution::Completed(e.result.clone()),
            ),
            EventType::ChildWorkflowInstanceFailed(e) => resolution(
                TaskKind::ChildWorkflow,
                e.task_scheduled_id,
                "ChildWorkflowInstanceFailed",
                Resolution::Failed(
                    e.failure_details
                        .as_ref()
                        .map(FailureDetails::from)
                        .unwrap_or_else(|| FailureDetails {
                            message: "Sub-orchestration failed".to_string(),
                            error_type: "Unknown".to_string(),
                            stack_trace: None,
                        }),
                ),
            ),
            EventType::EventRaised(e) => {
                Self::raise_event(inner, event, e, during_replay, options);
                return Ok(Resume::Yes);
            }
            EventType::ExecutionSuspended(_) => {
                tracing::info!(instance_id = %instance_id, "Orchestration suspended");
                inner.is_suspended = true;
                return Ok(Resume::No);
            }
            EventType::ExecutionResumed(_) => {
                tracing::info!(instance_id = %instance_id, "Orchestration resumed");
                inner.is_suspended = false;
                // The replay loop applies the held events next, one at a time.
                let held = std::mem::take(&mut inner.suspended_events);
                inner.resumed_events.extend(held);
                return Ok(Resume::No);
            }
            EventType::ExecutionTerminated(e) => {
                tracing::info!(instance_id = %instance_id, "Orchestration terminated");
                inner.is_terminated = true;
                match inner.completion().map(|c| c.workflow_status) {
                    // Continue-as-new never overrides a terminate.
                    Some(s) if s == proto::OrchestrationStatus::ContinuedAsNew as i32 => {
                        inner.pending_actions.retain(|_, a| {
                            !matches!(
                                a.workflow_action_type,
                                Some(WorkflowActionType::CompleteWorkflow(_))
                            )
                        });
                    }
                    // A completion recorded before the terminate wins.
                    Some(_) => return Ok(Resume::No),
                    None => {}
                }
                inner.set_complete(
                    proto::OrchestrationStatus::Terminated,
                    e.input.clone(),
                    None,
                );
                return Ok(Resume::No);
            }
            EventType::ExecutionCompleted(_)
            | EventType::WorkflowCompleted(_)
            | EventType::EventSent(_)
            | EventType::ContinueAsNew(_)
            | EventType::ExecutionStalled(_) => return Ok(Resume::No),
        };

        let (kind, id, buffered) = resolved;
        tracing::debug!(
            instance_id = %instance_id,
            resolution = %buffered.description,
            "Applying resolution"
        );
        inner.resolve(kind, id, buffered);
        Ok(Resume::Yes)
    }

    /// Retire the pending action a scheduling event in history records.
    ///
    /// Event-wait timers are tolerated in both directions so histories
    /// recorded by releases that emitted them differently still replay: an
    /// indefinite-wait timer the history lacks is dropped, and an unnamed
    /// event-wait timer (as earlier releases of this SDK recorded when the
    /// event was already buffered) that this execution did not emit is
    /// absorbed. Either shift may deliver a buffered resolution, so the
    /// orchestrator is then resumed.
    ///
    /// Detached workflow spawns are matched on the instance ID the call
    /// returned, as in durabletask-go (`onDetachedWorkflowCreated`).
    ///
    /// Beyond durabletask-go, activity and child workflow names and timer
    /// origins must match too, so a replay that diverges from its history
    /// fails loudly instead of delivering results to the wrong task. A
    /// repeated scheduling event for an already-retired ID (persisted by
    /// older runtimes when older SDK releases re-emitted in-flight actions)
    /// is ignored.
    fn retire_action(
        inner: &mut OrchestrationContextInner,
        event: &proto::HistoryEvent,
        kind: TaskKind,
        name: &str,
    ) -> Result<Resume, FailureDetails> {
        let id = event.event_id;
        let recorded_timer = match &event.event_type {
            Some(EventType::TimerCreated(t)) => Some(t),
            _ => None,
        };
        let recorded_event_timer = recorded_timer.and_then(recorded_event_timer);

        let mut resume = Resume::No;
        while inner.is_optional_event_timer_at(id) && recorded_event_timer != Some(true) {
            inner.drop_optional_event_timer_at(id);
            resume = Resume::Yes;
        }

        let pending = inner
            .pending_actions
            .get(&id)
            .and_then(|a| a.workflow_action_type.as_ref());
        let scheduled_name = match (pending, kind) {
            (Some(WorkflowActionType::ScheduleTask(a)), TaskKind::Activity) => {
                Some(a.name.as_str())
            }
            (Some(WorkflowActionType::CreateChildWorkflow(a)), TaskKind::ChildWorkflow) => {
                Some(a.name.as_str())
            }
            // A detached spawn is matched on the instance ID the call returned
            // to the workflow code.
            (Some(WorkflowActionType::CreateDetachedWorkflow(a)), TaskKind::DetachedWorkflow) => {
                Some(a.instance_id.as_str())
            }
            _ => None,
        };
        let matches = match (pending, kind) {
            (Some(WorkflowActionType::CreateTimer(a)), TaskKind::Timer) => {
                timer_matches(a, recorded_timer, recorded_event_timer)
            }
            _ => scheduled_name == Some(name),
        };
        if matches {
            inner.pending_actions.remove(&id);
            inner.retired_actions.insert(id, kind);
            return Ok(resume);
        }

        if inner.retired_actions.get(&id) == Some(&kind) {
            tracing::debug!(
                instance_id = %inner.instance_id,
                id,
                "Ignoring duplicate scheduling event"
            );
            return Ok(resume);
        }

        if let Some(t) = recorded_timer
            && recorded_event_timer.is_some()
            && t.name.is_none()
            && (0..=inner.sequence_number).contains(&id)
            && !inner.retired_actions.contains_key(&id)
        {
            inner.absorb_recorded_event_timer_at(id);
            inner.retired_actions.insert(id, TaskKind::Timer);
            return Ok(Resume::Yes);
        }

        let message = match (kind, scheduled_name) {
            (TaskKind::Activity, Some(current)) => format!(
                "a previous execution called CallActivity for '{name}' with sequence number {id} at this point in the workflow logic, but the current execution called CallActivity for '{current}'"
            ),
            (TaskKind::ChildWorkflow, Some(current)) => format!(
                "a previous execution called CallChildWorkflow for '{name}' with sequence number {id} at this point in the workflow logic, but the current execution called CallChildWorkflow for '{current}'"
            ),
            (TaskKind::Activity, None) => format!(
                "a previous execution called CallActivity for '{name}' and sequence number {id} at this point in the workflow logic, but the current execution doesn't have this action with this sequence number"
            ),
            (TaskKind::ChildWorkflow, None) => format!(
                "a previous execution called CallChildWorkflow for '{name}' and sequence number {id} at this point in the workflow logic, but the current execution doesn't have this action with this sequence number"
            ),
            (TaskKind::DetachedWorkflow, Some(current)) => format!(
                "a previous execution called ScheduleNewDetachedWorkflow for instance ID '{name}' and sequence number {id} at this point in the workflow logic, but the current execution scheduled instance ID '{current}'"
            ),
            (TaskKind::DetachedWorkflow, None) => format!(
                "a previous execution called ScheduleNewDetachedWorkflow for instance ID '{name}' and sequence number {id} at this point in the workflow logic, but the current execution doesn't have this action with this sequence number"
            ),
            (TaskKind::Timer, _) => format!(
                "a previous execution called CreateTimer with sequence number {id}, but the current execution doesn't have this action with this sequence number"
            ),
        };
        Err(FailureDetails {
            message,
            error_type: "NonDeterminismError".to_string(),
            stack_trace: None,
        })
    }

    fn raise_event(
        inner: &mut OrchestrationContextInner,
        event: &proto::HistoryEvent,
        e: &proto::EventRaisedEvent,
        during_replay: bool,
        options: &WorkerOptions,
    ) {
        let instance_id = inner.instance_id.clone();
        if let Err(err) = crate::internal::validate_identifier(
            &e.name,
            "event name",
            options.max_identifier_length,
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

        // Once the orchestrator has finished, its abandoned waiters must not
        // consume events: they are buffered so continue-as-new carries them over.
        if !inner.is_complete
            && let Some(tasks) = inner.pending_event_tasks.get_mut(&event_name)
        {
            while let Some(task) = tasks.pop_front() {
                if task.is_complete() {
                    continue;
                }
                task.complete_with_phase(e.input.clone(), during_replay);
                if tasks.is_empty() {
                    inner.pending_event_tasks.remove(&event_name);
                }
                return;
            }
            inner.pending_event_tasks.remove(&event_name);
        }

        if inner.buffered_events.len() >= inner.config.max_event_names
            && !inner.buffered_events.contains_key(&event_name)
        {
            tracing::warn!(
                instance_id = %instance_id,
                event_name = %e.name,
                "Event name limit reached, discarding event"
            );
            return;
        }

        let max_events = inner.config.max_events_per_name;
        let arrival = inner.buffered_event_count;
        let events = inner.buffered_events.entry(event_name).or_default();
        if events.len() >= max_events {
            tracing::warn!(
                instance_id = %instance_id,
                event_name = %e.name,
                "Event buffer limit reached, discarding event"
            );
            return;
        }
        events.push_back(BufferedEvent {
            event: event.clone(),
            during_replay,
            arrival,
        });
        inner.buffered_event_count += 1;
    }

    fn build_response(
        ctx: &OrchestrationContext,
        instance_id: &str,
        completion_token: String,
    ) -> proto::WorkflowResponse {
        let mut inner = lock_inner(&ctx.inner);

        if !inner.buffered_resolutions.is_empty() {
            let mut unconsumed: Vec<_> = inner.buffered_resolutions.iter().collect();
            unconsumed.sort_by_key(|(key, _)| **key);
            for (_, buffered) in unconsumed {
                tracing::warn!(
                    instance_id = %instance_id,
                    resolution = %buffered.description,
                    "Resolution arrived before the matching work was scheduled and was not \
                     consumed by the end of this execution; if it never matches this indicates \
                     a non-deterministic workflow or an out-of-order history"
                );
            }
        }

        // A suspended workflow returns no actions unless it was terminated.
        let mut actions: Vec<proto::WorkflowAction> = if inner.is_suspended && !inner.is_terminated
        {
            Vec::new()
        } else {
            std::mem::take(&mut inner.pending_actions)
                .into_values()
                .collect()
        };
        // A terminated workflow starts no new work: only its completion is sent.
        if inner.is_terminated {
            actions.retain(|a| {
                matches!(
                    a.workflow_action_type,
                    Some(WorkflowActionType::CompleteWorkflow(_))
                )
            });
        }

        if inner.save_events_on_continue {
            // Carry unconsumed events over in the order they arrived.
            let mut buffered: Vec<&BufferedEvent> =
                inner.buffered_events.values().flatten().collect();
            buffered.sort_by_key(|b| b.arrival);
            let carryover: Vec<proto::HistoryEvent> =
                buffered.into_iter().map(|b| b.event.clone()).collect();
            for action in &mut actions {
                if let Some(WorkflowActionType::CompleteWorkflow(c)) =
                    &mut action.workflow_action_type
                    && c.workflow_status == proto::OrchestrationStatus::ContinuedAsNew as i32
                {
                    c.carryover_events = carryover.clone();
                }
            }
        }

        // Report patches so the runtime records them on this turn's
        // WorkflowStarted event, enabling correct replay of patch-gated code.
        // Patches already in history are always reported, in history order
        // (including ones no longer checked, such as the retired
        // `dapr:external-event-timer`), or the runtime stalls the workflow.
        let patches = inner.reported_patches();
        let version = (!patches.is_empty()).then_some(proto::WorkflowVersion {
            patches,
            name: None,
        });

        proto::WorkflowResponse {
            instance_id: instance_id.to_string(),
            actions,
            custom_status: inner.custom_status.take(),
            completion_token,
            num_events_processed: None,
            version,
        }
    }
}

/// Classify a recorded timer: `Some(indefinite)` for an event-wait timer —
/// tagged with the ExternalEvent origin or, on runtimes that do not persist
/// origins, set in the far future — otherwise `None`. Earlier releases used a
/// far-future sentinel without the nanoseconds, so compare by second.
fn recorded_event_timer(t: &proto::TimerCreatedEvent) -> Option<bool> {
    let far_future = t
        .fire_at
        .as_ref()
        .is_some_and(|f| f.seconds >= FAR_FUTURE_TIMESTAMP.timestamp());
    match t.origin {
        Some(proto::timer_created_event::Origin::ExternalEvent(_)) => Some(far_future),
        None if far_future => Some(true),
        _ => None,
    }
}

/// Whether a pending timer action is the one a recorded timer describes. An
/// event-wait timer only matches an event-wait action for the same event;
/// other origins must agree when both are known (earlier releases recorded
/// none).
fn timer_matches(
    action: &proto::CreateTimerAction,
    recorded: Option<&proto::TimerCreatedEvent>,
    recorded_event_timer: Option<bool>,
) -> bool {
    use proto::create_timer_action::Origin as Action;
    use proto::timer_created_event::Origin as Recorded;
    match (&action.origin, recorded.and_then(|t| t.origin.as_ref())) {
        (Some(Action::ExternalEvent(a)), Some(Recorded::ExternalEvent(r))) => {
            a.name.to_lowercase() == r.name.to_lowercase()
        }
        (Some(Action::ExternalEvent(_)), None) => true,
        _ if recorded_event_timer.is_some() => false,
        (Some(Action::CreateTimer(_)), Some(Recorded::CreateTimer(_)))
        | (Some(Action::ActivityRetry(_)), Some(Recorded::ActivityRetry(_)))
        | (Some(Action::ChildWorkflowRetry(_)), Some(Recorded::ChildWorkflowRetry(_)))
        | (None, _)
        | (_, None) => true,
        _ => false,
    }
}

/// Render a caught panic payload as a failure message.
pub(crate) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_string());
    format!("panic: {detail}")
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
                history_propagation_scope: None,
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
                attestation: None,
                signer_certificate: None,
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
                attestation: None,
                signer_certificate: None,
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
            None,
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
            // A TaskScheduled event's ID is the action's sequence number.
            make_task_scheduled(0, "greet"),
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
            None,
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
            None,
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
            make_task_scheduled(0, "greet"),
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
            None,
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
            None,
        )
        .await
        .unwrap();

        assert!(resp.actions.is_empty());
    }

    #[tokio::test]
    async fn test_terminated_orchestration_emits_only_termination() {
        // The orchestrator runs up to its first await when ExecutionStarted is
        // applied; the terminate then withholds the scheduled activity.
        let orch_fn: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let result = ctx.call_activity("greet", "world").await?;
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
            None,
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
            other => panic!("expected CompleteWorkflow, got {other:?}"),
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
            None,
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
            None,
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
