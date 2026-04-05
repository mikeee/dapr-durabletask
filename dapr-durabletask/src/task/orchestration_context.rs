use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::api::{DurableTaskError, FailureDetails, OrchestrationStatus, RetryPolicy};
use crate::internal::{to_json, to_timestamp};
use crate::proto;

use super::completable_task::CompletableTask;
use super::options::{ActivityOptions, SubOrchestratorOptions};

/// Internal state shared between the context and the orchestration executor.
pub(crate) struct OrchestrationContextInner {
    pub(crate) instance_id: String,
    pub(crate) current_utc_datetime: chrono::DateTime<chrono::Utc>,
    pub(crate) is_replaying: bool,
    pub(crate) is_complete: bool,
    pub(crate) input: Option<String>,
    pub(crate) name: String,
    pub(crate) custom_status: Option<String>,
    pub(crate) sequence_number: i32,
    pub(crate) pending_tasks: HashMap<i32, CompletableTask>,
    pub(crate) pending_event_tasks: HashMap<String, Vec<CompletableTask>>,
    pub(crate) buffered_events: HashMap<String, Vec<Option<String>>>,
    pub(crate) pending_actions: Vec<proto::WorkflowAction>,
    pub(crate) completion_status: Option<OrchestrationStatus>,
    pub(crate) completion_result: Option<String>,
    pub(crate) completion_failure: Option<FailureDetails>,
    pub(crate) continue_as_new_input: Option<String>,
    pub(crate) save_events_on_continue: bool,
    pub(crate) is_suspended: bool,
    pub(crate) max_event_names: usize,
    pub(crate) max_events_per_name: usize,
    pub(crate) max_pending_tasks_per_name: usize,
    pub(crate) max_json_payload_size: usize,
    /// Patches recorded as applied in the orchestration history (from `WorkflowStarted` events).
    pub(crate) history_patches: std::collections::HashSet<String>,
    /// Cache of patch decisions made during the current execution.
    pub(crate) applied_patches: HashMap<String, bool>,
    /// Number of sequence-consuming scheduled actions recorded in history
    /// (TaskScheduled + TimerCreated + ChildWorkflowInstanceCreated).
    /// Used to determine whether `is_patched` is called mid-history or at the frontier.
    pub(crate) history_scheduled_count: i32,
}

/// The orchestration context provided to orchestrator functions.
///
/// All methods are safe to call from async code. The context is cloneable
/// and thread-safe (`Send + Sync`), backed by `Arc<Mutex<>>`.
#[derive(Clone)]
pub struct OrchestrationContext {
    pub(crate) inner: Arc<Mutex<OrchestrationContextInner>>,
}

impl OrchestrationContext {
    /// Create a new orchestration context with the given parameters.
    pub(crate) fn new(
        instance_id: String,
        name: String,
        input: Option<String>,
        current_utc_datetime: chrono::DateTime<chrono::Utc>,
        is_replaying: bool,
        options: &crate::worker::WorkerOptions,
        event_count_hint: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(OrchestrationContextInner {
                instance_id,
                current_utc_datetime,
                is_replaying,
                is_complete: false,
                input,
                name,
                custom_status: None,
                sequence_number: 0,
                pending_tasks: HashMap::with_capacity(event_count_hint / 2),
                pending_event_tasks: HashMap::new(),
                buffered_events: HashMap::new(),
                pending_actions: Vec::with_capacity(event_count_hint / 2),
                completion_status: None,
                completion_result: None,
                completion_failure: None,
                continue_as_new_input: None,
                save_events_on_continue: false,
                is_suspended: false,
                max_event_names: options.max_event_names,
                max_events_per_name: options.max_events_per_name,
                max_pending_tasks_per_name: options.max_pending_tasks_per_name,
                max_json_payload_size: options.max_json_payload_size,
                history_patches: std::collections::HashSet::new(),
                applied_patches: HashMap::new(),
                history_scheduled_count: 0,
            })),
        }
    }

    /// Get the instance ID.
    pub fn instance_id(&self) -> String {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .instance_id
            .clone()
    }

    /// Get the current UTC datetime (deterministic, from history events).
    pub fn current_utc_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .current_utc_datetime
    }

    /// Check if the orchestrator is currently replaying.
    pub fn is_replaying(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_replaying
    }

    /// Get the orchestration name.
    pub fn name(&self) -> String {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .name
            .clone()
    }

    /// Get the orchestration input, deserialised from JSON.
    pub fn get_input<T: DeserializeOwned>(&self) -> crate::api::Result<T> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        crate::internal::from_json(inner.input.as_deref(), inner.max_json_payload_size)
    }

    /// Set a custom status string.
    pub fn set_custom_status(&self, status: &str) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.custom_status = Some(status.to_string());
    }

    /// Schedule an activity for execution.
    ///
    /// Returns a [`CompletableTask`] that resolves when the activity completes.
    ///
    /// During replay: if the corresponding `TaskCompleted`/`TaskFailed` event
    /// exists in history, the task will already be complete.
    /// During new execution: creates a `ScheduleTaskAction`.
    pub fn call_activity(&self, name: &str, input: impl Serialize) -> CompletableTask {
        tracing::debug!(activity = %name, "Scheduling activity");
        self.call_activity_inner(name, input, None)
    }

    /// Schedule an activity with an `app_id` for cross-app scenarios.
    pub fn call_activity_with_app_id(
        &self,
        name: &str,
        input: impl Serialize,
        app_id: &str,
    ) -> CompletableTask {
        tracing::debug!(activity = %name, app_id = %app_id, "Scheduling activity with app_id");
        self.call_activity_inner(name, input, Some(app_id))
    }

    fn call_activity_inner(
        &self,
        name: &str,
        input: impl Serialize,
        app_id: Option<&str>,
    ) -> CompletableTask {
        let input_json = to_json(&input).ok().flatten();
        self.call_activity_raw(name, input_json, app_id)
    }

    /// Internal: schedule an activity using a pre-serialised JSON input.
    fn call_activity_raw(
        &self,
        name: &str,
        input_json: Option<String>,
        app_id: Option<&str>,
    ) -> CompletableTask {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let seq = inner.sequence_number;
        inner.sequence_number += 1;

        if let Some(existing) = inner.pending_tasks.get(&seq) {
            if existing.is_complete() {
                return existing.clone();
            }
        }

        let task = CompletableTask::new();
        inner.pending_tasks.insert(seq, task.clone());

        let router = app_id.map(|id| proto::TaskRouter {
            source_app_id: String::new(),
            target_app_id: Some(id.to_string()),
        });
        let action = proto::WorkflowAction {
            id: seq,
            router: None,
            workflow_action_type: Some(proto::workflow_action::WorkflowActionType::ScheduleTask(
                proto::ScheduleTaskAction {
                    name: name.to_string(),
                    version: None,
                    input: input_json,
                    router,
                    task_execution_id: String::new(),
                },
            )),
        };
        inner.pending_actions.push(action);

        task
    }

    /// Schedule an activity with options (retry policy, app ID).
    ///
    /// Returns a future that drives the activity to completion, transparently
    /// scheduling durable timers and re-issuing the activity on each retry.
    pub fn call_activity_with_options(
        &self,
        name: &str,
        input: impl Serialize,
        options: ActivityOptions,
    ) -> BoxFuture<'static, crate::api::Result<Option<String>>> {
        let input_json = to_json(&input).ok().flatten();
        let name = name.to_string();
        let app_id = options.app_id.clone();
        let ctx = self.clone();

        match options.retry_policy {
            Some(policy) => {
                let first_attempt_time = ctx.current_utc_datetime();
                let schedule: Arc<dyn Fn(&OrchestrationContext) -> CompletableTask + Send + Sync> =
                    Arc::new(move |c: &OrchestrationContext| {
                        c.call_activity_raw(&name, input_json.clone(), app_id.as_deref())
                    });
                call_with_retry(ctx, schedule, policy, 0, first_attempt_time)
            }
            None => {
                let task = self.call_activity_raw(&name, input_json, app_id.as_deref());
                Box::pin(task)
            }
        }
    }

    /// Schedule a sub-orchestration for execution.
    pub fn call_sub_orchestrator(
        &self,
        name: &str,
        input: impl Serialize,
        instance_id: Option<&str>,
    ) -> CompletableTask {
        tracing::debug!(
            sub_orchestrator = %name,
            sub_instance_id = ?instance_id,
            "Scheduling sub-orchestration"
        );
        self.call_sub_orchestrator_inner(name, input, instance_id, None)
    }

    /// Schedule a sub-orchestration targeting a specific Dapr app ID.
    pub fn call_sub_orchestrator_with_app_id(
        &self,
        name: &str,
        input: impl Serialize,
        instance_id: Option<&str>,
        app_id: &str,
    ) -> CompletableTask {
        tracing::debug!(
            sub_orchestrator = %name,
            sub_instance_id = ?instance_id,
            app_id = %app_id,
            "Scheduling sub-orchestration with app_id"
        );
        self.call_sub_orchestrator_inner(name, input, instance_id, Some(app_id))
    }

    fn call_sub_orchestrator_inner(
        &self,
        name: &str,
        input: impl Serialize,
        instance_id: Option<&str>,
        app_id: Option<&str>,
    ) -> CompletableTask {
        let input_json = to_json(&input).ok().flatten();
        self.call_sub_orchestrator_raw(name, input_json, instance_id, app_id)
    }

    /// Internal: schedule a sub-orchestration using a pre-serialised JSON input.
    fn call_sub_orchestrator_raw(
        &self,
        name: &str,
        input_json: Option<String>,
        instance_id: Option<&str>,
        app_id: Option<&str>,
    ) -> CompletableTask {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let seq = inner.sequence_number;
        inner.sequence_number += 1;

        if let Some(existing) = inner.pending_tasks.get(&seq) {
            if existing.is_complete() {
                return existing.clone();
            }
        }

        let task = CompletableTask::new();
        inner.pending_tasks.insert(seq, task.clone());

        let sub_instance_id = instance_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let router = app_id.map(|id| proto::TaskRouter {
            source_app_id: String::new(),
            target_app_id: Some(id.to_string()),
        });

        let action = proto::WorkflowAction {
            id: seq,
            router: None,
            workflow_action_type: Some(
                proto::workflow_action::WorkflowActionType::CreateChildWorkflow(
                    proto::CreateChildWorkflowAction {
                        instance_id: sub_instance_id,
                        name: name.to_string(),
                        version: None,
                        input: input_json,
                        router,
                    },
                ),
            ),
        };
        inner.pending_actions.push(action);

        task
    }

    /// Schedule a sub-orchestration with options (instance ID, retry policy, app ID).
    ///
    /// Returns a future that drives the sub-orchestration to completion,
    /// transparently scheduling durable timers and re-issuing the call on each retry.
    ///
    /// Note: when a retry policy is set and no explicit `instance_id` is given,
    /// each retry uses a freshly generated instance ID.
    pub fn call_sub_orchestrator_with_options(
        &self,
        name: &str,
        input: impl Serialize,
        options: SubOrchestratorOptions,
    ) -> BoxFuture<'static, crate::api::Result<Option<String>>> {
        let input_json = to_json(&input).ok().flatten();
        let name = name.to_string();
        let instance_id = options.instance_id.clone();
        let app_id = options.app_id.clone();
        let ctx = self.clone();

        match options.retry_policy {
            Some(policy) => {
                let first_attempt_time = ctx.current_utc_datetime();
                let schedule: Arc<dyn Fn(&OrchestrationContext) -> CompletableTask + Send + Sync> =
                    Arc::new(move |c: &OrchestrationContext| {
                        c.call_sub_orchestrator_raw(
                            &name,
                            input_json.clone(),
                            instance_id.as_deref(),
                            app_id.as_deref(),
                        )
                    });
                call_with_retry(ctx, schedule, policy, 0, first_attempt_time)
            }
            None => {
                let task = self.call_sub_orchestrator_raw(
                    &name,
                    input_json,
                    instance_id.as_deref(),
                    app_id.as_deref(),
                );
                Box::pin(task)
            }
        }
    }

    /// Create a durable timer that fires after the specified duration.
    pub fn create_timer(&self, delay: std::time::Duration) -> CompletableTask {
        tracing::debug!(delay_ms = delay.as_millis() as u64, "Creating timer");
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let seq = inner.sequence_number;
        inner.sequence_number += 1;

        if let Some(existing) = inner.pending_tasks.get(&seq) {
            if existing.is_complete() {
                return existing.clone();
            }
        }

        let task = CompletableTask::new();
        inner.pending_tasks.insert(seq, task.clone());

        let fire_at = inner.current_utc_datetime
            + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());
        let action = proto::WorkflowAction {
            id: seq,
            router: None,
            workflow_action_type: Some(proto::workflow_action::WorkflowActionType::CreateTimer(
                proto::CreateTimerAction {
                    fire_at: Some(to_timestamp(fire_at)),
                    name: None,
                    origin: None,
                },
            )),
        };
        inner.pending_actions.push(action);

        task
    }

    /// Wait for an external event with the given name.
    ///
    /// Event names are case-insensitive.
    pub fn wait_for_external_event(&self, name: &str) -> CompletableTask {
        tracing::debug!(event_name = %name, "Waiting for external event");
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let event_name = name.to_lowercase();

        if let Some(events) = inner.buffered_events.get_mut(&event_name) {
            if !events.is_empty() {
                let data = events.remove(0);
                let task = CompletableTask::new();
                task.complete(data);
                return task;
            }
        }

        let task = CompletableTask::new();
        let max_pending = inner.max_pending_tasks_per_name;
        let pending = inner.pending_event_tasks.entry(event_name).or_default();
        if pending.len() >= max_pending {
            tracing::warn!(event_name = %name, "Pending event task limit reached, discarding wait");
            return task;
        }
        pending.push(task.clone());
        task
    }

    /// Continue the orchestration as new with new input.
    pub fn continue_as_new(&self, input: impl Serialize, save_events: bool) {
        tracing::debug!(save_events = save_events, "Continuing orchestration as new");
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.continue_as_new_input = to_json(&input).ok().flatten();
        inner.save_events_on_continue = save_events;
    }

    /// Check whether a named patch should be applied in the current execution.
    ///
    /// This enables safe, deterministic code upgrades. Wrap new behaviour in
    /// `if ctx.is_patched("my-patch")` to ensure that:
    ///
    /// - Replaying executions that previously ran the *unpatched* path continue
    ///   on the unpatched path (preserving determinism).
    /// - Executions that previously ran the *patched* path continue on the
    ///   patched path.
    /// - Brand-new executions (at the history frontier) always take the patched
    ///   path.
    ///
    /// This matches the behaviour of the Go and Python SDKs.
    pub fn is_patched(&self, patch_name: &str) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // Return the cached decision from the current execution if available.
        if let Some(&cached) = inner.applied_patches.get(patch_name) {
            return cached;
        }

        // If this patch was recorded as applied in the history, honour it.
        if inner.history_patches.contains(patch_name) {
            inner.applied_patches.insert(patch_name.to_string(), true);
            return true;
        }

        // If the orchestrator hasn't yet consumed all scheduled actions from
        // history, this call is mid-replay.  The previous execution did NOT
        // apply this patch, so we must stay on the unpatched path to preserve
        // determinism.
        if inner.sequence_number < inner.history_scheduled_count {
            inner.applied_patches.insert(patch_name.to_string(), false);
            return false;
        }

        // We're at (or past) the history frontier — apply the patch.
        inner.applied_patches.insert(patch_name.to_string(), true);
        true
    }
}

// ── Retry helpers ─────────────────────────────────────────────────────────────

/// Compute the delay before the next retry attempt, or `None` if the retry
/// should not proceed (timeout exceeded or predicate returned false).
fn compute_retry_delay(
    policy: &RetryPolicy,
    attempt: u32,
    first_attempt_time: chrono::DateTime<chrono::Utc>,
    current_time: chrono::DateTime<chrono::Utc>,
    details: &FailureDetails,
) -> Option<std::time::Duration> {
    // Check custom predicate.
    if let Some(ref handle) = policy.handle {
        if !handle(details) {
            return None;
        }
    }

    // Check overall retry timeout.
    if let Some(timeout) = policy.retry_timeout {
        let elapsed = current_time - first_attempt_time;
        let timeout_dur = chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::zero());
        if elapsed >= timeout_dur {
            return None;
        }
    }

    // Exponential backoff.
    let first_ms = policy.first_retry_interval.as_millis() as f64;
    let next_ms = first_ms * policy.backoff_coefficient.powi(attempt as i32);

    let delay_ms = if let Some(max) = policy.max_retry_interval {
        next_ms.min(max.as_millis() as f64)
    } else {
        next_ms
    };

    Some(std::time::Duration::from_millis(delay_ms as u64))
}

/// Drive a task to completion, retrying on failure according to `policy`.
///
/// `schedule` is called once per attempt and must return a fresh [`CompletableTask`].
/// Between attempts a durable timer is created for the computed backoff delay,
/// preserving determinism across replays.
fn call_with_retry(
    ctx: OrchestrationContext,
    schedule: Arc<dyn Fn(&OrchestrationContext) -> CompletableTask + Send + Sync>,
    policy: RetryPolicy,
    attempt: u32,
    first_attempt_time: chrono::DateTime<chrono::Utc>,
) -> BoxFuture<'static, crate::api::Result<Option<String>>> {
    Box::pin(async move {
        let task = schedule(&ctx);
        match task.await {
            Ok(v) => Ok(v),
            Err(DurableTaskError::TaskFailed {
                message,
                failure_details,
            }) => {
                let details = failure_details.clone().unwrap_or_else(|| FailureDetails {
                    message: message.clone(),
                    error_type: "TaskFailed".to_string(),
                    stack_trace: None,
                });

                if attempt + 1 >= policy.max_number_of_attempts {
                    tracing::debug!(
                        attempt,
                        max = policy.max_number_of_attempts,
                        "Max retry attempts reached"
                    );
                    return Err(DurableTaskError::TaskFailed {
                        message,
                        failure_details,
                    });
                }

                let current_time = ctx.current_utc_datetime();
                let delay = match compute_retry_delay(
                    &policy,
                    attempt,
                    first_attempt_time,
                    current_time,
                    &details,
                ) {
                    Some(d) => d,
                    None => {
                        tracing::debug!(attempt, "Retry predicate or timeout prevented retry");
                        return Err(DurableTaskError::TaskFailed {
                            message,
                            failure_details,
                        });
                    }
                };

                tracing::debug!(
                    attempt,
                    delay_ms = delay.as_millis(),
                    "Scheduling retry timer"
                );
                ctx.create_timer(delay).await?;

                call_with_retry(ctx, schedule, policy, attempt + 1, first_attempt_time).await
            }
            Err(e) => Err(e),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx() -> OrchestrationContext {
        OrchestrationContext::new(
            "inst-1".to_string(),
            "my_orch".to_string(),
            Some("\"hello\"".to_string()),
            chrono::Utc::now(),
            false,
            &crate::worker::WorkerOptions::default(),
            0,
        )
    }

    #[test]
    fn test_basic_accessors() {
        let ctx = make_ctx();
        assert_eq!(ctx.instance_id(), "inst-1");
        assert_eq!(ctx.name(), "my_orch");
        assert!(!ctx.is_replaying());
    }

    #[test]
    fn test_get_input() {
        let ctx = make_ctx();
        let input: String = ctx.get_input().unwrap();
        assert_eq!(input, "hello");
    }

    #[test]
    fn test_set_custom_status() {
        let ctx = make_ctx();
        ctx.set_custom_status("processing");
        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.custom_status, Some("processing".to_string()));
    }

    #[test]
    fn test_call_activity_creates_action() {
        let ctx = make_ctx();
        let _task = ctx.call_activity("greet", "world");

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 1);
        assert_eq!(inner.pending_actions.len(), 1);
        assert_eq!(inner.pending_actions[0].id, 0);
        match &inner.pending_actions[0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::ScheduleTask(a)) => {
                assert_eq!(a.name, "greet");
                assert_eq!(a.input, Some("\"world\"".to_string()));
            }
            _ => panic!("expected ScheduleTask action"),
        }
    }

    #[test]
    fn test_call_activity_replay_returns_existing() {
        let ctx = make_ctx();

        // Pre-populate a completed task at sequence 0 (simulating replay)
        {
            let mut inner = ctx.inner.lock().unwrap();
            let task = CompletableTask::new();
            task.complete(Some("42".to_string()));
            inner.pending_tasks.insert(0, task);
        }

        let task = ctx.call_activity("greet", "world");
        assert!(task.is_complete());

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.pending_actions.len(), 0);
    }

    #[test]
    fn test_call_sub_orchestrator() {
        let ctx = make_ctx();
        let _task = ctx.call_sub_orchestrator("child_orch", "input", Some("child-1"));

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 1);
        match &inner.pending_actions[0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateChildWorkflow(a)) => {
                assert_eq!(a.name, "child_orch");
                assert_eq!(a.instance_id, "child-1");
            }
            _ => panic!("expected CreateChildWorkflow action"),
        }
    }

    #[test]
    fn test_create_timer() {
        let ctx = make_ctx();
        let _task = ctx.create_timer(std::time::Duration::from_secs(60));

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 1);
        match &inner.pending_actions[0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateTimer(a)) => {
                assert!(a.fire_at.is_some());
            }
            _ => panic!("expected CreateTimer action"),
        }
    }

    #[test]
    fn test_wait_for_external_event_buffered() {
        let ctx = make_ctx();

        // Buffer an event
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner
                .buffered_events
                .entry("approval".to_string())
                .or_default()
                .push(Some("\"yes\"".to_string()));
        }

        let task = ctx.wait_for_external_event("APPROVAL"); // case-insensitive
        assert!(task.is_complete());
    }

    #[test]
    fn test_wait_for_external_event_pending() {
        let ctx = make_ctx();
        let task = ctx.wait_for_external_event("approval");
        assert!(!task.is_complete());

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.pending_event_tasks.get("approval").unwrap().len(), 1);
    }

    #[test]
    fn test_continue_as_new() {
        let ctx = make_ctx();
        ctx.continue_as_new("new_input", true);

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(
            inner.continue_as_new_input,
            Some("\"new_input\"".to_string())
        );
        assert!(inner.save_events_on_continue);
    }

    #[test]
    fn test_sequence_numbers_increment() {
        let ctx = make_ctx();
        let _t1 = ctx.call_activity("a", ());
        let _t2 = ctx.call_activity("b", ());
        let _t3 = ctx.create_timer(std::time::Duration::from_secs(1));

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 3);
        assert_eq!(inner.pending_actions[0].id, 0);
        assert_eq!(inner.pending_actions[1].id, 1);
        assert_eq!(inner.pending_actions[2].id, 2);
    }

    #[test]
    fn test_call_sub_orchestrator_with_app_id() {
        let ctx = make_ctx();
        let _task = ctx.call_sub_orchestrator_with_app_id(
            "child_orch",
            "input",
            Some("child-1"),
            "other-app",
        );

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 1);
        match &inner.pending_actions[0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateChildWorkflow(a)) => {
                assert_eq!(a.name, "child_orch");
                assert_eq!(a.instance_id, "child-1");
                let router = a.router.as_ref().expect("expected router");
                assert_eq!(router.target_app_id, Some("other-app".to_string()));
            }
            _ => panic!("expected CreateChildWorkflow action"),
        }
    }

    #[test]
    fn test_is_patched_new_execution_returns_true() {
        // No history → always at the frontier → patch applies.
        let ctx = make_ctx();
        assert!(ctx.is_patched("my-patch"));
    }

    #[test]
    fn test_is_patched_in_history_returns_true() {
        // Patch recorded in history → return true.
        let ctx = make_ctx();
        ctx.inner
            .lock()
            .unwrap()
            .history_patches
            .insert("my-patch".to_string());
        assert!(ctx.is_patched("my-patch"));
    }

    #[test]
    fn test_is_patched_mid_replay_returns_false() {
        // history_scheduled_count = 2, but seq = 0 → mid-replay, unpatched.
        let ctx = make_ctx();
        ctx.inner.lock().unwrap().history_scheduled_count = 2;
        assert!(!ctx.is_patched("my-patch"));
    }

    #[test]
    fn test_is_patched_at_frontier_after_history_returns_true() {
        // history_scheduled_count = 1, seq = 1 → at frontier.
        let ctx = make_ctx();
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner.history_scheduled_count = 1;
            inner.sequence_number = 1;
        }
        assert!(ctx.is_patched("my-patch"));
    }

    #[test]
    fn test_is_patched_caches_decision() {
        let ctx = make_ctx();
        // First call caches the result.
        assert!(ctx.is_patched("my-patch"));
        // Second call uses the cache regardless of state changes.
        ctx.inner.lock().unwrap().history_scheduled_count = 99;
        assert!(ctx.is_patched("my-patch"));
    }
}
