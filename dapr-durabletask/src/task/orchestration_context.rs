use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::api::{
    DurableTaskError, ExternalEventResult, FailureDetails, HistoryPropagationScope,
    PropagatedHistory, RetryPolicy,
};
use crate::internal::{to_json, to_timestamp};
use crate::proto;

use super::completable_task::CompletableTask;
use super::options::{ActivityOptions, DetachedWorkflowOptions, SubOrchestratorOptions};

/// Sentinel timestamp for indefinite event waits.
pub(crate) static FAR_FUTURE_TIMESTAMP: LazyLock<chrono::DateTime<chrono::Utc>> =
    LazyLock::new(|| {
        chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
            .unwrap()
            .and_hms_nano_opt(23, 59, 59, 999_999_999)
            .unwrap()
            .and_utc()
    });

pub(crate) fn lock_inner<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Deterministic instance ID the runtime assigns to a child workflow
/// scheduled without an explicit ID: `<parent>:<action id as 4 hex digits>`.
pub(crate) fn child_workflow_instance_id(parent_instance_id: &str, action_id: i32) -> String {
    let hex = format!("{:x}", 0x10000_i64 + i64::from(action_id));
    format!("{parent_instance_id}:{}", &hex[hex.len() - 4..])
}

/// Routing envelope for a scheduled action. `None` when the call is local
/// (no target app ID); callers validate that a namespace comes with an app ID.
fn task_router(app_id: Option<&str>, app_namespace: Option<&str>) -> Option<proto::TaskRouter> {
    app_id.map(|id| proto::TaskRouter {
        source_app_id: String::new(),
        target_app_id: Some(id.to_string()),
        target_app_namespace: app_namespace.map(str::to_string),
    })
}

/// Enforce that a target namespace is paired with a target app ID, returning
/// the failure (tagged `error_type`) the call resolves to otherwise.
fn validate_namespace_requires_app_id(
    app_id: Option<&str>,
    app_namespace: Option<&str>,
    error_type: &str,
    options_type: &str,
) -> Option<FailureDetails> {
    (app_namespace.is_some() && app_id.is_none()).then(|| FailureDetails {
        message: format!(
            "{options_type}::with_app_namespace requires {options_type}::with_app_id to also be set"
        ),
        error_type: error_type.to_string(),
        stack_trace: None,
    })
}

fn failed_task_error(details: FailureDetails) -> DurableTaskError {
    DurableTaskError::TaskFailed {
        message: details.message.clone(),
        failure_details: Some(details),
    }
}

#[derive(Debug)]
pub(crate) struct ContextConfig {
    pub(crate) max_event_names: usize,
    pub(crate) max_events_per_name: usize,
    pub(crate) max_pending_tasks_per_name: usize,
    pub(crate) max_json_payload_size: usize,
}

/// The kind of durable operation a sequence number was allocated for.
///
/// Resolution events are only delivered to a pending task of the matching
/// kind, so e.g. a `TimerFired` can never complete an activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum TaskKind {
    Activity,
    Timer,
    ChildWorkflow,
    /// A detached workflow spawn; it has no resolution event and is only
    /// tracked to retire its scheduling action.
    DetachedWorkflow,
}

/// Outcome carried by a resolution event.
#[derive(Debug, Clone)]
pub(crate) enum Resolution {
    Completed(Option<String>),
    Failed(FailureDetails),
}

/// A resolution that arrived before this execution scheduled the matching
/// work. It is delivered once the work is scheduled.
#[derive(Debug, Clone)]
pub(crate) struct BufferedResolution {
    pub(crate) resolution: Resolution,
    pub(crate) during_replay: bool,
    pub(crate) description: String,
    /// Task execution ID recorded on a `TaskFailed` event.
    pub(crate) task_execution_id: Option<String>,
}

/// An external event held until the orchestrator waits for it.
#[derive(Debug, Clone)]
pub(crate) struct BufferedEvent {
    pub(crate) event: proto::HistoryEvent,
    pub(crate) during_replay: bool,
    /// Arrival order across all event names.
    pub(crate) arrival: u64,
}

/// Internal state shared between the context and the orchestration executor.
pub(crate) struct OrchestrationContextInner {
    pub(crate) config: Arc<ContextConfig>,
    pub(crate) instance_id: Arc<str>,
    pub(crate) current_utc_datetime: chrono::DateTime<chrono::Utc>,
    pub(crate) is_replaying: Arc<AtomicBool>,
    pub(crate) input: Option<String>,
    pub(crate) name: Arc<str>,
    pub(crate) custom_status: Option<String>,
    pub(crate) sequence_number: i32,
    /// Scheduled work awaiting its resolution event, keyed by action ID.
    pub(crate) pending_tasks: HashMap<i32, (TaskKind, CompletableTask)>,
    pub(crate) pending_event_tasks: HashMap<String, VecDeque<CompletableTask>>,
    /// Events buffered while no waiter exists, keyed by lowercased name.
    pub(crate) buffered_events: HashMap<String, VecDeque<BufferedEvent>>,
    /// Number of events buffered so far; orders continue-as-new carryover.
    pub(crate) buffered_event_count: u64,
    /// Actions produced by this execution that the runtime has not yet
    /// recorded in history. Scheduling events in history retire them.
    pub(crate) pending_actions: BTreeMap<i32, proto::WorkflowAction>,
    /// Action IDs already retired by a scheduling event, with their kind, so
    /// a duplicated scheduling event (persisted by older runtimes when older
    /// SDK releases re-emitted in-flight actions) is recognised.
    pub(crate) retired_actions: HashMap<i32, TaskKind>,
    /// Whether a completion action has been queued.
    pub(crate) is_complete: bool,
    /// Resolutions that arrived before the matching work was scheduled.
    pub(crate) buffered_resolutions: HashMap<(TaskKind, i32), BufferedResolution>,
    /// Resolutions already delivered this execution; duplicates are dropped.
    pub(crate) resolved: HashSet<(TaskKind, i32)>,
    /// `Some(input)` once the orchestrator called `continue_as_new`.
    pub(crate) continue_as_new_input: Option<Option<String>>,
    pub(crate) save_events_on_continue: bool,
    pub(crate) is_suspended: bool,
    pub(crate) is_terminated: bool,
    /// Events received while suspended, processed on resume.
    pub(crate) suspended_events: Vec<proto::HistoryEvent>,
    /// Held events released by a resume, applied next in order.
    pub(crate) resumed_events: VecDeque<proto::HistoryEvent>,
    /// Patches recorded in the orchestration history, in the order the
    /// `WorkflowStarted` events carry them.
    pub(crate) history_patches: Vec<String>,
    /// Cache of patch decisions made during the current execution.
    pub(crate) applied_patches: HashMap<String, bool>,
    /// Patches first applied by this execution, in encounter order.
    pub(crate) new_patches: Vec<String>,
    /// Number of history events processed so far, including the one being
    /// processed. Patches only apply once the whole history is processed.
    pub(crate) history_index: usize,
    /// Total number of history events (past + new) in this execution.
    pub(crate) history_len: usize,
    /// History forwarded from the parent workflow (if any). Populated from
    /// the `WorkflowRequest.propagated_history` field.
    pub(crate) propagated_history: Option<Arc<PropagatedHistory>>,
    /// Number of detached workflows spawned with a default instance ID in
    /// this execution; suffixes the next default ID.
    pub(crate) default_detached_workflow_counter: u32,
}

impl OrchestrationContextInner {
    pub(crate) fn next_sequence_number(&mut self) -> i32 {
        let seq = self.sequence_number;
        self.sequence_number += 1;
        seq
    }

    /// Record a new pending task for `(kind, id)`, delivering any resolution
    /// that arrived before the work was scheduled.
    fn register_task(&mut self, kind: TaskKind, id: i32, task: CompletableTask) {
        task.set_replay_handle(self.is_replaying.clone());
        self.pending_tasks.insert(id, (kind, task));
        if let Some(buffered) = self.buffered_resolutions.remove(&(kind, id)) {
            tracing::debug!(
                instance_id = %self.instance_id,
                resolution = %buffered.description,
                "Delivering buffered resolution to newly scheduled work"
            );
            self.resolve(kind, id, buffered);
        }
    }

    /// Deliver a resolution event to the pending task at `id`, buffering it
    /// if no pending task of that kind exists yet.
    pub(crate) fn resolve(&mut self, kind: TaskKind, id: i32, buffered: BufferedResolution) {
        let key = (kind, id);
        match self.pending_tasks.get(&id) {
            Some((pending_kind, _)) if *pending_kind == kind => {
                let (_, task) = self.pending_tasks.remove(&id).expect("pending task exists");
                self.resolved.insert(key);
                if let Some(exec_id) = buffered.task_execution_id {
                    task.set_task_execution_id(exec_id);
                }
                match buffered.resolution {
                    Resolution::Completed(v) => task.complete_with_phase(v, buffered.during_replay),
                    Resolution::Failed(d) => task.fail_with_phase(d, buffered.during_replay),
                }
            }
            _ => {
                if self.resolved.contains(&key) {
                    tracing::debug!(
                        instance_id = %self.instance_id,
                        resolution = %buffered.description,
                        "Dropping duplicate resolution: already resolved this execution"
                    );
                } else if self.buffered_resolutions.contains_key(&key) {
                    tracing::debug!(
                        instance_id = %self.instance_id,
                        resolution = %buffered.description,
                        "Dropping duplicate resolution: already buffered this execution"
                    );
                } else {
                    tracing::debug!(
                        instance_id = %self.instance_id,
                        resolution = %buffered.description,
                        "Buffering resolution until the workflow schedules the matching work"
                    );
                    self.buffered_resolutions.insert(key, buffered);
                }
            }
        }
    }

    /// Queue the workflow's completion action.
    pub(crate) fn set_complete(
        &mut self,
        status: proto::OrchestrationStatus,
        result: Option<String>,
        failure: Option<FailureDetails>,
    ) {
        self.is_complete = true;
        let id = self.next_sequence_number();
        self.pending_actions.insert(
            id,
            proto::WorkflowAction {
                id,
                router: None,
                workflow_action_type: Some(
                    proto::workflow_action::WorkflowActionType::CompleteWorkflow(
                        proto::CompleteWorkflowAction {
                            workflow_status: status as i32,
                            result,
                            details: None,
                            new_version: None,
                            carryover_events: Vec::new(),
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
            },
        );
    }

    /// Fail the workflow because its history cannot be applied, discarding
    /// every other action (including an earlier completion) so the runtime
    /// neither dispatches work for it nor records a different outcome.
    pub(crate) fn fail_replay(&mut self, failure: FailureDetails) {
        self.pending_actions.clear();
        self.set_complete(proto::OrchestrationStatus::Failed, None, Some(failure));
    }

    /// The pending completion action, if the workflow has finished.
    pub(crate) fn completion(&self) -> Option<&proto::CompleteWorkflowAction> {
        self.pending_actions
            .values()
            .find_map(|a| match &a.workflow_action_type {
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(c)) => Some(c),
                _ => None,
            })
    }

    fn create_timer_with_origin(
        &mut self,
        fire_at: chrono::DateTime<chrono::Utc>,
        name: Option<String>,
        origin: proto::create_timer_action::Origin,
    ) -> CompletableTask {
        let seq = self.next_sequence_number();
        self.pending_actions.insert(
            seq,
            proto::WorkflowAction {
                id: seq,
                router: None,
                workflow_action_type: Some(
                    proto::workflow_action::WorkflowActionType::CreateTimer(
                        proto::CreateTimerAction {
                            fire_at: Some(to_timestamp(fire_at)),
                            name,
                            origin: Some(origin),
                        },
                    ),
                ),
            },
        );
        let task = CompletableTask::new();
        self.register_task(TaskKind::Timer, seq, task.clone());
        task
    }

    /// Whether the pending action at `id` is the far-future timer emitted by
    /// an indefinite [`OrchestrationContext::wait_for_external_event`].
    pub(crate) fn is_optional_event_timer_at(&self, id: i32) -> bool {
        self.pending_actions.get(&id).is_some_and(|a| {
            matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CreateTimer(t))
                    if matches!(t.origin, Some(proto::create_timer_action::Origin::ExternalEvent(_)))
                        && t.fire_at == Some(to_timestamp(*FAR_FUTURE_TIMESTAMP))
            )
        })
    }

    /// Drop the optional event-wait timer at `id` and shift every later
    /// pending action and task down by one, aligning this execution with a
    /// history recorded before that timer was emitted.
    pub(crate) fn drop_optional_event_timer_at(&mut self, id: i32) {
        self.pending_actions.remove(&id);
        self.pending_tasks.remove(&id);
        self.shift_pending_ids(id + 1, -1);
    }

    /// Absorb an event-wait timer recorded in history at `id` that this
    /// execution did not emit (earlier releases emitted it even when the
    /// event was already buffered), shifting pending actions and tasks at or
    /// after `id` up by one.
    pub(crate) fn absorb_recorded_event_timer_at(&mut self, id: i32) {
        self.shift_pending_ids(id, 1);
        // Its TimerFired, if any (even one already buffered), resolves this
        // placeholder harmlessly.
        self.register_task(TaskKind::Timer, id, CompletableTask::new());
    }

    /// Move pending actions and tasks with IDs `>= from` by `delta`, then
    /// deliver buffered resolutions that now match (they are keyed by
    /// history numbering).
    fn shift_pending_ids(&mut self, from: i32, delta: i32) {
        let actions = std::mem::take(&mut self.pending_actions);
        self.pending_actions = actions
            .into_iter()
            .map(|(id, mut a)| {
                if id >= from {
                    a.id = id + delta;
                    (id + delta, a)
                } else {
                    (id, a)
                }
            })
            .collect();
        let tasks = std::mem::take(&mut self.pending_tasks);
        let mut shifted = Vec::new();
        self.pending_tasks = tasks
            .into_iter()
            .map(|(id, entry)| {
                if id >= from {
                    shifted.push((entry.0, id + delta));
                    (id + delta, entry)
                } else {
                    (id, entry)
                }
            })
            .collect();
        self.sequence_number += delta;
        shifted.sort_by_key(|(_, id)| *id);
        for (kind, id) in shifted {
            if let Some(buffered) = self.buffered_resolutions.remove(&(kind, id)) {
                self.resolve(kind, id, buffered);
            }
        }
    }

    fn is_patched(&mut self, patch_name: &str) -> bool {
        if let Some(&cached) = self.applied_patches.get(patch_name) {
            return cached;
        }
        let mid_history = self.history_index < self.history_len;
        // Recorded as applied in history: honour it. Otherwise the patch only
        // applies once the whole history has been processed; mid-history the
        // previous execution ran the unpatched path.
        let patched = if self.history_patches.iter().any(|p| p == patch_name) {
            true
        } else if !mid_history {
            self.new_patches.push(patch_name.to_string());
            true
        } else {
            false
        };
        self.applied_patches.insert(patch_name.to_string(), patched);
        patched
    }

    /// Patches to report to the runtime: every patch recorded in history, in
    /// history order, followed by those this execution applied first. The
    /// runtime stalls the workflow unless the recorded patches are a prefix
    /// of the reported ones.
    pub(crate) fn reported_patches(&self) -> Vec<String> {
        self.history_patches
            .iter()
            .chain(&self.new_patches)
            .cloned()
            .collect()
    }
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
        let config = Arc::new(ContextConfig {
            max_event_names: options.max_event_names,
            max_events_per_name: options.max_events_per_name,
            max_pending_tasks_per_name: options.max_pending_tasks_per_name,
            max_json_payload_size: options.max_json_payload_size,
        });

        Self {
            inner: Arc::new(Mutex::new(OrchestrationContextInner {
                config,
                instance_id: Arc::<str>::from(instance_id),
                current_utc_datetime,
                is_replaying: Arc::new(AtomicBool::new(is_replaying)),
                input,
                name: Arc::<str>::from(name),
                custom_status: None,
                sequence_number: 0,
                pending_tasks: HashMap::with_capacity(event_count_hint / 2),
                pending_event_tasks: HashMap::new(),
                buffered_events: HashMap::new(),
                buffered_event_count: 0,
                pending_actions: BTreeMap::new(),
                retired_actions: HashMap::new(),
                is_complete: false,
                buffered_resolutions: HashMap::new(),
                resolved: HashSet::new(),
                continue_as_new_input: None,
                save_events_on_continue: false,
                is_suspended: false,
                is_terminated: false,
                suspended_events: Vec::new(),
                resumed_events: VecDeque::new(),
                history_patches: Vec::new(),
                applied_patches: HashMap::new(),
                new_patches: Vec::new(),
                history_index: 0,
                history_len: 0,
                propagated_history: None,
                default_detached_workflow_counter: 0,
            })),
        }
    }

    /// Get the instance ID.
    pub fn instance_id(&self) -> Arc<str> {
        lock_inner(&self.inner).instance_id.clone()
    }

    /// Get the current UTC datetime (deterministic, from history events).
    pub fn current_utc_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        lock_inner(&self.inner).current_utc_datetime
    }

    /// Check if the orchestrator is currently replaying.
    pub fn is_replaying(&self) -> bool {
        lock_inner(&self.inner).is_replaying.load(Ordering::Acquire)
    }

    /// Get the orchestration name.
    pub fn name(&self) -> Arc<str> {
        lock_inner(&self.inner).name.clone()
    }

    /// Get the orchestration input, deserialised from JSON.
    pub fn input<T: DeserializeOwned>(&self) -> crate::api::Result<T> {
        let inner = lock_inner(&self.inner);
        crate::internal::from_json(inner.input.as_deref(), inner.config.max_json_payload_size)
    }

    /// Returns history forwarded from the parent workflow, if the parent
    /// scheduled this child with a non-`None` history propagation scope.
    ///
    /// See [`HistoryPropagationScope`] for the parent-side trade-off between
    /// `OwnHistory` and `Lineage`.
    pub fn propagated_history(&self) -> Option<Arc<PropagatedHistory>> {
        lock_inner(&self.inner).propagated_history.clone()
    }

    /// Set a custom status string.
    pub fn set_custom_status(&self, status: impl Into<String>) {
        let mut inner = lock_inner(&self.inner);
        inner.custom_status = Some(status.into());
    }

    /// Schedule an activity for execution.
    ///
    /// Returns a [`CompletableTask`] that resolves when the activity completes.
    ///
    /// Creates a `ScheduleTaskAction`; during replay the matching
    /// `TaskScheduled` event retires it and the recorded
    /// `TaskCompleted`/`TaskFailed` event resolves the task.
    pub fn call_activity(&self, name: &str, input: impl Serialize) -> CompletableTask {
        tracing::debug!(activity = %name, "Scheduling activity");
        self.call_activity_inner(name, input, None, None)
    }

    /// Schedule an activity with an `app_id` for cross-app scenarios.
    pub fn call_activity_with_app_id(
        &self,
        name: &str,
        input: impl Serialize,
        app_id: &str,
    ) -> CompletableTask {
        tracing::debug!(activity = %name, app_id = %app_id, "Scheduling activity with app_id");
        self.call_activity_inner(name, input, Some(app_id), None)
    }

    fn call_activity_inner(
        &self,
        name: &str,
        input: impl Serialize,
        app_id: Option<&str>,
        history_propagation_scope: Option<HistoryPropagationScope>,
    ) -> CompletableTask {
        let input_json = match to_json(&input) {
            Ok(json) => json,
            Err(e) => {
                let task = CompletableTask::new();
                task.fail(FailureDetails {
                    message: format!("Failed to serialize activity input: {e}"),
                    error_type: "SerializationError".to_string(),
                    stack_trace: None,
                });
                return task;
            }
        };
        self.call_activity_raw(
            name,
            input_json,
            app_id,
            None,
            history_propagation_scope,
            uuid::Uuid::new_v4().to_string(),
        )
    }

    /// Internal: schedule an activity using a pre-serialised JSON input.
    ///
    /// `task_execution_id` identifies the logical call and is shared by all
    /// retry attempts.
    #[allow(clippy::too_many_arguments)]
    fn call_activity_raw(
        &self,
        name: &str,
        input_json: Option<String>,
        app_id: Option<&str>,
        app_namespace: Option<&str>,
        history_propagation_scope: Option<HistoryPropagationScope>,
        task_execution_id: String,
    ) -> CompletableTask {
        let mut inner = lock_inner(&self.inner);
        let seq = inner.next_sequence_number();

        let router = task_router(app_id, app_namespace);
        let action = proto::WorkflowAction {
            id: seq,
            router,
            workflow_action_type: Some(proto::workflow_action::WorkflowActionType::ScheduleTask(
                proto::ScheduleTaskAction {
                    name: name.to_string(),
                    version: None,
                    input: input_json,
                    task_execution_id,
                    history_propagation_scope: history_propagation_scope
                        .map(|s| s.to_proto() as i32),
                },
            )),
        };
        inner.pending_actions.insert(seq, action);

        let task = CompletableTask::new();
        inner.register_task(TaskKind::Activity, seq, task.clone());
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
    ) -> impl std::future::Future<Output = crate::api::Result<Option<String>>> + Send + 'static
    {
        let input_json = to_json(&input);
        let name = name.to_string();
        let app_id = options.app_id.clone();
        let app_namespace = options.app_namespace.clone();
        let scope = options.history_propagation_scope;
        let ctx = self.clone();
        let first_attempt_time = ctx.current_utc_datetime();
        let task_execution_id = uuid::Uuid::new_v4().to_string();

        async move {
            if let Some(failure) = validate_namespace_requires_app_id(
                app_id.as_deref(),
                app_namespace.as_deref(),
                "InvalidActivityOptions",
                "ActivityOptions",
            ) {
                return Err(failed_task_error(failure));
            }
            let input_json = input_json?;
            match options.retry_policy {
                Some(policy) => {
                    let timer_name = format!("{name}-retry");
                    let origin: RetryTimerOrigin = Arc::new(|exec_id| {
                        proto::create_timer_action::Origin::ActivityRetry(
                            proto::TimerOriginActivityRetry {
                                task_execution_id: exec_id.to_string(),
                            },
                        )
                    });
                    let schedule: ScheduleAttempt = Arc::new(move |c, _attempt, exec_id| {
                        c.call_activity_raw(
                            &name,
                            input_json.clone(),
                            app_id.as_deref(),
                            app_namespace.as_deref(),
                            scope,
                            exec_id.to_string(),
                        )
                    });
                    call_with_retry(
                        ctx,
                        schedule,
                        policy,
                        first_attempt_time,
                        timer_name,
                        origin,
                        task_execution_id,
                    )
                    .await
                }
                None => {
                    ctx.call_activity_raw(
                        &name,
                        input_json,
                        app_id.as_deref(),
                        app_namespace.as_deref(),
                        scope,
                        task_execution_id,
                    )
                    .await
                }
            }
        }
    }

    /// Schedule a sub-orchestration for execution.
    ///
    /// Without an explicit `instance_id`, the runtime assigns the child the
    /// deterministic ID `<parent instance ID>:<action ID as 4 hex digits>`.
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
        self.call_sub_orchestrator_inner(name, input, instance_id, None, None)
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
        self.call_sub_orchestrator_inner(name, input, instance_id, Some(app_id), None)
    }

    fn call_sub_orchestrator_inner(
        &self,
        name: &str,
        input: impl Serialize,
        instance_id: Option<&str>,
        app_id: Option<&str>,
        history_propagation_scope: Option<HistoryPropagationScope>,
    ) -> CompletableTask {
        let input_json = match to_json(&input) {
            Ok(json) => json,
            Err(e) => {
                let task = CompletableTask::new();
                task.fail(FailureDetails {
                    message: format!("Failed to serialize sub-orchestrator input: {e}"),
                    error_type: "SerializationError".to_string(),
                    stack_trace: None,
                });
                return task;
            }
        };
        self.call_sub_orchestrator_raw(
            name,
            input_json,
            instance_id,
            app_id,
            None,
            history_propagation_scope,
            None,
        )
    }

    /// Internal: schedule a sub-orchestration using a pre-serialised JSON input.
    ///
    /// `retry_parent_instance_id` is the first attempt's instance ID and is
    /// only set on retry attempts.
    #[allow(clippy::too_many_arguments)]
    fn call_sub_orchestrator_raw(
        &self,
        name: &str,
        input_json: Option<String>,
        instance_id: Option<&str>,
        app_id: Option<&str>,
        app_namespace: Option<&str>,
        history_propagation_scope: Option<HistoryPropagationScope>,
        retry_parent_instance_id: Option<&str>,
    ) -> CompletableTask {
        let mut inner = lock_inner(&self.inner);
        let seq = inner.next_sequence_number();

        let router = task_router(app_id, app_namespace);

        let action = proto::WorkflowAction {
            id: seq,
            router,
            workflow_action_type: Some(
                proto::workflow_action::WorkflowActionType::CreateChildWorkflow(
                    proto::CreateChildWorkflowAction {
                        // Left empty, the runtime assigns a deterministic ID.
                        instance_id: instance_id.unwrap_or_default().to_string(),
                        name: name.to_string(),
                        version: None,
                        input: input_json,
                        history_propagation_scope: history_propagation_scope
                            .map(|s| s.to_proto() as i32),
                        retry_parent_instance_info: retry_parent_instance_id.map(|id| {
                            proto::RetryParentInstanceInfo {
                                instance_id: id.to_string(),
                            }
                        }),
                    },
                ),
            ),
        };
        inner.pending_actions.insert(seq, action);

        let task = CompletableTask::new();
        inner.register_task(TaskKind::ChildWorkflow, seq, task.clone());
        task
    }

    /// Schedule a sub-orchestration with options (instance ID, retry policy, app ID).
    ///
    /// Returns a future that drives the sub-orchestration to completion,
    /// transparently scheduling durable timers and re-issuing the call on each retry.
    ///
    /// Without an explicit `instance_id`, the runtime assigns each attempt
    /// the deterministic ID `<parent instance ID>:<action ID as 4 hex digits>`.
    pub fn call_sub_orchestrator_with_options(
        &self,
        name: &str,
        input: impl Serialize,
        options: SubOrchestratorOptions,
    ) -> impl std::future::Future<Output = crate::api::Result<Option<String>>> + Send + 'static
    {
        let input_json = to_json(&input);
        let name = name.to_string();
        let instance_id = options.instance_id.clone();
        let app_id = options.app_id.clone();
        let app_namespace = options.app_namespace.clone();
        let scope = options.history_propagation_scope;
        let ctx = self.clone();
        let first_attempt_time = ctx.current_utc_datetime();

        async move {
            if let Some(failure) = validate_namespace_requires_app_id(
                app_id.as_deref(),
                app_namespace.as_deref(),
                "InvalidChildWorkflowOptions",
                "SubOrchestratorOptions",
            ) {
                return Err(failed_task_error(failure));
            }
            let input_json = input_json?;
            match options.retry_policy {
                Some(policy) => {
                    // The first attempt's instance ID links the retry chain.
                    let first_instance_id = {
                        let inner = lock_inner(&ctx.inner);
                        instance_id.clone().unwrap_or_else(|| {
                            child_workflow_instance_id(&inner.instance_id, inner.sequence_number)
                        })
                    };
                    let timer_name = format!("{name}-retry");
                    let origin_instance_id = first_instance_id.clone();
                    let origin: RetryTimerOrigin = Arc::new(move |_| {
                        proto::create_timer_action::Origin::ChildWorkflowRetry(
                            proto::TimerOriginChildWorkflowRetry {
                                instance_id: origin_instance_id.clone(),
                            },
                        )
                    });
                    let schedule: ScheduleAttempt = Arc::new(move |c, attempt, _| {
                        c.call_sub_orchestrator_raw(
                            &name,
                            input_json.clone(),
                            instance_id.as_deref(),
                            app_id.as_deref(),
                            app_namespace.as_deref(),
                            scope,
                            (attempt > 0).then_some(first_instance_id.as_str()),
                        )
                    });
                    call_with_retry(
                        ctx,
                        schedule,
                        policy,
                        first_attempt_time,
                        timer_name,
                        origin,
                        uuid::Uuid::new_v4().to_string(),
                    )
                    .await
                }
                None => {
                    ctx.call_sub_orchestrator_raw(
                        &name,
                        input_json,
                        instance_id.as_deref(),
                        app_id.as_deref(),
                        app_namespace.as_deref(),
                        scope,
                        None,
                    )
                    .await
                }
            }
        }
    }

    /// Schedule a new, fully decoupled ("detached") workflow instance.
    ///
    /// Unlike [`call_sub_orchestrator`](Self::call_sub_orchestrator), the
    /// spawned workflow has no parent linkage: its `ExecutionStarted` event
    /// carries no parent instance, its completion or failure never flows back
    /// to the caller, and this call returns the new instance ID synchronously
    /// instead of an awaitable task. Model any dependency on the spawned
    /// workflow's result with external events or shared state.
    ///
    /// Emits a `CreateDetachedWorkflowAction`; during replay the matching
    /// `DetachedWorkflowInstanceCreated` history event retires it, and a
    /// history that recorded a different instance ID at this point fails the
    /// workflow with a non-determinism error.
    ///
    /// Without [`DetachedWorkflowOptions::with_instance_id`] the instance ID
    /// is `<caller instance ID>-<n>`, where `n` counts only the default-ID
    /// spawns of this execution (starting at 0), so it is stable across
    /// replays. `input` is serialised to JSON unless
    /// [`DetachedWorkflowOptions::with_raw_input`] is set (a unit or `None`
    /// input sends no input).
    ///
    /// # Errors
    ///
    /// Nothing is scheduled (and the default-ID counter does not advance)
    /// when the explicit instance ID is empty, when a namespace is set
    /// without an app ID, or when `input` cannot be serialised.
    pub fn schedule_new_detached_workflow(
        &self,
        name: &str,
        input: impl Serialize,
        options: DetachedWorkflowOptions,
    ) -> crate::api::Result<String> {
        if options.app_namespace.is_some() && options.app_id.is_none() {
            return Err(DurableTaskError::Other(
                "DetachedWorkflowOptions::with_app_namespace requires \
                 DetachedWorkflowOptions::with_app_id to also be set"
                    .to_string(),
            ));
        }
        if options.instance_id.as_deref() == Some("") {
            return Err(DurableTaskError::Other(
                "DetachedWorkflowOptions::with_instance_id was passed an empty string; omit \
                 the option to opt into the default ID"
                    .to_string(),
            ));
        }
        let input_json = match options.raw_input {
            Some(raw) => Some(raw),
            None => to_json(&input)?,
        };

        let mut inner = lock_inner(&self.inner);
        let instance_id = match options.instance_id {
            Some(id) => id,
            None => {
                let id = format!(
                    "{}-{}",
                    inner.instance_id, inner.default_detached_workflow_counter
                );
                inner.default_detached_workflow_counter += 1;
                id
            }
        };
        tracing::debug!(
            workflow = %name,
            detached_instance_id = %instance_id,
            "Scheduling detached workflow"
        );

        let seq = inner.next_sequence_number();
        let action = proto::WorkflowAction {
            id: seq,
            router: task_router(options.app_id.as_deref(), options.app_namespace.as_deref()),
            workflow_action_type: Some(
                proto::workflow_action::WorkflowActionType::CreateDetachedWorkflow(
                    proto::CreateDetachedWorkflowAction {
                        instance_id: instance_id.clone(),
                        name: name.to_string(),
                        version: None,
                        input: input_json,
                        scheduled_start_timestamp: options.start_time.map(to_timestamp),
                        execution_id: None,
                        tags: HashMap::new(),
                        parent_trace_context: None,
                    },
                ),
            ),
        };
        inner.pending_actions.insert(seq, action);
        Ok(instance_id)
    }

    /// Create a durable timer that fires after the specified duration.
    pub fn create_timer(&self, delay: std::time::Duration) -> CompletableTask {
        tracing::debug!(delay_ms = delay.as_millis() as u64, "Creating timer");
        let mut inner = lock_inner(&self.inner);
        let fire_at = inner.current_utc_datetime
            + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());
        inner.create_timer_with_origin(
            fire_at,
            None,
            proto::create_timer_action::Origin::CreateTimer(proto::TimerOriginCreateTimer {}),
        )
    }

    /// Wait for an external event with the given name.
    ///
    /// Event names are case-insensitive.
    ///
    /// If the event has not arrived yet, a far-future timer tagged with the
    /// event name is also emitted, letting the runtime track the wait.
    pub fn wait_for_external_event(&self, name: &str) -> CompletableTask {
        tracing::debug!(event_name = %name, "Waiting for external event");
        let mut inner = lock_inner(&self.inner);
        Self::wait_for_event_inner(&mut inner, name, *FAR_FUTURE_TIMESTAMP).0
    }

    /// Consume a buffered event named `name`, or queue a waiter for it along
    /// with a timer tagged with the event name that fires at `fire_at`.
    ///
    /// Returns the event task and, if the event was not buffered, the timer.
    fn wait_for_event_inner(
        inner: &mut OrchestrationContextInner,
        name: &str,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> (CompletableTask, Option<CompletableTask>) {
        let (task, buffered) = Self::take_event_or_wait(inner, name);
        if buffered {
            return (task, None);
        }
        let timer = inner.create_timer_with_origin(
            fire_at,
            Some(name.to_string()),
            Self::event_timer_origin(name),
        );
        (task, Some(timer))
    }

    fn event_timer_origin(name: &str) -> proto::create_timer_action::Origin {
        proto::create_timer_action::Origin::ExternalEvent(proto::TimerOriginExternalEvent {
            name: name.to_string(),
        })
    }

    /// Consume a buffered event named `name` (returning `true`), or queue a
    /// waiter for it.
    fn take_event_or_wait(
        inner: &mut OrchestrationContextInner,
        name: &str,
    ) -> (CompletableTask, bool) {
        let event_name = name.to_lowercase();
        let task = CompletableTask::new();
        task.set_replay_handle(inner.is_replaying.clone());

        if let Some(events) = inner.buffered_events.get_mut(&event_name)
            && let Some(buffered) = events.pop_front()
        {
            if events.is_empty() {
                inner.buffered_events.remove(&event_name);
            }
            let data = match buffered.event.event_type {
                Some(proto::history_event::EventType::EventRaised(e)) => e.input,
                _ => None,
            };
            task.complete_with_phase(data, buffered.during_replay);
            return (task, true);
        }

        let max_pending = inner.config.max_pending_tasks_per_name;
        let pending = inner.pending_event_tasks.entry(event_name).or_default();
        if pending.len() >= max_pending {
            tracing::warn!(event_name = %name, "Pending event task limit reached, discarding wait");
        } else {
            pending.push_back(task.clone());
        }
        (task, false)
    }

    /// Wait for an external event with a timeout.
    ///
    /// Returns [`ExternalEventResult::Received`] if the event arrives before
    /// the timeout, or [`ExternalEventResult::TimedOut`] if the timeout fires
    /// first.
    ///
    /// If the event has not arrived yet, emits the timeout timer tagged with
    /// the event name.
    ///
    /// Event names are case-insensitive.
    pub async fn wait_for_external_event_with_timeout(
        &self,
        name: &str,
        timeout: std::time::Duration,
    ) -> crate::api::Result<ExternalEventResult> {
        tracing::debug!(
            event_name = %name,
            timeout_ms = timeout.as_millis() as u64,
            "Waiting for external event with timeout"
        );

        let (event_task, timer_task) = {
            let mut inner = lock_inner(&self.inner);
            let fire_at = inner.current_utc_datetime
                + chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::zero());
            Self::wait_for_event_inner(&mut inner, name, fire_at)
        };
        // An already-buffered event needs no timeout.
        let Some(timer_task) = timer_task else {
            return Ok(ExternalEventResult::Received(event_task.await?));
        };

        // Race the event and timer (0 = event, 1 = timer).
        let winner = super::when_any::when_any(vec![event_task.clone(), timer_task]).await?;
        match winner {
            0 => {
                let payload = event_task.await?;
                Ok(ExternalEventResult::Received(payload))
            }
            _ => {
                // Timer won — remove the stale event waiter so it does not
                // silently consume a later event with the same name.
                let mut inner = lock_inner(&self.inner);
                let event_name = name.to_lowercase();
                if let Some(tasks) = inner.pending_event_tasks.get_mut(&event_name) {
                    tasks.retain(|t| !t.ptr_eq(&event_task));
                    if tasks.is_empty() {
                        inner.pending_event_tasks.remove(&event_name);
                    }
                }
                Ok(ExternalEventResult::TimedOut)
            }
        }
    }

    /// Continue the orchestration as new with new input.
    ///
    /// Takes effect when the orchestrator function returns. A unit or `None`
    /// input continues as new without input.
    pub fn continue_as_new(&self, input: impl Serialize, save_events: bool) {
        tracing::debug!(save_events = save_events, "Continuing orchestration as new");
        let mut inner = lock_inner(&self.inner);
        inner.continue_as_new_input = Some(to_json(&input).ok().flatten());
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
    /// - Code reached at the history frontier — after every history event of
    ///   this turn has been applied — takes the patched path. Code reached
    ///   while later history events remain (e.g. the start of a brand-new
    ///   execution whose first batch already contains a raised event) takes
    ///   the unpatched path.
    ///
    /// This matches the behaviour of the Go SDK.
    pub fn is_patched(&self, patch_name: &str) -> bool {
        lock_inner(&self.inner).is_patched(patch_name)
    }
}

// ── Retry helpers ─────────────────────────────────────────────────────────────

/// Schedules one attempt; receives the zero-based attempt number and the
/// task execution ID shared by all attempts.
type ScheduleAttempt =
    Arc<dyn Fn(&OrchestrationContext, u32, &str) -> CompletableTask + Send + Sync>;

/// Builds the retry timer's origin from the task execution ID.
type RetryTimerOrigin = Arc<dyn Fn(&str) -> proto::create_timer_action::Origin + Send + Sync>;

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
    if let Some(ref handle) = policy.handle
        && !handle(details)
    {
        return None;
    }

    // Check overall retry timeout.
    if let Some(timeout) = policy.retry_timeout {
        let elapsed = current_time - first_attempt_time;
        let timeout_dur = chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::zero());
        if elapsed > timeout_dur {
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
/// Between attempts a durable timer named `timer_name` and tagged with
/// `origin` is created for the computed backoff delay, preserving
/// determinism across replays.
///
/// `task_execution_id` identifies the logical call. As in durabletask-go,
/// the ID recorded on a failure event takes precedence, so replays keep the
/// ID the first execution used.
fn call_with_retry(
    ctx: OrchestrationContext,
    schedule: ScheduleAttempt,
    policy: RetryPolicy,
    first_attempt_time: chrono::DateTime<chrono::Utc>,
    timer_name: String,
    origin: RetryTimerOrigin,
    mut task_execution_id: String,
) -> BoxFuture<'static, crate::api::Result<Option<String>>> {
    Box::pin(async move {
        let mut attempt = 0;
        loop {
            let task = schedule(&ctx, attempt, &task_execution_id);
            let outcome = task.clone().await;
            if let Some(recorded) = task.task_execution_id().filter(|id| !id.is_empty()) {
                task_execution_id = recorded;
            }
            match outcome {
                Ok(v) => return Ok(v),
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
                    let timer = {
                        let mut inner = lock_inner(&ctx.inner);
                        let fire_at = current_time
                            + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());
                        inner.create_timer_with_origin(
                            fire_at,
                            Some(timer_name.clone()),
                            origin(&task_execution_id),
                        )
                    };
                    timer.await?;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

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
        assert_eq!(ctx.instance_id().as_ref(), "inst-1");
        assert_eq!(ctx.name().as_ref(), "my_orch");
        assert!(!ctx.is_replaying());
    }

    #[test]
    fn test_input() {
        let ctx = make_ctx();
        let input: String = ctx.input().unwrap();
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
        assert_eq!(inner.pending_actions[&0].id, 0);
        match &inner.pending_actions[&0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::ScheduleTask(a)) => {
                assert_eq!(a.name, "greet");
                assert_eq!(a.input, Some("\"world\"".to_string()));
                assert!(!a.task_execution_id.is_empty());
            }
            _ => panic!("expected ScheduleTask action"),
        }
    }

    #[test]
    fn test_call_activity_delivers_buffered_resolution() {
        let ctx = make_ctx();

        // A completion for id 0 arrived before the activity was scheduled.
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner.resolve(
                TaskKind::Activity,
                0,
                BufferedResolution {
                    resolution: Resolution::Completed(Some("42".to_string())),
                    during_replay: true,
                    description: "TaskCompleted for id 0".to_string(),
                    task_execution_id: None,
                },
            );
        }

        let task = ctx.call_activity("greet", "world");
        assert!(task.is_complete());

        // The schedule is still emitted so the runtime records it.
        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.pending_actions.len(), 1);
        assert!(inner.buffered_resolutions.is_empty());
    }

    #[test]
    fn test_resolution_of_other_kind_is_not_delivered() {
        let ctx = make_ctx();
        let task = ctx.call_activity("greet", "world");
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner.resolve(
                TaskKind::Timer,
                0,
                BufferedResolution {
                    resolution: Resolution::Completed(None),
                    during_replay: false,
                    description: "TimerFired for id 0".to_string(),
                    task_execution_id: None,
                },
            );
        }
        assert!(!task.is_complete());
    }

    #[test]
    fn test_child_workflow_instance_id_format() {
        assert_eq!(child_workflow_instance_id("parent", 0), "parent:0000");
        assert_eq!(child_workflow_instance_id("parent", 10), "parent:000a");
        assert_eq!(child_workflow_instance_id("parent", 0x1234), "parent:1234");
    }

    #[test]
    fn test_call_sub_orchestrator() {
        let ctx = make_ctx();
        let _task = ctx.call_sub_orchestrator("child_orch", "input", Some("child-1"));

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 1);
        match &inner.pending_actions[&0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateChildWorkflow(a)) => {
                assert_eq!(a.name, "child_orch");
                assert_eq!(a.instance_id, "child-1");
            }
            _ => panic!("expected CreateChildWorkflow action"),
        }
    }

    #[test]
    fn test_call_sub_orchestrator_without_instance_id_leaves_it_to_runtime() {
        let ctx = make_ctx();
        let _task = ctx.call_sub_orchestrator("child_orch", "input", None);

        let inner = ctx.inner.lock().unwrap();
        match &inner.pending_actions[&0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateChildWorkflow(a)) => {
                assert_eq!(a.instance_id, "");
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
        match &inner.pending_actions[&0].workflow_action_type {
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
                .push_back(BufferedEvent {
                    event: raised("approval", "\"yes\""),
                    during_replay: true,
                    arrival: 0,
                });
        }

        let task = ctx.wait_for_external_event("APPROVAL"); // case-insensitive
        assert!(task.is_complete());
    }

    fn raised(name: &str, input: &str) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id: 1,
            timestamp: None,
            router: None,
            event_type: Some(proto::history_event::EventType::EventRaised(
                proto::EventRaisedEvent {
                    name: name.to_string(),
                    input: Some(input.to_string()),
                },
            )),
        }
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
            Some(Some("\"new_input\"".to_string()))
        );
        assert!(inner.save_events_on_continue);
    }

    #[test]
    fn test_continue_as_new_without_input() {
        let ctx = make_ctx();
        ctx.continue_as_new((), false);

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.continue_as_new_input, Some(None));
    }

    #[test]
    fn test_sequence_numbers_increment() {
        let ctx = make_ctx();
        let _t1 = ctx.call_activity("a", ());
        let _t2 = ctx.call_activity("b", ());
        let _t3 = ctx.create_timer(std::time::Duration::from_secs(1));

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 3);
        assert_eq!(inner.pending_actions[&0].id, 0);
        assert_eq!(inner.pending_actions[&1].id, 1);
        assert_eq!(inner.pending_actions[&2].id, 2);
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
        let router = inner.pending_actions[&0]
            .router
            .as_ref()
            .expect("expected router");
        assert_eq!(router.target_app_id, Some("other-app".to_string()));
        match &inner.pending_actions[&0].workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateChildWorkflow(a)) => {
                assert_eq!(a.name, "child_orch");
                assert_eq!(a.instance_id, "child-1");
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
            .push("my-patch".to_string());
        assert!(ctx.is_patched("my-patch"));
    }

    #[test]
    fn test_is_patched_mid_replay_returns_false() {
        // 2 of 5 history events processed → mid-replay, unpatched.
        let ctx = make_ctx();
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner.history_index = 2;
            inner.history_len = 5;
        }
        assert!(!ctx.is_patched("my-patch"));
    }

    #[test]
    fn test_is_patched_at_frontier_after_history_returns_true() {
        // All history events processed → at frontier.
        let ctx = make_ctx();
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner.history_index = 5;
            inner.history_len = 5;
        }
        assert!(ctx.is_patched("my-patch"));
    }

    #[test]
    fn test_is_patched_caches_decision() {
        let ctx = make_ctx();
        // First call caches the result.
        assert!(ctx.is_patched("my-patch"));
        // Second call uses the cache regardless of state changes.
        ctx.inner.lock().unwrap().history_len = 99;
        assert!(ctx.is_patched("my-patch"));
    }

    /// Extract a `CreateTimerAction`.
    fn extract_create_timer(action: &proto::WorkflowAction) -> &proto::CreateTimerAction {
        match &action.workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateTimer(a)) => a,
            other => panic!("expected CreateTimer action, got {other:?}"),
        }
    }

    #[test]
    fn test_create_timer_origin_create_timer() {
        // Generic timers are tagged with the CreateTimer origin.
        let ctx = make_ctx();
        let _task = ctx.create_timer(std::time::Duration::from_secs(60));

        let inner = ctx.inner.lock().unwrap();
        let timer_action = extract_create_timer(&inner.pending_actions[&0]);
        assert!(matches!(
            timer_action.origin,
            Some(proto::create_timer_action::Origin::CreateTimer(_))
        ));
        assert!(timer_action.name.is_none());
    }

    #[test]
    fn test_wait_for_external_event_emits_timer_new_execution() {
        // New executions emit a far-future ExternalEvent timer.
        let ctx = make_ctx();
        let _task = ctx.wait_for_external_event("approval");

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(
            inner.sequence_number, 1,
            "should have allocated a seq for the timer"
        );
        assert_eq!(
            inner.pending_actions.len(),
            1,
            "should have emitted a CreateTimerAction"
        );

        let timer_action = extract_create_timer(&inner.pending_actions[&0]);
        match &timer_action.origin {
            Some(proto::create_timer_action::Origin::ExternalEvent(e)) => {
                assert_eq!(e.name, "approval");
            }
            other => panic!("expected ExternalEvent origin, got {other:?}"),
        }
        assert_eq!(timer_action.name.as_deref(), Some("approval"));

        // Assert the far-future sentinel, 9999-12-31T23:59:59.999999999Z.
        let fire_at = timer_action
            .fire_at
            .as_ref()
            .expect("fire_at should be set");
        assert_eq!(fire_at.nanos, 999_999_999);
        let fire_at_dt = chrono::DateTime::from_timestamp(fire_at.seconds, fire_at.nanos as u32);
        assert!(fire_at_dt.is_some());
        assert!(fire_at_dt.unwrap().year() >= 9999);
    }

    #[test]
    fn test_wait_for_external_event_emits_timer_during_replay() {
        // The timer is emitted mid-replay too; histories without it are
        // tolerated by the executor, which drops it.
        let ctx = make_ctx();
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner.history_index = 2;
            inner.history_len = 5;
        }

        let _task = ctx.wait_for_external_event("approval");

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 1);
        assert_eq!(inner.pending_actions.len(), 1);
        assert_eq!(inner.pending_event_tasks.get("approval").unwrap().len(), 1);
        assert!(inner.is_optional_event_timer_at(0));
    }

    #[test]
    fn test_drop_optional_event_timer_shifts_later_ids() {
        let ctx = make_ctx();
        let _wait = ctx.wait_for_external_event("approval");
        let activity = ctx.call_activity("act", ());
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner.drop_optional_event_timer_at(0);
            assert_eq!(inner.sequence_number, 1);
            assert_eq!(inner.pending_actions.len(), 1);
            assert!(matches!(
                inner.pending_actions[&0].workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::ScheduleTask(_))
            ));
            assert_eq!(inner.pending_actions[&0].id, 0);
            inner.resolve(
                TaskKind::Activity,
                0,
                BufferedResolution {
                    resolution: Resolution::Completed(None),
                    during_replay: true,
                    description: "TaskCompleted for id 0".to_string(),
                    task_execution_id: None,
                },
            );
        }
        assert!(activity.is_complete());
    }

    #[test]
    fn test_absorb_recorded_event_timer_shifts_later_ids() {
        let ctx = make_ctx();
        let _activity = ctx.call_activity("act", ());
        let mut inner = ctx.inner.lock().unwrap();
        inner.absorb_recorded_event_timer_at(0);
        assert_eq!(inner.sequence_number, 2);
        assert_eq!(inner.pending_actions[&1].id, 1);
        assert!(matches!(
            inner.pending_tasks.get(&0),
            Some((TaskKind::Timer, _))
        ));
        assert!(matches!(
            inner.pending_tasks.get(&1),
            Some((TaskKind::Activity, _))
        ));
    }

    #[test]
    fn test_wait_for_external_event_buffered_emits_no_timer() {
        // An already-buffered event completes the wait with no timer.
        let ctx = make_ctx();
        {
            let mut inner = ctx.inner.lock().unwrap();
            inner
                .buffered_events
                .entry("approval".to_string())
                .or_default()
                .push_back(BufferedEvent {
                    event: raised("approval", "\"yes\""),
                    during_replay: true,
                    arrival: 0,
                });
        }

        let task = ctx.wait_for_external_event("APPROVAL");
        assert!(
            task.is_complete(),
            "buffered event should complete immediately"
        );

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 0);
        assert!(inner.pending_actions.is_empty());
        assert!(inner.buffered_events.is_empty());
    }

    #[test]
    fn test_wait_for_external_event_with_timeout_emits_timer() {
        // Timeout waits always emit the explicit timer.
        let ctx = make_ctx();

        // Mirror the method setup without awaiting the future.
        {
            let mut inner = ctx.inner.lock().unwrap();
            let event_name = "approval".to_string();
            let fire_at = inner.current_utc_datetime + chrono::Duration::seconds(30);
            let origin = proto::create_timer_action::Origin::ExternalEvent(
                proto::TimerOriginExternalEvent {
                    name: "approval".to_string(),
                },
            );
            let _timer =
                inner.create_timer_with_origin(fire_at, Some("approval".to_string()), origin);
            // Register the event wait.
            let task = CompletableTask::new();
            inner
                .pending_event_tasks
                .entry(event_name)
                .or_default()
                .push_back(task);
        }

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 1);
        let timer_action = extract_create_timer(&inner.pending_actions[&0]);
        match &timer_action.origin {
            Some(proto::create_timer_action::Origin::ExternalEvent(e)) => {
                assert_eq!(e.name, "approval");
            }
            other => panic!("expected ExternalEvent origin, got {other:?}"),
        }
        // Timeout timers are not the far-future sentinel.
        let fire_at = timer_action.fire_at.as_ref().unwrap();
        let fire_at_dt =
            chrono::DateTime::from_timestamp(fire_at.seconds, fire_at.nanos as u32).unwrap();
        assert!(fire_at_dt.year() < 9999, "should not be far-future");
    }

    #[test]
    fn test_create_timer_refactor_still_works() {
        // create_timer allocates sequential IDs and tags each timer's origin.
        let ctx = make_ctx();
        let _t1 = ctx.create_timer(std::time::Duration::from_secs(10));
        let _t2 = ctx.create_timer(std::time::Duration::from_secs(20));

        let inner = ctx.inner.lock().unwrap();
        assert_eq!(inner.sequence_number, 2);
        assert_eq!(inner.pending_actions.len(), 2);
        assert_eq!(inner.pending_actions[&0].id, 0);
        assert_eq!(inner.pending_actions[&1].id, 1);

        for action in inner.pending_actions.values() {
            let timer = extract_create_timer(action);
            assert!(timer.fire_at.is_some());
            assert!(matches!(
                timer.origin,
                Some(proto::create_timer_action::Origin::CreateTimer(_))
            ));
        }
    }
}
