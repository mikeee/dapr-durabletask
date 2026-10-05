# Changelog

All notable changes to `dapr-durabletask` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). While the
crate is `0.0.x`, any release may contain breaking changes; they are listed
first under **⚠️ Breaking changes** in each release.

## [0.0.4]

### ⚠️ Breaking changes

- **New replay model; drain in-flight workflows before upgrading.** The
  orchestrator is now resumed after each history event instead of once per
  turn (see Added). Workflows started on 0.0.3 replay with the new semantics:
  most continue normally, but those whose history replays differently (for
  example concurrent branches that schedule follow-up work, an event and its
  timeout arriving in the same batch, or retry decisions made with the old
  clock) fail with `NonDeterminismError`. Let in-flight workflows finish, or
  purge them, before upgrading, and do not run 0.0.3 and 0.0.4 workers side
  by side or roll back once 0.0.4 has run a workflow.
- **Child workflows scheduled without an explicit instance ID** get the
  runtime-assigned ID `<parent instance ID>:<action ID as 4 hex digits>`
  instead of a random UUID (as in durabletask-go). These IDs repeat in each
  continue-as-new generation; pass an explicit ID if a previous generation's
  child may still be running.
- **`PropagatedHistory::workflow_by_name`** returns the last matching chunk
  (the nearest ancestor, as durabletask-go's `GetLastWorkflowByName`) instead
  of the first.
- **`PropagatedHistory::from_proto`** returns `None` for malformed input
  (a chunk with an empty app ID or an undecodable raw event) instead of
  silently dropping the undecodable events.
- **Malformed propagated history fails the work item**, as in
  durabletask-go: activities fail with `InvalidPropagatedHistory`, and
  orchestration turns fail with `invalid propagated history: ...`.
  Previously the history was dropped and the work ran without it.
- **Replays that diverge from their history now fail** with
  `NonDeterminismError` (activity and child workflow names and timer origins
  must match the recorded ones) instead of silently continuing or
  delivering results to the wrong task.
- **Panics now fail instead of hanging:** a panicking activity fails with
  `TaskActivityPanic` (`panic: <message>`), and a panicking orchestrator
  fails the workflow with `OrchestratorPanic`. Previously the work item was
  never completed.
- **`is_patched` follows durabletask-go's rule** for new executions: a check
  reached while later history events of the turn are still to be applied
  (e.g. at the start of an execution whose first batch already contains a
  raised event) takes the unpatched path. Reported patches are now in
  history order, followed by new patches in the order they were first
  checked.
- **`RetryPolicy::retry_timeout` is enforced.** The replay clock previously
  stayed at the latest turn's time, so the timeout never expired; it is now
  measured in orchestration time from when the call is first made.
- **`dapr-durabletask-proto` 0.0.2** (#47): the protobuf types re-exported
  with the `proto` feature follow durabletask-protobuf v0.5.0 (new fields,
  moved `router` fields, `WORKER_CAPABILITY_HISTORY_STREAMING` removed). See
  the `dapr-durabletask-proto` changelog.
- **OpenTelemetry 0.33** (#50): the `otel` helpers exposed with the
  `opentelemetry` feature use `opentelemetry` 0.33 types.
- **New public fields** break struct-literal construction (use `Default` or
  the builders): `WorkerOptions` (`stateful_history`, `history_cache`),
  `OrchestrationState` (`started_at`, `parent_instance_id`,
  `parent_app_id`), and `ActivityOptions` / `SubOrchestratorOptions`
  (`app_namespace`).
- **Stateful history is on by default:** workers advertise the
  stateful-history capability and cache instance histories, so the runtime
  sends only new events on later turns. Disable with
  `WorkerOptions::with_stateful_history_disabled()`.
- **Workflow versions are pinned:** the worker reports the orchestrator
  version that ran, and a turn whose pinned version is not registered stalls
  (`WorkflowVersionNotAvailable`) instead of failing the orchestration.
- **Shutdown cancels in-flight activities:** activities receive a
  cancellation signal on worker shutdown, and one that returns an error after
  cancellation is abandoned for redelivery instead of reported as failed.
  Work items still waiting for a concurrency slot are not started.

### Added

- Per-event replay matching durabletask-go's `task/orchestrator.go`: history
  events are applied one at a time and the orchestrator is resumed after each
  event that can unblock it, so `current_utc_datetime`, `is_replaying` and
  `is_patched` are accurate at every point of the replay.
- Early resolutions (a completion that arrives before its scheduling event)
  are buffered per operation kind and delivered when the work is scheduled;
  unconsumed ones are logged as warnings at the end of the turn.
- Timers carry origins (`CreateTimer`, `ExternalEvent`, `ActivityRetry`,
  `ChildWorkflowRetry`) and names (the event name for event waits,
  `<name>-retry` for retry timers).
- Activities carry a `task_execution_id` (UUID) that is reused across retries
  and recovered from the recorded failure on replay.
- Child workflow retries carry `RetryParentInstanceInfo` pointing at the
  first attempt.
- `PropagatedHistory::try_from_proto` and `InvalidPropagatedHistoryError`
  for validated decoding with durabletask-go's error messages.
- Detached workflows: `OrchestrationContext::schedule_new_detached_workflow`
  with `DetachedWorkflowOptions` (instance ID, raw input, start time, app ID,
  namespace).
- Target namespace routing for activities and child workflows
  (`with_app_namespace` on `ActivityOptions` / `SubOrchestratorOptions`),
  validated as in durabletask-go.
- Client options: `_with_options` variants of every `TaskHubGrpcClient` call
  (`schedule_new_orchestration_with_options`,
  `get_orchestration_state_with_options`,
  `wait_for_orchestration_start_with_options`,
  `wait_for_orchestration_completion_with_options`,
  `raise_orchestration_event_with_options`, `terminate_orchestration_with_options`,
  `suspend_orchestration_with_options`, `resume_orchestration_with_options`,
  `purge_orchestration_with_options`,
  `purge_orchestrations_by_filter_with_options`), taking
  `NewOrchestrationOptions`, `FetchOptions`, `RaiseEventOptions`,
  `TerminateOptions`, `SuspendOptions`, `ResumeOptions` and `PurgeOptions`.
  Each accepts an app ID to route the request to another app, and routed
  requests are checked with the new `validate_task_router`. The existing
  methods are unchanged and delegate to these.
- Enforce-unique instance IDs when scheduling
  (`NewOrchestrationOptions::with_enforce_unique_instance_id`) and force purge
  (`PurgeOptions::with_force`).
- `TaskHubGrpcClient::list_instance_ids` (`ListInstanceIdsOptions` with page
  size and continuation token, returning an `InstanceIdPage`),
  `get_instance_history`, and `rerun_orchestration_from_event` (`RerunOptions`:
  new instance ID, input override, new child workflow instance ID, app ID).
- `OrchestrationState::started_at`, `parent_instance_id` and `parent_app_id`.
- Worker history cache for stateful history: the worker rebuilds each
  turn's history from cached events plus the runtime's delta, falling back to
  `GetInstanceHistory` on a miss. Configured with `HistoryCacheOptions` (TTL,
  sweep interval, instance and byte limits, with `effective_*` accessors and
  the `DEFAULT_HISTORY_CACHE_TTL`, `DEFAULT_HISTORY_CACHE_SWEEP_INTERVAL` and
  `DEFAULT_HISTORY_CACHE_MAX_INSTANCES` defaults) and the `WorkerOptions`
  builders `with_stateful_history`, `with_stateful_history_disabled`,
  `with_history_cache`, `with_history_cache_ttl`,
  `with_history_cache_sweep_interval`, `with_history_cache_max_instances` and
  `with_history_cache_max_bytes`. Entries are evicted by TTL,
  least-recently-used order and on completion, and stale per-instance
  dispatch markers are swept so the cache stays bounded on long-lived
  workers.
- `ActivityContext` cancellation: `with_cancellation_token`,
  `cancellation_token`, `is_cancelled` and `cancelled`.
- `PropagatedHistory::workflows_by_name` and activity / child workflow result
  lookups on `PropagatedHistoryChunk` (`activities_by_name`,
  `last_activity_by_name`, `child_workflows_by_name`,
  `last_child_workflow_by_name`) returning `ActivityResult` /
  `ChildWorkflowResult`.
- Histories recorded by 0.0.3 replay where possible: duplicate scheduling
  events persisted by older runtimes (when releases before #51 re-emitted
  in-flight actions) are ignored, and event-wait timers that 0.0.3 recorded
  differently are absorbed or dropped.
- Test coverage ported from durabletask-go (`tests/orchestration_executor.rs`,
  `tests/e2e.rs` and unit tests), plus leak-prevention and regression tests
  (#41).

### Changed

- `wait_for_external_event` emits its far-future tracking timer
  (`9999-12-31T23:59:59.999999999Z`) only when the event has not already
  arrived, without the `dapr:external-event-timer` patch gate;
  `wait_for_external_event_with_timeout` emits no timeout timer for an
  already-arrived event.
- Patches recorded in history are always reported back, as daprd requires.
- Completion actions use the next sequence number as their ID.
- Events carried over by `continue_as_new(.., true)` keep their arrival order
  and original casing.
- The orchestration span covers the whole replay.
- `when_all` only re-checks tasks that signalled completion, keeping fan-in
  linear; a task may be awaited by several combinators at once.
- `OrchestrationState` serialises its new `started_at`, `parent_instance_id`
  and `parent_app_id` fields (missing fields deserialise as `None`).
- `TaskHubGrpcWorker::start` documents the shutdown behaviour: an activity
  that fails after shutdown is signalled is abandoned for redelivery, one
  that completes is still reported, and queued work items are not started.

### Fixed

- Actions already scheduled in history are no longer re-emitted on replay
  (#51).
- An operation whose result arrived before its scheduling event never sent
  its schedule action.
- An event raised after a wait timed out could be delivered to that wait on
  replay, flipping the outcome and consuming the event meant for a later
  wait.
- Concurrent branches (e.g. `join!` with retries) could get different
  sequence numbers on replay.
- A `TimerFired` could complete an activity with the same ID.
- `continue_as_new` with a unit or `None` input completed the workflow
  instead of continuing it.
- A terminate recorded after the workflow completed in the same batch
  overrode the completion; continue-as-new could override a terminate.
- Events received while suspended were applied immediately instead of after
  resuming; they are now applied one at a time in their original order, so
  for example a timeout that fired before its event still times out.
- Reported patches were sorted, which daprd could reject as a mismatch.
- A worker cancelled before receiving its first work item did not shut down.
- Spans created inside activities were not parented to the activity span.

### Dependencies

- `dapr-durabletask-proto` 0.0.2 (#47).
- OpenTelemetry 0.33 (#50).

## [0.0.3] - 2026-06-11

### Added

- Explicit external event timer tracking (#37):
  `wait_for_external_event_with_timeout` returning `ExternalEventResult`,
  and event-wait timers tagged with the `ExternalEvent` origin (for
  indefinite waits gated by the `dapr:external-event-timer` patch).

### Fixed

- Finished work item tasks are pruned so the worker's task set does not grow
  unbounded (#38).
- Version regex for the proto dependency in the release tooling (#32).

## [0.0.2] - 2026-05-24

### ⚠️ Breaking changes

- Crate refactor (#26):
  - `OrchestrationContext::get_input` is renamed to `input`.
  - The `WhenAllTask` and `WhenAnyTask` constructors are removed; use
    `when_all` and `when_any`.
  - Worker errors use more specific error types.
- Minimum supported Rust version is 1.88 (#25).

### Added

- User agent on worker connections (#11).
- All protos are built from durabletask-protobuf (#15).
- `Debug` for `Registry`.

### Fixed

- `is_replaying` at the end of replay (#10).
- Clearer connection error messages and jitter randomness (#26).

## [0.0.1] - 2026-05-20

### Added

- Initial Durable Task SDK for Dapr (#2): gRPC client, worker, orchestration
  executor, activities, sub-orchestrations, timers, external events,
  continue-as-new, suspend/resume/terminate/purge, retry policies,
  reconnect policy and patching.
- History propagation to activities and child workflows (#4).
- Optional OpenTelemetry tracing (`opentelemetry` feature, 0.32) (#6).
- Package metadata (#8).

[0.0.4]: https://github.com/mikeee/dapr-durabletask/compare/dapr-durabletask-v0.0.3...HEAD
[0.0.3]: https://github.com/mikeee/dapr-durabletask/compare/dapr-durabletask-v0.0.2...dapr-durabletask-v0.0.3
[0.0.2]: https://github.com/mikeee/dapr-durabletask/compare/v0.0.1...dapr-durabletask-v0.0.2
[0.0.1]: https://github.com/mikeee/dapr-durabletask/releases/tag/v0.0.1
