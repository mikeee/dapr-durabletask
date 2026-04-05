//! Integration tests for the OrchestrationExecutor.
//!
//! No running sidecar is required.

use std::sync::Arc;

use dapr_durabletask::api::DurableTaskError;
use dapr_durabletask::proto;
use dapr_durabletask::proto::history_event::EventType;
use dapr_durabletask::task::{when_all, when_any};
use dapr_durabletask::worker::{OrchestrationExecutor, OrchestratorFn, WorkerOptions};

// ---------------------------------------------------------------------------
// Event construction helpers
// ---------------------------------------------------------------------------

fn ts_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

fn to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> proto::prost_types::Timestamp {
    proto::prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

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
        timestamp: Some(to_timestamp(ts_now())),
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
        timestamp: Some(to_timestamp(ts_now())),
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
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::TaskCompleted(proto::TaskCompletedEvent {
            task_scheduled_id,
            result,
            task_execution_id: String::new(),
        })),
    }
}

fn make_task_failed(
    event_id: i32,
    task_scheduled_id: i32,
    error_type: &str,
    message: &str,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::TaskFailed(proto::TaskFailedEvent {
            task_scheduled_id,
            failure_details: Some(proto::TaskFailureDetails {
                error_type: error_type.to_string(),
                error_message: message.to_string(),
                stack_trace: None,
                inner_failure: None,
                is_non_retriable: false,
            }),
            task_execution_id: String::new(),
        })),
    }
}

fn make_timer_created(
    event_id: i32,
    fire_at: chrono::DateTime<chrono::Utc>,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::TimerCreated(proto::TimerCreatedEvent {
            fire_at: Some(to_timestamp(fire_at)),
            name: None,
            rerun_parent_instance_info: None,
            origin: None,
        })),
    }
}

fn make_timer_fired(event_id: i32, timer_id: i32) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::TimerFired(proto::TimerFiredEvent {
            fire_at: None,
            timer_id,
        })),
    }
}

fn make_sub_orchestration_created(
    event_id: i32,
    name: &str,
    instance_id: &str,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::ChildWorkflowInstanceCreated(
            proto::ChildWorkflowInstanceCreatedEvent {
                instance_id: instance_id.to_string(),
                name: name.to_string(),
                version: None,
                input: None,
                parent_trace_context: None,
                rerun_parent_instance_info: None,
            },
        )),
    }
}

fn make_sub_orchestration_completed(
    event_id: i32,
    task_scheduled_id: i32,
    result: Option<String>,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::ChildWorkflowInstanceCompleted(
            proto::ChildWorkflowInstanceCompletedEvent {
                task_scheduled_id,
                result,
            },
        )),
    }
}

fn make_sub_orchestration_failed(
    event_id: i32,
    task_scheduled_id: i32,
    error_type: &str,
    message: &str,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::ChildWorkflowInstanceFailed(
            proto::ChildWorkflowInstanceFailedEvent {
                task_scheduled_id,
                failure_details: Some(proto::TaskFailureDetails {
                    error_type: error_type.to_string(),
                    error_message: message.to_string(),
                    stack_trace: None,
                    inner_failure: None,
                    is_non_retriable: false,
                }),
            },
        )),
    }
}

fn make_event_raised(name: &str, input: Option<String>) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id: -1,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::EventRaised(proto::EventRaisedEvent {
            name: name.to_string(),
            input,
        })),
    }
}

fn make_suspended() -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id: -1,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::ExecutionSuspended(
            proto::ExecutionSuspendedEvent { input: None },
        )),
    }
}

fn make_resumed() -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id: -1,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::ExecutionResumed(proto::ExecutionResumedEvent {
            input: None,
        })),
    }
}

fn make_terminated(output: Option<String>) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id: -1,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::ExecutionTerminated(
            proto::ExecutionTerminatedEvent {
                input: output,
                recurse: false,
            },
        )),
    }
}

// ---------------------------------------------------------------------------
// Response inspection helpers
// ---------------------------------------------------------------------------

fn get_complete_action(
    actions: &[proto::WorkflowAction],
) -> Option<&proto::CompleteWorkflowAction> {
    actions.iter().find_map(|a| match &a.workflow_action_type {
        Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) => Some(cw),
        _ => None,
    })
}

fn get_schedule_actions(actions: &[proto::WorkflowAction]) -> Vec<&proto::ScheduleTaskAction> {
    actions
        .iter()
        .filter_map(|a| match &a.workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::ScheduleTask(st)) => Some(st),
            _ => None,
        })
        .collect()
}

fn get_timer_actions(actions: &[proto::WorkflowAction]) -> Vec<&proto::CreateTimerAction> {
    actions
        .iter()
        .filter_map(|a| match &a.workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateTimer(ct)) => Some(ct),
            _ => None,
        })
        .collect()
}

fn get_child_workflow_actions(
    actions: &[proto::WorkflowAction],
) -> Vec<&proto::CreateChildWorkflowAction> {
    actions
        .iter()
        .filter_map(|a| match &a.workflow_action_type {
            Some(proto::workflow_action::WorkflowActionType::CreateChildWorkflow(cw)) => Some(cw),
            _ => None,
        })
        .collect()
}

/// Execute with default instance ID and empty completion token.
async fn run_executor(
    orch_fn: &OrchestratorFn,
    old_events: Vec<proto::HistoryEvent>,
    new_events: Vec<proto::HistoryEvent>,
) -> dapr_durabletask::api::Result<proto::WorkflowResponse> {
    OrchestrationExecutor::execute(
        orch_fn,
        "test-instance",
        old_events,
        new_events,
        String::new(),
        &WorkerOptions::default(),
    )
    .await
}

// ===========================================================================
// Basic orchestration lifecycle
// ===========================================================================

#[tokio::test]
async fn test_empty_orchestration() {
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { Ok(Some("\"done\"".to_string())) }));

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    assert_eq!(resp.instance_id, "test-instance");
    let cw = get_complete_action(&resp.actions).expect("should have CompleteWorkflow");
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"done\"".to_string()));
}

#[tokio::test]
async fn test_orchestration_with_input() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let input: String = ctx.get_input().unwrap();
            Ok(Some(format!("\"got: {}\"", input)))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started(
            "test_orch",
            Some("\"hello world\"".to_string()),
        )],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"got: hello world\"".to_string()));
}

#[tokio::test]
async fn test_orchestration_with_no_output() {
    let orch_fn: OrchestratorFn = Arc::new(|_ctx| Box::pin(async { Ok(None) }));

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, None);
}

// ===========================================================================
// Activity execution
// ===========================================================================

#[tokio::test]
async fn test_single_activity_scheduling() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("greet", "world").await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    // Should produce a ScheduleTask action and no CompleteWorkflow
    let schedules = get_schedule_actions(&resp.actions);
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "greet");
    assert_eq!(schedules[0].input, Some("\"world\"".to_string()));
    assert!(get_complete_action(&resp.actions).is_none());
}

#[tokio::test]
async fn test_single_activity_completion() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("greet", "world").await?;
            Ok(result)
        })
    });

    // Activity already completed in history
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "greet"),
            make_task_completed(4, 0, Some("\"hello world\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"hello world\"".to_string()));
}

#[tokio::test]
async fn test_activity_sequence() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.call_activity("step_a", ()).await?;
            let b = ctx.call_activity("step_b", ()).await?;
            let c = ctx.call_activity("step_c", ()).await?;
            // Return the last result
            Ok(c.or(b).or(a))
        })
    });

    // Round 1: No history — should schedule step_a
    let resp1 = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let schedules1 = get_schedule_actions(&resp1.actions);
    assert_eq!(schedules1.len(), 1);
    assert_eq!(schedules1[0].name, "step_a");
    assert!(get_complete_action(&resp1.actions).is_none());

    // Round 2: step_a completed — should schedule step_b
    let resp2 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "step_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let schedules2 = get_schedule_actions(&resp2.actions);
    assert_eq!(schedules2.len(), 1);
    assert_eq!(schedules2[0].name, "step_b");
    assert!(get_complete_action(&resp2.actions).is_none());

    // Round 3: step_a + step_b completed — should schedule step_c
    let resp3 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "step_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
            make_task_scheduled(5, "step_b"),
            make_task_completed(6, 1, Some("\"result_b\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let schedules3 = get_schedule_actions(&resp3.actions);
    assert_eq!(schedules3.len(), 1);
    assert_eq!(schedules3[0].name, "step_c");
    assert!(get_complete_action(&resp3.actions).is_none());

    // Round 4: All completed — orchestrator completes
    let resp4 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "step_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
            make_task_scheduled(5, "step_b"),
            make_task_completed(6, 1, Some("\"result_b\"".to_string())),
            make_task_scheduled(7, "step_c"),
            make_task_completed(8, 2, Some("\"result_c\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp4.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"result_c\"".to_string()));
}

#[tokio::test]
async fn test_activity_failure_propagation() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("flaky", ()).await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "flaky"),
            make_task_failed(4, 0, "ActivityError", "something went wrong"),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_type, "ActivityError");
    assert_eq!(fd.error_message, "something went wrong");
}

#[tokio::test]
async fn test_activity_failure_caught() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("flaky", ()).await;
            match result {
                Ok(v) => Ok(v),
                Err(DurableTaskError::TaskFailed { message, .. }) => {
                    Ok(Some(format!("\"caught: {}\"", message)))
                }
                Err(e) => Err(e),
            }
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "flaky"),
            make_task_failed(4, 0, "TestError", "boom"),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"caught: boom\"".to_string()));
}

// ===========================================================================
// Timer execution
// ===========================================================================

#[tokio::test]
async fn test_timer_scheduling() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(60)).await?;
            Ok(Some("\"timer done\"".to_string()))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let timers = get_timer_actions(&resp.actions);
    assert_eq!(timers.len(), 1);
    assert!(timers[0].fire_at.is_some());
    assert!(get_complete_action(&resp.actions).is_none());
}

#[tokio::test]
async fn test_timer_completion() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(60)).await?;
            Ok(Some("\"timer done\"".to_string()))
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(60);
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created(3, fire_at),
            make_timer_fired(4, 0),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"timer done\"".to_string()));
}

// ===========================================================================
// Sub-orchestration execution
// ===========================================================================

#[tokio::test]
async fn test_sub_orchestration_scheduling() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .call_sub_orchestrator("child_orch", "child_input", Some("child-1"))
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let children = get_child_workflow_actions(&resp.actions);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].name, "child_orch");
    assert_eq!(children[0].instance_id, "child-1");
    assert_eq!(children[0].input, Some("\"child_input\"".to_string()));
    assert!(get_complete_action(&resp.actions).is_none());
}

#[tokio::test]
async fn test_sub_orchestration_completion() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .call_sub_orchestrator("child_orch", (), Some("child-1"))
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_sub_orchestration_created(3, "child_orch", "child-1"),
            make_sub_orchestration_completed(4, 0, Some("\"child result\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"child result\"".to_string()));
}

#[tokio::test]
async fn test_sub_orchestration_failure() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .call_sub_orchestrator("child_orch", (), Some("child-1"))
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_sub_orchestration_created(3, "child_orch", "child-1"),
            make_sub_orchestration_failed(4, 0, "ChildError", "child failed"),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_type, "ChildError");
    assert_eq!(fd.error_message, "child failed");
}

// ===========================================================================
// External events
// ===========================================================================

#[tokio::test]
async fn test_external_event_received() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    // Event arrives as a new event
    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("approval", Some("\"approved\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"approved\"".to_string()));
}

#[tokio::test]
async fn test_external_event_buffered() {
    // Event arrives before wait_for_external_event is called
    // The event is delivered as part of new_events before the orchestrator
    // calls wait_for_external_event
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    // Event arrives in new_events before ExecutionStarted triggers the orch
    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("approval", Some("\"pre-buffered\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"pre-buffered\"".to_string()));
}

#[tokio::test]
async fn test_external_event_case_insensitive() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            // Wait for lowercase
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    // Event arrives with uppercase name
    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("APPROVAL", Some("\"yes\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"yes\"".to_string()));
}

#[tokio::test]
async fn test_multiple_external_events() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.wait_for_external_event("event_a").await?;
            let b = ctx.wait_for_external_event("event_b").await?;
            let c = ctx.wait_for_external_event("event_c").await?;
            // Concatenate results
            let combined = format!(
                "\"{},{},{}\"",
                a.as_deref().unwrap_or(""),
                b.as_deref().unwrap_or(""),
                c.as_deref().unwrap_or("")
            );
            Ok(Some(combined))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("event_a", Some("\"A\"".to_string())),
            make_event_raised("event_b", Some("\"B\"".to_string())),
            make_event_raised("event_c", Some("\"C\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"\"A\",\"B\",\"C\"\"".to_string()));
}

// ===========================================================================
// Fan-out / fan-in patterns
// ===========================================================================

#[tokio::test]
async fn test_fan_out_scheduling() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let mut tasks = Vec::new();
            for i in 0..5 {
                tasks.push(ctx.call_activity("worker", i));
            }
            let results = when_all(tasks).await?;
            Ok(Some(format!("{}", results.len())))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    // Should have 5 ScheduleTask actions and no CompleteWorkflow
    let schedules = get_schedule_actions(&resp.actions);
    assert_eq!(schedules.len(), 5);
    for s in &schedules {
        assert_eq!(s.name, "worker");
    }
    assert!(get_complete_action(&resp.actions).is_none());
}

#[tokio::test]
async fn test_fan_out_fan_in_completion() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let mut tasks = Vec::new();
            for i in 0..5 {
                tasks.push(ctx.call_activity("worker", i));
            }
            let results = when_all(tasks).await?;
            // Count completed results
            let count = results.iter().filter(|r| r.is_some()).count();
            Ok(Some(format!("{}", count)))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "worker"),
            make_task_completed(4, 0, Some("\"r0\"".to_string())),
            make_task_scheduled(5, "worker"),
            make_task_completed(6, 1, Some("\"r1\"".to_string())),
            make_task_scheduled(7, "worker"),
            make_task_completed(8, 2, Some("\"r2\"".to_string())),
            make_task_scheduled(9, "worker"),
            make_task_completed(10, 3, Some("\"r3\"".to_string())),
            make_task_scheduled(11, "worker"),
            make_task_completed(12, 4, Some("\"r4\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("5".to_string()));
}

#[tokio::test]
async fn test_fan_out_partial_failure() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let mut tasks = Vec::new();
            for i in 0..5 {
                tasks.push(ctx.call_activity("worker", i));
            }
            let results = when_all(tasks).await?;
            Ok(Some(format!("{}", results.len())))
        })
    });

    // 4 succeed, task at index 2 fails
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "worker"),
            make_task_completed(4, 0, Some("\"r0\"".to_string())),
            make_task_scheduled(5, "worker"),
            make_task_completed(6, 1, Some("\"r1\"".to_string())),
            make_task_scheduled(7, "worker"),
            make_task_failed(8, 2, "WorkerError", "worker 2 crashed"),
            make_task_scheduled(9, "worker"),
            make_task_completed(10, 3, Some("\"r3\"".to_string())),
            make_task_scheduled(11, "worker"),
            make_task_completed(12, 4, Some("\"r4\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_message, "worker 2 crashed");
}

// ===========================================================================
// When-any pattern
// ===========================================================================

#[tokio::test]
async fn test_when_any_first_completes() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let t0 = ctx.call_activity("slow", ());
            let t1 = ctx.call_activity("fast", ());
            let t2 = ctx.call_activity("medium", ());
            let winner = when_any(vec![t0, t1, t2]).await?;
            Ok(Some(format!("{}", winner)))
        })
    });

    // Only the second task (index 1) has completed
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "slow"),
            make_task_scheduled(4, "fast"),
            make_task_completed(5, 1, Some("\"fast result\"".to_string())),
            make_task_scheduled(6, "medium"),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("1".to_string()));
}

#[tokio::test]
async fn test_when_any_with_timer_timeout() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let activity_task = ctx.call_activity("slow_activity", ());
            let timer_task = ctx.create_timer(std::time::Duration::from_secs(30));
            let winner = when_any(vec![activity_task, timer_task]).await?;
            Ok(Some(format!("{}", winner)))
        })
    });

    // Timer fires first (index 1)
    let fire_at = ts_now() + chrono::Duration::seconds(30);
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "slow_activity"),
            make_timer_created(4, fire_at),
            make_timer_fired(5, 1),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    // Timer is at index 1
    assert_eq!(cw.result, Some("1".to_string()));
}

// ===========================================================================
// Continue-as-new
// ===========================================================================

#[tokio::test]
async fn test_continue_as_new() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.continue_as_new("next_iteration", false);
            Ok(None)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::ContinuedAsNew as i32
    );
    assert_eq!(cw.result, Some("\"next_iteration\"".to_string()));
    assert!(cw.carryover_events.is_empty());
}

#[tokio::test]
async fn test_continue_as_new_with_save_events() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.continue_as_new("next", true);
            Ok(None)
        })
    });

    // Include a buffered event that should be carried over
    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("pending_event", Some("\"data\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::ContinuedAsNew as i32
    );
    // The buffered event should be carried over
    assert!(!cw.carryover_events.is_empty());
    // Verify the carryover event
    let carryover = &cw.carryover_events[0];
    match &carryover.event_type {
        Some(EventType::EventRaised(e)) => {
            assert_eq!(e.name, "pending_event");
            assert_eq!(e.input, Some("\"data\"".to_string()));
        }
        _ => panic!("expected EventRaised carryover"),
    }
}

// ===========================================================================
// Suspend / Resume
// ===========================================================================

#[tokio::test]
async fn test_suspend_prevents_execution() {
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { panic!("should not execute when suspended") }));

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None), make_suspended()],
    )
    .await
    .unwrap();

    // No actions should be generated since orchestrator was not run
    assert!(resp.actions.is_empty());
}

#[tokio::test]
async fn test_suspend_and_resume() {
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { Ok(Some("\"resumed and done\"".to_string())) }));

    // Suspended then resumed — should run normally
    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_suspended(),
            make_resumed(),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"resumed and done\"".to_string()));
}

// ===========================================================================
// Terminate
// ===========================================================================

#[tokio::test]
async fn test_terminate_prevents_execution() {
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { panic!("should not execute when terminated") }));

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_terminated(None),
        ],
    )
    .await
    .unwrap();

    // Terminated orchestrations produce a CompleteWorkflow action with Terminated status
    assert_eq!(resp.actions.len(), 1);
    match &resp.actions[0].workflow_action_type {
        Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) => {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Terminated as i32
            );
        }
        other => panic!("expected CompleteWorkflow, got {:?}", other),
    }
}

// ===========================================================================
// Custom status
// ===========================================================================

#[tokio::test]
async fn test_custom_status() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.set_custom_status("step 1 of 3");
            Ok(Some("\"done\"".to_string()))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    assert_eq!(resp.custom_status, Some("step 1 of 3".to_string()));
    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
}

// ===========================================================================
// Error handling
// ===========================================================================

#[tokio::test]
async fn test_activity_error_handling_with_catch() {
    // Orchestrator catches a failing activity and calls a compensating activity
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("risky_operation", ()).await;
            match result {
                Ok(v) => Ok(v),
                Err(DurableTaskError::TaskFailed { .. }) => {
                    let compensate = ctx.call_activity("compensate", ()).await?;
                    Ok(compensate)
                }
                Err(e) => Err(e),
            }
        })
    });

    // Round 1: risky_operation fails
    let resp1 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "risky_operation"),
            make_task_failed(4, 0, "RiskyError", "it broke"),
        ],
        vec![],
    )
    .await
    .unwrap();

    // Should schedule the compensating activity
    let schedules = get_schedule_actions(&resp1.actions);
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "compensate");
    assert!(get_complete_action(&resp1.actions).is_none());

    // Round 2: compensate completes
    let resp2 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "risky_operation"),
            make_task_failed(4, 0, "RiskyError", "it broke"),
            make_task_scheduled(5, "compensate"),
            make_task_completed(6, 1, Some("\"compensated\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp2.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"compensated\"".to_string()));
}

// ===========================================================================
// Complex replay scenarios
// ===========================================================================

#[tokio::test]
async fn test_multi_round_replay() {
    // Simulate a multi-round execution:
    // Round 1: schedule activity A
    // Round 2: A completed, schedule B
    // Round 3: B completed, orchestrator completes
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.call_activity("activity_a", "input_a").await?;
            let b = ctx
                .call_activity("activity_b", a.as_deref().unwrap_or(""))
                .await?;
            Ok(b)
        })
    });

    // Round 1: fresh start
    let resp1 = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let s1 = get_schedule_actions(&resp1.actions);
    assert_eq!(s1.len(), 1);
    assert_eq!(s1[0].name, "activity_a");

    // Round 2: A completed
    let resp2 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "activity_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let s2 = get_schedule_actions(&resp2.actions);
    assert_eq!(s2.len(), 1);
    assert_eq!(s2[0].name, "activity_b");
    assert!(get_complete_action(&resp2.actions).is_none());

    // Round 3: A and B completed
    let resp3 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "activity_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
            make_task_scheduled(5, "activity_b"),
            make_task_completed(6, 1, Some("\"final_result\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp3.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"final_result\"".to_string()));
}

// ===========================================================================
// Additional edge-case tests
// ===========================================================================

#[tokio::test]
async fn test_orchestrator_context_accessors() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let name = ctx.name();
            let iid = ctx.instance_id();
            Ok(Some(format!("\"{}:{}\"", name, iid)))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("my_orch", None)],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(cw.result, Some("\"my_orch:test-instance\"".to_string()));
}

#[tokio::test]
async fn test_activity_with_new_event_completion() {
    // Activity result arrives in new_events (not old_events)
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("greet", ()).await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "greet"),
        ],
        vec![make_task_completed(4, 0, Some("\"new hello\"".to_string()))],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"new hello\"".to_string()));
}

#[tokio::test]
async fn test_multiple_suspend_resume_cycles() {
    // Suspend → Resume → Suspend → Resume should work
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { Ok(Some("\"alive\"".to_string())) }));

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_suspended(),
            make_resumed(),
            make_suspended(),
            make_resumed(),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
}

#[tokio::test]
async fn test_orchestrator_error_becomes_failed() {
    // Orchestrator returns a non-TaskFailed error
    let orch_fn: OrchestratorFn = Arc::new(|_ctx| {
        Box::pin(async { Err(DurableTaskError::Other("unexpected crash".to_string())) })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_type, "OrchestratorError");
    assert!(fd.error_message.contains("unexpected crash"));
}

#[tokio::test]
async fn test_timer_and_activity_sequence() {
    // Activity → timer → activity
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _a = ctx.call_activity("step1", ()).await?;
            ctx.create_timer(std::time::Duration::from_secs(10)).await?;
            let b = ctx.call_activity("step2", ()).await?;
            Ok(b)
        })
    });

    // All completed in history
    let fire_at = ts_now() + chrono::Duration::seconds(10);
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "step1"),
            make_task_completed(4, 0, Some("\"s1\"".to_string())),
            make_timer_created(5, fire_at),
            make_timer_fired(6, 1),
            make_task_scheduled(7, "step2"),
            make_task_completed(8, 2, Some("\"s2\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"s2\"".to_string()));
}

#[tokio::test]
async fn test_fan_out_fan_in_with_when_all_empty() {
    // when_all on empty list completes immediately
    let orch_fn: OrchestratorFn = Arc::new(|_ctx| {
        Box::pin(async move {
            let results = when_all(vec![]).await?;
            Ok(Some(format!("{}", results.len())))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("0".to_string()));
}

#[tokio::test]
async fn test_event_not_yet_received() {
    // Orchestrator waits for an event that hasn't arrived
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    // No complete action — orchestrator is waiting
    assert!(get_complete_action(&resp.actions).is_none());
    // No schedule/timer actions either
    assert!(resp.actions.is_empty());
}

#[tokio::test]
async fn test_activity_with_struct_input() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            #[derive(serde::Serialize)]
            struct Input {
                x: i32,
                y: i32,
            }
            let _task = ctx.call_activity("add", Input { x: 1, y: 2 });
            // Don't await — just check the action was created
            Ok(None)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let schedules = get_schedule_actions(&resp.actions);
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "add");
    assert_eq!(schedules[0].input, Some("{\"x\":1,\"y\":2}".to_string()));
}

#[tokio::test]
async fn test_completion_token_preserved() {
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { Ok(Some("\"done\"".to_string())) }));

    let resp = OrchestrationExecutor::execute(
        &orch_fn,
        "test-instance",
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
        "my-token-123".to_string(),
        &WorkerOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(resp.completion_token, "my-token-123");
}

#[tokio::test]
async fn test_action_ids_are_sequential() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _a = ctx.call_activity("a", ());
            let _b = ctx.call_activity("b", ());
            let _c = ctx.call_activity("c", ());
            Ok(None)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    // The pending actions should have sequential IDs
    for (i, action) in resp.actions.iter().enumerate() {
        assert_eq!(action.id, i as i32, "Action {} should have id {}", i, i);
    }
}

#[tokio::test]
async fn test_sub_orchestration_with_auto_instance_id() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _task = ctx.call_sub_orchestrator("child_orch", (), None);
            Ok(None)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let children = get_child_workflow_actions(&resp.actions);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].name, "child_orch");
    // Instance ID should be auto-generated (UUID format)
    assert!(!children[0].instance_id.is_empty());
}

#[tokio::test]
async fn test_continue_as_new_with_activity_before() {
    // Activity → continue_as_new
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("get_count", ()).await?;
            let count: i32 = serde_json::from_str(result.as_deref().unwrap_or("0")).unwrap_or(0);
            if count < 3 {
                ctx.continue_as_new(count + 1, false);
            }
            Ok(None)
        })
    });

    // Activity returned count=1
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "get_count"),
            make_task_completed(4, 0, Some("1".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::ContinuedAsNew as i32
    );
    assert_eq!(cw.result, Some("2".to_string()));
}

#[tokio::test]
async fn test_external_event_with_null_data() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("signal").await?;
            // result should be None for null data
            Ok(Some(format!("{}", result.is_none())))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("signal", None),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("true".to_string()));
}

#[tokio::test]
async fn test_custom_status_updated_mid_execution() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.set_custom_status("starting");
            let _ = ctx.call_activity("step1", ()).await?;
            ctx.set_custom_status("step 1 done");
            Ok(Some("\"done\"".to_string()))
        })
    });

    // step1 completed
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "step1"),
            make_task_completed(4, 0, Some("\"ok\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    // Custom status should reflect the last set value
    assert_eq!(resp.custom_status, Some("step 1 done".to_string()));
}

#[tokio::test]
async fn test_terminate_with_output() {
    let orch_fn: OrchestratorFn = Arc::new(|_ctx| Box::pin(async { panic!("should not run") }));

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_terminated(Some("\"terminated reason\"".to_string())),
        ],
    )
    .await
    .unwrap();

    // Terminated — CompleteWorkflow action with Terminated status and output
    assert_eq!(resp.actions.len(), 1);
    match &resp.actions[0].workflow_action_type {
        Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) => {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Terminated as i32
            );
            assert_eq!(cw.result, Some("\"terminated reason\"".to_string()));
        }
        other => panic!("expected CompleteWorkflow, got {:?}", other),
    }
}

#[tokio::test]
async fn test_mixed_event_types_in_replay() {
    // Complex scenario: activity + timer + external event in replay
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.call_activity("fetch_data", ()).await?;
            ctx.create_timer(std::time::Duration::from_secs(5)).await?;
            let evt = ctx.wait_for_external_event("user_input").await?;
            let combined = format!(
                "\"data={},event={}\"",
                a.as_deref().unwrap_or(""),
                evt.as_deref().unwrap_or("")
            );
            Ok(Some(combined))
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(5);
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            // activity at seq 0
            make_task_scheduled(3, "fetch_data"),
            make_task_completed(4, 0, Some("\"fetched\"".to_string())),
            // timer at seq 1
            make_timer_created(5, fire_at),
            make_timer_fired(6, 1),
        ],
        vec![
            // external event arrives in new events
            make_event_raised("user_input", Some("\"clicked\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    // The inner values include JSON quotes, so the format! result has embedded quotes
    assert_eq!(
        cw.result,
        Some("\"data=\"fetched\",event=\"clicked\"\"".to_string())
    );
}

#[tokio::test]
async fn test_when_any_activity_completes_before_timer() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let activity_task = ctx.call_activity("fast_activity", ());
            let timer_task = ctx.create_timer(std::time::Duration::from_secs(60));
            let winner = when_any(vec![activity_task, timer_task]).await?;
            Ok(Some(format!("{}", winner)))
        })
    });

    // Activity completes first (index 0)
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "fast_activity"),
            make_task_completed(4, 0, Some("\"fast\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("0".to_string()));
}

// ── is_patched tests ─────────────────────────────────────────────────────────

fn make_workflow_started_with_patches(
    ts: chrono::DateTime<chrono::Utc>,
    patches: Vec<String>,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id: 1,
        timestamp: Some(to_timestamp(ts)),
        router: None,
        event_type: Some(EventType::WorkflowStarted(proto::WorkflowStartedEvent {
            version: Some(proto::WorkflowVersion {
                patches,
                name: None,
            }),
        })),
    }
}

#[tokio::test]
async fn test_is_patched_new_execution_applies_patch() {
    // Brand-new orchestration with no history: is_patched returns true → patched path runs.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            if ctx.is_patched("new-feature") {
                Ok(Some("patched".to_string()))
            } else {
                Ok(Some("unpatched".to_string()))
            }
        })
    });

    let resp = run_executor(&orch_fn, vec![], vec![]).await.unwrap();
    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(cw.result, Some("patched".to_string()));
}

#[tokio::test]
async fn test_is_patched_mid_replay_uses_unpatched_path() {
    // History has one TaskScheduled but no history patches → mid-replay → returns false.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            if ctx.is_patched("new-feature") {
                // patched: schedules "new_act"
                ctx.call_activity("new_act", ()).await?;
            } else {
                // unpatched: schedules "old_act"
                ctx.call_activity("old_act", ()).await?;
            }
            Ok(Some("done".to_string()))
        })
    });

    // History contains the TaskScheduled/Completed for "old_act" at seq 0.
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "old_act"),
            make_task_completed(4, 0, Some("\"ok\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("done".to_string()));
}

#[tokio::test]
async fn test_is_patched_history_patch_applies() {
    // History WorkflowStarted event carries the patch name → is_patched returns true.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            if ctx.is_patched("new-feature") {
                ctx.call_activity("new_act", ()).await?;
            } else {
                ctx.call_activity("old_act", ()).await?;
            }
            Ok(Some("done".to_string()))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started_with_patches(ts_now(), vec!["new-feature".to_string()]),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "new_act"),
            make_task_completed(4, 0, Some("\"ok\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("done".to_string()));
}

// ── Retry tests ───────────────────────────────────────────────────────────────

use dapr_durabletask::api::RetryPolicy;
use dapr_durabletask::task::{ActivityOptions, SubOrchestratorOptions};
use std::time::Duration;

#[tokio::test]
async fn test_retry_activity_succeeds_on_second_attempt() {
    // First attempt fails, second attempt (after a timer) succeeds.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let opts = ActivityOptions::new()
                .with_retry_policy(RetryPolicy::new(3, Duration::from_secs(1)));
            let result = ctx.call_activity_with_options("flaky", (), opts).await?;
            Ok(result)
        })
    });

    // seq 0: first attempt fails; seq 1: retry timer fires; seq 2: second attempt succeeds
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "flaky"),
            make_task_failed(4, 0, "IOError", "transient"),
            make_timer_created(5, ts_now() + chrono::Duration::seconds(1)),
            make_timer_fired(6, 1),
            make_task_scheduled(7, "flaky"),
            make_task_completed(8, 2, Some("\"ok\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"ok\"".to_string()));
}

#[tokio::test]
async fn test_retry_activity_fails_after_max_attempts() {
    // All three attempts fail — orchestration should fail.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let opts = ActivityOptions::new()
                .with_retry_policy(RetryPolicy::new(2, Duration::from_secs(1)));
            ctx.call_activity_with_options("bad", (), opts).await?;
            Ok(None)
        })
    });

    // seq 0 fails; seq 1 is the timer; seq 2 fails again — max attempts (2) reached
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "bad"),
            make_task_failed(4, 0, "IOError", "still broken"),
            make_timer_created(5, ts_now() + chrono::Duration::seconds(1)),
            make_timer_fired(6, 1),
            make_task_scheduled(7, "bad"),
            make_task_failed(8, 2, "IOError", "still broken"),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
}

#[tokio::test]
async fn test_retry_activity_predicate_blocks_retry() {
    // The handle predicate returns false for "FatalError" — no retry.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let policy = RetryPolicy::new(5, Duration::from_secs(1))
                .with_handle(|details| details.error_type != "FatalError");
            let opts = ActivityOptions::new().with_retry_policy(policy);
            ctx.call_activity_with_options("fatal_act", (), opts)
                .await?;
            Ok(None)
        })
    });

    // First attempt fails with FatalError — predicate blocks retry → immediate fail
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "fatal_act"),
            make_task_failed(4, 0, "FatalError", "cannot retry"),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    // No timer action should have been scheduled
    let non_complete: Vec<_> = resp
        .actions
        .iter()
        .filter(|a| {
            !matches!(
                &a.workflow_action_type,
                Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(_))
            )
        })
        .collect();
    assert!(non_complete.is_empty(), "expected no retry timer action");
}

#[tokio::test]
async fn test_retry_activity_predicate_allows_retry() {
    // The handle predicate returns true for "RetryableError" — retry proceeds.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let policy = RetryPolicy::new(3, Duration::from_secs(1))
                .with_handle(|details| details.error_type == "RetryableError");
            let opts = ActivityOptions::new().with_retry_policy(policy);
            let result = ctx
                .call_activity_with_options("retryable_act", (), opts)
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "retryable_act"),
            make_task_failed(4, 0, "RetryableError", "try again"),
            make_timer_created(5, ts_now() + chrono::Duration::seconds(1)),
            make_timer_fired(6, 1),
            make_task_scheduled(7, "retryable_act"),
            make_task_completed(8, 2, Some("\"recovered\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"recovered\"".to_string()));
}

#[tokio::test]
async fn test_retry_sub_orchestrator_succeeds_on_second_attempt() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let opts = SubOrchestratorOptions::new()
                .with_instance_id("child-1".to_string())
                .with_retry_policy(RetryPolicy::new(3, Duration::from_secs(2)));
            let result = ctx
                .call_sub_orchestrator_with_options("child_orch", (), opts)
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            // seq 0: child workflow created, then fails
            make_sub_orchestration_created(3, "child_orch", "child-1"),
            make_sub_orchestration_failed(4, 0, "ChildError", "child failed"),
            // seq 1: retry timer
            make_timer_created(5, ts_now() + chrono::Duration::seconds(2)),
            make_timer_fired(6, 1),
            // seq 2: second child workflow attempt succeeds
            make_sub_orchestration_created(7, "child_orch", "child-1"),
            make_sub_orchestration_completed(8, 2, Some("\"child_ok\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"child_ok\"".to_string()));
}

#[tokio::test]
async fn test_retry_no_retry_on_success() {
    // No failures — activity succeeds first try, no timer should be scheduled.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let opts = ActivityOptions::new()
                .with_retry_policy(RetryPolicy::new(5, Duration::from_secs(1)));
            let result = ctx
                .call_activity_with_options("instant_ok", (), opts)
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "instant_ok"),
            make_task_completed(4, 0, Some("\"done\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"done\"".to_string()));
}
