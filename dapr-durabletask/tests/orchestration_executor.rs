//! Integration tests for the OrchestrationExecutor.
//!
//! No running sidecar is required.

use std::sync::Arc;

use chrono::Datelike;
use dapr_durabletask::api::DurableTaskError;
use dapr_durabletask::task::{when_all, when_any};
use dapr_durabletask::worker::{OrchestrationExecutor, OrchestratorFn, WorkerOptions};
use dapr_durabletask_proto as proto;
use dapr_durabletask_proto::history_event::EventType;

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
        timestamp: Some(to_timestamp(ts_now())),
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
            attestation: None,
            signer_certificate: None,
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
                history_propagation_scope: None,
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
                attestation: None,
                signer_certificate: None,
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
                attestation: None,
                signer_certificate: None,
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
        None,
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
            let input: String = ctx.input().unwrap();
            Ok(Some(format!("\"got: {input}\"")))
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
            Ok(c.or(b).or(a))
        })
    });

    // Round 1: should schedule step_a
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

    // Round 2: step_a completed → schedule step_b
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

    // Round 3: step_a + step_b completed → schedule step_c
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

    // Round 4: all completed → orchestrator completes
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
                    Ok(Some(format!("\"caught: {message}\"")))
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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

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
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

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
            let count = results.iter().filter(|r| r.is_some()).count();
            Ok(Some(format!("{count}")))
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
            Ok(Some(format!("{winner}")))
        })
    });

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
            Ok(Some(format!("{winner}")))
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
    assert!(!cw.carryover_events.is_empty());
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

    assert!(resp.actions.is_empty());
}

#[tokio::test]
async fn test_suspend_and_resume() {
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { Ok(Some("\"resumed and done\"".to_string())) }));

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

    assert_eq!(resp.actions.len(), 1);
    match &resp.actions[0].workflow_action_type {
        Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) => {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Terminated as i32
            );
        }
        other => panic!("expected CompleteWorkflow, got {other:?}"),
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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.call_activity("activity_a", "input_a").await?;
            let b = ctx
                .call_activity("activity_b", a.as_deref().unwrap_or(""))
                .await?;
            Ok(b)
        })
    });

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
            Ok(Some(format!("\"{name}:{iid}\"")))
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
    // Activity result arrives in new_events (not old_events).
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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _a = ctx.call_activity("step1", ()).await?;
            ctx.create_timer(std::time::Duration::from_secs(10)).await?;
            let b = ctx.call_activity("step2", ()).await?;
            Ok(b)
        })
    });

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

    assert!(get_complete_action(&resp.actions).is_none());
    let timers = get_timer_actions(&resp.actions);
    assert_eq!(timers.len(), 1, "should emit a tracking timer");
    assert!(
        matches!(
            &timers[0].origin,
            Some(proto::create_timer_action::Origin::ExternalEvent(e)) if e.name == "approval"
        ),
        "timer origin should be ExternalEvent(approval)"
    );
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
        None,
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

    for (i, action) in resp.actions.iter().enumerate() {
        assert_eq!(action.id, i as i32, "Action {i} should have id {i}");
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
    assert!(!children[0].instance_id.is_empty());
}

#[tokio::test]
async fn test_continue_as_new_with_activity_before() {
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

    assert_eq!(resp.actions.len(), 1);
    match &resp.actions[0].workflow_action_type {
        Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) => {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Terminated as i32
            );
            assert_eq!(cw.result, Some("\"terminated reason\"".to_string()));
        }
        other => panic!("expected CompleteWorkflow, got {other:?}"),
    }
}

#[tokio::test]
async fn test_mixed_event_types_in_replay() {
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
            make_task_scheduled(3, "fetch_data"),
            make_task_completed(4, 0, Some("\"fetched\"".to_string())),
            make_timer_created(5, fire_at),
            make_timer_fired(6, 1),
        ],
        vec![make_event_raised(
            "user_input",
            Some("\"clicked\"".to_string()),
        )],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
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
            Ok(Some(format!("{winner}")))
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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let opts = ActivityOptions::new()
                .with_retry_policy(RetryPolicy::new(3, Duration::from_secs(1)));
            let result = ctx.call_activity_with_options("flaky", (), opts).await?;
            Ok(result)
        })
    });

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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let opts = ActivityOptions::new()
                .with_retry_policy(RetryPolicy::new(2, Duration::from_secs(1)));
            ctx.call_activity_with_options("bad", (), opts).await?;
            Ok(None)
        })
    });

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
            make_sub_orchestration_created(3, "child_orch", "child-1"),
            make_sub_orchestration_failed(4, 0, "ChildError", "child failed"),
            make_timer_created(5, ts_now() + chrono::Duration::seconds(2)),
            make_timer_fired(6, 1),
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

// ===========================================================================
// History propagation
// ===========================================================================

use dapr_durabletask::api::{HistoryPropagationScope, PropagatedHistory};
use dapr_durabletask_proto::prost::Message as _;

fn make_propagated_history(
    scope: proto::HistoryPropagationScope,
    chunks: Vec<(&str, &str, &str, Vec<i32>)>,
) -> proto::PropagatedHistory {
    let chunks = chunks
        .into_iter()
        .map(|(app, inst, name, ev_ids)| proto::PropagatedHistoryChunk {
            raw_events: ev_ids
                .into_iter()
                .map(|id| {
                    proto::HistoryEvent {
                        event_id: id,
                        timestamp: None,
                        router: None,
                        event_type: None,
                    }
                    .encode_to_vec()
                })
                .collect(),
            app_id: app.to_string(),
            instance_id: inst.to_string(),
            workflow_name: name.to_string(),
            raw_signatures: vec![],
            signing_cert_chains: vec![],
        })
        .collect();
    proto::PropagatedHistory {
        scope: scope as i32,
        chunks,
    }
}

#[tokio::test]
async fn test_schedule_activity_emits_history_propagation_scope() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _ = ctx
                .call_activity_with_options(
                    "verify",
                    serde_json::Value::Null,
                    ActivityOptions::new()
                        .with_history_propagation(HistoryPropagationScope::Lineage),
                )
                .await;
            Ok(None)
        })
    });

    let ts = chrono::Utc::now();
    let old_events = vec![
        make_workflow_started(ts),
        make_execution_started("test", None),
    ];
    let resp = run_executor(&orch_fn, old_events, vec![]).await.unwrap();

    let scheduled = get_schedule_actions(&resp.actions);
    assert_eq!(scheduled.len(), 1);
    assert_eq!(
        scheduled[0].history_propagation_scope,
        Some(proto::HistoryPropagationScope::Lineage as i32)
    );
}

#[tokio::test]
async fn test_schedule_child_workflow_emits_own_history_scope() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _ = ctx
                .call_sub_orchestrator_with_options(
                    "child",
                    serde_json::Value::Null,
                    SubOrchestratorOptions::new()
                        .with_instance_id("child-1")
                        .with_history_propagation(HistoryPropagationScope::OwnHistory),
                )
                .await;
            Ok(None)
        })
    });

    let ts = chrono::Utc::now();
    let old_events = vec![
        make_workflow_started(ts),
        make_execution_started("test", None),
    ];
    let resp = run_executor(&orch_fn, old_events, vec![]).await.unwrap();

    let children = get_child_workflow_actions(&resp.actions);
    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].history_propagation_scope,
        Some(proto::HistoryPropagationScope::OwnHistory as i32)
    );
}

#[tokio::test]
async fn test_no_history_propagation_scope_when_unset() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _ = ctx.call_activity("plain", "x").await;
            Ok(None)
        })
    });
    let ts = chrono::Utc::now();
    let old_events = vec![
        make_workflow_started(ts),
        make_execution_started("test", None),
    ];
    let resp = run_executor(&orch_fn, old_events, vec![]).await.unwrap();
    let scheduled = get_schedule_actions(&resp.actions);
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].history_propagation_scope, None);
}

#[tokio::test]
async fn test_propagated_history_lineage_visible_to_child_workflow() {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    let captured: StdArc<StdMutex<Option<StdArc<PropagatedHistory>>>> =
        StdArc::new(StdMutex::new(None));
    let captured_clone = captured.clone();
    let orch_fn: OrchestratorFn = Arc::new(move |ctx| {
        let captured = captured_clone.clone();
        Box::pin(async move {
            *captured.lock().unwrap() = ctx.propagated_history();
            Ok(None)
        })
    });

    let propagated = make_propagated_history(
        proto::HistoryPropagationScope::Lineage,
        vec![
            ("grandparent-app", "gp-inst", "Grandparent", vec![1, 2]),
            ("parent-app", "parent-inst", "Parent", vec![3, 4, 5]),
        ],
    );

    let ts = chrono::Utc::now();
    let old_events = vec![
        make_workflow_started(ts),
        make_execution_started("child", None),
    ];

    let _resp = OrchestrationExecutor::execute(
        &orch_fn,
        "child-1",
        old_events,
        vec![],
        String::new(),
        &WorkerOptions::default(),
        PropagatedHistory::from_proto(propagated),
    )
    .await
    .unwrap();

    let history = captured
        .lock()
        .unwrap()
        .clone()
        .expect("propagated history present");
    assert_eq!(history.scope, HistoryPropagationScope::Lineage);
    assert_eq!(history.chunks.len(), 2);
    assert_eq!(history.events.len(), 5);
    assert_eq!(
        history.app_ids(),
        vec!["grandparent-app".to_string(), "parent-app".to_string()]
    );
    assert_eq!(
        history.workflow_by_name("Grandparent").unwrap().event_count,
        2
    );
    assert_eq!(history.events_by_workflow_name("Parent").unwrap().len(), 3);
}

#[tokio::test]
async fn test_propagated_history_own_history_drops_ancestors() {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    let captured: StdArc<StdMutex<Option<StdArc<PropagatedHistory>>>> =
        StdArc::new(StdMutex::new(None));
    let captured_clone = captured.clone();
    let orch_fn: OrchestratorFn = Arc::new(move |ctx| {
        let captured = captured_clone.clone();
        Box::pin(async move {
            *captured.lock().unwrap() = ctx.propagated_history();
            Ok(None)
        })
    });

    let propagated = make_propagated_history(
        proto::HistoryPropagationScope::OwnHistory,
        vec![("parent-app", "parent-inst", "Parent", vec![10, 11])],
    );

    let ts = chrono::Utc::now();
    let old_events = vec![
        make_workflow_started(ts),
        make_execution_started("child", None),
    ];

    let _resp = OrchestrationExecutor::execute(
        &orch_fn,
        "child-1",
        old_events,
        vec![],
        String::new(),
        &WorkerOptions::default(),
        PropagatedHistory::from_proto(propagated),
    )
    .await
    .unwrap();

    let history = captured
        .lock()
        .unwrap()
        .clone()
        .expect("propagated history present");
    assert_eq!(history.scope, HistoryPropagationScope::OwnHistory);
    assert_eq!(history.chunks.len(), 1);
    assert_eq!(history.app_ids(), vec!["parent-app".to_string()]);
    assert!(
        history.workflow_by_name("Grandparent").is_err(),
        "OwnHistory must drop ancestor chunks"
    );
    assert_eq!(history.events_by_app_id("parent-app").unwrap().len(), 2);
}

#[tokio::test]
async fn test_propagated_history_absent_returns_none() {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    let initial = StdArc::new(PropagatedHistory {
        scope: HistoryPropagationScope::OwnHistory,
        events: vec![],
        chunks: vec![],
    });
    let captured: StdArc<StdMutex<Option<StdArc<PropagatedHistory>>>> =
        StdArc::new(StdMutex::new(Some(initial)));
    let captured_clone = captured.clone();
    let orch_fn: OrchestratorFn = Arc::new(move |ctx| {
        let captured = captured_clone.clone();
        Box::pin(async move {
            *captured.lock().unwrap() = ctx.propagated_history();
            Ok(None)
        })
    });
    let ts = chrono::Utc::now();
    let _ = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts),
            make_execution_started("child", None),
        ],
        vec![],
    )
    .await
    .unwrap();
    assert!(captured.lock().unwrap().is_none());
}

// ===========================================================================
// External event timer origins
// ===========================================================================

use dapr_durabletask::api::ExternalEventResult;

fn make_timer_created_with_origin(
    event_id: i32,
    fire_at: chrono::DateTime<chrono::Utc>,
    origin: Option<proto::timer_created_event::Origin>,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::TimerCreated(proto::TimerCreatedEvent {
            fire_at: Some(to_timestamp(fire_at)),
            name: None,
            rerun_parent_instance_info: None,
            origin,
        })),
    }
}

#[tokio::test]
async fn test_external_event_with_timeout_event_wins() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .wait_for_external_event_with_timeout(
                    "approval",
                    std::time::Duration::from_secs(30),
                )
                .await?;
            match result {
                ExternalEventResult::Received(data) => Ok(data),
                ExternalEventResult::TimedOut => Ok(Some("\"timed_out\"".to_string())),
            }
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(30);
    let origin =
        proto::timer_created_event::Origin::ExternalEvent(proto::TimerOriginExternalEvent {
            name: "approval".to_string(),
        });
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created_with_origin(3, fire_at, Some(origin)),
        ],
        vec![make_event_raised("approval", Some("\"yes\"".to_string()))],
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
async fn test_external_event_with_timeout_timer_wins() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .wait_for_external_event_with_timeout(
                    "approval",
                    std::time::Duration::from_secs(30),
                )
                .await?;
            match result {
                ExternalEventResult::Received(data) => Ok(data),
                ExternalEventResult::TimedOut => Ok(Some("\"timed_out\"".to_string())),
            }
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(30);
    let origin =
        proto::timer_created_event::Origin::ExternalEvent(proto::TimerOriginExternalEvent {
            name: "approval".to_string(),
        });
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created_with_origin(3, fire_at, Some(origin)),
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
    assert_eq!(cw.result, Some("\"timed_out\"".to_string()));
}

#[tokio::test]
async fn test_external_event_with_timeout_immediate_event() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .wait_for_external_event_with_timeout(
                    "approval",
                    std::time::Duration::from_secs(60),
                )
                .await?;
            match result {
                ExternalEventResult::Received(data) => Ok(data),
                ExternalEventResult::TimedOut => Ok(Some("\"timed_out\"".to_string())),
            }
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("approval", Some("\"instant\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"instant\"".to_string()));
}

#[tokio::test]
async fn test_wait_for_external_event_emits_far_future_timer_in_new_execution() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("approval", Some("\"yes\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(cw.result, Some("\"yes\"".to_string()));

    let timers = get_timer_actions(&resp.actions);
    assert_eq!(timers.len(), 1);
    match &timers[0].origin {
        Some(proto::create_timer_action::Origin::ExternalEvent(e)) => {
            assert_eq!(e.name, "approval");
        }
        other => panic!("expected ExternalEvent origin, got {other:?}"),
    }
    let fire_at = timers[0].fire_at.as_ref().unwrap();
    let dt = chrono::DateTime::from_timestamp(fire_at.seconds, fire_at.nanos as u32).unwrap();
    assert!(dt.year() >= 9999, "fire_at should be far-future");
}

#[tokio::test]
async fn test_generic_timer_has_no_external_event_origin() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _timer = ctx.create_timer(std::time::Duration::from_secs(10));
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

    let timers = get_timer_actions(&resp.actions);
    assert_eq!(timers.len(), 1);
    assert!(
        timers[0].origin.is_none(),
        "generic timer should have no origin"
    );
}

#[tokio::test]
async fn test_wait_for_external_event_with_timeout_action_origin() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .wait_for_external_event_with_timeout(
                    "my_event",
                    std::time::Duration::from_secs(45),
                )
                .await?;
            match result {
                ExternalEventResult::Received(data) => Ok(data),
                ExternalEventResult::TimedOut => Ok(Some("\"timeout\"".to_string())),
            }
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("my_event", Some("\"payload\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(cw.result, Some("\"payload\"".to_string()));

    let timers = get_timer_actions(&resp.actions);
    assert_eq!(timers.len(), 1);
    match &timers[0].origin {
        Some(proto::create_timer_action::Origin::ExternalEvent(e)) => {
            assert_eq!(e.name, "my_event");
        }
        other => panic!("expected ExternalEvent origin, got {other:?}"),
    }
    let fire_at = timers[0].fire_at.as_ref().unwrap();
    let dt = chrono::DateTime::from_timestamp(fire_at.seconds, fire_at.nanos as u32).unwrap();
    assert!(dt.year() < 9999, "should not be far-future");
}

#[tokio::test]
async fn test_external_event_with_timeout_backwards_compat_no_origin() {
    // Old history without origin on TimerCreatedEvent must still replay correctly.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .wait_for_external_event_with_timeout(
                    "approval",
                    std::time::Duration::from_secs(30),
                )
                .await?;
            match result {
                ExternalEventResult::Received(data) => Ok(data),
                ExternalEventResult::TimedOut => Ok(Some("\"timed_out\"".to_string())),
            }
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(30);
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created_with_origin(3, fire_at, None),
        ],
        vec![make_event_raised("approval", Some("\"yes\"".to_string()))],
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
async fn test_wait_for_external_event_replay_with_tracking_timer() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    let far_future = chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
        .unwrap()
        .and_hms_opt(23, 59, 59)
        .unwrap()
        .and_utc();

    let origin =
        proto::timer_created_event::Origin::ExternalEvent(proto::TimerOriginExternalEvent {
            name: "approval".to_string(),
        });
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started_with_patches(
                ts_now(),
                vec!["dapr:external-event-timer".to_string()],
            ),
            make_execution_started("test_orch", None),
            make_timer_created_with_origin(3, far_future, Some(origin)),
        ],
        vec![make_event_raised("approval", Some("\"hello\"".to_string()))],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"hello\"".to_string()));
}

#[tokio::test]
async fn test_version_patches_populated_in_response() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("approval", Some("\"yes\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(cw.result, Some("\"yes\"".to_string()));

    let version = resp.version.as_ref().expect("version should be set");
    assert!(
        version
            .patches
            .contains(&"dapr:external-event-timer".to_string()),
        "patches should include external-event-timer, got: {:?}",
        version.patches,
    );
}

#[tokio::test]
async fn test_no_version_patches_when_no_patch_applied() {
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { Ok(Some("\"done\"".to_string())) }));

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    assert!(
        resp.version.is_none(),
        "version should be None when no patches applied"
    );
}

// ===========================================================================
// Regression: replay / history edge cases
// ===========================================================================

#[tokio::test]
async fn test_duplicate_task_completion_is_idempotent() {
    // Two TaskCompleted events for the same sequence must not panic or
    // double-complete — the second one is silently ignored.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("act", ()).await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "act"),
            make_task_completed(4, 0, Some("\"first\"".to_string())),
            // Duplicate completion for same sequence id 0
            make_task_completed(5, 0, Some("\"second\"".to_string())),
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
    assert_eq!(cw.result, Some("\"first\"".to_string()));
}

#[tokio::test]
async fn test_duplicate_timer_fired_is_idempotent() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(10)).await?;
            Ok(Some("\"ok\"".to_string()))
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(10);
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created(3, fire_at),
            make_timer_fired(4, 0),
            make_timer_fired(5, 0),
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
async fn test_timer_fired_in_new_events() {
    // Timer result arrives in new_events (not old_events).
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(5)).await?;
            Ok(Some("\"timer done\"".to_string()))
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(5);
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created(3, fire_at),
        ],
        vec![make_timer_fired(4, 0)],
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

#[tokio::test]
async fn test_sub_orchestration_completion_in_new_events() {
    // Sub-orchestration result arrives in new_events.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .call_sub_orchestrator("child", (), Some("child-1"))
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_sub_orchestration_created(3, "child", "child-1"),
        ],
        vec![make_sub_orchestration_completed(
            4,
            0,
            Some("\"child new\"".to_string()),
        )],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"child new\"".to_string()));
}

#[tokio::test]
async fn test_sub_orchestration_failure_in_new_events() {
    // Sub-orchestration failure arrives in new_events.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .call_sub_orchestrator("child", (), Some("child-1"))
                .await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_sub_orchestration_created(3, "child", "child-1"),
        ],
        vec![make_sub_orchestration_failed(
            4,
            0,
            "ChildCrash",
            "child crashed",
        )],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_type, "ChildCrash");
}

// ===========================================================================
// Regression: terminate prevents orchestrator execution
// ===========================================================================

#[tokio::test]
async fn test_terminate_prevents_orchestrator_execution() {
    let orch_fn: OrchestratorFn = Arc::new(|_ctx| Box::pin(async { panic!("should not run") }));

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "some_activity"),
        ],
        vec![make_terminated(Some("\"forced\"".to_string()))],
    )
    .await
    .unwrap();

    assert_eq!(resp.actions.len(), 1);
    match &resp.actions[0].workflow_action_type {
        Some(proto::workflow_action::WorkflowActionType::CompleteWorkflow(cw)) => {
            assert_eq!(
                cw.workflow_status,
                proto::OrchestrationStatus::Terminated as i32
            );
            assert_eq!(cw.result, Some("\"forced\"".to_string()));
        }
        other => panic!("expected CompleteWorkflow(Terminated), got {other:?}"),
    }
}

// ===========================================================================
// Regression: sub-orchestration failure caught + recovery
// ===========================================================================

#[tokio::test]
async fn test_sub_orchestration_failure_caught() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx
                .call_sub_orchestrator("risky_child", (), Some("c-1"))
                .await;
            match result {
                Ok(v) => Ok(v),
                Err(DurableTaskError::TaskFailed { .. }) => {
                    let compensated = ctx.call_activity("fallback", ()).await?;
                    Ok(compensated)
                }
                Err(e) => Err(e),
            }
        })
    });

    let resp1 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_sub_orchestration_created(3, "risky_child", "c-1"),
            make_sub_orchestration_failed(4, 0, "ChildErr", "child blew up"),
        ],
        vec![],
    )
    .await
    .unwrap();

    let sched = get_schedule_actions(&resp1.actions);
    assert_eq!(sched.len(), 1);
    assert_eq!(sched[0].name, "fallback");
    assert!(get_complete_action(&resp1.actions).is_none());

    let resp2 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_sub_orchestration_created(3, "risky_child", "c-1"),
            make_sub_orchestration_failed(4, 0, "ChildErr", "child blew up"),
            make_task_scheduled(5, "fallback"),
            make_task_completed(6, 1, Some("\"recovered\"".to_string())),
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
    assert_eq!(cw.result, Some("\"recovered\"".to_string()));
}

// ===========================================================================
// Regression: multiple same-name events consumed in FIFO order
// ===========================================================================

#[tokio::test]
async fn test_same_name_events_consumed_fifo() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let first = ctx.wait_for_external_event("signal").await?;
            let second = ctx.wait_for_external_event("signal").await?;
            let first: String =
                serde_json::from_str(first.as_deref().expect("first signal payload")).unwrap();
            let second: String =
                serde_json::from_str(second.as_deref().expect("second signal payload")).unwrap();
            Ok(Some(
                serde_json::to_string(&format!("{first},{second}")).unwrap(),
            ))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("signal", Some("\"A\"".to_string())),
            make_event_raised("signal", Some("\"B\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"A,B\"".to_string()));
}

// ===========================================================================
// Regression: is_replaying flag
// ===========================================================================

#[tokio::test]
async fn test_is_replaying_true_during_replay() {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};
    let was_replaying: StdArc<StdMutex<Option<bool>>> = StdArc::new(StdMutex::new(None));
    let was_replaying_clone = was_replaying.clone();

    let orch_fn: OrchestratorFn = Arc::new(move |ctx| {
        let was_replaying = was_replaying_clone.clone();
        Box::pin(async move {
            let result = ctx.call_activity("act", ()).await?;
            *was_replaying.lock().unwrap() = Some(ctx.is_replaying());
            Ok(result)
        })
    });

    let _resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "act"),
            make_task_completed(4, 0, Some("\"ok\"".to_string())),
        ],
        vec![],
    )
    .await
    .unwrap();

    assert_eq!(*was_replaying.lock().unwrap(), Some(true));
}

#[tokio::test]
async fn test_is_replaying_false_on_new_execution() {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};
    let was_replaying: StdArc<StdMutex<Option<bool>>> = StdArc::new(StdMutex::new(None));
    let was_replaying_clone = was_replaying.clone();

    let orch_fn: OrchestratorFn = Arc::new(move |ctx| {
        let was_replaying = was_replaying_clone.clone();
        Box::pin(async move {
            *was_replaying.lock().unwrap() = Some(ctx.is_replaying());
            Ok(Some("\"done\"".to_string()))
        })
    });

    let _resp = run_executor(
        &orch_fn,
        vec![],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    assert_eq!(*was_replaying.lock().unwrap(), Some(false));
}

// ===========================================================================
// Regression: current_utc_datetime set from WorkflowStarted
// ===========================================================================

#[tokio::test]
async fn test_current_utc_datetime_from_workflow_started() {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};
    let captured_dt: StdArc<StdMutex<Option<chrono::DateTime<chrono::Utc>>>> =
        StdArc::new(StdMutex::new(None));
    let captured_clone = captured_dt.clone();

    let fixed_ts = chrono::DateTime::parse_from_rfc3339("2025-03-15T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let orch_fn: OrchestratorFn = Arc::new(move |ctx| {
        let captured = captured_clone.clone();
        Box::pin(async move {
            *captured.lock().unwrap() = Some(ctx.current_utc_datetime());
            Ok(None)
        })
    });

    let _resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(fixed_ts)],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    let dt = captured_dt.lock().unwrap().unwrap();
    assert_eq!(
        dt, fixed_ts,
        "current_utc_datetime should match WorkflowStarted timestamp"
    );
}

// ===========================================================================
// Regression: suspend with pending activity, then resume + complete
// ===========================================================================

#[tokio::test]
async fn test_suspend_with_pending_activity_then_resume_complete() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("slow_task", ()).await?;
            Ok(result)
        })
    });

    let resp1 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "slow_task"),
        ],
        vec![make_suspended()],
    )
    .await
    .unwrap();

    assert!(resp1.actions.is_empty(), "suspended → no actions");

    let resp2 = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "slow_task"),
            make_suspended(),
            make_resumed(),
            make_task_completed(4, 0, Some("\"finally\"".to_string())),
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
    assert_eq!(cw.result, Some("\"finally\"".to_string()));
}

// ===========================================================================
// Regression: activity completing with None result
// ===========================================================================

#[tokio::test]
async fn test_activity_completion_with_none_result() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("void_act", ()).await?;
            Ok(Some(serde_json::to_string(&result.is_none()).unwrap()))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "void_act"),
            make_task_completed(4, 0, None),
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
    assert_eq!(cw.result, Some("true".to_string()));
}

// ===========================================================================
// Regression: when_all with all tasks failing
// ===========================================================================

#[tokio::test]
async fn test_fan_out_all_fail() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let mut tasks = Vec::new();
            for i in 0..3 {
                tasks.push(ctx.call_activity("bad_worker", i));
            }
            let results = when_all(tasks).await?;
            Ok(Some(format!("{}", results.len())))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "bad_worker"),
            make_task_failed(4, 0, "Err", "fail 0"),
            make_task_scheduled(5, "bad_worker"),
            make_task_failed(6, 1, "Err", "fail 1"),
            make_task_scheduled(7, "bad_worker"),
            make_task_failed(8, 2, "Err", "fail 2"),
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
    assert_eq!(fd.error_message, "fail 0");
}

// ===========================================================================
// Regression: when_any where the winning task is a failure
// ===========================================================================

#[tokio::test]
async fn test_when_any_failure_wins() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let t0 = ctx.call_activity("slow", ());
            let t1 = ctx.call_activity("fails_fast", ());
            let winner = when_any(vec![t0, t1]).await?;
            Ok(Some(format!("\"won: {winner}\"")))
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "slow"),
            make_task_scheduled(4, "fails_fast"),
            make_task_failed(5, 1, "FastFail", "boom fast"),
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
    assert_eq!(cw.result, Some("\"won: 1\"".to_string()));
}

// ===========================================================================
// Regression: event buffer limits
// ===========================================================================

#[tokio::test]
async fn test_event_buffer_per_name_limit() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.wait_for_external_event("sig").await?;
            let b = ctx.wait_for_external_event("sig").await?;
            let c = ctx.wait_for_external_event("sig").await?;
            let combined = format!(
                "\"{},{},{}\"",
                a.as_deref().unwrap_or("none"),
                b.as_deref().unwrap_or("none"),
                c.as_deref().unwrap_or("none")
            );
            Ok(Some(combined))
        })
    });

    let options = WorkerOptions::new().with_max_events_per_name(2);
    let resp = OrchestrationExecutor::execute(
        &orch_fn,
        "test-instance",
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("sig", Some("\"1\"".to_string())),
            make_event_raised("sig", Some("\"2\"".to_string())),
            make_event_raised("sig", Some("\"3\"".to_string())),
        ],
        String::new(),
        &options,
        None,
    )
    .await
    .unwrap();

    assert!(
        get_complete_action(&resp.actions).is_none(),
        "should be pending — third event was discarded"
    );
}

#[tokio::test]
async fn test_event_buffer_name_limit() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.wait_for_external_event("alpha").await?;
            let b = ctx.wait_for_external_event("beta").await?;
            Ok(Some(format!(
                "\"{},{}\"",
                a.as_deref().unwrap_or("none"),
                b.as_deref().unwrap_or("none")
            )))
        })
    });

    let options = WorkerOptions::new().with_max_event_names(1);
    let resp = OrchestrationExecutor::execute(
        &orch_fn,
        "test-instance",
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("alpha", Some("\"A\"".to_string())),
            make_event_raised("beta", Some("\"B\"".to_string())),
        ],
        String::new(),
        &options,
        None,
    )
    .await
    .unwrap();

    assert!(
        get_complete_action(&resp.actions).is_none(),
        "should be pending — beta event was discarded"
    );
}

// ===========================================================================
// Regression: activity failure arriving in new_events
// ===========================================================================

#[tokio::test]
async fn test_activity_failure_in_new_events() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.call_activity("flakey", ()).await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "flakey"),
        ],
        vec![make_task_failed(4, 0, "NewEventFail", "new event failure")],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_type, "NewEventFail");
    assert_eq!(fd.error_message, "new event failure");
}

// ===========================================================================
// Regression: replay consistency — replaying same history twice
// ===========================================================================

#[tokio::test]
async fn test_replay_same_history_produces_consistent_result() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let a = ctx.call_activity("step", ()).await?;
            ctx.create_timer(std::time::Duration::from_secs(1)).await?;
            let b = ctx.call_activity("step", ()).await?;
            let a: String =
                serde_json::from_str(a.as_deref().expect("first step payload")).unwrap();
            let b: String =
                serde_json::from_str(b.as_deref().expect("second step payload")).unwrap();
            Ok(Some(serde_json::to_string(&format!("{a},{b}")).unwrap()))
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(1);
    let history = vec![
        make_workflow_started(ts_now()),
        make_execution_started("test_orch", None),
        make_task_scheduled(3, "step"),
        make_task_completed(4, 0, Some("\"r1\"".to_string())),
        make_timer_created(5, fire_at),
        make_timer_fired(6, 1),
        make_task_scheduled(7, "step"),
        make_task_completed(8, 2, Some("\"r2\"".to_string())),
    ];

    let resp1 = run_executor(&orch_fn, history.clone(), vec![])
        .await
        .unwrap();
    let resp2 = run_executor(&orch_fn, history, vec![]).await.unwrap();

    let cw1 = get_complete_action(&resp1.actions).unwrap();
    let cw2 = get_complete_action(&resp2.actions).unwrap();
    assert_eq!(
        cw1.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw1.result, Some("\"r1,r2\"".to_string()));
    assert_eq!(cw1.result, cw2.result);
    assert_eq!(cw1.workflow_status, cw2.workflow_status);
    assert_eq!(resp1.actions.len(), resp2.actions.len());
}

// ===========================================================================
// Regression: external event arrives during replay in old_events
// ===========================================================================

#[tokio::test]
async fn test_external_event_in_old_events() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let result = ctx.wait_for_external_event("signal").await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_event_raised("signal", Some("\"old data\"".to_string())),
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
    assert_eq!(cw.result, Some("\"old data\"".to_string()));
}

// ===========================================================================
// Regression: activity sequence where middle step arrives in new_events
// ===========================================================================

#[tokio::test]
async fn test_activity_sequence_mid_step_in_new_events() {
    // A is in old_events, B arrives in new_events → should schedule C.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let _a = ctx.call_activity("step_a", ()).await?;
            let _b = ctx.call_activity("step_b", ()).await?;
            let c = ctx.call_activity("step_c", ()).await?;
            Ok(c)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(3, "step_a"),
            make_task_completed(4, 0, Some("\"a\"".to_string())),
            make_task_scheduled(5, "step_b"),
        ],
        vec![make_task_completed(6, 1, Some("\"b\"".to_string()))],
    )
    .await
    .unwrap();

    let sched = get_schedule_actions(&resp.actions);
    assert_eq!(sched.len(), 1);
    assert_eq!(sched[0].name, "step_c");
    assert!(get_complete_action(&resp.actions).is_none());
}

// ===========================================================================
// Regression: continue_as_new preserves save_events across suspend/resume
// ===========================================================================

#[tokio::test]
async fn test_continue_as_new_no_carryover_when_save_events_false() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.continue_as_new("next", false);
            Ok(None)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![
            make_execution_started("test_orch", None),
            make_event_raised("buffered_event", Some("\"data\"".to_string())),
        ],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::ContinuedAsNew as i32
    );
    assert!(
        cw.carryover_events.is_empty(),
        "save_events=false should produce no carryover"
    );
}

// ===========================================================================
// Regression: orchestrator panic becomes failure
// ===========================================================================

#[tokio::test]
async fn test_orchestrator_returning_task_failed_error() {
    let orch_fn: OrchestratorFn = Arc::new(|_ctx| {
        Box::pin(async {
            Err(DurableTaskError::TaskFailed {
                message: "manual fail".to_string(),
                failure_details: Some(dapr_durabletask::api::FailureDetails {
                    error_type: "ManualError".to_string(),
                    message: "manual fail".to_string(),
                    stack_trace: None,
                }),
            })
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
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_type, "ManualError");
    assert_eq!(fd.error_message, "manual fail");
}
