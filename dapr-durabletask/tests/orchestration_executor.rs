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
                retry_parent_instance_info: None,
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
            make_task_scheduled(0, "greet"),
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
            make_task_scheduled(0, "step_a"),
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
            make_task_scheduled(0, "step_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
            make_task_scheduled(1, "step_b"),
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
            make_task_scheduled(0, "step_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
            make_task_scheduled(1, "step_b"),
            make_task_completed(6, 1, Some("\"result_b\"".to_string())),
            make_task_scheduled(2, "step_c"),
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
            make_task_scheduled(0, "flaky"),
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
            make_task_scheduled(0, "flaky"),
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
            make_timer_created(0, fire_at),
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
            make_sub_orchestration_created(0, "child_orch", "child-1"),
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
            make_sub_orchestration_created(0, "child_orch", "child-1"),
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
            make_task_scheduled(0, "worker"),
            make_task_completed(4, 0, Some("\"r0\"".to_string())),
            make_task_scheduled(1, "worker"),
            make_task_completed(6, 1, Some("\"r1\"".to_string())),
            make_task_scheduled(2, "worker"),
            make_task_completed(8, 2, Some("\"r2\"".to_string())),
            make_task_scheduled(3, "worker"),
            make_task_completed(10, 3, Some("\"r3\"".to_string())),
            make_task_scheduled(4, "worker"),
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
            make_task_scheduled(0, "worker"),
            make_task_completed(4, 0, Some("\"r0\"".to_string())),
            make_task_scheduled(1, "worker"),
            make_task_completed(6, 1, Some("\"r1\"".to_string())),
            make_task_scheduled(2, "worker"),
            make_task_failed(8, 2, "WorkerError", "worker 2 crashed"),
            make_task_scheduled(3, "worker"),
            make_task_completed(10, 3, Some("\"r3\"".to_string())),
            make_task_scheduled(4, "worker"),
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
            make_task_scheduled(0, "slow"),
            make_task_scheduled(1, "fast"),
            make_task_completed(5, 1, Some("\"fast result\"".to_string())),
            make_task_scheduled(2, "medium"),
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
            make_task_scheduled(0, "slow_activity"),
            make_timer_created(1, fire_at),
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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        // Runs up to its first await; the terminate then withholds the activity.
        Box::pin(async move { ctx.call_activity("never_scheduled", ()).await })
    });

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
            make_task_scheduled(0, "risky_operation"),
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
            make_task_scheduled(0, "risky_operation"),
            make_task_failed(4, 0, "RiskyError", "it broke"),
            make_task_scheduled(1, "compensate"),
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
            make_task_scheduled(0, "activity_a"),
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
            make_task_scheduled(0, "activity_a"),
            make_task_completed(4, 0, Some("\"result_a\"".to_string())),
            make_task_scheduled(1, "activity_b"),
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
            make_task_scheduled(0, "greet"),
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
            make_task_scheduled(0, "step1"),
            make_task_completed(4, 0, Some("\"s1\"".to_string())),
            make_timer_created(1, fire_at),
            make_timer_fired(6, 1),
            make_task_scheduled(2, "step2"),
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

    // Left empty: the runtime assigns the deterministic `<parent>:0000` ID.
    let children = get_child_workflow_actions(&resp.actions);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].name, "child_orch");
    assert!(children[0].instance_id.is_empty());
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
            make_task_scheduled(0, "get_count"),
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
            make_task_scheduled(0, "step1"),
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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        // Runs up to its first await; the terminate then withholds the activity.
        Box::pin(async move { ctx.call_activity("never_scheduled", ()).await })
    });

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
            make_task_scheduled(0, "fetch_data"),
            make_task_completed(4, 0, Some("\"fetched\"".to_string())),
            make_timer_created(1, fire_at),
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
            make_task_scheduled(0, "fast_activity"),
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

    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();
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
            make_task_scheduled(0, "old_act"),
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
            make_task_scheduled(0, "new_act"),
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
            make_task_scheduled(0, "flaky"),
            make_task_failed(4, 0, "IOError", "transient"),
            make_timer_created(1, ts_now() + chrono::Duration::seconds(1)),
            make_timer_fired(6, 1),
            make_task_scheduled(2, "flaky"),
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
            make_task_scheduled(0, "bad"),
            make_task_failed(4, 0, "IOError", "still broken"),
            make_timer_created(1, ts_now() + chrono::Duration::seconds(1)),
            make_timer_fired(6, 1),
            make_task_scheduled(2, "bad"),
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
            make_task_scheduled(0, "fatal_act"),
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
            make_task_scheduled(0, "retryable_act"),
            make_task_failed(4, 0, "RetryableError", "try again"),
            make_timer_created(1, ts_now() + chrono::Duration::seconds(1)),
            make_timer_fired(6, 1),
            make_task_scheduled(2, "retryable_act"),
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
            make_sub_orchestration_created(0, "child_orch", "child-1"),
            make_sub_orchestration_failed(4, 0, "ChildError", "child failed"),
            make_timer_created(1, ts_now() + chrono::Duration::seconds(2)),
            make_timer_fired(6, 1),
            make_sub_orchestration_created(2, "child_orch", "child-1"),
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
            make_task_scheduled(0, "instant_ok"),
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
            make_timer_created_with_origin(0, fire_at, Some(origin)),
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
            make_timer_created_with_origin(0, fire_at, Some(origin)),
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

// The runtime rejects a turn that re-creates an operation already in history
// (daprd 1.18.4+), so in-flight actions must not be re-emitted on replay.
#[tokio::test]
async fn test_replay_does_not_reemit_scheduled_actions() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.call_activity("step_a", ()).await?;
            ctx.wait_for_external_event_with_timeout(
                "approval",
                std::time::Duration::from_secs(60),
            )
            .await?;
            ctx.call_activity("step_b", ()).await
        })
    });

    let fire_at = ts_now() + chrono::Duration::seconds(60);
    let origin =
        proto::timer_created_event::Origin::ExternalEvent(proto::TimerOriginExternalEvent {
            name: "approval".to_string(),
        });
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "step_a"),
            make_task_completed(-1, 0, None),
            make_timer_created_with_origin(1, fire_at, Some(origin)),
        ],
        vec![make_event_raised("approval", Some("\"yes\"".to_string()))],
    )
    .await
    .unwrap();

    assert!(get_timer_actions(&resp.actions).is_empty());
    let schedules = get_schedule_actions(&resp.actions);
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "step_b");
    assert_eq!(resp.actions[0].id, 2);
}

#[tokio::test]
async fn test_replay_does_not_reemit_in_flight_activity_or_child() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let activity = ctx.call_activity("step_a", ());
            let child = ctx.call_sub_orchestrator("child_orch", (), Some("child-1"));
            when_all(vec![activity, child]).await?;
            Ok(None)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "step_a"),
            make_sub_orchestration_created(1, "child_orch", "child-1"),
        ],
        vec![make_event_raised("unrelated", None)],
    )
    .await
    .unwrap();

    assert!(resp.actions.is_empty(), "{:?}", resp.actions);
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
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    assert!(get_complete_action(&resp.actions).is_none());

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
async fn test_wait_for_external_event_raised_in_same_batch_emits_timer() {
    // The event arrives in the same batch as ExecutionStarted but is only
    // applied after the orchestrator starts waiting; the wait still emits
    // its tracking timer, matching durabletask-go.
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
    assert_eq!(get_timer_actions(&resp.actions).len(), 1);
    assert!(resp.version.is_none());
}

#[tokio::test]
async fn test_generic_timer_has_create_timer_origin() {
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
        matches!(
            timers[0].origin,
            Some(proto::create_timer_action::Origin::CreateTimer(_))
        ),
        "generic timer should have the CreateTimer origin"
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
            make_timer_created_with_origin(0, fire_at, None),
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
            make_timer_created_with_origin(0, far_future, Some(origin)),
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
            let _ = ctx.is_patched("b-patch");
            let _ = ctx.is_patched("a-patch");
            let result = ctx.wait_for_external_event("approval").await?;
            Ok(result)
        })
    });

    // The patches are checked with the whole history processed, so they
    // apply and are recorded in the order they were first checked.
    let resp = run_executor(
        &orch_fn,
        vec![make_workflow_started(ts_now())],
        vec![make_execution_started("test_orch", None)],
    )
    .await
    .unwrap();

    assert!(get_complete_action(&resp.actions).is_none());

    let version = resp.version.as_ref().expect("version should be set");
    assert_eq!(version.patches, vec!["b-patch", "a-patch"]);
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
            make_task_scheduled(0, "act"),
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
            make_timer_created(0, fire_at),
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
            make_timer_created(0, fire_at),
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
            make_sub_orchestration_created(0, "child", "child-1"),
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
            make_sub_orchestration_created(0, "child", "child-1"),
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
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        // Replays up to its pending activity; the terminate stops it there.
        Box::pin(async move { ctx.call_activity("some_activity", ()).await })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "some_activity"),
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
            make_sub_orchestration_created(0, "risky_child", "c-1"),
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
            make_sub_orchestration_created(0, "risky_child", "c-1"),
            make_sub_orchestration_failed(4, 0, "ChildErr", "child blew up"),
            make_task_scheduled(1, "fallback"),
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
            make_task_scheduled(0, "act"),
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
            make_task_scheduled(0, "slow_task"),
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
            make_task_scheduled(0, "slow_task"),
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
            make_task_scheduled(0, "void_act"),
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
            make_task_scheduled(0, "bad_worker"),
            make_task_failed(4, 0, "Err", "fail 0"),
            make_task_scheduled(1, "bad_worker"),
            make_task_failed(6, 1, "Err", "fail 1"),
            make_task_scheduled(2, "bad_worker"),
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
            make_task_scheduled(0, "slow"),
            make_task_scheduled(1, "fails_fast"),
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
    // Events arrive while the orchestrator is blocked on a timer, so they are
    // buffered until it starts waiting for them.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(1)).await?;
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
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created(0, ts_now()),
        ],
        vec![
            make_event_raised("sig", Some("\"1\"".to_string())),
            make_event_raised("sig", Some("\"2\"".to_string())),
            make_event_raised("sig", Some("\"3\"".to_string())),
            make_timer_fired(10, 0),
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
    // Events arrive while the orchestrator is blocked on a timer, so they are
    // buffered until it starts waiting for them.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(1)).await?;
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
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created(0, ts_now()),
        ],
        vec![
            make_event_raised("alpha", Some("\"A\"".to_string())),
            make_event_raised("beta", Some("\"B\"".to_string())),
            make_timer_fired(10, 0),
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
            make_task_scheduled(0, "flakey"),
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
        make_task_scheduled(0, "step"),
        make_task_completed(4, 0, Some("\"r1\"".to_string())),
        make_timer_created(1, fire_at),
        make_timer_fired(6, 1),
        make_task_scheduled(2, "step"),
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
            make_task_scheduled(0, "step_a"),
            make_task_completed(4, 0, Some("\"a\"".to_string())),
            make_task_scheduled(1, "step_b"),
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

// ===========================================================================
// Replay compatibility: histories recorded by earlier releases and runtimes
// ===========================================================================

fn make_event_timer_created(
    event_id: i32,
    fire_at: chrono::DateTime<chrono::Utc>,
    event_name: &str,
    timer_name: Option<&str>,
) -> proto::HistoryEvent {
    proto::HistoryEvent {
        event_id,
        timestamp: Some(to_timestamp(ts_now())),
        router: None,
        event_type: Some(EventType::TimerCreated(proto::TimerCreatedEvent {
            fire_at: Some(to_timestamp(fire_at)),
            name: timer_name.map(str::to_string),
            rerun_parent_instance_info: None,
            origin: Some(proto::timer_created_event::Origin::ExternalEvent(
                proto::TimerOriginExternalEvent {
                    name: event_name.to_string(),
                },
            )),
        })),
    }
}

fn assert_nondeterminism(resp: &proto::WorkflowResponse) -> &proto::TaskFailureDetails {
    assert_eq!(resp.actions.len(), 1, "{:?}", resp.actions);
    let cw = get_complete_action(&resp.actions).expect("CompleteWorkflow");
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Failed as i32
    );
    let fd = cw.failure_details.as_ref().unwrap();
    assert_eq!(fd.error_type, "NonDeterminismError");
    fd
}

#[tokio::test]
async fn test_patches_reported_in_history_order_then_new() {
    // daprd stalls a workflow unless the patches recorded in history are a
    // prefix of the reported ones. Earlier releases recorded
    // `dapr:external-event-timer` (no longer checked) and sorted patches;
    // the durabletask-go backend records the full list on every turn.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(1)).await?;
            assert!(ctx.is_patched("zeta"));
            assert!(ctx.is_patched("alpha"));
            Ok(None)
        })
    });

    let recorded = vec!["zeta".to_string(), "dapr:external-event-timer".to_string()];
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started_with_patches(ts_now(), recorded.clone()),
            make_execution_started("test_orch", None),
            make_timer_created(0, ts_now()),
        ],
        vec![
            make_workflow_started_with_patches(ts_now(), recorded),
            make_timer_fired(10, 0),
        ],
    )
    .await
    .unwrap();

    let version = resp.version.as_ref().expect("version should be set");
    assert_eq!(
        version.patches,
        vec!["zeta", "dapr:external-event-timer", "alpha"]
    );
}

#[tokio::test]
async fn test_duplicate_scheduling_events_are_ignored() {
    // Releases before #51 re-emitted in-flight actions every turn, and older
    // runtimes persisted the duplicates.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let timer = ctx.create_timer(std::time::Duration::from_secs(1));
            let result = ctx.call_activity("a", ()).await?;
            timer.await?;
            Ok(result)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_timer_created(0, ts_now()),
            make_task_scheduled(1, "a"),
            make_workflow_started(ts_now()),
            make_timer_created(0, ts_now()),
            make_task_scheduled(1, "a"),
            make_timer_fired(10, 0),
        ],
        vec![make_task_completed(11, 1, Some("\"done\"".to_string()))],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"done\"".to_string()));
    assert_eq!(resp.actions.len(), 1, "{:?}", resp.actions);
}

#[tokio::test]
async fn test_absorbs_recorded_event_timer_before_user_timer() {
    // Earlier releases emitted the (unnamed) timeout timer even when the
    // event was already buffered; the current release does not. The user
    // timer that follows must still line up with its own TimerCreated.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.call_activity("A", ()).await?;
            let received = ctx
                .wait_for_external_event_with_timeout("ev", std::time::Duration::from_secs(30))
                .await?;
            ctx.create_timer(std::time::Duration::from_secs(1)).await?;
            match received {
                ExternalEventResult::Received(data) => Ok(data),
                ExternalEventResult::TimedOut => Ok(Some("\"timed out\"".to_string())),
            }
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "A"),
            make_workflow_started(ts_now()),
            make_event_raised("ev", Some("\"payload\"".to_string())),
            make_task_completed(10, 0, None),
            make_event_timer_created(1, ts_now() + chrono::Duration::seconds(30), "ev", None),
            make_timer_created(2, ts_now()),
        ],
        vec![make_workflow_started(ts_now()), make_timer_fired(11, 2)],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"payload\"".to_string()));
}

#[tokio::test]
async fn test_named_event_timer_is_not_absorbed() {
    // The current release names its event-wait timers, so a named one the
    // replay does not produce is a genuine divergence.
    let orch_fn: OrchestratorFn =
        Arc::new(|ctx| Box::pin(async move { ctx.call_activity("a", ()).await }));

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_event_timer_created(0, ts_now(), "ev", Some("ev")),
            make_task_scheduled(1, "a"),
        ],
        vec![],
    )
    .await
    .unwrap();
    assert_nondeterminism(&resp);
}

#[tokio::test]
async fn test_event_timer_does_not_match_user_timer() {
    // A recorded event-wait timer never retires a user timer at the same ID.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.create_timer(std::time::Duration::from_secs(1)).await?;
            Ok(None)
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_event_timer_created(0, ts_now(), "ev", Some("ev")),
        ],
        vec![],
    )
    .await
    .unwrap();
    assert_nondeterminism(&resp);
}

#[tokio::test]
async fn test_activity_name_mismatch_is_nondeterministic() {
    let orch_fn: OrchestratorFn =
        Arc::new(|ctx| Box::pin(async move { ctx.call_activity("b", ()).await }));

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "a"),
        ],
        vec![make_task_completed(10, 0, Some("\"a-result\"".to_string()))],
    )
    .await
    .unwrap();
    let fd = assert_nondeterminism(&resp);
    assert!(fd.error_message.contains("'a'"), "{}", fd.error_message);
    assert!(fd.error_message.contains("'b'"), "{}", fd.error_message);
}

#[tokio::test]
async fn test_nondeterminism_discards_other_actions() {
    // The replay returns before reaching the recorded activity: only the
    // failure is reported, not the earlier completion.
    let orch_fn: OrchestratorFn =
        Arc::new(|_ctx| Box::pin(async { Ok(Some("\"early\"".to_string())) }));

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "a"),
        ],
        vec![],
    )
    .await
    .unwrap();
    assert_nondeterminism(&resp);
}

#[tokio::test]
async fn test_panic_creating_orchestrator_future_fails_workflow() {
    let orch_fn: OrchestratorFn = Arc::new(
        |_ctx| -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = dapr_durabletask::worker::OrchestratorResult>
                    + Send,
            >,
        > { panic!("sync boom") },
    );

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
    assert_eq!(fd.error_type, "OrchestratorPanic");
    assert_eq!(fd.error_message, "panic: sync boom");
}

#[tokio::test]
async fn test_zero_delay_retry_still_retries() {
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            let opts =
                ActivityOptions::new().with_retry_policy(RetryPolicy::new(3, Duration::ZERO));
            ctx.call_activity_with_options("flaky", (), opts).await
        })
    });

    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "flaky"),
        ],
        vec![make_task_failed(10, 0, "Boom", "boom")],
    )
    .await
    .unwrap();

    assert!(
        get_complete_action(&resp.actions).is_none(),
        "{:?}",
        resp.actions
    );
    let timers = get_timer_actions(&resp.actions);
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].name.as_deref(), Some("flaky-retry"));
}

#[tokio::test]
async fn test_absorbs_originless_far_future_timer_before_user_timer() {
    // Runtimes that did not persist timer origins recorded the earlier
    // releases' indefinite event-wait timer only by its far-future fire time.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            ctx.call_activity("A", ()).await?;
            let data = ctx.wait_for_external_event("ev").await?;
            ctx.create_timer(std::time::Duration::from_secs(1)).await?;
            Ok(data)
        })
    });

    let far_future = chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
        .unwrap()
        .and_hms_opt(23, 59, 59)
        .unwrap()
        .and_utc();
    let resp = run_executor(
        &orch_fn,
        vec![
            make_workflow_started(ts_now()),
            make_execution_started("test_orch", None),
            make_task_scheduled(0, "A"),
            make_workflow_started(ts_now()),
            make_event_raised("ev", Some("\"payload\"".to_string())),
            make_task_completed(10, 0, None),
            make_timer_created(1, far_future),
            make_timer_created(2, ts_now()),
        ],
        vec![make_workflow_started(ts_now()), make_timer_fired(11, 2)],
    )
    .await
    .unwrap();

    let cw = get_complete_action(&resp.actions).unwrap();
    assert_eq!(
        cw.workflow_status,
        proto::OrchestrationStatus::Completed as i32
    );
    assert_eq!(cw.result, Some("\"payload\"".to_string()));
}

mod task_executor {
    //! Every test drives the public `OrchestrationExecutor::execute` API. Where the
    //! Go test inspects SDK-internal state that the Rust SDK does not expose, the
    //! nearest observable equivalent (emitted actions, completion status/result)
    //! is asserted instead and a comment says so.

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use dapr_durabletask::api::{DurableTaskError, ExternalEventResult, RetryPolicy};
    use dapr_durabletask::task::{ActivityOptions, CompletableTask};
    use dapr_durabletask::worker::{OrchestrationExecutor, OrchestratorFn, WorkerOptions};
    use dapr_durabletask_proto as proto;
    use dapr_durabletask_proto::history_event::EventType;
    use dapr_durabletask_proto::workflow_action::WorkflowActionType;
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    type Dt = chrono::DateTime<chrono::Utc>;

    // ---------------------------------------------------------------------------
    // Event construction helpers (mirroring the Go protos literals)
    // ---------------------------------------------------------------------------

    fn now() -> Dt {
        chrono::Utc::now()
    }

    fn ts(dt: Dt) -> proto::prost_types::Timestamp {
        proto::prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }
    }

    fn from_ts(t: &proto::prost_types::Timestamp) -> Dt {
        chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32).expect("valid timestamp")
    }

    fn ev(event_id: i32, at: Dt, event_type: EventType) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id,
            timestamp: Some(ts(at)),
            router: None,
            event_type: Some(event_type),
        }
    }

    fn workflow_started(at: Dt) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::WorkflowStarted(proto::WorkflowStartedEvent { version: None }),
        )
    }

    fn execution_started_at(name: &str, instance_id: &str, at: Dt) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: name.to_string(),
                version: None,
                input: None,
                workflow_instance: Some(proto::WorkflowInstance {
                    instance_id: instance_id.to_string(),
                    execution_id: Some(uuid::Uuid::new_v4().to_string()),
                }),
                parent_instance: None,
                scheduled_start_timestamp: None,
                parent_trace_context: None,
                workflow_span_id: None,
                tags: Default::default(),
            }),
        )
    }

    fn execution_started(name: &str, instance_id: &str) -> proto::HistoryEvent {
        execution_started_at(name, instance_id, now())
    }

    fn task_scheduled_at(event_id: i32, name: &str, at: Dt) -> proto::HistoryEvent {
        ev(
            event_id,
            at,
            EventType::TaskScheduled(proto::TaskScheduledEvent {
                name: name.to_string(),
                version: None,
                input: None,
                parent_trace_context: None,
                task_execution_id: String::new(),
                rerun_parent_instance_info: None,
                history_propagation_scope: None,
            }),
        )
    }

    fn task_scheduled(event_id: i32, name: &str) -> proto::HistoryEvent {
        task_scheduled_at(event_id, name, now())
    }

    fn task_completed_at(
        task_scheduled_id: i32,
        result: Option<&str>,
        at: Dt,
    ) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::TaskCompleted(proto::TaskCompletedEvent {
                task_scheduled_id,
                result: result.map(str::to_string),
                task_execution_id: String::new(),
                attestation: None,
                signer_certificate: None,
            }),
        )
    }

    fn task_completed(task_scheduled_id: i32, result: Option<&str>) -> proto::HistoryEvent {
        task_completed_at(task_scheduled_id, result, now())
    }

    fn task_failed_at(
        task_scheduled_id: i32,
        exec_id: &str,
        error_type: &str,
        message: &str,
        at: Dt,
    ) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::TaskFailed(proto::TaskFailedEvent {
                task_scheduled_id,
                failure_details: Some(proto::TaskFailureDetails {
                    error_type: error_type.to_string(),
                    error_message: message.to_string(),
                    stack_trace: None,
                    inner_failure: None,
                    is_non_retriable: false,
                }),
                task_execution_id: exec_id.to_string(),
                attestation: None,
                signer_certificate: None,
            }),
        )
    }

    /// Mirrors Go `evTaskFailed`.
    fn task_failed(task_scheduled_id: i32, exec_id: &str) -> proto::HistoryEvent {
        task_failed_at(
            task_scheduled_id,
            exec_id,
            "TestError",
            "injected failure",
            now(),
        )
    }

    fn timer_created_at(
        event_id: i32,
        fire_at: Dt,
        origin: Option<proto::timer_created_event::Origin>,
        at: Dt,
    ) -> proto::HistoryEvent {
        ev(
            event_id,
            at,
            EventType::TimerCreated(proto::TimerCreatedEvent {
                fire_at: Some(ts(fire_at)),
                name: None,
                rerun_parent_instance_info: None,
                origin,
            }),
        )
    }

    fn timer_fired_at(timer_id: i32, fire_at: Dt, at: Dt) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::TimerFired(proto::TimerFiredEvent {
                fire_at: Some(ts(fire_at)),
                timer_id,
            }),
        )
    }

    fn timer_fired(timer_id: i32) -> proto::HistoryEvent {
        let n = now();
        timer_fired_at(timer_id, n, n)
    }

    fn child_created_at(
        event_id: i32,
        name: &str,
        instance_id: &str,
        at: Dt,
    ) -> proto::HistoryEvent {
        ev(
            event_id,
            at,
            EventType::ChildWorkflowInstanceCreated(proto::ChildWorkflowInstanceCreatedEvent {
                instance_id: instance_id.to_string(),
                name: name.to_string(),
                version: None,
                input: None,
                parent_trace_context: None,
                rerun_parent_instance_info: None,
                history_propagation_scope: None,
                retry_parent_instance_info: None,
            }),
        )
    }

    fn child_completed_at(
        task_scheduled_id: i32,
        result: Option<&str>,
        at: Dt,
    ) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::ChildWorkflowInstanceCompleted(proto::ChildWorkflowInstanceCompletedEvent {
                task_scheduled_id,
                result: result.map(str::to_string),
                attestation: None,
                signer_certificate: None,
            }),
        )
    }

    fn child_completed(task_scheduled_id: i32, result: Option<&str>) -> proto::HistoryEvent {
        child_completed_at(task_scheduled_id, result, now())
    }

    /// Mirrors Go `evChildFailed`.
    fn child_failed(task_scheduled_id: i32) -> proto::HistoryEvent {
        ev(
            -1,
            now(),
            EventType::ChildWorkflowInstanceFailed(proto::ChildWorkflowInstanceFailedEvent {
                task_scheduled_id,
                failure_details: Some(proto::TaskFailureDetails {
                    error_type: "TestError".to_string(),
                    error_message: "injected child failure".to_string(),
                    stack_trace: None,
                    inner_failure: None,
                    is_non_retriable: false,
                }),
                attestation: None,
                signer_certificate: None,
            }),
        )
    }

    fn event_raised_at(name: &str, input: Option<&str>, at: Dt) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::EventRaised(proto::EventRaisedEvent {
                name: name.to_string(),
                input: input.map(str::to_string),
            }),
        )
    }

    fn event_raised(name: &str, input: Option<&str>) -> proto::HistoryEvent {
        event_raised_at(name, input, now())
    }

    fn suspended() -> proto::HistoryEvent {
        ev(
            -1,
            now(),
            EventType::ExecutionSuspended(proto::ExecutionSuspendedEvent { input: None }),
        )
    }

    fn resumed() -> proto::HistoryEvent {
        ev(
            -1,
            now(),
            EventType::ExecutionResumed(proto::ExecutionResumedEvent { input: None }),
        )
    }

    fn terminated_at(input: Option<&str>, at: Dt) -> proto::HistoryEvent {
        ev(
            -1,
            at,
            EventType::ExecutionTerminated(proto::ExecutionTerminatedEvent {
                input: input.map(str::to_string),
                recurse: false,
            }),
        )
    }

    fn terminated(input: Option<&str>) -> proto::HistoryEvent {
        terminated_at(input, now())
    }

    fn external_event_origin(name: &str) -> Option<proto::timer_created_event::Origin> {
        Some(proto::timer_created_event::Origin::ExternalEvent(
            proto::TimerOriginExternalEvent {
                name: name.to_string(),
            },
        ))
    }

    fn create_timer_origin() -> Option<proto::timer_created_event::Origin> {
        Some(proto::timer_created_event::Origin::CreateTimer(
            proto::TimerOriginCreateTimer {},
        ))
    }

    /// `time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)` from Go.
    fn go_infinite_fire_at() -> Dt {
        chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
            .unwrap()
            .and_hms_nano_opt(23, 59, 59, 999_999_999)
            .unwrap()
            .and_utc()
    }

    // ---------------------------------------------------------------------------
    // Execution / inspection helpers
    // ---------------------------------------------------------------------------

    async fn execute(
        orch_fn: &OrchestratorFn,
        instance_id: &str,
        old_events: Vec<proto::HistoryEvent>,
        new_events: Vec<proto::HistoryEvent>,
    ) -> dapr_durabletask::api::Result<proto::WorkflowResponse> {
        OrchestrationExecutor::execute(
            orch_fn,
            instance_id,
            old_events,
            new_events,
            String::new(),
            &WorkerOptions::default(),
            None,
        )
        .await
    }

    async fn run(
        orch_fn: &OrchestratorFn,
        instance_id: &str,
        old_events: Vec<proto::HistoryEvent>,
        new_events: Vec<proto::HistoryEvent>,
    ) -> proto::WorkflowResponse {
        execute(orch_fn, instance_id, old_events, new_events)
            .await
            .expect("executor must not return an error")
    }

    /// Mirrors Go `runBuffered` (instance id "buffered-test", logger captured).
    async fn run_buffered(
        orch_fn: &OrchestratorFn,
        old_events: Vec<proto::HistoryEvent>,
        new_events: Vec<proto::HistoryEvent>,
    ) -> (Vec<proto::WorkflowAction>, WarnCapture) {
        let cl = WarnCapture::default();
        let resp = {
            let _guard = cl.install();
            run(orch_fn, "buffered-test", old_events, new_events).await
        };
        (resp.actions, cl)
    }

    fn complete_action(
        actions: &[proto::WorkflowAction],
    ) -> Option<&proto::CompleteWorkflowAction> {
        actions.iter().find_map(|a| match &a.workflow_action_type {
            Some(WorkflowActionType::CompleteWorkflow(c)) => Some(c),
            _ => None,
        })
    }

    fn get_complete(a: &proto::WorkflowAction) -> Option<&proto::CompleteWorkflowAction> {
        match &a.workflow_action_type {
            Some(WorkflowActionType::CompleteWorkflow(c)) => Some(c),
            _ => None,
        }
    }

    fn get_create_timer(a: &proto::WorkflowAction) -> Option<&proto::CreateTimerAction> {
        match &a.workflow_action_type {
            Some(WorkflowActionType::CreateTimer(c)) => Some(c),
            _ => None,
        }
    }

    fn is_schedule_task(a: &proto::WorkflowAction) -> bool {
        matches!(
            &a.workflow_action_type,
            Some(WorkflowActionType::ScheduleTask(_))
        )
    }

    fn is_create_child(a: &proto::WorkflowAction) -> bool {
        matches!(
            &a.workflow_action_type,
            Some(WorkflowActionType::CreateChildWorkflow(_))
        )
    }

    fn count_actions(
        actions: &[proto::WorkflowAction],
        pred: impl Fn(&proto::WorkflowAction) -> bool,
    ) -> usize {
        actions.iter().filter(|a| pred(a)).count()
    }

    fn status(s: proto::OrchestrationStatus) -> i32 {
        s as i32
    }

    /// Mirrors Go `Task.Await(&out)`: decoding errors fail the orchestration
    /// rather than panicking the test.
    fn decode<T: DeserializeOwned>(raw: Option<String>) -> dapr_durabletask::api::Result<T> {
        Ok(serde_json::from_str(raw.as_deref().unwrap_or("null"))?)
    }

    fn encode<T: Serialize>(v: &T) -> Option<String> {
        Some(serde_json::to_string(v).expect("encodable payload"))
    }

    /// Go's `ErrTaskCanceled` message.
    const TASK_CANCELED: &str = "the task was canceled";

    /// Go's `emittedNotDispatched` assertion message.
    const EMITTED_NOT_DISPATCHED: &str =
        "the resolved schedule must still be emitted; the applier withholds its dispatch";

    // ---------------------------------------------------------------------------
    // Warning capture (Rust stand-in for Go's captureLogger)
    // ---------------------------------------------------------------------------

    /// Captures WARN-level `tracing` events emitted by the SDK while installed.
    ///
    /// Go's `captureLogger` receives only the workflow context's own replay
    /// diagnostics. The Rust executor additionally logs orchestration outcomes at
    /// WARN (e.g. "Orchestration failed due to task failure"); those are not replay
    /// diagnostics and are excluded so that `assert.Empty(cl.warns)` keeps its Go
    /// meaning.
    #[derive(Clone, Default)]
    struct WarnCapture {
        warns: Arc<Mutex<Vec<String>>>,
    }

    struct FieldCollector<'a>(&'a mut String);

    impl tracing::field::Visit for FieldCollector<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write;
            let _ = write!(self.0, "{}={:?} ", field.name(), value);
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN
                || !event.metadata().target().starts_with("dapr_durabletask")
            {
                return;
            }
            let mut line = String::new();
            event.record(&mut FieldCollector(&mut line));
            if line.contains("Orchestration failed") {
                return;
            }
            self.warns.lock().unwrap().push(line);
        }
    }

    impl WarnCapture {
        /// Installs the capture as the thread-default subscriber. `#[tokio::test]`
        /// uses a current-thread runtime and the executor never spawns, so every
        /// SDK event of the execution is observed.
        fn install(&self) -> tracing::subscriber::DefaultGuard {
            use tracing_subscriber::layer::SubscriberExt;
            tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
        }

        fn warns(&self) -> Vec<String> {
            self.warns.lock().unwrap().clone()
        }

        fn warns_containing(&self, sub: &str) -> usize {
            self.warns().iter().filter(|w| w.contains(sub)).count()
        }
    }

    // ===========================================================================
    // tests/task_executor_test.go
    // ===========================================================================

    #[tokio::test]
    async fn test_executor_wait_for_event_schedules_timer() {
        let timer_duration = Duration::from_secs(5);
        let orch: OrchestratorFn = Arc::new(move |ctx| {
            Box::pin(async move {
                let value: i32 = match ctx
                    .wait_for_external_event_with_timeout("MyEvent", timer_duration)
                    .await?
                {
                    ExternalEventResult::Received(v) => decode(v)?,
                    ExternalEventResult::TimedOut => 0,
                };
                Ok(encode(&value))
            })
        });

        let start_ts = now();
        let new_events = vec![
            workflow_started(start_ts),
            execution_started("Workflow", "abc123"),
        ];

        let resp = run(&orch, "abc123", vec![], new_events).await;
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a single action to be scheduled"
        );
        let ct = get_create_timer(&resp.actions[0])
            .expect("Expected the scheduled action to be a timer");
        assert_eq!(
            from_ts(ct.fire_at.as_ref().unwrap()),
            start_ts + chrono::Duration::from_std(timer_duration).unwrap()
        );
        assert_eq!(ct.name.as_deref(), Some("MyEvent"));
        match &ct.origin {
            Some(proto::create_timer_action::Origin::ExternalEvent(e)) => {
                assert_eq!(e.name, "MyEvent")
            }
            other => panic!("expected ExternalEvent origin, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_executor_wait_for_event_without_timeout_creates_infinite_timer() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let value: i32 = decode(ctx.wait_for_external_event("MyEvent").await?)?;
                Ok(encode(&value))
            })
        });

        let new_events = vec![
            workflow_started(now()),
            execution_started("Orchestration", "abc123"),
        ];
        let resp = run(&orch, "abc123", vec![], new_events).await;
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a timer to be created even for indefinite waits"
        );
        let ct = get_create_timer(&resp.actions[0]).expect("Expected the action to be a timer");
        assert_eq!(
            from_ts(ct.fire_at.as_ref().unwrap()),
            go_infinite_fire_at(),
            "indefinite-wait timer must fire at 9999-12-31T23:59:59.999999999Z"
        );
        match &ct.origin {
            Some(proto::create_timer_action::Origin::ExternalEvent(e)) => {
                assert_eq!(e.name, "MyEvent")
            }
            other => panic!("expected ExternalEvent origin, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_executor_create_timer_sets_create_timer_origin() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.create_timer(Duration::from_secs(5)).await?;
                Ok(None)
            })
        });

        let new_events = vec![
            workflow_started(now()),
            execution_started("Orchestration", "abc123"),
        ];
        let resp = run(&orch, "abc123", vec![], new_events).await;
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a single action to be scheduled"
        );
        let ct = get_create_timer(&resp.actions[0])
            .expect("Expected the scheduled action to be a timer");
        assert!(
            matches!(
                ct.origin,
                Some(proto::create_timer_action::Origin::CreateTimer(_))
            ),
            "Expected the timer action to carry CreateTimer origin, got {:?}",
            ct.origin
        );
    }

    #[tokio::test]
    async fn test_executor_wait_for_event_timer_fires_cancels_task() {
        // Go: WaitForSingleEvent(...).Await returns ErrTaskCanceled when the timer
        // wins and the orchestrator returns that error. Rust surfaces the same
        // outcome as `ExternalEventResult::TimedOut`, so the orchestrator maps it
        // to an error carrying Go's message; the test asserts the timeout was
        // observed and that the executor turns it into a single FAILED completion.
        let timer_duration = Duration::from_secs(5);
        let observed_timeout = Arc::new(AtomicBool::new(false));
        let flag = observed_timeout.clone();
        let orch: OrchestratorFn = Arc::new(move |ctx| {
            let flag = flag.clone();
            Box::pin(async move {
                match ctx
                    .wait_for_external_event_with_timeout("MyEvent", timer_duration)
                    .await?
                {
                    ExternalEventResult::Received(v) => {
                        let value: i32 = decode(v)?;
                        Ok(encode(&value))
                    }
                    ExternalEventResult::TimedOut => {
                        flag.store(true, Ordering::SeqCst);
                        Err(DurableTaskError::Other(TASK_CANCELED.to_string()))
                    }
                }
            })
        });

        let start = now();
        let fire = start + chrono::Duration::from_std(timer_duration).unwrap();
        let old_events = vec![
            workflow_started(start),
            execution_started_at("Orchestration", "abc123", start),
            timer_created_at(0, fire, None, start),
        ];
        let new_events = vec![workflow_started(fire), timer_fired_at(0, fire, fire)];

        let resp = run(&orch, "abc123", old_events, new_events).await;
        assert!(
            observed_timeout.load(Ordering::SeqCst),
            "the fired timer must cancel the event wait"
        );
        assert_eq!(resp.actions.len(), 1, "Expected a single completion action");
        let co = get_complete(&resp.actions[0]).expect("Expected a CompleteOrchestration action");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Failed)
        );
        let fd = co.failure_details.as_ref().expect("failure details");
        assert!(fd.error_message.contains(TASK_CANCELED));
    }

    #[tokio::test]
    async fn test_executor_suspend_stops_all_actions() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let _ = ctx
                    .wait_for_external_event_with_timeout("MyEvent", Duration::from_secs(5))
                    .await?;
                Ok(encode(&0))
            })
        });

        let new_events = vec![
            workflow_started(now()),
            execution_started("SuspendResumeWorkflow", "abc123"),
            suspended(),
        ];
        let resp = run(&orch, "abc123", vec![], new_events).await;
        assert!(
            resp.actions.is_empty(),
            "Suspended workflows should not have any actions"
        );
    }

    #[tokio::test]
    async fn test_executor_replay_pre_patch_indefinite_wait_for_event() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let _v: i32 = decode(ctx.wait_for_external_event("MyEvent").await?)?;
                let out: String = decode(ctx.call_activity("MyActivity", ()).await?)?;
                Ok(encode(&out))
            })
        });

        let start = now();
        let old_events = vec![
            workflow_started(start),
            execution_started_at("Orchestration", "abc123", start),
            event_raised_at("MyEvent", Some("42"), start),
            task_scheduled_at(0, "MyActivity", start),
        ];
        let new_events = vec![
            workflow_started(start),
            task_completed_at(0, Some(r#""ok""#), start),
        ];

        let resp = execute(&orch, "abc123", old_events, new_events)
            .await
            .expect("Replay of pre-patch history must not fail the determinism check");
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a single CompleteWorkflow action after replay"
        );
        let co = get_complete(&resp.actions[0]).expect("Expected CompleteWorkflow action");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""ok""#));
        for a in &resp.actions {
            assert!(
                get_create_timer(a).is_none(),
                "Optional CreateTimer must not be emitted on replay of a pre-patch history"
            );
        }
    }

    #[tokio::test]
    async fn test_executor_replay_post_patch_indefinite_wait_for_event() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let v: i32 = decode(ctx.wait_for_external_event("MyEvent").await?)?;
                Ok(encode(&v))
            })
        });

        let start = now();
        let old_events = vec![
            workflow_started(start),
            execution_started_at("Orchestration", "abc123", start),
            timer_created_at(
                0,
                go_infinite_fire_at(),
                external_event_origin("MyEvent"),
                start,
            ),
        ];
        let new_events = vec![
            workflow_started(start),
            event_raised_at("MyEvent", Some("42"), start),
        ];

        let resp = execute(&orch, "abc123", old_events, new_events)
            .await
            .expect("Post-patch replay must succeed");
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a single CompleteWorkflow action"
        );
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn test_executor_replay_pre_patch_indefinite_wait_for_event_child_workflow() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let _v: i32 = decode(ctx.wait_for_external_event("MyEvent").await?)?;
                let out: String = decode(ctx.call_sub_orchestrator("Child", (), None).await?)?;
                Ok(encode(&out))
            })
        });

        let start = now();
        let old_events = vec![
            workflow_started(start),
            execution_started_at("Parent", "parent-1", start),
            event_raised_at("MyEvent", Some("1"), start),
            child_created_at(0, "Child", "child-1", start),
        ];
        let new_events = vec![
            workflow_started(start),
            child_completed_at(0, Some(r#""child-result""#), start),
        ];

        let resp = execute(&orch, "parent-1", old_events, new_events)
            .await
            .expect("Replay of pre-patch history with a child workflow must not fail");
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a single CompleteWorkflow action"
        );
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""child-result""#));
        for a in &resp.actions {
            assert!(
                get_create_timer(a).is_none(),
                "Optional CreateTimer must not be emitted"
            );
        }
    }

    #[tokio::test]
    async fn test_executor_replay_pre_patch_indefinite_wait_for_event_real_timer() {
        let timer_duration = Duration::from_secs(5);
        let orch: OrchestratorFn = Arc::new(move |ctx| {
            Box::pin(async move {
                let v: i32 = decode(ctx.wait_for_external_event("MyEvent").await?)?;
                ctx.create_timer(timer_duration).await?;
                Ok(encode(&v))
            })
        });

        let start = now();
        let fire = start + chrono::Duration::from_std(timer_duration).unwrap();
        let old_events = vec![
            workflow_started(start),
            execution_started_at("Orchestration", "abc123", start),
            event_raised_at("MyEvent", Some("7"), start),
            timer_created_at(0, fire, create_timer_origin(), start),
        ];
        let new_events = vec![workflow_started(fire), timer_fired_at(0, fire, fire)];

        let resp = execute(&orch, "abc123", old_events, new_events)
            .await
            .expect("Replay of pre-patch history with a real timer must not fail");
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a single CompleteWorkflow action"
        );
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some("7"));
        for a in &resp.actions {
            assert!(
                get_create_timer(a).is_none(),
                "Optional CreateTimer must not be emitted"
            );
        }
    }

    #[tokio::test]
    async fn test_executor_replay_pre_patch_indefinite_wait_for_event_multiple() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let _v: i32 = decode(ctx.wait_for_external_event("EventA").await?)?;
                let out_a: String = decode(ctx.call_activity("ActA", ()).await?)?;
                let _v: i32 = decode(ctx.wait_for_external_event("EventB").await?)?;
                let out_b: String = decode(ctx.call_activity("ActB", ()).await?)?;
                Ok(encode(&format!("{out_a}{out_b}")))
            })
        });

        let start = now();
        let old_events = vec![
            workflow_started(start),
            execution_started_at("Orchestration", "abc123", start),
            event_raised_at("EventA", Some("1"), start),
            task_scheduled_at(0, "ActA", start),
            task_completed_at(0, Some(r#""a""#), start),
            event_raised_at("EventB", Some("2"), start),
            task_scheduled_at(1, "ActB", start),
        ];
        let new_events = vec![
            workflow_started(start),
            task_completed_at(1, Some(r#""b""#), start),
        ];

        let resp = execute(&orch, "abc123", old_events, new_events)
            .await
            .expect("Replay of pre-patch history with multiple indefinite waits must not fail");
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected a single CompleteWorkflow action"
        );
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""ab""#));
        for a in &resp.actions {
            assert!(
                get_create_timer(a).is_none(),
                "No optional CreateTimer must leak through"
            );
        }
    }

    fn ping_ping_orchestrator() -> OrchestratorFn {
        Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_activity("Ping", ()).await?;
                ctx.call_activity("Ping", ()).await?;
                Ok(encode(&"done"))
            })
        })
    }

    fn ping_scheduled_history(instance_id: &str) -> Vec<proto::HistoryEvent> {
        vec![
            workflow_started(now()),
            execution_started("Workflow", instance_id),
            task_scheduled(0, "Ping"),
        ]
    }

    #[tokio::test]
    async fn test_executor_terminate_stops_subsequent_events() {
        let orch = ping_ping_orchestrator();
        let new_events = vec![
            workflow_started(now()),
            terminated(Some(r#""stop""#)),
            task_completed(0, Some(r#""pong""#)),
        ];
        let resp = run(
            &orch,
            "abc123",
            ping_scheduled_history("abc123"),
            new_events,
        )
        .await;
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected only the termination action"
        );
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Terminated)
        );
        assert_eq!(co.result.as_deref(), Some(r#""stop""#));
    }

    #[tokio::test]
    async fn test_executor_terminate_before_timer_fired() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.create_timer(Duration::from_secs(5)).await?;
                ctx.call_activity("Ping", ()).await?;
                Ok(encode(&"done"))
            })
        });

        let start = now();
        let fire = start + chrono::Duration::seconds(5);
        let old_events = vec![
            workflow_started(start),
            execution_started_at("Workflow", "abc123", start),
            timer_created_at(0, fire, None, start),
        ];
        let new_events = vec![
            workflow_started(fire),
            terminated_at(Some(r#""stop""#), fire),
            timer_fired_at(0, fire, fire),
        ];
        let resp = run(&orch, "abc123", old_events, new_events).await;
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected only the termination action"
        );
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Terminated)
        );
    }

    #[tokio::test]
    async fn test_executor_terminate_beats_continue_as_new() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_activity("Ping", ()).await?;
                ctx.continue_as_new((), false);
                Ok(None)
            })
        });

        let old_events = ping_scheduled_history("abc123");
        let term = terminated(Some(r#""stop""#));
        let completed = task_completed(0, Some(r#""pong""#));
        let started = workflow_started(now());

        let cases = [
            (
                "terminate first",
                vec![started.clone(), term.clone(), completed.clone()],
            ),
            (
                "terminate last",
                vec![started.clone(), completed.clone(), term.clone()],
            ),
        ];
        for (name, new_events) in cases {
            let resp = run(&orch, "abc123", old_events.clone(), new_events).await;
            assert_eq!(
                resp.actions.len(),
                1,
                "[{name}] Expected only the termination action"
            );
            let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
            assert_eq!(
                co.workflow_status,
                status(proto::OrchestrationStatus::Terminated),
                "[{name}]"
            );
        }
    }

    #[tokio::test]
    async fn test_executor_completion_before_terminate_wins() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_activity("Ping", ()).await?;
                Ok(encode(&"done"))
            })
        });

        let new_events = vec![
            workflow_started(now()),
            task_completed(0, Some(r#""pong""#)),
            terminated(Some(r#""stop""#)),
        ];
        let resp = run(
            &orch,
            "abc123",
            ping_scheduled_history("abc123"),
            new_events,
        )
        .await;
        assert_eq!(resp.actions.len(), 1, "Expected only the completion action");
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed),
            "a workflow that completed earlier in the batch must keep its completion"
        );
        assert_eq!(co.result.as_deref(), Some(r#""done""#));
    }

    #[tokio::test]
    async fn test_executor_terminate_while_suspended() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                match ctx
                    .wait_for_external_event_with_timeout("MyEvent", Duration::from_secs(5))
                    .await?
                {
                    ExternalEventResult::Received(v) => {
                        let value: i32 = decode(v)?;
                        Ok(encode(&value))
                    }
                    ExternalEventResult::TimedOut => {
                        Err(DurableTaskError::Other(TASK_CANCELED.to_string()))
                    }
                }
            })
        });

        let new_events = vec![
            workflow_started(now()),
            execution_started("Workflow", "abc123"),
            suspended(),
            terminated(Some(r#""stop""#)),
        ];
        let resp = run(&orch, "abc123", vec![], new_events).await;
        assert_eq!(
            resp.actions.len(),
            1,
            "Expected only the termination action; work withheld by the suspension must stay withheld"
        );
        let co = get_complete(&resp.actions[0]).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Terminated)
        );
    }

    #[tokio::test]
    async fn test_executor_actions_deterministic_order() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let tasks: Vec<CompletableTask> =
                    (0..5).map(|_| ctx.call_activity("Ping", ())).collect();
                for t in tasks {
                    t.await?;
                }
                Ok(None)
            })
        });

        let new_events = vec![
            workflow_started(now()),
            execution_started("Workflow", "abc123"),
        ];
        let resp = run(&orch, "abc123", vec![], new_events).await;
        assert_eq!(resp.actions.len(), 5);
        for (i, a) in resp.actions.iter().enumerate() {
            assert_eq!(a.id, i as i32, "Actions must be ordered by sequence number");
            assert!(is_schedule_task(a));
        }
    }

    // ===========================================================================
    // task/orchestrator_buffered_test.go
    // ===========================================================================

    /// Mirrors Go `waitThenActivityRegistry`: wait for "go" (synthetic timer at
    /// id 0), then call "act" (id 1) and return its output.
    fn wait_then_activity() -> OrchestratorFn {
        Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                let out: String = decode(ctx.call_activity("act", ()).await?)?;
                Ok(encode(&out))
            })
        })
    }

    fn buffered_started() -> proto::HistoryEvent {
        execution_started("wf", "buffered-test")
    }

    fn assert_no_warns(cl: &WarnCapture) {
        assert!(
            cl.warns().is_empty(),
            "unexpected warnings: {:?}",
            cl.warns()
        );
    }

    #[tokio::test]
    async fn test_buffered_resolution_early_task_completed() {
        let (actions, cl) = run_buffered(
            &wait_then_activity(),
            vec![],
            vec![
                buffered_started(),
                task_completed(1, Some(r#""injected""#)),
                event_raised("go", None),
            ],
        )
        .await;

        let co =
            complete_action(&actions).expect("workflow must complete using the early completion");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""injected""#));
        assert_eq!(
            count_actions(&actions, is_schedule_task),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_early_task_failed() {
        // Go also asserts task.TaskExecutionId() == "exec-x". That assertion cannot
        // be reproduced: the Rust accessor `CompletableTask::task_execution_id` is
        // `pub(crate)`, and without a retry policy the recorded id reaches no
        // emitted action or failure detail (only the retry loop consumes it).
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                ctx.call_activity("act", ()).await?;
                Ok(None)
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![
                buffered_started(),
                task_failed(1, "exec-x"),
                event_raised("go", None),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Failed)
        );
        assert!(
            co.failure_details
                .as_ref()
                .map(|f| f.error_message.contains("injected failure"))
                .unwrap_or(false),
            "failure details: {:?}",
            co.failure_details
        );
        assert_eq!(
            count_actions(&actions, is_schedule_task),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_early_timer_fired() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                ctx.create_timer(Duration::from_secs(3600)).await?;
                Ok(encode(&"done"))
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![buffered_started(), timer_fired(1), event_raised("go", None)],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(
            count_actions(&actions, |a| get_create_timer(a).is_some() && a.id == 1),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_early_child_completed() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                let out: String = decode(ctx.call_sub_orchestrator("child", (), None).await?)?;
                Ok(encode(&out))
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![
                buffered_started(),
                child_completed(1, Some(r#""injected-child""#)),
                event_raised("go", None),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""injected-child""#));
        assert_eq!(
            count_actions(&actions, is_create_child),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_early_child_failed() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                ctx.call_sub_orchestrator("child", (), None).await?;
                Ok(None)
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![
                buffered_started(),
                child_failed(1),
                event_raised("go", None),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Failed)
        );
        assert_eq!(
            count_actions(&actions, is_create_child),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_early_timer_fired_for_external_event_timer() {
        // Go maps errors.Is(err, ErrTaskCanceled) to "timedout"; Rust reports the
        // cancellation as ExternalEventResult::TimedOut.
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("a").await?;
                match ctx
                    .wait_for_external_event_with_timeout("b", Duration::from_secs(3600))
                    .await?
                {
                    ExternalEventResult::TimedOut => Ok(encode(&"timedout")),
                    ExternalEventResult::Received(_) => Ok(None),
                }
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![buffered_started(), timer_fired(1), event_raised("a", None)],
        )
        .await;

        let co = complete_action(&actions)
            .expect("the buffered TimerFired must cancel the wait for event b immediately");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""timedout""#));
        assert_no_warns(&cl);
    }

    fn wait_go_only() -> OrchestratorFn {
        Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                Ok(None)
            })
        })
    }

    #[tokio::test]
    async fn test_buffered_resolution_unconsumed_orphan_warns() {
        let (actions, cl) = run_buffered(
            &wait_go_only(),
            vec![],
            vec![
                buffered_started(),
                task_completed(99, Some(r#""orphan""#)),
                event_raised("go", None),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(cl.warns_containing("TaskCompleted for id 99"), 1);
    }

    #[tokio::test]
    async fn test_buffered_resolution_unconsumed_orphan_warns_on_blocked_turn() {
        let (actions, cl) = run_buffered(
            &wait_go_only(),
            vec![],
            vec![buffered_started(), task_completed(99, Some(r#""orphan""#))],
        )
        .await;

        assert!(
            complete_action(&actions).is_none(),
            "workflow stays blocked on the external event"
        );
        assert_eq!(cl.warns_containing("TaskCompleted for id 99"), 1);
    }

    #[tokio::test]
    async fn test_buffered_resolution_duplicate_after_resolution_dropped() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                let out: String = decode(ctx.call_activity("act", ()).await?)?;
                Ok(encode(&out))
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![buffered_started(), task_scheduled(0, "act")],
            vec![
                task_completed(0, Some(r#""first""#)),
                task_completed(0, Some(r#""second""#)),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(
            co.result.as_deref(),
            Some(r#""first""#),
            "the first resolution wins; the duplicate is dropped"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_late_task_scheduled_after_delivery() {
        let (actions, cl) = run_buffered(
            &wait_then_activity(),
            vec![],
            vec![
                buffered_started(),
                task_completed(1, Some(r#""injected""#)),
                event_raised("go", None),
                task_scheduled(1, "act"),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""injected""#));
        assert_eq!(count_actions(&actions, is_schedule_task), 0);
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_kind_mismatch_not_delivered() {
        // A TimerFired for id 1 must not resolve the activity task at id 1: the
        // activity is dispatched as before and the unmatched timer resolution
        // warns at the end of the turn.
        let (actions, cl) = run_buffered(
            &wait_then_activity(),
            vec![],
            vec![buffered_started(), timer_fired(1), event_raised("go", None)],
        )
        .await;

        assert!(
            complete_action(&actions).is_none(),
            "a TimerFired for id 1 must not resolve the activity task at id 1"
        );
        assert_eq!(
            count_actions(&actions, is_schedule_task),
            1,
            "the activity dispatch is unaffected"
        );
        assert_eq!(
            cl.warns_containing("TimerFired for id 1"),
            1,
            "{:?}",
            cl.warns()
        );
    }

    #[tokio::test]
    async fn test_buffered_resolution_suspension_precedence() {
        let (actions, cl) = run_buffered(
            &wait_then_activity(),
            vec![],
            vec![
                buffered_started(),
                suspended(),
                task_completed(1, Some(r#""injected""#)),
                event_raised("go", None),
                resumed(),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""injected""#));
        assert_eq!(
            count_actions(&actions, is_schedule_task),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_fresh_context_per_execution() {
        let orch = wait_go_only();
        let (_, cl1) = run_buffered(
            &orch,
            vec![],
            vec![
                buffered_started(),
                task_completed(99, Some(r#""orphan""#)),
                event_raised("go", None),
            ],
        )
        .await;
        assert_eq!(cl1.warns_containing("TaskCompleted for id 99"), 1);

        let (_, cl2) = run_buffered(
            &orch,
            vec![],
            vec![buffered_started(), event_raised("go", None)],
        )
        .await;
        assert!(
            cl2.warns().is_empty(),
            "a fresh execution must not inherit buffered resolutions: {:?}",
            cl2.warns()
        );
    }

    #[tokio::test]
    async fn test_buffered_resolution_early_failure_with_retry_policy() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                let policy =
                    RetryPolicy::new(3, Duration::from_secs(1)).with_backoff_coefficient(2.0);
                ctx.call_activity_with_options(
                    "act",
                    (),
                    ActivityOptions::new().with_retry_policy(policy),
                )
                .await?;
                Ok(None)
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![
                buffered_started(),
                task_failed(1, "exec-x"),
                event_raised("go", None),
            ],
        )
        .await;

        // The buffered failure resolves attempt one at scheduling time and the
        // retry wrapper immediately arms the backoff timer: the workflow blocks on
        // the retry timer instead of completing, the failed attempt's
        // ScheduleTask is emitted for the record, and the retry timer is emitted.
        assert!(complete_action(&actions).is_none());
        assert_eq!(
            count_actions(&actions, is_schedule_task),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_eq!(
            count_actions(&actions, |a| get_create_timer(a).is_some() && a.id == 2),
            1,
            "the retry backoff timer must be armed"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_fan_out_two_early_completions() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                let t1 = ctx.call_activity("a", ());
                let t2 = ctx.call_activity("b", ());
                let out1: String = decode(t1.await?)?;
                let out2: String = decode(t2.await?)?;
                Ok(encode(&format!("{out1}{out2}")))
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![
                buffered_started(),
                task_completed(2, Some(r#""two""#)),
                task_completed(1, Some(r#""one""#)),
                event_raised("go", None),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""onetwo""#));
        assert_eq!(
            count_actions(&actions, is_schedule_task),
            2,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert_no_warns(&cl);
    }

    #[tokio::test]
    async fn test_buffered_resolution_terminated_turn_emits_only_completion() {
        let (actions, cl) = run_buffered(
            &wait_go_only(),
            vec![],
            vec![
                buffered_started(),
                task_completed(7, Some(r#""orphan""#)),
                terminated(Some(r#""stop""#)),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Terminated)
        );
        for a in &actions {
            assert!(
                get_complete(a).is_some(),
                "a terminated turn must emit only the completion action"
            );
        }
        assert_eq!(
            cl.warns_containing("TaskCompleted for id 7"),
            1,
            "{:?}",
            cl.warns()
        );
    }

    #[tokio::test]
    async fn test_buffered_resolution_continue_as_new_no_carryover() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                // WithKeepUnprocessedEvents() == save_events = true
                ctx.continue_as_new((), true);
                Ok(None)
            })
        });

        let (actions, cl) = run_buffered(
            &orch,
            vec![],
            vec![
                buffered_started(),
                task_completed(5, Some(r#""orphan""#)),
                event_raised("go", None),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::ContinuedAsNew)
        );
        for e in &co.carryover_events {
            assert!(
                !matches!(e.event_type, Some(EventType::TaskCompleted(_))),
                "buffered resolutions must not be carried into the next generation"
            );
        }
        assert_eq!(
            cl.warns_containing("TaskCompleted for id 5"),
            1,
            "{:?}",
            cl.warns()
        );
    }

    #[tokio::test]
    async fn test_buffered_resolution_executor_surfaces_warning() {
        let orch = wait_then_activity();
        let cl = WarnCapture::default();
        let _guard = cl.install();

        let resp = run(
            &orch,
            "exec-test",
            vec![],
            vec![
                buffered_started(),
                task_completed(1, Some(r#""injected""#)),
                event_raised("go", None),
            ],
        )
        .await;
        assert_eq!(
            count_actions(&resp.actions, is_schedule_task),
            1,
            "{EMITTED_NOT_DISPATCHED}"
        );
        assert!(cl.warns().is_empty(), "{:?}", cl.warns());

        let _ = run(
            &orch,
            "exec-test",
            vec![],
            vec![
                buffered_started(),
                task_completed(42, Some(r#""orphan""#)),
                event_raised("go", None),
            ],
        )
        .await;
        assert_eq!(cl.warns_containing("TaskCompleted for id 42"), 1);
    }

    /// Minimal stand-in for Go's `runtimestate.Applier`: records the scheduling
    /// history event for every emitted action, the way the runtime commits them.
    fn apply_actions(actions: &[proto::WorkflowAction], at: Dt) -> Vec<proto::HistoryEvent> {
        let mut out = Vec::new();
        for a in actions {
            match &a.workflow_action_type {
                Some(WorkflowActionType::ScheduleTask(st)) => {
                    out.push(task_scheduled_at(a.id, &st.name, at));
                }
                Some(WorkflowActionType::CreateTimer(ct)) => {
                    let origin = match &ct.origin {
                        Some(proto::create_timer_action::Origin::ExternalEvent(e)) => {
                            external_event_origin(&e.name)
                        }
                        Some(proto::create_timer_action::Origin::CreateTimer(_)) => {
                            create_timer_origin()
                        }
                        _ => None,
                    };
                    out.push(timer_created_at(
                        a.id,
                        from_ts(ct.fire_at.as_ref().unwrap()),
                        origin,
                        at,
                    ));
                }
                Some(WorkflowActionType::CreateChildWorkflow(cw)) => {
                    out.push(child_created_at(a.id, &cw.name, &cw.instance_id, at));
                }
                _ => {}
            }
        }
        out
    }

    #[tokio::test]
    async fn test_buffered_resolution_executor_replay_round_trip() {
        let orch: OrchestratorFn = Arc::new(|ctx| {
            Box::pin(async move {
                ctx.wait_for_external_event("go").await?;
                let out: String = decode(ctx.call_activity("act", ()).await?)?;
                ctx.call_activity("act2", ()).await?;
                Ok(encode(&out))
            })
        });

        let start = now();
        let first_turn = vec![
            buffered_started(),
            task_completed(1, Some(r#""injected""#)),
            event_raised("go", None),
        ];
        let resp = run(&orch, "exec-test", vec![], first_turn.clone()).await;
        assert_eq!(
            count_actions(&resp.actions, is_schedule_task),
            2,
            "{EMITTED_NOT_DISPATCHED}"
        );

        // Go then feeds the actions to durabletask-go's backend
        // `runtimestate.Applier` and asserts its dispatch list holds only #2 (the
        // applier withholds #1 because its resolution is already in the state).
        // That withholding is backend logic (the Dapr sidecar owns it) with no
        // counterpart in this worker-only SDK, so it cannot be asserted here. The
        // SDK's half of the contract is that both schedules are emitted, so the
        // runtime records TaskScheduled#1 next to the retained TaskCompleted#1
        // and dispatches #2.
        assert!(
            resp.actions
                .iter()
                .any(|a| is_schedule_task(a) && a.id == 1),
            "TaskScheduled#1 must be recorded so the completion keeps its match"
        );
        assert!(
            resp.actions
                .iter()
                .any(|a| is_schedule_task(a) && a.id == 2),
            "the unresolved activity must be dispatched"
        );

        // Go's `completed` check (TaskCompleted#1 retained in the state) holds by
        // construction here: the stand-in history keeps every first-turn event.
        //
        // The next turn replays the committed history with the second activity's
        // completion.
        let mut history = first_turn;
        history.extend(apply_actions(&resp.actions, start));
        let resp = run(
            &orch,
            "exec-test",
            history,
            vec![task_completed(2, Some("null"))],
        )
        .await;
        let co = complete_action(&resp.actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed),
            "{:?}",
            co.failure_details
        );
        assert_eq!(co.result.as_deref(), Some(r#""injected""#));
        assert_eq!(count_actions(&resp.actions, is_schedule_task), 0);
    }

    #[tokio::test]
    async fn test_completable_task_on_completed_after_completion() {
        // Go registers an onCompleted callback on an already-completed task and
        // expects it to fire immediately. The Rust CompletableTask has no callback
        // API; the equivalent contract is that awaiting an already-completed task
        // resolves on the first poll.
        let task = CompletableTask::new();
        task.complete(Some("x".to_string()));
        let mut fut = std::pin::pin!(task);
        let fired = matches!(futures::poll!(fut.as_mut()), std::task::Poll::Ready(Ok(Some(ref v))) if v == "x");
        assert!(
            fired,
            "awaiting an already completed task must resolve immediately"
        );
    }

    #[tokio::test]
    async fn test_buffered_resolution_kind_guard_on_occupied_id() {
        // A TaskCompleted whose id is occupied by a pending TIMER (here the
        // synthetic external event timer at id 0) must buffer rather than complete
        // the timer, which would wrongly cancel the external event wait.
        let (actions, cl) = run_buffered(
            &wait_then_activity(),
            vec![],
            vec![
                buffered_started(),
                task_completed(0, Some(r#""x""#)),
                event_raised("go", None),
            ],
        )
        .await;

        assert!(
            complete_action(&actions).is_none(),
            "the wait must complete via the event, not fail via a wrongly cancelled timer"
        );
        assert_eq!(
            count_actions(&actions, is_schedule_task),
            1,
            "the activity dispatch proceeds normally"
        );
        assert_eq!(
            cl.warns_containing("TaskCompleted for id 0"),
            1,
            "{:?}",
            cl.warns()
        );
    }

    #[tokio::test]
    async fn test_buffered_resolution_pre_synthetic_timer_migration() {
        let (actions, cl) = run_buffered(
            &wait_then_activity(),
            vec![],
            vec![
                buffered_started(),
                task_completed(0, Some(r#""migrated""#)),
                event_raised("go", None),
                task_scheduled(0, "act"),
            ],
        )
        .await;

        let co = complete_action(&actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Completed)
        );
        assert_eq!(co.result.as_deref(), Some(r#""migrated""#));
        assert_eq!(count_actions(&actions, is_schedule_task), 0);
        assert_no_warns(&cl);
    }

    // ===========================================================================
    // task/orchestrator_test.go
    // ===========================================================================

    /// One `Test_computeNextDelay` sub-case, exercised through the public retry
    /// API: an activity with `policy` fails for the `attempt`-th time (0-based)
    /// and the resulting retry timer's `fire_at - current_time` is the delay.
    ///
    /// The first call happens at `first_attempt` (Go `firstAttempt`) and the
    /// failure being handled is delivered at `current` (Go `currentTimeUtc`).
    /// Returns the observed delay, or `None` when no retry timer was scheduled.
    async fn observed_retry_delay(
        policy: RetryPolicy,
        attempt: i32,
        first_attempt: Dt,
        current: Dt,
    ) -> Option<Duration> {
        let orch: OrchestratorFn = Arc::new(move |ctx| {
            let policy = policy.clone();
            Box::pin(async move {
                ctx.call_activity_with_options(
                    "act",
                    (),
                    ActivityOptions::new().with_retry_policy(policy),
                )
                .await?;
                Ok(None)
            })
        });

        // Attempt k is scheduled at sequence 2k; its retry timer is at 2k+1.
        let mut old = vec![
            workflow_started(first_attempt),
            execution_started_at("wf", "retry-test", first_attempt),
            task_scheduled_at(0, "act", first_attempt),
        ];
        for k in 0..attempt {
            let at = first_attempt;
            old.push(workflow_started(at));
            old.push(task_failed_at(2 * k, "", "TestError", "boom", at));
            old.push(timer_created_at(2 * k + 1, at, None, at));
            old.push(workflow_started(at));
            old.push(timer_fired_at(2 * k + 1, at, at));
            old.push(task_scheduled_at(2 * k + 2, "act", at));
        }
        let new = vec![
            workflow_started(current),
            task_failed_at(2 * attempt, "", "TestError", "boom", current),
        ];

        let resp = run(&orch, "retry-test", old, new).await;
        let timers: Vec<_> = resp
            .actions
            .iter()
            .filter_map(|a| get_create_timer(a).map(|t| (a.id, t)))
            .collect();
        match timers.as_slice() {
            [] => {
                let co =
                    complete_action(&resp.actions).expect("no timer => orchestration must fail");
                assert_eq!(
                    co.workflow_status,
                    status(proto::OrchestrationStatus::Failed)
                );
                None
            }
            [(id, t)] => {
                assert_eq!(*id, 2 * attempt + 1, "retry timer sequence number");
                let fire_at = from_ts(t.fire_at.as_ref().unwrap());
                Some((fire_at - current).to_std().expect("non-negative delay"))
            }
            more => panic!("expected at most one retry timer, got {more:?}"),
        }
    }

    #[tokio::test]
    async fn test_compute_next_delay() {
        // Go calls computeNextDelay directly (MaxAttempts is not consulted there).
        // The Rust equivalent is private, so each sub-case drives the retry loop
        // via the public API; max_number_of_attempts is raised to 5 so the loop's
        // attempt-count guard never pre-empts attempts 0..=3.
        let time1 = now();
        let time2 = time1 + chrono::Duration::minutes(1);
        let policy = |backoff: f64, timeout: Duration| {
            RetryPolicy::new(5, Duration::from_secs(2))
                .with_backoff_coefficient(backoff)
                .with_max_retry_interval(Duration::from_secs(10))
                .with_handle(|_| true)
                .with_retry_timeout(timeout)
        };
        let two_min = Duration::from_secs(120);

        // (name, policy, attempt, want) — want == 0 means "no retry".
        let cases = vec![
            ("first attempt", policy(2.0, two_min), 0, 2),
            ("second attempt", policy(2.0, two_min), 1, 4),
            ("third attempt", policy(2.0, two_min), 2, 8),
            ("fourth attempt", policy(2.0, two_min), 3, 10),
            ("expired", policy(2.0, Duration::from_secs(30)), 3, 0),
            ("fourth attempt backoff 1", policy(1.0, two_min), 3, 2),
        ];

        let mut failures = Vec::new();
        for (name, p, attempt, want_secs) in cases {
            let got = observed_retry_delay(p, attempt, time1, time2).await;
            let want = (want_secs > 0).then(|| Duration::from_secs(want_secs));
            if got != want {
                failures.push(format!(
                    "{name}: computeNextDelay() = {got:?}, want {want:?}"
                ));
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    // ===========================================================================
    // task/router_test.go
    // ===========================================================================

    /// Runs a brand-new execution of `orch` and returns its emitted actions.
    async fn first_turn_actions(orch: OrchestratorFn) -> Vec<proto::WorkflowAction> {
        run(
            &orch,
            "test-id",
            vec![],
            vec![workflow_started(now()), execution_started("wf", "test-id")],
        )
        .await
        .actions
    }

    #[tokio::test]
    async fn test_call_activity_router_app_id_only() {
        let actions = first_turn_actions(Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_activity_with_app_id("dummyActivity", (), "target-app")
                    .await?;
                Ok(None)
            })
        }))
        .await;

        assert_eq!(actions.len(), 1);
        for a in &actions {
            let r = a.router.as_ref().expect("router");
            assert_eq!(r.target_app_id.as_deref(), Some("target-app"));
            assert_eq!(r.target_app_namespace.as_deref().unwrap_or(""), "");
        }
    }

    #[tokio::test]
    async fn test_call_activity_router_app_id_and_namespace() {
        // Go: CallActivity(..., WithActivityAppID("target-app"),
        // WithActivityAppNamespace("target-ns")) emits a router with both set.
        let actions = first_turn_actions(Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_activity_with_options(
                    "dummyActivity",
                    (),
                    ActivityOptions::new()
                        .with_app_id("target-app")
                        .with_app_namespace("target-ns"),
                )
                .await?;
                Ok(None)
            })
        }))
        .await;

        assert_eq!(actions.len(), 1);
        for a in &actions {
            let r = a.router.as_ref().expect("router");
            assert_eq!(r.target_app_id.as_deref(), Some("target-app"));
            assert_eq!(r.target_app_namespace.as_deref(), Some("target-ns"));
        }
    }

    /// Runs a first turn whose orchestrator awaits `call` once, records its
    /// result and then never finishes, so every emitted action comes from
    /// `call`. Returns the actions and the recorded result.
    async fn first_turn_call<F>(
        call: impl Fn(dapr_durabletask::task::OrchestrationContext) -> F + Send + Sync + 'static,
    ) -> (
        Vec<proto::WorkflowAction>,
        dapr_durabletask::api::Result<Option<String>>,
    )
    where
        F: std::future::Future<Output = dapr_durabletask::api::Result<Option<String>>>
            + Send
            + 'static,
    {
        let seen = Arc::new(Mutex::new(None));
        let (call, sink) = (Arc::new(call), seen.clone());
        let actions = first_turn_actions(Arc::new(move |ctx| {
            let (call, sink) = (call.clone(), sink.clone());
            Box::pin(async move {
                let result = call(ctx).await;
                *sink.lock().unwrap() = Some(result);
                std::future::pending::<()>().await;
                Ok(None)
            })
        }))
        .await;
        let result = seen.lock().unwrap().take().expect("call awaited");
        (actions, result)
    }

    /// Asserts `result` is an already-failed task of the given error type
    /// whose message names the missing app ID option.
    fn assert_invalid_options(
        result: dapr_durabletask::api::Result<Option<String>>,
        error_type: &str,
    ) {
        match result {
            Err(DurableTaskError::TaskFailed {
                failure_details: Some(fd),
                ..
            }) => {
                assert_eq!(fd.error_type, error_type);
                assert!(fd.message.contains("with_app_namespace"), "{}", fd.message);
                assert!(fd.message.contains("with_app_id"), "{}", fd.message);
            }
            other => panic!("expected a failed task, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_call_activity_namespace_without_app_id_fails() {
        // Go: CallActivity(..., WithActivityAppNamespace("target-ns")) without an
        // app id schedules no action and returns an already-failed task whose
        // failure ErrorType is "InvalidActivityOptions".
        let (actions, result) = first_turn_call(|ctx| async move {
            ctx.call_activity_with_options(
                "dummyActivity",
                (),
                ActivityOptions::new().with_app_namespace("target-ns"),
            )
            .await
        })
        .await;

        assert!(
            actions.is_empty(),
            "no action should be scheduled: {actions:?}"
        );
        assert_invalid_options(result, "InvalidActivityOptions");
    }

    #[tokio::test]
    async fn test_call_activity_no_router_by_default() {
        let actions = first_turn_actions(Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_activity("dummyActivity", ()).await?;
                Ok(None)
            })
        }))
        .await;

        assert_eq!(actions.len(), 1);
        for a in &actions {
            assert!(
                a.router.is_none(),
                "no router envelope should be emitted when no app/namespace target is set"
            );
        }
    }

    #[tokio::test]
    async fn test_call_child_workflow_router_app_id_only() {
        let actions = first_turn_actions(Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_sub_orchestrator_with_app_id("dummyWorkflow", (), None, "target-app")
                    .await?;
                Ok(None)
            })
        }))
        .await;

        assert_eq!(actions.len(), 1);
        for a in &actions {
            let r = a.router.as_ref().expect("router");
            assert_eq!(r.target_app_id.as_deref(), Some("target-app"));
            assert_eq!(r.target_app_namespace.as_deref().unwrap_or(""), "");
        }
    }

    #[tokio::test]
    async fn test_call_child_workflow_router_app_id_and_namespace() {
        // Go: CallChildWorkflow(..., WithChildWorkflowAppID("target-app"),
        // WithChildWorkflowAppNamespace("target-ns")) emits a router with both
        // set.
        let actions = first_turn_actions(Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_sub_orchestrator_with_options(
                    "dummyWorkflow",
                    (),
                    dapr_durabletask::task::SubOrchestratorOptions::new()
                        .with_app_id("target-app")
                        .with_app_namespace("target-ns"),
                )
                .await?;
                Ok(None)
            })
        }))
        .await;

        assert_eq!(actions.len(), 1);
        for a in &actions {
            let r = a.router.as_ref().expect("router");
            assert_eq!(r.target_app_id.as_deref(), Some("target-app"));
            assert_eq!(r.target_app_namespace.as_deref(), Some("target-ns"));
        }
    }

    #[tokio::test]
    async fn test_call_child_workflow_namespace_without_app_id_fails() {
        // Go: CallChildWorkflow(..., WithChildWorkflowAppNamespace("target-ns"))
        // without an app id schedules no action and returns an already-failed task
        // whose failure ErrorType is "InvalidChildWorkflowOptions".
        let (actions, result) = first_turn_call(|ctx| async move {
            ctx.call_sub_orchestrator_with_options(
                "dummyWorkflow",
                (),
                dapr_durabletask::task::SubOrchestratorOptions::new()
                    .with_app_namespace("target-ns"),
            )
            .await
        })
        .await;

        assert!(
            actions.is_empty(),
            "no action should be scheduled: {actions:?}"
        );
        assert_invalid_options(result, "InvalidChildWorkflowOptions");
    }

    #[tokio::test]
    async fn test_call_child_workflow_no_router_by_default() {
        let actions = first_turn_actions(Arc::new(|ctx| {
            Box::pin(async move {
                ctx.call_sub_orchestrator("dummyWorkflow", (), None).await?;
                Ok(None)
            })
        }))
        .await;

        assert_eq!(actions.len(), 1);
        for a in &actions {
            assert!(a.router.is_none());
        }
    }

    // ===========================================================================
    // task/detached_workflow_test.go
    // ===========================================================================
    //
    // Go inspects `ctx.pendingActions` directly; here the emitted actions of a
    // turn whose orchestrator never finishes are the observable equivalent.

    use dapr_durabletask::task::{DetachedWorkflowOptions, OrchestrationContext};

    /// Runs a first turn (instance "test-id") whose orchestrator calls `spawn`
    /// and then never finishes, so every emitted action comes from `spawn`.
    /// Returns the actions and what `spawn` returned.
    async fn detached_turn<T: Send + 'static>(
        spawn: impl Fn(&OrchestrationContext) -> T + Send + Sync + 'static,
    ) -> (Vec<proto::WorkflowAction>, T) {
        let seen = Arc::new(Mutex::new(None));
        let (spawn, sink) = (Arc::new(spawn), seen.clone());
        let actions = first_turn_actions(Arc::new(move |ctx| {
            *sink.lock().unwrap() = Some(spawn(&ctx));
            Box::pin(async move {
                std::future::pending::<()>().await;
                Ok(None)
            })
        }))
        .await;
        let out = seen.lock().unwrap().take().expect("spawn called");
        (actions, out)
    }

    fn get_detached(a: &proto::WorkflowAction) -> Option<&proto::CreateDetachedWorkflowAction> {
        match &a.workflow_action_type {
            Some(WorkflowActionType::CreateDetachedWorkflow(c)) => Some(c),
            _ => None,
        }
    }

    fn detached_created(id: i32, instance_id: &str) -> proto::HistoryEvent {
        ev(
            id,
            now(),
            EventType::DetachedWorkflowInstanceCreated(
                proto::DetachedWorkflowInstanceCreatedEvent {
                    instance_id: instance_id.to_string(),
                },
            ),
        )
    }

    /// An orchestrator that spawns a detached "dummyWorkflow" with the given
    /// instance ID and then never finishes.
    fn spawn_then_block(instance_id: &'static str) -> OrchestratorFn {
        Arc::new(move |ctx| {
            let spawned = ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                DetachedWorkflowOptions::new().with_instance_id(instance_id),
            );
            Box::pin(async move {
                assert_eq!(spawned?, instance_id);
                std::future::pending::<()>().await;
                Ok(None)
            })
        })
    }

    #[tokio::test]
    async fn test_schedule_new_workflow_emits_action() {
        // Go: ScheduleNewDetachedWorkflow(dummyWorkflow, instanceID "spawned-1",
        // input {"hello":"world"}, startTime 2030-01-02T03:04:05Z) returns
        // "spawned-1", registers no Task, and emits exactly one
        // CreateDetachedWorkflow action with InstanceId "spawned-1", Name
        // "dummyWorkflow", Input `{"hello":"world"}`, ScheduledStartTimestamp ==
        // startTime, no ExecutionId, no Tags and no Router. (No Task can be
        // registered: the Rust call returns the instance ID, not a task.)
        let start_time = chrono::DateTime::parse_from_rfc3339("2030-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let (actions, id) = detached_turn(move |ctx| {
            ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                serde_json::json!({"hello": "world"}),
                DetachedWorkflowOptions::new()
                    .with_instance_id("spawned-1")
                    .with_start_time(start_time),
            )
        })
        .await;

        assert_eq!(id.unwrap(), "spawned-1");
        assert_eq!(actions.len(), 1);
        for a in &actions {
            let dw = get_detached(a).expect("CreateDetachedWorkflow");
            assert_eq!(dw.instance_id, "spawned-1");
            assert_eq!(dw.name, "dummyWorkflow");
            assert_eq!(dw.input.as_deref(), Some(r#"{"hello":"world"}"#));
            assert_eq!(
                dw.scheduled_start_timestamp.as_ref().map(from_ts),
                Some(start_time)
            );
            assert!(
                dw.execution_id.is_none(),
                "execution IDs are runtime-minted; the SDK never sets one"
            );
            assert!(
                dw.tags.is_empty(),
                "tags are not settable from the workflow context"
            );
            assert!(
                a.router.is_none(),
                "no router envelope should be emitted when no app/namespace target is set"
            );
        }
    }

    #[tokio::test]
    async fn test_schedule_new_workflow_raw_input() {
        // Go: WithRawDetachedWorkflowInput(wrapperspb.String("raw-bytes")) is
        // emitted verbatim as the CreateDetachedWorkflow input ("raw-bytes").
        let (actions, id) = detached_turn(|ctx| {
            ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                DetachedWorkflowOptions::new()
                    .with_instance_id("spawned-raw")
                    .with_raw_input("raw-bytes"),
            )
        })
        .await;

        id.unwrap();
        assert_eq!(actions.len(), 1);
        for a in &actions {
            let dw = get_detached(a).expect("CreateDetachedWorkflow");
            assert_eq!(dw.input.as_deref(), Some("raw-bytes"));
        }
    }

    #[tokio::test]
    async fn test_schedule_new_workflow_explicit_empty_instance_id_errors() {
        // Go: WithDetachedWorkflowInstanceID("") returns an error containing
        // "empty string" and an empty instance ID, schedules no action and does
        // not advance the default-ID counter (observable: the next default-ID
        // spawn still gets "test-id-0").
        let (actions, (rejected, next)) = detached_turn(|ctx| {
            let rejected = ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                DetachedWorkflowOptions::new().with_instance_id(""),
            );
            let next = ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                DetachedWorkflowOptions::new(),
            );
            (rejected, next)
        })
        .await;

        let err = rejected.expect_err("an empty instance ID must be rejected");
        assert!(err.to_string().contains("empty string"), "{err}");
        assert_eq!(
            next.unwrap(),
            "test-id-0",
            "a rejected call must not advance the default-ID counter"
        );
        // Only the second (default-ID) spawn is scheduled.
        assert_eq!(actions.len(), 1, "{actions:?}");
        assert_eq!(get_detached(&actions[0]).unwrap().instance_id, "test-id-0");
    }

    #[tokio::test]
    async fn test_schedule_new_workflow_defaults_instance_id() {
        // Go: default IDs are "<caller>-<n>" ("test-id-0", then "test-id-1"
        // after an intervening CreateTimer, which must not advance the counter);
        // an explicit "custom-id" is kept and does not advance the counter, so the
        // next default is "test-id-2".
        let (actions, ids) = detached_turn(|ctx| {
            let default = || DetachedWorkflowOptions::new();
            let id0 = ctx.schedule_new_detached_workflow("dummyWorkflow", (), default());
            let _timer = ctx.create_timer(Duration::from_secs(1));
            let id1 = ctx.schedule_new_detached_workflow("dummyWorkflow", (), default());
            let explicit = ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                default().with_instance_id("custom-id"),
            );
            let id2 = ctx.schedule_new_detached_workflow("dummyWorkflow", (), default());
            [id0, id1, explicit, id2].map(|r| r.unwrap())
        })
        .await;

        assert_eq!(ids, ["test-id-0", "test-id-1", "custom-id", "test-id-2"]);
        let spawned: Vec<_> = actions
            .iter()
            .filter_map(get_detached)
            .map(|d| d.instance_id.as_str())
            .collect();
        assert_eq!(
            spawned,
            ["test-id-0", "test-id-1", "custom-id", "test-id-2"]
        );
    }

    #[tokio::test]
    async fn test_schedule_new_workflow_namespace_without_app_id_fails() {
        // Go: a namespace without an app id returns an error mentioning
        // "WithDetachedWorkflowAppID" (Rust: `with_app_id`), an empty instance
        // ID, and no action.
        let (actions, result) = detached_turn(|ctx| {
            ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                DetachedWorkflowOptions::new()
                    .with_instance_id("spawned-ns")
                    .with_app_namespace("target-ns"),
            )
        })
        .await;

        let err = result.expect_err("a namespace without an app ID must be rejected");
        assert!(err.to_string().contains("with_app_id"), "{err}");
        assert!(actions.is_empty(), "{actions:?}");
    }

    #[tokio::test]
    async fn test_schedule_new_workflow_router_app_id_only() {
        // Go: WithDetachedWorkflowAppID("target-app") emits a router with
        // TargetAppID "target-app" and an empty namespace.
        let (actions, id) = detached_turn(|ctx| {
            ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                DetachedWorkflowOptions::new()
                    .with_instance_id("spawned-app")
                    .with_app_id("target-app"),
            )
        })
        .await;

        id.unwrap();
        assert_eq!(actions.len(), 1);
        for a in &actions {
            let r = a.router.as_ref().expect("router");
            assert_eq!(r.target_app_id.as_deref(), Some("target-app"));
            assert_eq!(r.target_app_namespace.as_deref().unwrap_or(""), "");
        }
    }

    #[tokio::test]
    async fn test_schedule_new_workflow_router_app_id_and_namespace() {
        // Go: app id "target-app" + namespace "target-ns" are both set on the
        // emitted router.
        let (actions, id) = detached_turn(|ctx| {
            ctx.schedule_new_detached_workflow(
                "dummyWorkflow",
                (),
                DetachedWorkflowOptions::new()
                    .with_instance_id("spawned-cross")
                    .with_app_id("target-app")
                    .with_app_namespace("target-ns"),
            )
        })
        .await;

        id.unwrap();
        assert_eq!(actions.len(), 1);
        for a in &actions {
            let r = a.router.as_ref().expect("router");
            assert_eq!(r.target_app_id.as_deref(), Some("target-app"));
            assert_eq!(r.target_app_namespace.as_deref(), Some("target-ns"));
        }
    }

    #[tokio::test]
    async fn test_on_detached_workflow_created_retires_pending_action() {
        // Go: after scheduling "spawned-replay", a DetachedWorkflowInstanceCreated
        // history event with the action's id and the same instance id retires the
        // pending action (it is no longer emitted).
        let orch = spawn_then_block("spawned-replay");
        let resp = run(
            &orch,
            "test-id",
            vec![
                workflow_started(now()),
                execution_started("wf", "test-id"),
                detached_created(0, "spawned-replay"),
            ],
            vec![workflow_started(now())],
        )
        .await;

        assert!(
            resp.actions.is_empty(),
            "matching event should retire the pending action: {:?}",
            resp.actions
        );
    }

    #[tokio::test]
    async fn test_on_detached_workflow_created_instance_id_mismatch_errors() {
        // Go: scheduling "spawned-new" but replaying a
        // DetachedWorkflowInstanceCreated for "spawned-old" at the same id is a
        // non-determinism error mentioning both ids; the action is not retired.
        // Observable here: the workflow fails with that error and the spawn is
        // not dispatched (a failed replay discards every other action).
        let orch = spawn_then_block("spawned-new");
        let resp = run(
            &orch,
            "test-id",
            vec![
                workflow_started(now()),
                execution_started("wf", "test-id"),
                detached_created(0, "spawned-old"),
            ],
            vec![workflow_started(now())],
        )
        .await;

        let co = complete_action(&resp.actions).expect("CompleteWorkflow");
        assert_eq!(
            co.workflow_status,
            status(proto::OrchestrationStatus::Failed)
        );
        let fd = co.failure_details.as_ref().expect("failure details");
        assert_eq!(fd.error_type, "NonDeterminismError");
        assert!(
            fd.error_message.contains("spawned-old"),
            "{}",
            fd.error_message
        );
        assert!(
            fd.error_message.contains("spawned-new"),
            "{}",
            fd.error_message
        );
        assert_eq!(
            count_actions(&resp.actions, |a| get_detached(a).is_some()),
            0
        );
    }

    #[tokio::test]
    async fn test_on_detached_workflow_created_nondeterministic_replay_errors() {
        // Go: a DetachedWorkflowInstanceCreated (id 42, "spawned-mismatch") with no
        // matching pending action is a non-determinism error whose message names
        // ScheduleNewDetachedWorkflow, the instance id and the event id. The
        // nearest observable is either an executor error or a FAILED completion
        // carrying that message.
        let orch: OrchestratorFn = Arc::new(|_ctx| Box::pin(async move { Ok(None) }));
        let old_events = vec![
            workflow_started(now()),
            execution_started("wf", "test-id"),
            ev(
                42,
                now(),
                EventType::DetachedWorkflowInstanceCreated(
                    proto::DetachedWorkflowInstanceCreatedEvent {
                        instance_id: "spawned-mismatch".to_string(),
                    },
                ),
            ),
        ];

        let msg = match execute(&orch, "test-id", old_events, vec![workflow_started(now())]).await {
            Err(e) => e.to_string(),
            Ok(resp) => {
                let co = complete_action(&resp.actions).expect("CompleteWorkflow");
                assert_eq!(
                    co.workflow_status,
                    status(proto::OrchestrationStatus::Failed)
                );
                co.failure_details
                    .as_ref()
                    .map(|f| f.error_message.clone())
                    .unwrap_or_default()
            }
        };
        assert!(msg.contains("ScheduleNewDetachedWorkflow"), "{msg}");
        assert!(msg.contains("spawned-mismatch"), "{msg}");
        assert!(msg.contains("42"), "{msg}");
    }
}

#[tokio::test]
async fn test_held_events_apply_in_order_on_resume() {
    // While suspended, the timeout fires and then the event arrives. On
    // resume the held events must be applied one at a time, as without the
    // suspension: the wait times out and the later event stays buffered.
    let orch_fn: OrchestratorFn = Arc::new(|ctx| {
        Box::pin(async move {
            match ctx
                .wait_for_external_event_with_timeout("ev", std::time::Duration::from_secs(30))
                .await?
            {
                ExternalEventResult::Received(data) => Ok(data),
                ExternalEventResult::TimedOut => Ok(Some("\"timed out\"".to_string())),
            }
        })
    });
    let old = vec![
        make_workflow_started(ts_now()),
        make_execution_started("test_orch", None),
        make_event_timer_created(
            0,
            ts_now() + chrono::Duration::seconds(30),
            "ev",
            Some("ev"),
        ),
    ];
    let held = vec![
        make_suspended(),
        make_timer_fired(10, 0),
        make_event_raised("ev", Some("\"payload\"".to_string())),
        make_resumed(),
    ];
    let unsuspended = vec![
        make_timer_fired(10, 0),
        make_event_raised("ev", Some("\"payload\"".to_string())),
    ];

    for new in [unsuspended, held] {
        let resp = run_executor(&orch_fn, old.clone(), new).await.unwrap();
        let cw = get_complete_action(&resp.actions).unwrap();
        assert_eq!(cw.result.as_deref(), Some("\"timed out\""));
    }
}

/// Property-style replay tests: a small runtime simulator (mirroring the
/// durabletask-go backend applier) drives workflow programs turn by turn
/// with seeded delivery orders and batch sizes. Every turn checks replay
/// invariants, and variants of a run (suspension, split batches, duplicate
/// events, terminate) must reach the same outcome as the baseline run.
mod replay_properties {
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use dapr_durabletask::api::{DurableTaskError, ExternalEventResult, RetryPolicy};
    use dapr_durabletask::task::{ActivityOptions, OrchestrationContext, when_all, when_any};
    use dapr_durabletask::worker::{OrchestrationExecutor, OrchestratorFn, WorkerOptions};
    use dapr_durabletask_proto as proto;
    use dapr_durabletask_proto::history_event::EventType;
    use dapr_durabletask_proto::workflow_action::WorkflowActionType;

    const SEEDS: u64 = 40;

    // ── Programs ────────────────────────────────────────────────────────────

    type Program =
        fn(
            OrchestrationContext,
        )
            -> futures::future::BoxFuture<'static, dapr_durabletask::api::Result<Option<String>>>;

    fn json(v: impl serde::Serialize) -> Option<String> {
        Some(serde_json::to_string(&v).unwrap())
    }

    fn val(s: Option<String>) -> serde_json::Value {
        s.map(|s| serde_json::from_str(&s).unwrap())
            .unwrap_or(serde_json::Value::Null)
    }

    fn programs() -> Vec<(&'static str, Program)> {
        vec![
            ("sequential", |ctx| {
                Box::pin(async move {
                    let a = ctx.call_activity("double", 1).await?;
                    let b = ctx.call_activity("double", val(a)).await?;
                    Ok(json(val(b)))
                })
            }),
            ("fan_out_fan_in", |ctx| {
                Box::pin(async move {
                    let tasks = (0..5).map(|i| ctx.call_activity("double", i)).collect();
                    let results = when_all(tasks).await?;
                    Ok(json(results.into_iter().map(val).collect::<Vec<_>>()))
                })
            }),
            ("when_any_race", |ctx| {
                Box::pin(async move {
                    let work = ctx.call_activity("double", 7);
                    let timer = ctx.create_timer(Duration::from_secs(5));
                    let winner = when_any(vec![work, timer]).await?;
                    Ok(json(winner))
                })
            }),
            ("timer_then_activity", |ctx| {
                Box::pin(async move {
                    ctx.create_timer(Duration::from_secs(1)).await?;
                    let r = ctx.call_activity("double", 3).await?;
                    Ok(json(val(r)))
                })
            }),
            ("external_event", |ctx| {
                Box::pin(async move {
                    let data = ctx.wait_for_external_event("go").await?;
                    let r = ctx.call_activity("double", val(data)).await?;
                    Ok(json(val(r)))
                })
            }),
            ("event_or_timeout", |ctx| {
                Box::pin(async move {
                    let first = ctx
                        .wait_for_external_event_with_timeout("ev", Duration::from_secs(10))
                        .await?;
                    let second = ctx
                        .wait_for_external_event_with_timeout("ev", Duration::from_secs(10))
                        .await?;
                    let label = |r: ExternalEventResult| match r {
                        ExternalEventResult::Received(d) => format!("received:{}", val(d)),
                        ExternalEventResult::TimedOut => "timed_out".to_string(),
                    };
                    Ok(json((label(first), label(second))))
                })
            }),
            ("child_workflow", |ctx| {
                Box::pin(async move {
                    let r = ctx.call_sub_orchestrator("child", 4, None).await?;
                    let d = ctx.call_activity("double", val(r)).await?;
                    Ok(json(val(d)))
                })
            }),
            ("retries", |ctx| {
                Box::pin(async move {
                    let opts = ActivityOptions::new()
                        .with_retry_policy(RetryPolicy::new(4, Duration::from_secs(1)));
                    let r = ctx.call_activity_with_options("flaky", 5, opts).await?;
                    Ok(json(val(r)))
                })
            }),
            ("concurrent_branches", |ctx| {
                Box::pin(async move {
                    let a = {
                        let ctx = ctx.clone();
                        async move {
                            let x = ctx.call_activity("double", 1).await?;
                            ctx.call_activity("double", val(x)).await
                        }
                    };
                    let b = {
                        let ctx = ctx.clone();
                        async move {
                            let x = ctx.call_activity("double", 10).await?;
                            ctx.call_activity("double", val(x)).await
                        }
                    };
                    let (a, b) = futures::join!(a, b);
                    Ok(json((val(a?), val(b?))))
                })
            }),
            ("caught_failure", |ctx| {
                Box::pin(async move {
                    let r = match ctx.call_activity("boom", ()).await {
                        Err(DurableTaskError::TaskFailed { .. }) => {
                            ctx.call_activity("double", 100).await?
                        }
                        other => other?,
                    };
                    ctx.set_custom_status("recovered");
                    Ok(json(val(r)))
                })
            }),
            ("continue_as_new", |ctx| {
                Box::pin(async move {
                    let r = ctx.call_activity("double", 2).await?;
                    ctx.continue_as_new(val(r), true);
                    Ok(None)
                })
            }),
        ]
    }

    // ── Simulated runtime ───────────────────────────────────────────────────

    /// Deterministic PRNG (xorshift64*).
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// Result of an activity: a pure function of name and input, except
    /// "flaky" (fails its first two attempts) and "boom" (always fails).
    fn activity_result(
        name: &str,
        input: Option<&str>,
        attempt: u32,
    ) -> Result<Option<String>, String> {
        let n = input
            .and_then(|i| serde_json::from_str::<i64>(i).ok())
            .unwrap_or(0);
        match name {
            "boom" => Err("boom".into()),
            "flaky" if attempt < 2 => Err(format!("attempt {attempt}")),
            _ => Ok(json(n * 2)),
        }
    }

    fn event(event_id: i32, et: EventType, ts: i64) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id,
            timestamp: Some(proto::prost_types::Timestamp {
                seconds: ts,
                nanos: 0,
            }),
            router: None,
            event_type: Some(et),
        }
    }

    /// Identity of a deliverable event, used to rank delivery order.
    fn identity(e: &proto::HistoryEvent) -> (u8, i32, String) {
        match &e.event_type {
            Some(EventType::TaskCompleted(t)) => (1, t.task_scheduled_id, String::new()),
            Some(EventType::TaskFailed(t)) => (2, t.task_scheduled_id, String::new()),
            Some(EventType::TimerFired(t)) => (3, t.timer_id, String::new()),
            Some(EventType::ChildWorkflowInstanceCompleted(t)) => {
                (4, t.task_scheduled_id, String::new())
            }
            Some(EventType::EventRaised(r)) => (5, 0, format!("{r:?}")),
            _ => (9, e.event_id, String::new()),
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Variant {
        Baseline,
        /// Deliver this turn's events inside ExecutionSuspended/Resumed,
        /// optionally with the resume in the next turn.
        Suspend {
            turn: usize,
            resume_next_turn: bool,
        },
        /// Deliver this turn's events across two turns.
        Split {
            turn: usize,
        },
        /// Deliver one of this turn's completions twice.
        DuplicateCompletion {
            turn: usize,
        },
        /// Persist a duplicate of the scheduling events of this turn's
        /// response (as older runtimes did).
        DuplicateScheduling {
            turn: usize,
        },
        /// Deliver a terminate after this turn's events.
        Terminate {
            turn: usize,
        },
    }

    #[derive(Clone, Debug, PartialEq)]
    struct Outcome {
        status: i32,
        result: Option<String>,
        failure_type: Option<String>,
        custom_status: Option<String>,
    }

    struct Sim {
        program: Program,
        history: Vec<proto::HistoryEvent>,
        available: Vec<proto::HistoryEvent>,
        attempts: BTreeMap<(String, Option<String>), u32>,
        ts: i64,
        next_id: i32,
        custom_status: Option<String>,
        trace: String,
    }

    impl Sim {
        fn new(program: Program, external_events: Vec<proto::HistoryEvent>) -> Self {
            Self {
                program,
                history: Vec::new(),
                available: external_events,
                attempts: BTreeMap::new(),
                ts: 1_700_000_000,
                next_id: 1000,
                custom_status: None,
                trace: String::new(),
            }
        }

        fn orchestrator(&self) -> OrchestratorFn {
            let program = self.program;
            Arc::new(move |ctx| program(ctx))
        }

        fn uid(&mut self) -> i32 {
            self.next_id += 1;
            self.next_id
        }

        /// Execute one turn, check the replay invariants and apply the
        /// response like the runtime would. Returns the completion, if any.
        async fn turn(
            &mut self,
            mut new_events: Vec<proto::HistoryEvent>,
            duplicate_scheduling: bool,
        ) -> Option<proto::CompleteWorkflowAction> {
            self.ts += 1;
            new_events.insert(
                0,
                event(
                    -1,
                    EventType::WorkflowStarted(proto::WorkflowStartedEvent { version: None }),
                    self.ts,
                ),
            );
            self.trace += &format!("turn: {new_events:?}\n");
            let resp = execute(
                &self.orchestrator(),
                self.history.clone(),
                new_events.clone(),
            )
            .await;
            if let Some(cs) = &resp.custom_status {
                self.custom_status = Some(cs.clone());
            }

            // Invariant: no non-determinism, unique action IDs, and nothing
            // already recorded in history is emitted again.
            let scheduled: HashSet<i32> = self
                .history
                .iter()
                .filter(|e| {
                    matches!(
                        e.event_type,
                        Some(
                            EventType::TaskScheduled(_)
                                | EventType::TimerCreated(_)
                                | EventType::ChildWorkflowInstanceCreated(_)
                        )
                    )
                })
                .map(|e| e.event_id)
                .collect();
            let mut ids = HashSet::new();
            for a in &resp.actions {
                assert!(
                    ids.insert(a.id),
                    "duplicate action id {}\n{}",
                    a.id,
                    self.trace
                );
                if !matches!(
                    a.workflow_action_type,
                    Some(WorkflowActionType::CompleteWorkflow(_))
                ) {
                    assert!(
                        !scheduled.contains(&a.id),
                        "re-emitted action {} already in history\n{}",
                        a.id,
                        self.trace
                    );
                }
            }
            let completion = resp
                .actions
                .iter()
                .find_map(|a| match &a.workflow_action_type {
                    Some(WorkflowActionType::CompleteWorkflow(c)) => Some(c.clone()),
                    _ => None,
                });
            if let Some(c) = &completion {
                let failure = c.failure_details.as_ref().map(|f| f.error_type.as_str());
                assert_ne!(
                    failure,
                    Some("NonDeterminismError"),
                    "replay reported non-determinism: {:?}\n{}",
                    c.failure_details,
                    self.trace
                );
            }

            self.history.extend(new_events);
            let mut recorded = Vec::new();
            for a in resp.actions {
                let et = match a.workflow_action_type {
                    Some(WorkflowActionType::ScheduleTask(t)) => {
                        let attempt = self
                            .attempts
                            .entry((t.name.clone(), t.input.clone()))
                            .or_default();
                        let outcome = activity_result(&t.name, t.input.as_deref(), *attempt);
                        *attempt += 1;
                        let id = self.uid();
                        self.available.push(event(
                            id,
                            match outcome {
                                Ok(result) => EventType::TaskCompleted(proto::TaskCompletedEvent {
                                    task_scheduled_id: a.id,
                                    result,
                                    task_execution_id: t.task_execution_id.clone(),
                                    ..Default::default()
                                }),
                                Err(message) => EventType::TaskFailed(proto::TaskFailedEvent {
                                    task_scheduled_id: a.id,
                                    failure_details: Some(proto::TaskFailureDetails {
                                        error_type: "ActivityError".into(),
                                        error_message: message,
                                        ..Default::default()
                                    }),
                                    task_execution_id: t.task_execution_id.clone(),
                                    ..Default::default()
                                }),
                            },
                            0,
                        ));
                        EventType::TaskScheduled(proto::TaskScheduledEvent {
                            name: t.name,
                            input: t.input,
                            task_execution_id: t.task_execution_id,
                            ..Default::default()
                        })
                    }
                    Some(WorkflowActionType::CreateTimer(t)) => {
                        let indefinite = t
                            .fire_at
                            .as_ref()
                            .is_some_and(|f| f.seconds > 200_000_000_000);
                        if !indefinite {
                            let id = self.uid();
                            self.available.push(event(
                                id,
                                EventType::TimerFired(proto::TimerFiredEvent {
                                    fire_at: t.fire_at,
                                    timer_id: a.id,
                                }),
                                0,
                            ));
                        }
                        EventType::TimerCreated(proto::TimerCreatedEvent {
                            fire_at: t.fire_at,
                            name: t.name,
                            origin: t.origin.map(|o| match o {
                                proto::create_timer_action::Origin::CreateTimer(x) => {
                                    proto::timer_created_event::Origin::CreateTimer(x)
                                }
                                proto::create_timer_action::Origin::ExternalEvent(x) => {
                                    proto::timer_created_event::Origin::ExternalEvent(x)
                                }
                                proto::create_timer_action::Origin::ActivityRetry(x) => {
                                    proto::timer_created_event::Origin::ActivityRetry(x)
                                }
                                proto::create_timer_action::Origin::ChildWorkflowRetry(x) => {
                                    proto::timer_created_event::Origin::ChildWorkflowRetry(x)
                                }
                            }),
                            ..Default::default()
                        })
                    }
                    Some(WorkflowActionType::CreateChildWorkflow(c)) => {
                        let n = c
                            .input
                            .as_deref()
                            .and_then(|i| serde_json::from_str::<i64>(i).ok())
                            .unwrap_or(0);
                        let id = self.uid();
                        self.available.push(event(
                            id,
                            EventType::ChildWorkflowInstanceCompleted(
                                proto::ChildWorkflowInstanceCompletedEvent {
                                    task_scheduled_id: a.id,
                                    result: json(n + 1),
                                    ..Default::default()
                                },
                            ),
                            0,
                        ));
                        EventType::ChildWorkflowInstanceCreated(
                            proto::ChildWorkflowInstanceCreatedEvent {
                                name: c.name,
                                input: c.input,
                                instance_id: c.instance_id,
                                ..Default::default()
                            },
                        )
                    }
                    _ => continue,
                };
                recorded.push(event(a.id, et, self.ts));
            }
            if duplicate_scheduling {
                let dup = recorded.clone();
                recorded.extend(dup);
            }
            self.history.extend(recorded);
            completion
        }

        /// Remove and return up to `n` available events in ranked order.
        fn take(&mut self, n: usize, seed: u64) -> Vec<proto::HistoryEvent> {
            let rank = |e: &proto::HistoryEvent| {
                let id = identity(e);
                let mut h = seed ^ 0xA076_1D64_78BD_642F;
                for b in format!("{id:?}").bytes() {
                    h = (h ^ b as u64).wrapping_mul(0x1000_0000_01B3);
                }
                h
            };
            self.available.sort_by_key(rank);
            let n = n.min(self.available.len());
            self.available.drain(..n).collect()
        }
    }

    async fn execute(
        orch_fn: &OrchestratorFn,
        old: Vec<proto::HistoryEvent>,
        new: Vec<proto::HistoryEvent>,
    ) -> proto::WorkflowResponse {
        OrchestrationExecutor::execute(
            orch_fn,
            "prop-instance",
            old,
            new,
            String::new(),
            &WorkerOptions::default(),
            None,
        )
        .await
        .unwrap()
    }

    fn external_events(ts: i64) -> Vec<proto::HistoryEvent> {
        ["go", "ev", "ev"]
            .iter()
            .enumerate()
            .map(|(i, name)| {
                event(
                    -1,
                    EventType::EventRaised(proto::EventRaisedEvent {
                        name: name.to_string(),
                        input: json(i as i64 + 1),
                    }),
                    ts,
                )
            })
            .collect()
    }

    /// Run a program to completion with seeded delivery, applying `variant`.
    /// Returns the outcome and the number of turns.
    async fn run(program: Program, seed: u64, variant: Variant) -> (Outcome, usize, String) {
        let mut sim = Sim::new(program, external_events(0));
        let mut rng = Rng::new(seed);
        let start = vec![event(
            -1,
            EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: "prop".into(),
                ..Default::default()
            }),
            0,
        )];
        let mut completion = sim.turn(start, false).await;
        let mut turn = 1;
        while completion.is_none() {
            assert!(turn < 200, "workflow did not finish\n{}", sim.trace);
            if sim.available.is_empty() {
                panic!("workflow stuck with nothing to deliver\n{}", sim.trace);
            }
            let batch = 1 + rng.below(3);
            let order_seed = rng.next();
            let events = sim.take(batch, order_seed);
            completion = match variant {
                Variant::Suspend {
                    turn: t,
                    resume_next_turn,
                } if t == turn => {
                    let mut held = vec![super::make_suspended()];
                    held.extend(events);
                    if resume_next_turn {
                        let c = sim.turn(held, false).await;
                        assert!(c.is_none(), "suspended turn completed\n{}", sim.trace);
                        sim.turn(vec![super::make_resumed()], false).await
                    } else {
                        held.push(super::make_resumed());
                        sim.turn(held, false).await
                    }
                }
                Variant::Split { turn: t } if t == turn && events.len() > 1 => {
                    let mut events = events;
                    let rest = events.split_off(1);
                    match sim.turn(events, false).await {
                        Some(c) => Some(c),
                        None => sim.turn(rest, false).await,
                    }
                }
                Variant::DuplicateCompletion { turn: t } if t == turn => {
                    // Raising an event twice is two events, not a duplicate.
                    let mut events = events;
                    let completions: Vec<_> = events
                        .iter()
                        .filter(|e| !matches!(e.event_type, Some(EventType::EventRaised(_))))
                        .cloned()
                        .collect();
                    if !completions.is_empty() {
                        // A separate generator keeps the delivery order identical
                        // to the baseline run.
                        let pick = Rng::new(seed ^ 0xD0B1).below(completions.len());
                        events.push(completions[pick].clone());
                    }
                    sim.turn(events, false).await
                }
                Variant::DuplicateScheduling { turn: t } if t == turn => {
                    sim.turn(events, true).await
                }
                Variant::Terminate { turn: t } if t == turn => {
                    let mut events = events;
                    events.push(super::make_terminated(json("stopped")));
                    sim.turn(events, false).await
                }
                _ => sim.turn(events, false).await,
            };
            turn += 1;
        }

        // Replay stability: re-executing the complete history reproduces
        // the completion and schedules nothing new.
        let resp = execute(&sim.orchestrator(), sim.history.clone(), vec![]).await;
        let completion = completion.unwrap();
        let non_completion: Vec<_> = resp
            .actions
            .iter()
            .filter(|a| {
                !matches!(
                    a.workflow_action_type,
                    Some(WorkflowActionType::CompleteWorkflow(_))
                )
            })
            .collect();
        let terminated =
            completion.workflow_status == proto::OrchestrationStatus::Terminated as i32;
        if !terminated {
            assert!(
                non_completion.is_empty(),
                "replay scheduled {non_completion:?}\n{}",
                sim.trace
            );
            let replayed = resp
                .actions
                .iter()
                .find_map(|a| match &a.workflow_action_type {
                    Some(WorkflowActionType::CompleteWorkflow(c)) => Some(c.clone()),
                    _ => None,
                });
            assert_eq!(
                replayed.map(|c| (c.workflow_status, c.result)),
                Some((completion.workflow_status, completion.result.clone())),
                "full replay changed the outcome\n{}",
                sim.trace
            );
        }

        let outcome = Outcome {
            status: completion.workflow_status,
            result: completion.result.clone(),
            failure_type: completion.failure_details.map(|f| f.error_type),
            custom_status: sim.custom_status.clone(),
        };
        (outcome, turn, sim.trace)
    }

    /// Run every program over every seed and compare each variant's outcome
    /// with the baseline.
    async fn check_variant(make: impl Fn(usize, u64) -> Variant) {
        for (name, program) in programs() {
            for seed in 0..SEEDS {
                let (baseline, turns, _) = run(program, seed, Variant::Baseline).await;
                assert_ne!(
                    baseline.failure_type.as_deref(),
                    Some("NonDeterminismError"),
                    "{name} seed {seed}"
                );
                let variant = make(turns, seed);
                let (outcome, _, trace) = run(program, seed, variant).await;
                assert_eq!(
                    outcome, baseline,
                    "{name} seed {seed}: {variant:?} changed the outcome\n{trace}"
                );
            }
        }
    }

    fn pick_turn(turns: usize, seed: u64) -> usize {
        1 + (seed as usize % turns.max(2).saturating_sub(1).max(1))
    }

    #[tokio::test]
    async fn every_turn_replays_deterministically() {
        // `run` asserts the per-turn invariants and full-replay stability.
        for (name, program) in programs() {
            for seed in 0..SEEDS {
                let (outcome, _, trace) = run(program, seed, Variant::Baseline).await;
                assert!(
                    outcome.status == proto::OrchestrationStatus::Completed as i32
                        || outcome.status == proto::OrchestrationStatus::ContinuedAsNew as i32,
                    "{name} seed {seed}: {outcome:?}\n{trace}"
                );
            }
        }
    }

    #[tokio::test]
    async fn suspension_is_transparent() {
        check_variant(|turns, seed| Variant::Suspend {
            turn: pick_turn(turns, seed),
            resume_next_turn: false,
        })
        .await;
        check_variant(|turns, seed| Variant::Suspend {
            turn: pick_turn(turns, seed),
            resume_next_turn: true,
        })
        .await;
    }

    #[tokio::test]
    async fn batching_does_not_change_the_outcome() {
        check_variant(|turns, seed| Variant::Split {
            turn: pick_turn(turns, seed),
        })
        .await;
    }

    #[tokio::test]
    async fn duplicate_completions_are_ignored() {
        check_variant(|turns, seed| Variant::DuplicateCompletion {
            turn: pick_turn(turns, seed),
        })
        .await;
    }

    #[tokio::test]
    async fn duplicate_scheduling_events_are_ignored() {
        check_variant(|turns, seed| Variant::DuplicateScheduling {
            turn: pick_turn(turns, seed) - 1,
        })
        .await;
    }

    #[tokio::test]
    async fn terminate_stops_unless_already_completed() {
        for (name, program) in programs() {
            for seed in 0..SEEDS {
                let (baseline, turns, _) = run(program, seed, Variant::Baseline).await;
                let turn = pick_turn(turns, seed);
                let (outcome, _, trace) = run(program, seed, Variant::Terminate { turn }).await;
                let terminated = Outcome {
                    status: proto::OrchestrationStatus::Terminated as i32,
                    result: json("stopped"),
                    failure_type: None,
                    custom_status: outcome.custom_status.clone(),
                };
                // The terminate lands in the turn the baseline completed in
                // (completion wins) or earlier (terminated).
                assert!(
                    outcome == terminated || (turn == turns - 1 && outcome == baseline),
                    "{name} seed {seed} turn {turn}/{turns}: {outcome:?}\n{trace}"
                );
            }
        }
    }
}
