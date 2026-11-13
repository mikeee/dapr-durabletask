use chrono::{DateTime, Utc};

use super::{FailureDetails, OrchestrationStatus};
use crate::internal::from_timestamp;
use crate::proto;

/// A schedule that starts a workflow repeatedly.
///
/// Each run is a new, detached workflow instance with the ID
/// `<name>_<owner>_<fire time>`, where the owner is a 6-character tag of the
/// app that owns the schedule and the fire time is UTC formatted as
/// `20060102T150405Z`. A run started by
/// [`trigger_workflow_schedule`](crate::client::TaskHubGrpcClient::trigger_workflow_schedule)
/// has the ID `<name>_<owner>_<trigger time>_manual`, with the trigger time in
/// milliseconds. So the runs of a schedule are the instances whose ID starts
/// with `<name>_<owner>_`; list them with
/// [`ListInstanceIdsOptions::with_instance_id_prefix`](crate::client::ListInstanceIdsOptions::with_instance_id_prefix).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowSchedule {
    /// Identifies the schedule within the app: 1 to 30 letters, digits or `-`.
    pub name: String,
    /// Name of the workflow each run starts.
    pub workflow_name: String,
    /// When runs start, in the syntax of the Dapr Jobs API. See
    /// [`create_workflow_schedule`](crate::client::TaskHubGrpcClient::create_workflow_schedule).
    pub schedule: String,
    /// The raw (typically JSON) input given to every run.
    pub serialized_input: Option<String>,
    /// Time of the first run.
    pub due_time: Option<DateTime<Utc>>,
    /// Time after which no run starts.
    pub expiration_time: Option<DateTime<Utc>>,
    /// Number of runs after which the schedule completes.
    pub repeats: Option<u32>,
    /// App ID hosting the workflow each run starts. `None` means the app
    /// that owns the schedule.
    pub target_app_id: Option<String>,
    /// Namespace of [`Self::target_app_id`].
    pub target_app_namespace: Option<String>,
    /// Whether the times the schedule missed while the app was down start one
    /// run, after which it carries on from the next time after now. When
    /// `false` (the default), every missed time starts its own run, one after
    /// another.
    #[serde(default)]
    pub skip_missed: bool,
    /// How the schedule stands, as reported by the sidecar.
    pub status: Option<WorkflowScheduleStatus>,
}

/// The state of a [`WorkflowSchedule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WorkflowScheduleState {
    /// The schedule is waiting for its next run.
    Active,
    /// The schedule has no runs left, because of its repeats or expiration
    /// time.
    Completed,
    /// The schedule stopped with an error, given in
    /// [`WorkflowScheduleStatus::failure_message`].
    Failed,
}

/// How a [`WorkflowSchedule`] stands.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowScheduleStatus {
    pub state: WorkflowScheduleState,
    /// When the schedule was created.
    pub created_at: Option<DateTime<Utc>>,
    /// When the next run starts, unless it is triggered sooner. `None` once
    /// the schedule is not active.
    pub next_run_time: Option<DateTime<Utc>>,
    /// The time the last run was started for: its fire time, or when it was
    /// triggered.
    pub last_run_time: Option<DateTime<Utc>>,
    /// Number of runs started so far.
    pub run_count: u32,
    /// How the last run stands. `None` before the first run.
    pub last_run: Option<WorkflowScheduleRun>,
    /// Why the schedule failed, when the state is
    /// [`WorkflowScheduleState::Failed`].
    pub failure_message: Option<String>,
}

/// One run of a [`WorkflowSchedule`], read from its workflow instance. Only
/// `instance_id` is set when the instance cannot be read, for example
/// because it was purged.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowScheduleRun {
    pub instance_id: String,
    pub runtime_status: Option<OrchestrationStatus>,
    pub created_at: Option<DateTime<Utc>>,
    pub last_updated_at: Option<DateTime<Utc>>,
    pub serialized_output: Option<String>,
    pub failure_details: Option<FailureDetails>,
}

impl From<proto::WorkflowSchedule> for WorkflowSchedule {
    fn from(ws: proto::WorkflowSchedule) -> Self {
        Self {
            name: ws.name,
            workflow_name: ws.workflow_name,
            schedule: ws.schedule,
            serialized_input: ws.input,
            due_time: ws.due_time.as_ref().and_then(from_timestamp),
            expiration_time: ws.expiration_time.as_ref().and_then(from_timestamp),
            repeats: ws.repeats,
            target_app_id: ws.target_app_id,
            target_app_namespace: ws.target_app_namespace,
            skip_missed: ws.skip_missed.unwrap_or(false),
            status: ws.status.map(Into::into),
        }
    }
}

impl From<proto::WorkflowScheduleStatus> for WorkflowScheduleStatus {
    fn from(status: proto::WorkflowScheduleStatus) -> Self {
        Self {
            state: status.state().into(),
            created_at: status.created_at.as_ref().and_then(from_timestamp),
            next_run_time: status.next_run_time.as_ref().and_then(from_timestamp),
            last_run_time: status.last_run_time.as_ref().and_then(from_timestamp),
            run_count: status.run_count,
            last_run: status.last_run.map(Into::into),
            failure_message: status.failure_message,
        }
    }
}

impl From<proto::WorkflowScheduleState> for WorkflowScheduleState {
    fn from(state: proto::WorkflowScheduleState) -> Self {
        match state {
            proto::WorkflowScheduleState::Active => Self::Active,
            proto::WorkflowScheduleState::Completed => Self::Completed,
            proto::WorkflowScheduleState::Failed => Self::Failed,
        }
    }
}

impl From<proto::WorkflowScheduleRun> for WorkflowScheduleRun {
    fn from(run: proto::WorkflowScheduleRun) -> Self {
        Self {
            instance_id: run.instance_id,
            runtime_status: run
                .runtime_status
                .and_then(|s| OrchestrationStatus::try_from(s).ok()),
            created_at: run.created_timestamp.as_ref().and_then(from_timestamp),
            last_updated_at: run.last_updated_timestamp.as_ref().and_then(from_timestamp),
            serialized_output: run.output,
            failure_details: run.failure_details.as_ref().map(FailureDetails::from),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::prost_types::Timestamp;

    fn ts(seconds: i64) -> Option<Timestamp> {
        Some(Timestamp { seconds, nanos: 0 })
    }

    #[test]
    fn from_proto_with_status_and_last_run() {
        let ws = WorkflowSchedule::from(proto::WorkflowSchedule {
            name: "nightly".into(),
            workflow_name: "report".into(),
            input: Some("42".into()),
            schedule: "@daily".into(),
            due_time: ts(1),
            expiration_time: ts(2),
            repeats: Some(3),
            target_app_id: Some("app2".into()),
            target_app_namespace: Some("ns2".into()),
            skip_missed: Some(true),
            status: Some(proto::WorkflowScheduleStatus {
                state: proto::WorkflowScheduleState::Failed as i32,
                created_at: ts(10),
                next_run_time: None,
                last_run_time: ts(20),
                run_count: 4,
                last_run: Some(proto::WorkflowScheduleRun {
                    instance_id: "nightly_19700101T000020Z".into(),
                    runtime_status: Some(proto::OrchestrationStatus::Failed as i32),
                    created_timestamp: ts(20),
                    last_updated_timestamp: ts(21),
                    output: Some("\"out\"".into()),
                    failure_details: Some(proto::TaskFailureDetails {
                        error_type: "E".into(),
                        error_message: "boom".into(),
                        ..Default::default()
                    }),
                }),
                failure_message: Some("bad".into()),
            }),
        });
        assert_eq!(ws.name, "nightly");
        assert_eq!(ws.workflow_name, "report");
        assert_eq!(ws.schedule, "@daily");
        assert_eq!(ws.serialized_input.as_deref(), Some("42"));
        assert_eq!(ws.due_time.unwrap().timestamp(), 1);
        assert_eq!(ws.expiration_time.unwrap().timestamp(), 2);
        assert_eq!(ws.repeats, Some(3));
        assert_eq!(ws.target_app_id.as_deref(), Some("app2"));
        assert_eq!(ws.target_app_namespace.as_deref(), Some("ns2"));
        assert!(ws.skip_missed);

        let status = ws.status.expect("status");
        assert_eq!(status.state, WorkflowScheduleState::Failed);
        assert_eq!(status.created_at.unwrap().timestamp(), 10);
        assert!(status.next_run_time.is_none());
        assert_eq!(status.last_run_time.unwrap().timestamp(), 20);
        assert_eq!(status.run_count, 4);
        assert_eq!(status.failure_message.as_deref(), Some("bad"));

        let run = status.last_run.expect("last run");
        assert_eq!(run.instance_id, "nightly_19700101T000020Z");
        assert_eq!(run.runtime_status, Some(OrchestrationStatus::Failed));
        assert_eq!(run.created_at.unwrap().timestamp(), 20);
        assert_eq!(run.last_updated_at.unwrap().timestamp(), 21);
        assert_eq!(run.serialized_output.as_deref(), Some("\"out\""));
        assert_eq!(run.failure_details.unwrap().message, "boom");
    }

    #[test]
    fn from_proto_minimal() {
        let ws = WorkflowSchedule::from(proto::WorkflowSchedule {
            name: "s".into(),
            status: Some(proto::WorkflowScheduleStatus {
                last_run: Some(proto::WorkflowScheduleRun {
                    instance_id: "s_x".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(ws.serialized_input.is_none() && ws.due_time.is_none() && ws.repeats.is_none());
        assert!(!ws.skip_missed);
        let status = ws.status.expect("status");
        assert_eq!(status.state, WorkflowScheduleState::Active);
        let run = status.last_run.expect("last run");
        assert_eq!(run.instance_id, "s_x");
        assert!(run.runtime_status.is_none() && run.failure_details.is_none());
    }

    #[test]
    fn state_from_proto() {
        assert_eq!(
            WorkflowScheduleState::from(proto::WorkflowScheduleState::Active),
            WorkflowScheduleState::Active
        );
        assert_eq!(
            WorkflowScheduleState::from(proto::WorkflowScheduleState::Completed),
            WorkflowScheduleState::Completed
        );
        assert_eq!(
            WorkflowScheduleState::from(proto::WorkflowScheduleState::Failed),
            WorkflowScheduleState::Failed
        );
    }
}
