use chrono::{DateTime, Utc};

use super::{DurableTaskError, FailureDetails, OrchestrationStatus};
use crate::proto;

/// Snapshot of an orchestration instance's current state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OrchestrationState {
    pub instance_id: String,
    pub name: String,
    pub runtime_status: OrchestrationStatus,
    pub created_at: Option<DateTime<Utc>>,
    pub last_updated_at: Option<DateTime<Utc>>,
    pub serialized_input: Option<String>,
    pub serialized_output: Option<String>,
    pub serialized_custom_status: Option<String>,
    pub failure_details: Option<FailureDetails>,
}

/// Error returned when converting a `GetInstanceResponse` to an `OrchestrationState`
/// when the instance does not exist or has no workflow state.
#[derive(Debug, Clone)]
pub struct InstanceNotFound;

impl TryFrom<&proto::GetInstanceResponse> for OrchestrationState {
    type Error = InstanceNotFound;

    /// Constructs an `OrchestrationState` from a proto `GetInstanceResponse`.
    ///
    /// Returns `Err(InstanceNotFound)` if the response indicates the instance
    /// does not exist or has no workflow state.
    fn try_from(response: &proto::GetInstanceResponse) -> std::result::Result<Self, Self::Error> {
        let state = response.workflow_state.as_ref().ok_or(InstanceNotFound)?;
        if !response.exists {
            return Err(InstanceNotFound);
        }

        Ok(Self {
            instance_id: state.instance_id.clone(),
            name: state.name.clone(),
            runtime_status: OrchestrationStatus::try_from(state.workflow_status)
                .unwrap_or(OrchestrationStatus::Running),
            created_at: state.created_timestamp.as_ref().map(timestamp_to_datetime),
            last_updated_at: state
                .last_updated_timestamp
                .as_ref()
                .map(timestamp_to_datetime),
            serialized_input: state.input.clone(),
            serialized_output: state.output.clone(),
            serialized_custom_status: state.custom_status.clone(),
            failure_details: state.failure_details.as_ref().map(FailureDetails::from),
        })
    }
}

impl OrchestrationState {
    /// Returns `Ok(())` if the orchestration has not failed, or an error with
    /// the failure details if it has.
    pub fn raise_if_failed(&self) -> super::Result<()> {
        if self.runtime_status == OrchestrationStatus::Failed {
            let message = self
                .failure_details
                .as_ref()
                .map(|d| d.message.clone())
                .unwrap_or_else(|| "unknown failure".to_string());
            return Err(DurableTaskError::OrchestrationFailed {
                instance_id: self.instance_id.clone(),
                message,
                failure_details: self.failure_details.clone(),
            });
        }
        Ok(())
    }
}

fn timestamp_to_datetime(ts: &crate::proto::prost_types::Timestamp) -> DateTime<Utc> {
    let nanos = if ts.nanos >= 0 && ts.nanos <= 999_999_999 {
        ts.nanos as u32
    } else {
        0
    };
    DateTime::from_timestamp(ts.seconds, nanos).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto;
    use crate::proto::prost_types::Timestamp;

    fn make_workflow_state(status: i32) -> proto::WorkflowState {
        proto::WorkflowState {
            instance_id: "test-id".into(),
            name: "TestOrch".into(),
            version: None,
            workflow_status: status,
            scheduled_start_timestamp: None,
            created_timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 500_000_000,
            }),
            last_updated_timestamp: Some(Timestamp {
                seconds: 1_700_000_100,
                nanos: 0,
            }),
            input: Some(r#""hello""#.into()),
            output: Some(r#""world""#.into()),
            custom_status: None,
            failure_details: None,
            execution_id: None,
            completed_timestamp: None,
            parent_instance_id: None,
            tags: Default::default(),
        }
    }

    #[test]
    fn timestamp_valid_seconds_and_nanos() {
        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos: 500_000_000,
        };
        let dt = timestamp_to_datetime(&ts);
        assert_eq!(dt.timestamp(), 1_700_000_000);
        assert_eq!(dt.timestamp_subsec_nanos(), 500_000_000);
    }

    #[test]
    fn timestamp_negative_nanos_clamps_to_zero() {
        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos: -1,
        };
        let dt = timestamp_to_datetime(&ts);
        assert_eq!(dt.timestamp(), 1_700_000_000);
        assert_eq!(dt.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn timestamp_overflow_nanos_clamps_to_zero() {
        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos: 1_000_000_000,
        };
        let dt = timestamp_to_datetime(&ts);
        assert_eq!(dt.timestamp(), 1_700_000_000);
        assert_eq!(dt.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn timestamp_negative_seconds() {
        let ts = Timestamp {
            seconds: -1,
            nanos: 0,
        };
        let dt = timestamp_to_datetime(&ts);
        assert_eq!(dt.timestamp(), -1);
    }

    #[test]
    fn try_from_valid_response() {
        let resp = proto::GetInstanceResponse {
            exists: true,
            workflow_state: Some(make_workflow_state(1)), // Completed
        };
        let state = OrchestrationState::try_from(&resp).unwrap();
        assert_eq!(state.instance_id, "test-id");
        assert_eq!(state.name, "TestOrch");
        assert_eq!(state.runtime_status, OrchestrationStatus::Completed);
        assert!(state.created_at.is_some());
        assert_eq!(state.created_at.unwrap().timestamp(), 1_700_000_000);
        assert_eq!(state.serialized_input.as_deref(), Some(r#""hello""#));
    }

    #[test]
    fn try_from_not_exists() {
        let resp = proto::GetInstanceResponse {
            exists: false,
            workflow_state: Some(make_workflow_state(0)),
        };
        assert!(OrchestrationState::try_from(&resp).is_err());
    }

    #[test]
    fn try_from_no_workflow_state() {
        let resp = proto::GetInstanceResponse {
            exists: true,
            workflow_state: None,
        };
        assert!(OrchestrationState::try_from(&resp).is_err());
    }

    #[test]
    fn raise_if_failed_ok_for_completed() {
        let state = OrchestrationState {
            instance_id: "i".into(),
            name: "n".into(),
            runtime_status: OrchestrationStatus::Completed,
            created_at: None,
            last_updated_at: None,
            serialized_input: None,
            serialized_output: None,
            serialized_custom_status: None,
            failure_details: None,
        };
        assert!(state.raise_if_failed().is_ok());
    }

    #[test]
    fn raise_if_failed_returns_error_for_failed() {
        let state = OrchestrationState {
            instance_id: "i1".into(),
            name: "n".into(),
            runtime_status: OrchestrationStatus::Failed,
            created_at: None,
            last_updated_at: None,
            serialized_input: None,
            serialized_output: None,
            serialized_custom_status: None,
            failure_details: None,
        };
        let err = state.raise_if_failed().unwrap_err();
        match err {
            DurableTaskError::OrchestrationFailed {
                instance_id,
                message,
                ..
            } => {
                assert_eq!(instance_id, "i1");
                assert_eq!(message, "unknown failure");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn raise_if_failed_with_failure_details() {
        let details = FailureDetails {
            message: "boom".into(),
            error_type: "TestError".into(),
            stack_trace: Some("at line 1".into()),
        };
        let state = OrchestrationState {
            instance_id: "i2".into(),
            name: "n".into(),
            runtime_status: OrchestrationStatus::Failed,
            created_at: None,
            last_updated_at: None,
            serialized_input: None,
            serialized_output: None,
            serialized_custom_status: None,
            failure_details: Some(details.clone()),
        };
        let err = state.raise_if_failed().unwrap_err();
        match err {
            DurableTaskError::OrchestrationFailed {
                message,
                failure_details,
                ..
            } => {
                assert_eq!(message, "boom");
                let fd = failure_details.unwrap();
                assert_eq!(fd.error_type, "TestError");
                assert_eq!(fd.stack_trace.as_deref(), Some("at line 1"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
