use super::FailureDetails;

/// Error types for the Durable Task SDK.
#[derive(Debug, thiserror::Error)]
pub enum DurableTaskError {
    #[error("gRPC error: {0}")]
    GrpcError(Box<tonic::Status>),

    #[error("Orchestration '{instance_id}' failed: {message}")]
    OrchestrationFailed {
        instance_id: String,
        message: String,
        failure_details: Option<FailureDetails>,
    },

    #[error("Orchestration '{instance_id}' not found")]
    InstanceNotFound { instance_id: String },

    #[error("Task failed: {message}")]
    TaskFailed {
        message: String,
        failure_details: Option<FailureDetails>,
    },

    #[error("Non-determinism error: {message}")]
    NonDeterminism { message: String },

    #[error("Orchestration state error: {message}")]
    OrchestrationState { message: String },

    #[error("Timeout waiting for orchestration")]
    Timeout,

    #[error("Serialisation error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("{0}")]
    Other(String),
}

impl From<tonic::Status> for DurableTaskError {
    fn from(status: tonic::Status) -> Self {
        DurableTaskError::GrpcError(Box::new(status))
    }
}

/// Convenience alias for `Result<T, DurableTaskError>`.
pub type Result<T> = std::result::Result<T, DurableTaskError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_grpc_error() {
        let err = DurableTaskError::GrpcError(Box::new(tonic::Status::internal("oops")));
        let msg = err.to_string();
        assert!(msg.starts_with("gRPC error: "));
        assert!(msg.contains("oops"));
    }

    #[test]
    fn display_orchestration_failed() {
        let err = DurableTaskError::OrchestrationFailed {
            instance_id: "abc".into(),
            message: "boom".into(),
            failure_details: None,
        };
        assert_eq!(err.to_string(), "Orchestration 'abc' failed: boom");
    }

    #[test]
    fn display_instance_not_found() {
        let err = DurableTaskError::InstanceNotFound {
            instance_id: "xyz".into(),
        };
        assert_eq!(err.to_string(), "Orchestration 'xyz' not found");
    }

    #[test]
    fn display_task_failed() {
        let err = DurableTaskError::TaskFailed {
            message: "task err".into(),
            failure_details: None,
        };
        assert_eq!(err.to_string(), "Task failed: task err");
    }

    #[test]
    fn display_timeout() {
        assert_eq!(
            DurableTaskError::Timeout.to_string(),
            "Timeout waiting for orchestration"
        );
    }

    #[test]
    fn display_other() {
        let err = DurableTaskError::Other("custom".into());
        assert_eq!(err.to_string(), "custom");
    }

    #[test]
    fn from_tonic_status() {
        let status = tonic::Status::internal("test");
        let err: DurableTaskError = status.into();
        matches!(err, DurableTaskError::GrpcError(_));
    }

    #[test]
    fn from_serde_json_error() {
        let json_err = serde_json::from_str::<String>("not valid json").unwrap_err();
        let err: DurableTaskError = json_err.into();
        matches!(err, DurableTaskError::Serialization(_));
    }
}
