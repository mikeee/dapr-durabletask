use crate::proto;

/// Details about a task or orchestration failure.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FailureDetails {
    pub message: String,
    pub error_type: String,
    pub stack_trace: Option<String>,
}

impl FailureDetails {
    /// Constructs a `FailureDetails` from the proto `TaskFailureDetails` message.
    pub fn from_proto(details: &proto::TaskFailureDetails) -> Self {
        Self {
            message: details.error_message.clone(),
            error_type: details.error_type.clone(),
            stack_trace: details.stack_trace.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_proto_all_fields() {
        let proto_details = proto::TaskFailureDetails {
            error_type: "RuntimeError".into(),
            error_message: "something broke".into(),
            stack_trace: Some("at main.rs:42".into()),
            inner_failure: None,
            is_non_retriable: false,
        };
        let fd = FailureDetails::from_proto(&proto_details);
        assert_eq!(fd.message, "something broke");
        assert_eq!(fd.error_type, "RuntimeError");
        assert_eq!(fd.stack_trace.as_deref(), Some("at main.rs:42"));
    }

    #[test]
    fn from_proto_no_stack_trace() {
        let proto_details = proto::TaskFailureDetails {
            error_type: "Error".into(),
            error_message: "msg".into(),
            stack_trace: None,
            inner_failure: None,
            is_non_retriable: true,
        };
        let fd = FailureDetails::from_proto(&proto_details);
        assert_eq!(fd.message, "msg");
        assert_eq!(fd.error_type, "Error");
        assert!(fd.stack_trace.is_none());
    }
}
