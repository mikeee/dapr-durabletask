use crate::proto;

/// Details about a task or orchestration failure.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FailureDetails {
    pub message: String,
    pub error_type: String,
    pub stack_trace: Option<String>,
}

impl From<&proto::TaskFailureDetails> for FailureDetails {
    fn from(details: &proto::TaskFailureDetails) -> Self {
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
        let fd = FailureDetails::from(&proto_details);
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
        let fd = FailureDetails::from(&proto_details);
        assert_eq!(fd.message, "msg");
        assert_eq!(fd.error_type, "Error");
        assert!(fd.stack_trace.is_none());
    }

    #[test]
    fn from_proto_empty_strings() {
        let proto_details = proto::TaskFailureDetails {
            error_type: "".into(),
            error_message: "".into(),
            stack_trace: Some("".into()),
            inner_failure: None,
            is_non_retriable: false,
        };
        let fd = FailureDetails::from(&proto_details);
        assert_eq!(fd.message, "");
        assert_eq!(fd.error_type, "");
        assert_eq!(fd.stack_trace.as_deref(), Some(""));
    }

    #[test]
    fn serde_json_roundtrip_all_fields() {
        let fd = FailureDetails {
            message: "boom".into(),
            error_type: "TestError".into(),
            stack_trace: Some("at line 1".into()),
        };
        let json = serde_json::to_string(&fd).unwrap();
        let back: FailureDetails = serde_json::from_str(&json).unwrap();
        assert_eq!(back.message, "boom");
        assert_eq!(back.error_type, "TestError");
        assert_eq!(back.stack_trace.as_deref(), Some("at line 1"));
    }

    #[test]
    fn serde_json_wire_shape() {
        let fd = FailureDetails {
            message: "err".into(),
            error_type: "E".into(),
            stack_trace: None,
        };
        let json = serde_json::to_string(&fd).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["message"], "err");
        assert_eq!(v["error_type"], "E");
        assert_eq!(v["stack_trace"], serde_json::Value::Null);
    }

    #[test]
    fn clone_is_independent() {
        let fd = FailureDetails {
            message: "a".into(),
            error_type: "b".into(),
            stack_trace: Some("c".into()),
        };
        let fd2 = fd.clone();
        assert_eq!(fd.message, fd2.message);
        assert_eq!(fd.error_type, fd2.error_type);
        assert_eq!(fd.stack_trace, fd2.stack_trace);
    }
}
