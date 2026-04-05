use crate::api::DurableTaskError;
use crate::proto::prost_types::Timestamp;

#[cfg(test)]
use crate::proto::{
    EventRaisedEvent, ExecutionResumedEvent, ExecutionStartedEvent, ExecutionSuspendedEvent,
    ExecutionTerminatedEvent, HistoryEvent, ParentInstanceInfo, TraceContext, WorkflowInstance,
    history_event::EventType,
};

/// Maximum default allowed length for names and instance IDs.
#[cfg(test)]
pub const DEFAULT_MAX_IDENTIFIER_LENGTH: usize = 1_024;

/// Validate that a string identifier is within bounds and contains no control characters.
pub fn validate_identifier(
    value: &str,
    label: &str,
    max_length: usize,
) -> Result<(), DurableTaskError> {
    if value.is_empty() {
        return Err(DurableTaskError::Other(format!("{label} cannot be empty")));
    }
    if value.len() > max_length {
        return Err(DurableTaskError::Other(format!(
            "{label} exceeds maximum length of {max_length}"
        )));
    }
    if value.contains('\0') || value.chars().any(|c| c.is_control()) {
        return Err(DurableTaskError::Other(format!(
            "{label} contains invalid characters"
        )));
    }
    Ok(())
}

/// Convert a chrono DateTime<Utc> to a prost Timestamp.
pub fn to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

/// Convert a prost Timestamp to a chrono DateTime<Utc>.
pub fn from_timestamp(ts: &Timestamp) -> Option<chrono::DateTime<chrono::Utc>> {
    if ts.nanos < 0 || ts.nanos > 999_999_999 {
        return None;
    }
    chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
}

/// Create a new HistoryEvent with the given event_id and timestamp.
#[cfg(test)]
pub fn new_history_event(event_id: i32, timestamp: Timestamp) -> HistoryEvent {
    HistoryEvent {
        event_id,
        timestamp: Some(timestamp),
        router: None,
        event_type: None,
    }
}

/// Create an ExecutionStarted history event.
#[cfg(test)]
pub fn new_execution_started_event(
    name: &str,
    instance_id: &str,
    input: Option<&str>,
    parent_info: Option<ParentInstanceInfo>,
    parent_trace_context: Option<TraceContext>,
    scheduled_start_timestamp: Option<Timestamp>,
) -> HistoryEvent {
    let now = chrono::Utc::now();
    let mut event = new_history_event(-1, to_timestamp(now));
    event.event_type = Some(EventType::ExecutionStarted(ExecutionStartedEvent {
        name: name.to_string(),
        version: None,
        input: input.map(|s| s.to_string()),
        workflow_instance: Some(WorkflowInstance {
            instance_id: instance_id.to_string(),
            execution_id: None,
        }),
        parent_instance: parent_info,
        scheduled_start_timestamp,
        parent_trace_context,
        workflow_span_id: None,
        tags: Default::default(),
    }));
    event
}

/// Create an EventRaised history event.
#[cfg(test)]
pub fn new_event_raised_event(name: &str, input: Option<&str>) -> HistoryEvent {
    let now = chrono::Utc::now();
    let mut event = new_history_event(-1, to_timestamp(now));
    event.event_type = Some(EventType::EventRaised(EventRaisedEvent {
        name: name.to_string(),
        input: input.map(|s| s.to_string()),
    }));
    event
}

/// Create an ExecutionTerminated history event.
#[cfg(test)]
pub fn new_execution_terminated_event(output: Option<&str>, recursive: bool) -> HistoryEvent {
    let now = chrono::Utc::now();
    let mut event = new_history_event(-1, to_timestamp(now));
    event.event_type = Some(EventType::ExecutionTerminated(ExecutionTerminatedEvent {
        input: output.map(|s| s.to_string()),
        recurse: recursive,
    }));
    event
}

/// Create an ExecutionSuspended history event.
#[cfg(test)]
pub fn new_execution_suspended_event(reason: Option<&str>) -> HistoryEvent {
    let now = chrono::Utc::now();
    let mut event = new_history_event(-1, to_timestamp(now));
    event.event_type = Some(EventType::ExecutionSuspended(ExecutionSuspendedEvent {
        input: reason.map(|s| s.to_string()),
    }));
    event
}

/// Create an ExecutionResumed history event.
#[cfg(test)]
pub fn new_execution_resumed_event(reason: Option<&str>) -> HistoryEvent {
    let now = chrono::Utc::now();
    let mut event = new_history_event(-1, to_timestamp(now));
    event.event_type = Some(EventType::ExecutionResumed(ExecutionResumedEvent {
        input: reason.map(|s| s.to_string()),
    }));
    event
}

/// Get the string value from an `Option<String>` (like `google.protobuf.StringValue`).
#[cfg(test)]
pub fn get_string_value(value: &Option<String>) -> Option<&str> {
    value.as_deref()
}

/// Wrap a string into an `Option<String>` (like `google.protobuf.StringValue`).
#[cfg(test)]
pub fn to_string_value(value: &str) -> Option<String> {
    Some(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_roundtrip() {
        let now = chrono::Utc::now();
        let ts = to_timestamp(now);
        let back = from_timestamp(&ts).expect("should convert back");
        assert_eq!(now.timestamp(), back.timestamp());
        assert_eq!(now.timestamp_subsec_nanos(), back.timestamp_subsec_nanos());
    }

    #[test]
    fn test_new_history_event() {
        let ts = to_timestamp(chrono::Utc::now());
        let event = new_history_event(42, ts);
        assert_eq!(event.event_id, 42);
        assert_eq!(event.timestamp, Some(ts));
        assert!(event.event_type.is_none());
    }

    #[test]
    fn test_execution_started_event() {
        let event =
            new_execution_started_event("my_orch", "inst-1", Some("\"hello\""), None, None, None);
        assert_eq!(event.event_id, -1);
        match event.event_type {
            Some(EventType::ExecutionStarted(e)) => {
                assert_eq!(e.name, "my_orch");
                assert_eq!(e.input, Some("\"hello\"".to_string()));
                let wi = e.workflow_instance.unwrap();
                assert_eq!(wi.instance_id, "inst-1");
            }
            _ => panic!("expected ExecutionStarted"),
        }
    }

    #[test]
    fn test_event_raised_event() {
        let event = new_event_raised_event("approval", Some("\"yes\""));
        match event.event_type {
            Some(EventType::EventRaised(e)) => {
                assert_eq!(e.name, "approval");
                assert_eq!(e.input, Some("\"yes\"".to_string()));
            }
            _ => panic!("expected EventRaised"),
        }
    }

    #[test]
    fn test_execution_terminated_event() {
        let event = new_execution_terminated_event(Some("\"reason\""), true);
        match event.event_type {
            Some(EventType::ExecutionTerminated(e)) => {
                assert_eq!(e.input, Some("\"reason\"".to_string()));
                assert!(e.recurse);
            }
            _ => panic!("expected ExecutionTerminated"),
        }
    }

    #[test]
    fn test_execution_suspended_event() {
        let event = new_execution_suspended_event(Some("pause"));
        match event.event_type {
            Some(EventType::ExecutionSuspended(e)) => {
                assert_eq!(e.input, Some("pause".to_string()));
            }
            _ => panic!("expected ExecutionSuspended"),
        }
    }

    #[test]
    fn test_execution_resumed_event() {
        let event = new_execution_resumed_event(None);
        match event.event_type {
            Some(EventType::ExecutionResumed(e)) => {
                assert_eq!(e.input, None);
            }
            _ => panic!("expected ExecutionResumed"),
        }
    }

    #[test]
    fn test_string_value_helpers() {
        let some = Some("hello".to_string());
        assert_eq!(get_string_value(&some), Some("hello"));

        let none: Option<String> = None;
        assert_eq!(get_string_value(&none), None);

        assert_eq!(to_string_value("world"), Some("world".to_string()));
    }

    #[test]
    fn test_from_timestamp_rejects_negative_nanos() {
        let ts = Timestamp {
            seconds: 1_000_000,
            nanos: -1,
        };
        assert!(from_timestamp(&ts).is_none());
    }

    #[test]
    fn test_from_timestamp_rejects_overflow_nanos() {
        let ts = Timestamp {
            seconds: 1_000_000,
            nanos: 1_000_000_000,
        };
        assert!(from_timestamp(&ts).is_none());
    }

    #[test]
    fn test_validate_identifier_valid() {
        assert!(
            validate_identifier("my_orchestrator", "name", DEFAULT_MAX_IDENTIFIER_LENGTH).is_ok()
        );
        assert!(
            validate_identifier(
                "a".repeat(DEFAULT_MAX_IDENTIFIER_LENGTH).as_str(),
                "name",
                DEFAULT_MAX_IDENTIFIER_LENGTH
            )
            .is_ok()
        );
    }

    #[test]
    fn test_validate_identifier_too_long() {
        let long = "a".repeat(DEFAULT_MAX_IDENTIFIER_LENGTH + 1);
        let err = validate_identifier(&long, "name", DEFAULT_MAX_IDENTIFIER_LENGTH).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum length"));
    }

    #[test]
    fn test_validate_identifier_null_byte() {
        let err =
            validate_identifier("hello\0world", "name", DEFAULT_MAX_IDENTIFIER_LENGTH).unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn test_validate_identifier_control_char() {
        let err = validate_identifier("hello\x01world", "name", DEFAULT_MAX_IDENTIFIER_LENGTH)
            .unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    #[test]
    fn test_validate_identifier_empty() {
        let err =
            validate_identifier("", "instance ID", DEFAULT_MAX_IDENTIFIER_LENGTH).unwrap_err();
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_identifier_custom_max_length() {
        assert!(validate_identifier("abc", "name", 3).is_ok());
        let err = validate_identifier("abcd", "name", 3).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum length"));
    }
}
