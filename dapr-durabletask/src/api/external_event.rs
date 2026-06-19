use serde::de::DeserializeOwned;

/// Result of waiting for an external event with a timeout.
///
/// Returned by [`OrchestrationContext::wait_for_external_event_with_timeout`].
///
/// [`OrchestrationContext::wait_for_external_event_with_timeout`]: crate::task::OrchestrationContext::wait_for_external_event_with_timeout
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalEventResult {
    /// Event received before the timeout.
    ///
    /// Raw JSON payload, or `None` if the event carried no data.
    Received(Option<String>),

    /// Event not received before the timeout.
    TimedOut,
}

impl ExternalEventResult {
    /// `true` if the event was received.
    pub fn is_received(&self) -> bool {
        matches!(self, Self::Received(_))
    }

    /// `true` if the wait timed out.
    pub fn is_timed_out(&self) -> bool {
        matches!(self, Self::TimedOut)
    }

    /// Deserialise the payload, returning `None` if timed out or absent.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Option<super::Result<T>> {
        match self {
            Self::Received(Some(json)) => {
                Some(serde_json::from_str(json).map_err(super::DurableTaskError::Serialization))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn received_with_payload_is_received() {
        let r = ExternalEventResult::Received(Some(r#""hello""#.into()));
        assert!(r.is_received());
        assert!(!r.is_timed_out());
    }

    #[test]
    fn received_without_payload_is_received() {
        let r = ExternalEventResult::Received(None);
        assert!(r.is_received());
        assert!(!r.is_timed_out());
    }

    #[test]
    fn timed_out_is_timed_out() {
        let r = ExternalEventResult::TimedOut;
        assert!(r.is_timed_out());
        assert!(!r.is_received());
    }

    #[test]
    fn deserialize_valid_json() {
        let r = ExternalEventResult::Received(Some("42".into()));
        let val: i32 = r.deserialize().unwrap().unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn deserialize_string_payload() {
        let r = ExternalEventResult::Received(Some(r#""world""#.into()));
        let val: String = r.deserialize().unwrap().unwrap();
        assert_eq!(val, "world");
    }

    #[test]
    fn deserialize_invalid_json_returns_error() {
        let r = ExternalEventResult::Received(Some("not json".into()));
        let res: Option<super::super::Result<String>> = r.deserialize();
        assert!(res.unwrap().is_err());
    }

    #[test]
    fn deserialize_timed_out_returns_none() {
        let r = ExternalEventResult::TimedOut;
        let res: Option<super::super::Result<i32>> = r.deserialize();
        assert!(res.is_none());
    }

    #[test]
    fn deserialize_received_none_returns_none() {
        let r = ExternalEventResult::Received(None);
        let res: Option<super::super::Result<i32>> = r.deserialize();
        assert!(res.is_none());
    }

    #[test]
    fn equality() {
        assert_eq!(ExternalEventResult::TimedOut, ExternalEventResult::TimedOut);
        assert_eq!(
            ExternalEventResult::Received(Some("1".into())),
            ExternalEventResult::Received(Some("1".into()))
        );
        assert_ne!(
            ExternalEventResult::Received(Some("1".into())),
            ExternalEventResult::TimedOut
        );
        assert_ne!(
            ExternalEventResult::Received(None),
            ExternalEventResult::Received(Some("1".into()))
        );
    }
}
