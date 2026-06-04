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
