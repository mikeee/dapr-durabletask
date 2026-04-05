use std::fmt;

/// The runtime status of an orchestration instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum OrchestrationStatus {
    Running,
    Completed,
    ContinuedAsNew,
    Failed,
    Canceled,
    Terminated,
    Pending,
    Suspended,
    Stalled,
}

impl OrchestrationStatus {
    /// Returns `true` if the orchestration has reached a terminal state
    /// (completed, failed, cancelled, or terminated).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Terminated
        )
    }

    /// Returns `true` if the orchestration is actively running.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

impl TryFrom<i32> for OrchestrationStatus {
    type Error = i32;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Running),
            1 => Ok(Self::Completed),
            2 => Ok(Self::ContinuedAsNew),
            3 => Ok(Self::Failed),
            4 => Ok(Self::Canceled),
            5 => Ok(Self::Terminated),
            6 => Ok(Self::Pending),
            7 => Ok(Self::Suspended),
            8 => Ok(Self::Stalled),
            _ => Err(value),
        }
    }
}

impl From<OrchestrationStatus> for i32 {
    fn from(status: OrchestrationStatus) -> Self {
        match status {
            OrchestrationStatus::Running => 0,
            OrchestrationStatus::Completed => 1,
            OrchestrationStatus::ContinuedAsNew => 2,
            OrchestrationStatus::Failed => 3,
            OrchestrationStatus::Canceled => 4,
            OrchestrationStatus::Terminated => 5,
            OrchestrationStatus::Pending => 6,
            OrchestrationStatus::Suspended => 7,
            OrchestrationStatus::Stalled => 8,
        }
    }
}

impl fmt::Display for OrchestrationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_conversion() {
        for value in 0..=8 {
            let status = OrchestrationStatus::try_from(value).unwrap();
            let back: i32 = status.into();
            assert_eq!(value, back);
        }
    }

    #[test]
    fn test_unknown_value_returns_error() {
        assert!(OrchestrationStatus::try_from(99).is_err());
    }

    #[test]
    fn test_is_terminal() {
        assert!(OrchestrationStatus::Completed.is_terminal());
        assert!(OrchestrationStatus::Failed.is_terminal());
        assert!(OrchestrationStatus::Canceled.is_terminal());
        assert!(OrchestrationStatus::Terminated.is_terminal());
        assert!(!OrchestrationStatus::Running.is_terminal());
        assert!(!OrchestrationStatus::Pending.is_terminal());
    }

    #[test]
    fn test_is_running() {
        assert!(OrchestrationStatus::Running.is_running());
        assert!(!OrchestrationStatus::Completed.is_running());
    }

    #[test]
    fn test_display() {
        assert_eq!(
            OrchestrationStatus::ContinuedAsNew.to_string(),
            "ContinuedAsNew"
        );
    }
}
