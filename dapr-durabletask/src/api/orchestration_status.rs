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

impl From<i32> for OrchestrationStatus {
    fn from(value: i32) -> Self {
        match value {
            0 => Self::Running,
            1 => Self::Completed,
            2 => Self::ContinuedAsNew,
            3 => Self::Failed,
            4 => Self::Canceled,
            5 => Self::Terminated,
            6 => Self::Pending,
            7 => Self::Suspended,
            8 => Self::Stalled,
            _ => Self::Running,
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
        match self {
            Self::Running => write!(f, "Running"),
            Self::Completed => write!(f, "Completed"),
            Self::ContinuedAsNew => write!(f, "ContinuedAsNew"),
            Self::Failed => write!(f, "Failed"),
            Self::Canceled => write!(f, "Canceled"),
            Self::Terminated => write!(f, "Terminated"),
            Self::Pending => write!(f, "Pending"),
            Self::Suspended => write!(f, "Suspended"),
            Self::Stalled => write!(f, "Stalled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_conversion() {
        for value in 0..=8 {
            let status = OrchestrationStatus::from(value);
            let back: i32 = status.into();
            assert_eq!(value, back);
        }
    }

    #[test]
    fn test_unknown_value_defaults_to_running() {
        assert_eq!(OrchestrationStatus::from(99), OrchestrationStatus::Running);
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
