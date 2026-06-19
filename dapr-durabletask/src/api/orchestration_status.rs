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
        let s = match self {
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::ContinuedAsNew => "ContinuedAsNew",
            Self::Failed => "Failed",
            Self::Canceled => "Canceled",
            Self::Terminated => "Terminated",
            Self::Pending => "Pending",
            Self::Suspended => "Suspended",
            Self::Stalled => "Stalled",
        };
        f.write_str(s)
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
    fn test_is_terminal_non_terminal_variants() {
        assert!(!OrchestrationStatus::ContinuedAsNew.is_terminal());
        assert!(!OrchestrationStatus::Suspended.is_terminal());
        assert!(!OrchestrationStatus::Stalled.is_terminal());
    }

    #[test]
    fn test_is_running_all_non_running() {
        let non_running = [
            OrchestrationStatus::Completed,
            OrchestrationStatus::ContinuedAsNew,
            OrchestrationStatus::Failed,
            OrchestrationStatus::Canceled,
            OrchestrationStatus::Terminated,
            OrchestrationStatus::Pending,
            OrchestrationStatus::Suspended,
            OrchestrationStatus::Stalled,
        ];
        for s in non_running {
            assert!(!s.is_running(), "{s:?} should not be running");
        }
    }

    #[test]
    fn test_negative_i32_returns_error() {
        assert!(OrchestrationStatus::try_from(-1).is_err());
        assert_eq!(OrchestrationStatus::try_from(-1).unwrap_err(), -1);
    }

    #[test]
    fn test_boundary_i32_value_9_returns_error() {
        assert!(OrchestrationStatus::try_from(9).is_err());
        assert_eq!(OrchestrationStatus::try_from(9).unwrap_err(), 9);
    }

    #[test]
    fn serde_json_roundtrip() {
        // Regression: JSON wire shape must remain stable (quoted variant names)
        let status = OrchestrationStatus::ContinuedAsNew;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#""ContinuedAsNew""#);
        let back: OrchestrationStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn serde_json_all_variants_stable() {
        let expected = [
            (OrchestrationStatus::Running, r#""Running""#),
            (OrchestrationStatus::Completed, r#""Completed""#),
            (OrchestrationStatus::ContinuedAsNew, r#""ContinuedAsNew""#),
            (OrchestrationStatus::Failed, r#""Failed""#),
            (OrchestrationStatus::Canceled, r#""Canceled""#),
            (OrchestrationStatus::Terminated, r#""Terminated""#),
            (OrchestrationStatus::Pending, r#""Pending""#),
            (OrchestrationStatus::Suspended, r#""Suspended""#),
            (OrchestrationStatus::Stalled, r#""Stalled""#),
        ];
        for (variant, wire) in expected {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, wire, "wire format mismatch for {variant:?}");
            let back: OrchestrationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn serde_json_unknown_variant_rejected() {
        let result = serde_json::from_str::<OrchestrationStatus>(r#""Unknown""#);
        assert!(result.is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!(OrchestrationStatus::Running.to_string(), "Running");
        assert_eq!(OrchestrationStatus::Completed.to_string(), "Completed");
        assert_eq!(
            OrchestrationStatus::ContinuedAsNew.to_string(),
            "ContinuedAsNew"
        );
        assert_eq!(OrchestrationStatus::Failed.to_string(), "Failed");
        assert_eq!(OrchestrationStatus::Canceled.to_string(), "Canceled");
        assert_eq!(OrchestrationStatus::Terminated.to_string(), "Terminated");
        assert_eq!(OrchestrationStatus::Pending.to_string(), "Pending");
        assert_eq!(OrchestrationStatus::Suspended.to_string(), "Suspended");
        assert_eq!(OrchestrationStatus::Stalled.to_string(), "Stalled");
    }
}
