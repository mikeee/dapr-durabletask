use crate::api::RetryPolicy;

/// Options for scheduling an activity call from an orchestrator.
#[derive(Default, Clone)]
pub struct ActivityOptions {
    /// Route the activity to a specific Dapr app ID (cross-app invocation).
    pub app_id: Option<String>,
    /// Retry policy to apply when the activity fails.
    pub retry_policy: Option<RetryPolicy>,
}

impl ActivityOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }
}

/// Options for scheduling a sub-orchestration call from an orchestrator.
#[derive(Default, Clone)]
pub struct SubOrchestratorOptions {
    /// Explicit instance ID for the sub-orchestration.
    /// If `None`, a random UUID is generated for each attempt.
    pub instance_id: Option<String>,
    /// Route the sub-orchestration to a specific Dapr app ID.
    pub app_id: Option<String>,
    /// Retry policy to apply when the sub-orchestration fails.
    pub retry_policy: Option<RetryPolicy>,
}

impl SubOrchestratorOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_retry_policy() -> RetryPolicy {
        RetryPolicy::new(3, Duration::from_secs(1))
    }

    #[test]
    fn activity_options_defaults() {
        let opts = ActivityOptions::new();
        assert!(opts.app_id.is_none());
        assert!(opts.retry_policy.is_none());
    }

    #[test]
    fn activity_options_with_app_id() {
        let opts = ActivityOptions::new().with_app_id("my-app");
        assert_eq!(opts.app_id.as_deref(), Some("my-app"));
    }

    #[test]
    fn activity_options_with_retry_policy() {
        let opts = ActivityOptions::new().with_retry_policy(test_retry_policy());
        assert!(opts.retry_policy.is_some());
    }

    #[test]
    fn activity_options_builder_chaining() {
        let opts = ActivityOptions::new()
            .with_app_id("chained")
            .with_retry_policy(test_retry_policy());
        assert_eq!(opts.app_id.as_deref(), Some("chained"));
        assert!(opts.retry_policy.is_some());
    }

    #[test]
    fn sub_orchestrator_options_defaults() {
        let opts = SubOrchestratorOptions::new();
        assert!(opts.instance_id.is_none());
        assert!(opts.app_id.is_none());
        assert!(opts.retry_policy.is_none());
    }

    #[test]
    fn sub_orchestrator_options_with_instance_id() {
        let opts = SubOrchestratorOptions::new().with_instance_id("inst-1");
        assert_eq!(opts.instance_id.as_deref(), Some("inst-1"));
    }

    #[test]
    fn sub_orchestrator_options_with_app_id() {
        let opts = SubOrchestratorOptions::new().with_app_id("sub-app");
        assert_eq!(opts.app_id.as_deref(), Some("sub-app"));
    }

    #[test]
    fn sub_orchestrator_options_with_retry_policy() {
        let opts = SubOrchestratorOptions::new().with_retry_policy(test_retry_policy());
        assert!(opts.retry_policy.is_some());
    }

    #[test]
    fn sub_orchestrator_options_builder_chaining() {
        let opts = SubOrchestratorOptions::new()
            .with_instance_id("inst-2")
            .with_app_id("sub-app-2")
            .with_retry_policy(test_retry_policy());
        assert_eq!(opts.instance_id.as_deref(), Some("inst-2"));
        assert_eq!(opts.app_id.as_deref(), Some("sub-app-2"));
        assert!(opts.retry_policy.is_some());
    }
}
