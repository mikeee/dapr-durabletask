use crate::api::{HistoryPropagationScope, RetryPolicy};

/// Options for scheduling an activity call from an orchestrator.
#[derive(Default, Clone)]
pub struct ActivityOptions {
    /// Route the activity to a specific Dapr app ID (cross-app invocation).
    pub app_id: Option<String>,
    /// Route the activity to an app in another Dapr namespace. Requires
    /// [`app_id`](Self::app_id) to also be set; otherwise the activity call
    /// fails immediately with error type `InvalidActivityOptions`.
    pub app_namespace: Option<String>,
    /// Retry policy to apply when the activity fails.
    pub retry_policy: Option<RetryPolicy>,
    /// Forward the calling workflow's history to the activity. See
    /// [`HistoryPropagationScope`] for the trade-off between
    /// `OwnHistory` (caller only) and `Lineage` (caller + ancestors).
    pub history_propagation_scope: Option<HistoryPropagationScope>,
}

impl ActivityOptions {
    /// Create an `ActivityOptions` with all fields unset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Route the activity to a specific Dapr app ID (cross-app invocation).
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Route the activity to an app in another Dapr namespace
    /// (cross-namespace invocation).
    ///
    /// Must be combined with [`with_app_id`](Self::with_app_id); a namespace
    /// without an app ID makes the activity call fail immediately with error
    /// type `InvalidActivityOptions`, without scheduling anything. The target
    /// app must explicitly permit calls from the caller's namespace and app.
    pub fn with_app_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.app_namespace = Some(namespace.into());
        self
    }

    /// Attach a retry policy applied when the activity fails.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Forward the calling workflow's history to the activity under the given
    /// scope.
    pub fn with_history_propagation(mut self, scope: HistoryPropagationScope) -> Self {
        self.history_propagation_scope = Some(scope);
        self
    }
}

/// Options for scheduling a sub-orchestration call from an orchestrator.
#[derive(Default, Clone)]
pub struct SubOrchestratorOptions {
    /// Explicit instance ID for the sub-orchestration.
    /// If `None`, the runtime assigns each attempt the deterministic ID
    /// `<parent instance ID>:<action ID as 4 hex digits>`.
    pub instance_id: Option<String>,
    /// Route the sub-orchestration to a specific Dapr app ID.
    pub app_id: Option<String>,
    /// Route the sub-orchestration to an app in another Dapr namespace.
    /// Requires [`app_id`](Self::app_id) to also be set; otherwise the call
    /// fails immediately with error type `InvalidChildWorkflowOptions`.
    pub app_namespace: Option<String>,
    /// Retry policy to apply when the sub-orchestration fails.
    pub retry_policy: Option<RetryPolicy>,
    /// Forward the calling workflow's history to the child workflow.
    pub history_propagation_scope: Option<HistoryPropagationScope>,
}

impl SubOrchestratorOptions {
    /// Create a `SubOrchestratorOptions` with all fields unset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Use the given explicit instance ID for the sub-orchestration.
    /// If unset, the runtime assigns each attempt the deterministic ID
    /// `<parent instance ID>:<action ID as 4 hex digits>`.
    pub fn with_instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    /// Route the sub-orchestration to a specific Dapr app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Route the sub-orchestration to an app in another Dapr namespace
    /// (durable cross-namespace dispatch).
    ///
    /// Must be combined with [`with_app_id`](Self::with_app_id); a namespace
    /// without an app ID makes the call fail immediately with error type
    /// `InvalidChildWorkflowOptions`, without scheduling anything. The target
    /// app must explicitly permit calls from the caller's namespace and app.
    pub fn with_app_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.app_namespace = Some(namespace.into());
        self
    }

    /// Attach a retry policy applied when the sub-orchestration fails.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Forward the calling workflow's history to the child workflow under the
    /// given scope.
    pub fn with_history_propagation(mut self, scope: HistoryPropagationScope) -> Self {
        self.history_propagation_scope = Some(scope);
        self
    }
}

/// Options for [`OrchestrationContext::schedule_new_detached_workflow`].
///
/// A detached workflow is fire-and-forget: the caller receives the new
/// instance ID synchronously but neither waits on nor is notified of the
/// spawned workflow's completion, and the spawned workflow has no parent.
///
/// [`OrchestrationContext::schedule_new_detached_workflow`]:
///     crate::task::OrchestrationContext::schedule_new_detached_workflow
#[derive(Default, Clone, Debug)]
pub struct DetachedWorkflowOptions {
    /// Instance ID of the detached workflow. When `None`, a deterministic ID
    /// `<caller instance ID>-<n>` is generated, where `n` counts the
    /// default-ID spawns of the current execution (starting at 0). An empty
    /// string is rejected.
    pub instance_id: Option<String>,
    /// Pre-serialised input. When set, it is sent verbatim and takes
    /// precedence over the `input` argument of
    /// `schedule_new_detached_workflow`.
    pub raw_input: Option<String>,
    /// Defer the start of the detached workflow until this time.
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Dapr app ID hosting the detached workflow (cross-app spawn).
    pub app_id: Option<String>,
    /// Dapr namespace hosting the detached workflow. Requires
    /// [`app_id`](Self::app_id) to also be set.
    pub app_namespace: Option<String>,
}

impl DetachedWorkflowOptions {
    /// Create a `DetachedWorkflowOptions` with all fields unset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Use the given instance ID for the detached workflow. Passing an empty
    /// string makes scheduling fail; omit the option to get the default ID.
    pub fn with_instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    /// Send a pre-serialised input verbatim, overriding the `input` argument.
    pub fn with_raw_input(mut self, input: impl Into<String>) -> Self {
        self.raw_input = Some(input.into());
        self
    }

    /// Defer the start of the detached workflow until `start_time`.
    pub fn with_start_time(mut self, start_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.start_time = Some(start_time);
        self
    }

    /// Spawn the detached workflow on the given Dapr app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Spawn the detached workflow in the given Dapr namespace. Must be
    /// combined with [`with_app_id`](Self::with_app_id).
    pub fn with_app_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.app_namespace = Some(namespace.into());
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
