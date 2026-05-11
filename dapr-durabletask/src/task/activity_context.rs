use crate::api::PropagatedHistory;

/// Context provided to activity functions during execution.
pub struct ActivityContext {
    pub(crate) orchestration_id: String,
    pub(crate) task_id: i32,
    pub(crate) task_execution_id: String,
    pub(crate) propagated_history: Option<PropagatedHistory>,
}

impl ActivityContext {
    pub fn new(orchestration_id: String, task_id: i32, task_execution_id: String) -> Self {
        Self {
            orchestration_id,
            task_id,
            task_execution_id,
            propagated_history: None,
        }
    }

    /// Construct an activity context with an attached propagated history
    /// (delivered by the worker via `ActivityRequest.propagated_history`).
    pub fn with_propagated_history(mut self, history: Option<PropagatedHistory>) -> Self {
        self.propagated_history = history;
        self
    }

    pub fn orchestration_id(&self) -> &str {
        &self.orchestration_id
    }

    pub fn task_id(&self) -> i32 {
        self.task_id
    }

    /// A unique identifier for this specific activity execution.
    ///
    /// Unlike [`task_id`](Self::task_id), which is deterministic and reused across retries,
    /// `task_execution_id` is unique per attempt and can be used for
    /// idempotency keys or deduplication.
    pub fn task_execution_id(&self) -> &str {
        &self.task_execution_id
    }

    /// Returns history forwarded from the calling workflow, if the workflow
    /// scheduled this activity with a non-`None` history propagation scope.
    pub fn propagated_history(&self) -> Option<&PropagatedHistory> {
        self.propagated_history.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activity_context() {
        let ctx = ActivityContext::new("inst-1".to_string(), 42, "exec-abc".to_string());
        assert_eq!(ctx.orchestration_id(), "inst-1");
        assert_eq!(ctx.task_id(), 42);
        assert_eq!(ctx.task_execution_id(), "exec-abc");
    }
}
