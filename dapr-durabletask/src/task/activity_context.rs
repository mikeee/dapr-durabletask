/// Context provided to activity functions during execution.
pub struct ActivityContext {
    pub(crate) orchestration_id: String,
    pub(crate) task_id: i32,
    pub(crate) task_execution_id: String,
}

impl ActivityContext {
    pub fn new(orchestration_id: String, task_id: i32, task_execution_id: String) -> Self {
        Self {
            orchestration_id,
            task_id,
            task_execution_id,
        }
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
