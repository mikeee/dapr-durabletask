use tokio_util::sync::CancellationToken;

use crate::api::PropagatedHistory;

/// Context provided to activity functions during execution.
///
/// Besides identifying the activity invocation, the context carries a
/// cancellation signal (see [`cancelled`](Self::cancelled)) that the worker
/// fires when it is shutting down, the Rust equivalent of the `context.Context`
/// durabletask-go hands to its activities.
pub struct ActivityContext {
    pub(crate) orchestration_id: String,
    pub(crate) task_id: i32,
    pub(crate) task_execution_id: String,
    pub(crate) propagated_history: Option<PropagatedHistory>,
    pub(crate) cancellation: CancellationToken,
}

impl ActivityContext {
    pub fn new(orchestration_id: String, task_id: i32, task_execution_id: String) -> Self {
        Self {
            orchestration_id,
            task_id,
            task_execution_id,
            propagated_history: None,
            cancellation: CancellationToken::new(),
        }
    }

    /// Attach the cancellation token this activity observes. The worker
    /// passes a child of its shutdown token, so the token fires when the
    /// worker stops.
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation = token;
        self
    }

    /// The token cancelled when this activity should stop early (the worker is
    /// shutting down). Clone it to hand it to spawned sub-tasks.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Whether cancellation has been requested for this activity.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Completes when cancellation is requested for this activity, typically
    /// because the worker is shutting down.
    ///
    /// Long-running activities should race their work against this future.
    /// An activity that returns an error after cancellation is *abandoned*:
    /// the worker does not report the failure, so the sidecar redelivers the
    /// work item to the next worker (durabletask-go settles a work item whose
    /// processor observed `ctx.Done()` the same way). An activity that ignores
    /// the signal and completes normally is still drained and reported.
    ///
    /// ```rust,no_run
    /// # use dapr_durabletask::task::ActivityContext;
    /// # use dapr_durabletask::api::DurableTaskError;
    /// async fn slow(ctx: ActivityContext, _input: Option<String>)
    ///     -> dapr_durabletask::api::Result<Option<String>> {
    ///     tokio::select! {
    ///         _ = ctx.cancelled() => Err(DurableTaskError::Other("cancelled".into())),
    ///         _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => Ok(None),
    ///     }
    /// }
    /// ```
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await
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
        assert!(!ctx.is_cancelled());
    }

    #[tokio::test]
    async fn test_activity_context_cancellation() {
        let token = CancellationToken::new();
        let ctx = ActivityContext::new("inst-1".to_string(), 1, String::new())
            .with_cancellation_token(token.child_token());
        assert!(!ctx.is_cancelled());
        token.cancel();
        assert!(ctx.is_cancelled());
        assert!(ctx.cancellation_token().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(1), ctx.cancelled())
            .await
            .expect("cancelled() must resolve once the token fires");
    }
}
