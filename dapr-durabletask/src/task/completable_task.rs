use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::api::{DurableTaskError, FailureDetails};

/// The result of a completed task.
#[derive(Debug, Clone)]
pub enum TaskResult {
    /// Task completed successfully with an optional JSON-serialised result.
    Completed(Option<String>),
    /// Task failed with failure details.
    Failed(FailureDetails),
}

struct CompletableTaskInner {
    result: Option<TaskResult>,
    waker: Option<Waker>,
    /// `true` if the result came from history replay, `false` if from a
    /// newly-arrived event. Stand-alone tasks default to `true` so they
    /// never flip the owning context's replay flag.
    completed_during_replay: bool,
    /// Shared `is_replaying` flag of the owning orchestration context, if any.
    replay_handle: Option<Arc<AtomicBool>>,
}

/// A task that can be completed by the orchestration executor.
///
/// This is the primary awaitable type used by orchestrator functions.
/// During replay, tasks that already completed return their results immediately.
/// New tasks suspend execution until completed by the executor.
#[derive(Clone)]
pub struct CompletableTask {
    inner: Arc<Mutex<CompletableTaskInner>>,
}

impl CompletableTask {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CompletableTaskInner {
                result: None,
                waker: None,
                completed_during_replay: true,
                replay_handle: None,
            })),
        }
    }

    /// Attach the owning context's shared `is_replaying` flag. The task
    /// clears it on resolution when its result came from a new event.
    pub(crate) fn set_replay_handle(&self, handle: Arc<AtomicBool>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.replay_handle = Some(handle);
    }

    /// Complete the task with a successful result.
    pub fn complete(&self, result: Option<String>) {
        self.complete_with_phase(result, true);
    }

    /// Complete the task, tagging whether the value came from history replay
    /// or from a newly-arrived event.
    pub(crate) fn complete_with_phase(&self, result: Option<String>, during_replay: bool) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.result = Some(TaskResult::Completed(result));
        inner.completed_during_replay = during_replay;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }

    /// Fail the task with failure details.
    pub fn fail(&self, details: FailureDetails) {
        self.fail_with_phase(details, true);
    }

    /// Fail the task, tagging whether the failure came from history replay
    /// or from a newly-arrived event.
    pub(crate) fn fail_with_phase(&self, details: FailureDetails, during_replay: bool) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.result = Some(TaskResult::Failed(details));
        inner.completed_during_replay = during_replay;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }

    /// Check if the task is complete (success or failure).
    pub fn is_complete(&self) -> bool {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.result.is_some()
    }

    /// Check if the task failed.
    pub fn is_failed(&self) -> bool {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        matches!(inner.result, Some(TaskResult::Failed(_)))
    }

    /// Get the result, if complete. Returns `None` if not yet complete.
    pub fn get_result(&self) -> Option<TaskResult> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.result.clone()
    }

    /// Check if two tasks share the same inner state (are clones of each other).
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Default for CompletableTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Future for CompletableTask {
    type Output = crate::api::Result<Option<String>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match &inner.result {
            Some(TaskResult::Completed(value)) => {
                let value = value.clone();
                if !inner.completed_during_replay
                    && let Some(handle) = inner.replay_handle.as_ref()
                {
                    handle.store(false, Ordering::Release);
                }
                Poll::Ready(Ok(value))
            }
            Some(TaskResult::Failed(details)) => {
                let details = details.clone();
                if !inner.completed_during_replay
                    && let Some(handle) = inner.replay_handle.as_ref()
                {
                    handle.store(false, Ordering::Release);
                }
                Poll::Ready(Err(DurableTaskError::TaskFailed {
                    message: details.message.clone(),
                    failure_details: Some(details),
                }))
            }
            None => {
                inner.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Waker;

    fn noop_waker() -> Waker {
        Waker::noop().clone()
    }

    #[test]
    fn test_new_task_is_not_complete() {
        let task = CompletableTask::new();
        assert!(!task.is_complete());
        assert!(!task.is_failed());
        assert!(task.get_result().is_none());
    }

    #[test]
    fn test_complete_task() {
        let task = CompletableTask::new();
        task.complete(Some("42".to_string()));
        assert!(task.is_complete());
        assert!(!task.is_failed());
        match task.get_result() {
            Some(TaskResult::Completed(v)) => assert_eq!(v, Some("42".to_string())),
            _ => panic!("expected Completed"),
        }
    }

    #[test]
    fn test_fail_task() {
        let task = CompletableTask::new();
        let details = FailureDetails {
            message: "boom".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        };
        task.fail(details);
        assert!(task.is_complete());
        assert!(task.is_failed());
    }

    #[test]
    fn test_poll_pending_then_ready() {
        let task = CompletableTask::new();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut t = task.clone();
        assert!(Pin::new(&mut t).poll(&mut cx).is_pending());

        task.complete(Some("\"hello\"".to_string()));

        let mut t2 = task.clone();
        match Pin::new(&mut t2).poll(&mut cx) {
            Poll::Ready(Ok(v)) => assert_eq!(v, Some("\"hello\"".to_string())),
            other => panic!("expected Ready(Ok), got {other:?}"),
        }
    }

    #[test]
    fn test_poll_failed() {
        let task = CompletableTask::new();
        let details = FailureDetails {
            message: "oops".to_string(),
            error_type: "TestError".to_string(),
            stack_trace: None,
        };
        task.fail(details);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut t = task.clone();
        match Pin::new(&mut t).poll(&mut cx) {
            Poll::Ready(Err(DurableTaskError::TaskFailed { message, .. })) => {
                assert_eq!(message, "oops");
            }
            other => panic!("expected Ready(Err(TaskFailed)), got {other:?}"),
        }
    }

    #[test]
    fn test_clone_shares_state() {
        let task = CompletableTask::new();
        let clone = task.clone();
        task.complete(Some("shared".to_string()));
        assert!(clone.is_complete());
    }

    #[test]
    fn test_default_is_not_complete() {
        let task = CompletableTask::default();
        assert!(!task.is_complete());
        assert!(!task.is_failed());
        assert!(task.get_result().is_none());
    }

    #[test]
    fn test_complete_with_none_value() {
        let task = CompletableTask::new();
        task.complete(None);
        assert!(task.is_complete());
        assert!(!task.is_failed());
        match task.get_result() {
            Some(TaskResult::Completed(v)) => assert!(v.is_none()),
            _ => panic!("expected Completed(None)"),
        }
    }

    #[test]
    fn test_double_complete_overwrites() {
        // Regression: completing a task twice should overwrite the first result.
        let task = CompletableTask::new();
        task.complete(Some("first".to_string()));
        task.complete(Some("second".to_string()));
        assert!(task.is_complete());
        match task.get_result() {
            Some(TaskResult::Completed(v)) => assert_eq!(v, Some("second".to_string())),
            _ => panic!("expected Completed with second value"),
        }
    }

    #[test]
    fn test_complete_then_fail_overwrites() {
        // Regression: failing after completion overwrites result to Failed.
        let task = CompletableTask::new();
        task.complete(Some("ok".to_string()));
        task.fail(FailureDetails {
            message: "late failure".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });
        assert!(task.is_complete());
        assert!(task.is_failed());
    }

    #[test]
    fn test_fail_then_complete_overwrites() {
        // Regression: completing after failure overwrites result to Completed.
        let task = CompletableTask::new();
        task.fail(FailureDetails {
            message: "err".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });
        task.complete(Some("recovered".to_string()));
        assert!(task.is_complete());
        assert!(!task.is_failed());
        match task.get_result() {
            Some(TaskResult::Completed(v)) => assert_eq!(v, Some("recovered".to_string())),
            _ => panic!("expected Completed after overwrite"),
        }
    }

    #[test]
    fn test_ptr_eq_clone_vs_new() {
        let task = CompletableTask::new();
        let clone = task.clone();
        let other = CompletableTask::new();
        assert!(task.ptr_eq(&clone));
        assert!(!task.ptr_eq(&other));
    }

    #[test]
    fn test_poll_completed_is_idempotent() {
        // Regression: polling a completed task multiple times returns Ready each time.
        let task = CompletableTask::new();
        task.complete(Some("val".to_string()));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut t1 = task.clone();
        assert!(Pin::new(&mut t1).poll(&mut cx).is_ready());
        let mut t2 = task.clone();
        assert!(Pin::new(&mut t2).poll(&mut cx).is_ready());
    }

    #[test]
    fn test_poll_failed_is_idempotent() {
        let task = CompletableTask::new();
        task.fail(FailureDetails {
            message: "err".to_string(),
            error_type: "E".to_string(),
            stack_trace: None,
        });
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut t1 = task.clone();
        assert!(Pin::new(&mut t1).poll(&mut cx).is_ready());
        let mut t2 = task.clone();
        assert!(Pin::new(&mut t2).poll(&mut cx).is_ready());
    }

    #[test]
    fn test_replay_handle_not_cleared_during_replay() {
        // When completed_during_replay=true (default), the replay handle stays true.
        let handle = Arc::new(AtomicBool::new(true));
        let task = CompletableTask::new();
        task.set_replay_handle(handle.clone());
        task.complete(Some("replayed".to_string())); // during_replay defaults true

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut t = task.clone();
        let _ = Pin::new(&mut t).poll(&mut cx);
        assert!(
            handle.load(Ordering::Acquire),
            "replay flag should stay true"
        );
    }

    #[test]
    fn test_replay_handle_cleared_for_new_event() {
        // When completed_during_replay=false, polling should clear the replay handle.
        let handle = Arc::new(AtomicBool::new(true));
        let task = CompletableTask::new();
        task.set_replay_handle(handle.clone());
        task.complete_with_phase(Some("new".to_string()), false);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut t = task.clone();
        let _ = Pin::new(&mut t).poll(&mut cx);
        assert!(
            !handle.load(Ordering::Acquire),
            "replay flag should be cleared for new events"
        );
    }

    #[test]
    fn test_fail_replay_handle_cleared_for_new_event() {
        let handle = Arc::new(AtomicBool::new(true));
        let task = CompletableTask::new();
        task.set_replay_handle(handle.clone());
        task.fail_with_phase(
            FailureDetails {
                message: "new fail".to_string(),
                error_type: "E".to_string(),
                stack_trace: None,
            },
            false,
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut t = task.clone();
        let _ = Pin::new(&mut t).poll(&mut cx);
        assert!(
            !handle.load(Ordering::Acquire),
            "replay flag should be cleared for new failure events"
        );
    }
}
