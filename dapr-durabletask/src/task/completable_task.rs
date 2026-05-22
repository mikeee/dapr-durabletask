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
                if !inner.completed_during_replay {
                    if let Some(handle) = inner.replay_handle.as_ref() {
                        handle.store(false, Ordering::Release);
                    }
                }
                Poll::Ready(Ok(value))
            }
            Some(TaskResult::Failed(details)) => {
                let details = details.clone();
                if !inner.completed_during_replay {
                    if let Some(handle) = inner.replay_handle.as_ref() {
                        handle.store(false, Ordering::Release);
                    }
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
            other => panic!("expected Ready(Ok), got {:?}", other),
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
            other => panic!("expected Ready(Err(TaskFailed)), got {:?}", other),
        }
    }

    #[test]
    fn test_clone_shares_state() {
        let task = CompletableTask::new();
        let clone = task.clone();
        task.complete(Some("shared".to_string()));
        assert!(clone.is_complete());
    }
}
