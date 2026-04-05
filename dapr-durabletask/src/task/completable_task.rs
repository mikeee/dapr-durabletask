use std::future::Future;
use std::pin::Pin;
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
            })),
        }
    }

    /// Complete the task with a successful result.
    pub fn complete(&self, result: Option<String>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.result = Some(TaskResult::Completed(result));
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }

    /// Fail the task with failure details.
    pub fn fail(&self, details: FailureDetails) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.result = Some(TaskResult::Failed(details));
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
            Some(TaskResult::Completed(value)) => Poll::Ready(Ok(value.clone())),
            Some(TaskResult::Failed(details)) => Poll::Ready(Err(DurableTaskError::TaskFailed {
                message: details.message.clone(),
                failure_details: Some(details.clone()),
            })),
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
    use std::task::{RawWaker, RawWakerVTable};

    fn noop_waker() -> Waker {
        fn noop(_: *const ()) {}
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
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
