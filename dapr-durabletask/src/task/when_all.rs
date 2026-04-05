use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::api::DurableTaskError;

use super::completable_task::{CompletableTask, TaskResult};

/// A future that completes when all tasks complete, or fails if any task fails.
/// Returns a `Vec` of JSON-serialised results on success.
pub struct WhenAllTask {
    tasks: Vec<CompletableTask>,
}

impl WhenAllTask {
    pub fn new(tasks: Vec<CompletableTask>) -> Self {
        Self { tasks }
    }
}

impl Future for WhenAllTask {
    type Output = crate::api::Result<Vec<Option<String>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        for task in &this.tasks {
            if let Some(TaskResult::Failed(details)) = task.get_result() {
                return Poll::Ready(Err(DurableTaskError::TaskFailed {
                    message: details.message.clone(),
                    failure_details: Some(details),
                }));
            }
        }

        let mut all_complete = true;
        for task in &mut this.tasks {
            let mut pinned = Pin::new(task);
            match pinned.as_mut().poll(cx) {
                Poll::Ready(_) => {}
                Poll::Pending => {
                    all_complete = false;
                }
            }
        }

        if all_complete {
            let results: crate::api::Result<Vec<Option<String>>> = this
                .tasks
                .iter()
                .map(|t| match t.get_result() {
                    Some(TaskResult::Completed(v)) => Ok(v),
                    Some(TaskResult::Failed(d)) => Err(DurableTaskError::TaskFailed {
                        message: d.message.clone(),
                        failure_details: Some(d),
                    }),
                    None => Err(DurableTaskError::Other(
                        "internal error: task state inconsistency in when_all".to_string(),
                    )),
                })
                .collect();
            Poll::Ready(results)
        } else {
            Poll::Pending
        }
    }
}

/// Wait for all tasks to complete. Fails if any task fails.
pub fn when_all(tasks: Vec<CompletableTask>) -> WhenAllTask {
    WhenAllTask::new(tasks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::FailureDetails;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        fn noop(_: *const ()) {}
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    #[test]
    fn test_when_all_empty() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => assert!(results.is_empty()),
            other => panic!("expected Ready(Ok([])), got {:?}", other),
        }
    }

    #[test]
    fn test_when_all_all_complete() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t1.complete(Some("1".to_string()));
        t2.complete(Some("2".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(results.len(), 2);
                assert_eq!(results[0], Some("1".to_string()));
                assert_eq!(results[1], Some("2".to_string()));
            }
            other => panic!("expected Ready(Ok), got {:?}", other),
        }
    }

    #[test]
    fn test_when_all_pending_then_complete() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t1.complete(Some("1".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1, t2.clone()]);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        t2.complete(Some("2".to_string()));
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => assert_eq!(results.len(), 2),
            other => panic!("expected Ready(Ok), got {:?}", other),
        }
    }

    #[test]
    fn test_when_all_fails_on_any_failure() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t1.complete(Some("1".to_string()));
        t2.fail(FailureDetails {
            message: "boom".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Err(DurableTaskError::TaskFailed { message, .. })) => {
                assert_eq!(message, "boom");
            }
            other => panic!("expected Ready(Err(TaskFailed)), got {:?}", other),
        }
    }
}
