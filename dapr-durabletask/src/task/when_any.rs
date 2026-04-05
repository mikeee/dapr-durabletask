use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::completable_task::CompletableTask;

/// A future that completes when ANY task completes (success or failure).
/// Returns the index of the first completed task.
pub struct WhenAnyTask {
    tasks: Vec<CompletableTask>,
}

impl WhenAnyTask {
    pub fn new(tasks: Vec<CompletableTask>) -> Self {
        Self { tasks }
    }
}

impl Future for WhenAnyTask {
    type Output = crate::api::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Single pass: poll registers wakers and detects the first ready task.
        for (i, task) in this.tasks.iter_mut().enumerate() {
            match Pin::new(task).poll(cx) {
                Poll::Ready(_) => return Poll::Ready(Ok(i)),
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}

/// Wait for any task to complete. Returns the index of the first completed task.
pub fn when_any(tasks: Vec<CompletableTask>) -> WhenAnyTask {
    WhenAnyTask::new(tasks)
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
    fn test_when_any_first_complete() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t1.complete(Some("first".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 0),
            other => panic!("expected Ready(Ok(0)), got {:?}", other),
        }
    }

    #[test]
    fn test_when_any_second_complete() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t2.complete(Some("second".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 1),
            other => panic!("expected Ready(Ok(1)), got {:?}", other),
        }
    }

    #[test]
    fn test_when_any_pending_then_ready() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t1.clone(), t2]);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        t1.complete(None);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 0),
            other => panic!("expected Ready(Ok(0)), got {:?}", other),
        }
    }

    #[test]
    fn test_when_any_failed_task_counts() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t2.fail(FailureDetails {
            message: "boom".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 1),
            other => panic!("expected Ready(Ok(1)), got {:?}", other),
        }
    }
}
