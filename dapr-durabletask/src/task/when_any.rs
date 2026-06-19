use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::completable_task::CompletableTask;

/// A future that completes when ANY task completes (success or failure).
/// Returns the index of the first completed task.
pub struct WhenAnyTask {
    pub(crate) tasks: Vec<CompletableTask>,
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
    WhenAnyTask { tasks }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::FailureDetails;
    use std::task::Waker;

    fn noop_waker() -> Waker {
        Waker::noop().clone()
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
            other => panic!("expected Ready(Ok(0)), got {other:?}"),
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
            other => panic!("expected Ready(Ok(1)), got {other:?}"),
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
            other => panic!("expected Ready(Ok(0)), got {other:?}"),
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
            other => panic!("expected Ready(Ok(1)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_any_empty_is_pending() {
        // Regression: empty task list should never resolve.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![]);
        assert!(
            Pin::new(&mut fut).poll(&mut cx).is_pending(),
            "when_any with no tasks should remain Pending"
        );
    }

    #[test]
    fn test_when_any_single_task() {
        let t = CompletableTask::new();
        t.complete(Some("solo".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 0),
            other => panic!("expected Ready(Ok(0)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_any_all_complete_returns_first() {
        // Regression: when all tasks are complete, the first (lowest index)
        // should be returned deterministically.
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        let t3 = CompletableTask::new();
        t3.complete(Some("c".to_string()));
        t2.complete(Some("b".to_string()));
        t1.complete(Some("a".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t1, t2, t3]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 0, "should return first completed index"),
            other => panic!("expected Ready(Ok(0)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_any_complete_with_none() {
        let t = CompletableTask::new();
        t.complete(None);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 0),
            other => panic!("expected Ready(Ok(0)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_any_later_task_completes_first() {
        // Regression: only the third task completes; verify correct index returned.
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        let t3 = CompletableTask::new();
        t3.complete(Some("third".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t1, t2, t3]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 2),
            other => panic!("expected Ready(Ok(2)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_any_pending_then_later_ready() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        let t3 = CompletableTask::new();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_any(vec![t1, t2.clone(), t3]);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        t2.complete(Some("mid".to_string()));
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 1),
            other => panic!("expected Ready(Ok(1)), got {other:?}"),
        }
    }
}
