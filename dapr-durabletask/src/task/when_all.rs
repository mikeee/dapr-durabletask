use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::api::DurableTaskError;

use super::completable_task::{CompletableTask, TaskResult};

/// A future that completes when all tasks complete, or fails if any task fails.
/// Returns a `Vec` of JSON-serialised results on success.
pub struct WhenAllTask {
    pub(crate) tasks: Vec<CompletableTask>,
}

impl Future for WhenAllTask {
    type Output = crate::api::Result<Vec<Option<String>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Single pass: poll each task, short-circuit on failure, track readiness.
        let mut all_complete = true;
        for task in &mut this.tasks {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(_)) => {}
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
    WhenAllTask { tasks }
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
    fn test_when_all_empty() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => assert!(results.is_empty()),
            other => panic!("expected Ready(Ok([])), got {other:?}"),
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
            other => panic!("expected Ready(Ok), got {other:?}"),
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
            other => panic!("expected Ready(Ok), got {other:?}"),
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
            other => panic!("expected Ready(Err(TaskFailed)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_single_task() {
        let t = CompletableTask::new();
        t.complete(Some("only".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], Some("only".to_string()));
            }
            other => panic!("expected Ready(Ok), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_first_failure_short_circuits() {
        // Regression: when multiple tasks fail, the first failure in iteration
        // order should be returned due to short-circuit polling.
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t1.fail(FailureDetails {
            message: "first-fail".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });
        t2.fail(FailureDetails {
            message: "second-fail".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Err(DurableTaskError::TaskFailed { message, .. })) => {
                assert_eq!(
                    message, "first-fail",
                    "should short-circuit on first failure"
                );
            }
            other => panic!("expected Ready(Err(TaskFailed)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_failure_while_others_pending() {
        // Regression: a failure should short-circuit even if other tasks are pending.
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t2.fail(FailureDetails {
            message: "early-fail".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Err(DurableTaskError::TaskFailed { message, .. })) => {
                assert_eq!(message, "early-fail");
            }
            other => panic!("expected Ready(Err(TaskFailed)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_preserves_result_order() {
        // Regression: results must be in the same order as input tasks,
        // regardless of completion order.
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        let t3 = CompletableTask::new();
        t3.complete(Some("c".to_string()));
        t2.complete(Some("b".to_string()));
        t1.complete(Some("a".to_string()));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1, t2, t3]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(
                    results,
                    vec![
                        Some("a".to_string()),
                        Some("b".to_string()),
                        Some("c".to_string()),
                    ]
                );
            }
            other => panic!("expected Ready(Ok), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_with_none_values() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();
        t1.complete(None);
        t2.complete(None);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1, t2]);
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(results, vec![None, None]);
            }
            other => panic!("expected Ready(Ok), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_many_pending_then_complete_incrementally() {
        // Regression: tasks completing one at a time; should remain Pending until all done.
        let tasks: Vec<CompletableTask> = (0..5).map(|_| CompletableTask::new()).collect();
        let clones = tasks.to_vec();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(tasks);

        for (i, t) in clones.iter().enumerate() {
            if i < clones.len() - 1 {
                t.complete(Some(format!("{i}")));
                assert!(
                    Pin::new(&mut fut).poll(&mut cx).is_pending(),
                    "should still be Pending after completing {}/{} tasks",
                    i + 1,
                    clones.len()
                );
            }
        }
        clones.last().unwrap().complete(Some("last".to_string()));
        assert!(Pin::new(&mut fut).poll(&mut cx).is_ready());
    }
}
