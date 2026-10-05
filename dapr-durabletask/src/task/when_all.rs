use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::api::DurableTaskError;

use super::completable_task::{CompletableTask, TaskResult};

/// A future that completes when all tasks complete, or fails if any task fails.
/// Returns a `Vec` of JSON-serialised results on success.
///
/// After the first poll only tasks that signalled completion are re-checked,
/// so awaiting a fan-out of N tasks costs O(N) in total even though the
/// orchestration executor polls the orchestrator after every history event.
pub struct WhenAllTask {
    pub(crate) tasks: Vec<CompletableTask>,
    tracking: Option<Tracking>,
}

/// Per-task completion tracking, set up on the first poll.
struct Tracking {
    ready: Arc<ReadyQueue>,
    /// One waker per task, registered with that task so its completion
    /// enqueues its index.
    wakers: Vec<Waker>,
    done: Vec<bool>,
    remaining: usize,
}

/// Indices of tasks that completed since the last poll, and the waker of
/// whoever awaits the combinator.
#[derive(Default)]
struct ReadyQueue {
    indices: Mutex<Vec<usize>>,
    parent: Mutex<Option<Waker>>,
}

struct IndexWaker {
    index: usize,
    ready: Arc<ReadyQueue>,
}

impl Wake for IndexWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready
            .indices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(self.index);
        let parent = self
            .ready
            .parent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(parent) = parent {
            parent.wake();
        }
    }
}

impl Tracking {
    fn new(n: usize) -> Self {
        let ready = Arc::new(ReadyQueue::default());
        Self {
            wakers: (0..n)
                .map(|index| {
                    Waker::from(Arc::new(IndexWaker {
                        index,
                        ready: ready.clone(),
                    }))
                })
                .collect(),
            ready,
            done: vec![false; n],
            remaining: n,
        }
    }

    fn set_parent(&self, waker: &Waker) {
        let mut parent = self.ready.parent.lock().unwrap_or_else(|e| e.into_inner());
        if !parent.as_ref().is_some_and(|w| w.will_wake(waker)) {
            *parent = Some(waker.clone());
        }
    }

    /// Indices signalled since the last poll, in task order.
    fn take_ready(&self) -> Vec<usize> {
        let mut ready =
            std::mem::take(&mut *self.ready.indices.lock().unwrap_or_else(|e| e.into_inner()));
        ready.sort_unstable();
        ready.dedup();
        ready
    }

    /// Poll task `i` with its own waker, returning its failure if it failed.
    fn check(&mut self, tasks: &mut [CompletableTask], i: usize) -> Option<DurableTaskError> {
        if self.done[i] {
            return None;
        }
        let mut cx = Context::from_waker(&self.wakers[i]);
        match Pin::new(&mut tasks[i]).poll(&mut cx) {
            Poll::Ready(Err(e)) => Some(e),
            Poll::Ready(Ok(_)) => {
                self.done[i] = true;
                self.remaining -= 1;
                None
            }
            Poll::Pending => None,
        }
    }
}

impl Future for WhenAllTask {
    type Output = crate::api::Result<Vec<Option<String>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let first_poll = this.tracking.is_none();
        let tracking = this
            .tracking
            .get_or_insert_with(|| Tracking::new(this.tasks.len()));
        tracking.set_parent(cx.waker());

        // The first poll checks every task (registering a waker with each);
        // later polls only those that signalled completion. Either way the
        // first failure in task order short-circuits.
        let candidates = if first_poll {
            (0..this.tasks.len()).collect()
        } else {
            tracking.take_ready()
        };
        for i in candidates {
            if let Some(e) = tracking.check(&mut this.tasks, i) {
                return Poll::Ready(Err(e));
            }
        }
        if tracking.remaining > 0 {
            return Poll::Pending;
        }

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
    }
}

/// Wait for all tasks to complete. Fails if any task fails.
pub fn when_all(tasks: Vec<CompletableTask>) -> WhenAllTask {
    WhenAllTask {
        tasks,
        tracking: None,
    }
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

    #[test]
    fn test_when_all_failure_after_first_poll_short_circuits() {
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1.clone(), t2.clone()]);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        t2.fail(FailureDetails {
            message: "late-fail".to_string(),
            error_type: "Error".to_string(),
            stack_trace: None,
        });
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Err(DurableTaskError::TaskFailed { message, .. })) => {
                assert_eq!(message, "late-fail");
            }
            other => panic!("expected Ready(Err(TaskFailed)), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_task_awaited_elsewhere_still_notifies() {
        // Another waiter polling the same task must not steal the
        // combinator's completion notification.
        let t1 = CompletableTask::new();
        let t2 = CompletableTask::new();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(vec![t1.clone(), t2.clone()]);
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        let mut elsewhere = t1.clone();
        assert!(Pin::new(&mut elsewhere).poll(&mut cx).is_pending());

        t1.complete(Some("1".to_string()));
        t2.complete(Some("2".to_string()));
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Ok(results)) => {
                assert_eq!(results, vec![Some("1".to_string()), Some("2".to_string())]);
            }
            other => panic!("expected Ready(Ok), got {other:?}"),
        }
    }

    #[test]
    fn test_when_all_large_fan_in_polled_per_completion() {
        // Polling after every completion stays linear overall.
        let tasks: Vec<CompletableTask> = (0..20_000).map(|_| CompletableTask::new()).collect();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = when_all(tasks.clone());
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

        let start = std::time::Instant::now();
        for (i, t) in tasks.iter().enumerate().rev() {
            t.complete(Some(i.to_string()));
            let poll = Pin::new(&mut fut).poll(&mut cx);
            assert_eq!(poll.is_ready(), i == 0);
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "fan-in took {:?}",
            start.elapsed()
        );
    }
}
