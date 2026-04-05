use std::sync::Arc;
use std::time::Duration;

use crate::api::FailureDetails;

/// A predicate that decides whether a failed task should be retried.
///
/// Receives the [`FailureDetails`] of the failure. Return `true` to retry,
/// `false` to propagate the error immediately.
pub type RetryHandle = Arc<dyn Fn(&FailureDetails) -> bool + Send + Sync>;

/// Policy that governs automatic retry behaviour for activities and sub-orchestrations.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryPolicy {
    /// Maximum total number of attempts (first execution + retries).
    pub max_number_of_attempts: u32,
    /// Delay before the first retry.
    pub first_retry_interval: Duration,
    /// Multiplier applied to the delay on each successive retry.
    /// `1.0` means constant interval; `2.0` means exponential doubling.
    pub backoff_coefficient: f64,
    /// Upper bound on the computed delay between retries.
    pub max_retry_interval: Option<Duration>,
    /// Maximum total wall-clock time spent retrying (measured from the first
    /// failure). Once elapsed, no further retries are scheduled.
    pub retry_timeout: Option<Duration>,
    /// Optional predicate called on each failure to decide whether to retry.
    /// If `None`, all failures are retried (up to `max_number_of_attempts`).
    #[serde(skip)]
    pub handle: Option<RetryHandle>,
}

impl std::fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_number_of_attempts", &self.max_number_of_attempts)
            .field("first_retry_interval", &self.first_retry_interval)
            .field("backoff_coefficient", &self.backoff_coefficient)
            .field("max_retry_interval", &self.max_retry_interval)
            .field("retry_timeout", &self.retry_timeout)
            .field("handle", &self.handle.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl RetryPolicy {
    /// Creates a simple retry policy with the given attempt count and interval.
    pub fn new(max_number_of_attempts: u32, first_retry_interval: Duration) -> Self {
        Self {
            max_number_of_attempts,
            first_retry_interval,
            backoff_coefficient: 1.0,
            max_retry_interval: None,
            retry_timeout: None,
            handle: None,
        }
    }

    /// Set the backoff coefficient (multiplier applied to the delay each retry).
    pub fn with_backoff_coefficient(mut self, coefficient: f64) -> Self {
        self.backoff_coefficient = coefficient;
        self
    }

    /// Set the maximum delay between retries.
    pub fn with_max_retry_interval(mut self, interval: Duration) -> Self {
        self.max_retry_interval = Some(interval);
        self
    }

    /// Set the maximum total wall-clock time spent retrying.
    pub fn with_retry_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = Some(timeout);
        self
    }

    /// Set a custom predicate to decide whether a failure should be retried.
    ///
    /// # Example
    /// ```ignore
    /// let policy = RetryPolicy::new(3, Duration::from_secs(1))
    ///     .with_handle(|details| details.error_type != "NonRetryableError");
    /// ```
    pub fn with_handle<F>(mut self, f: F) -> Self
    where
        F: Fn(&FailureDetails) -> bool + Send + Sync + 'static,
    {
        self.handle = Some(Arc::new(f));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_defaults() {
        let p = RetryPolicy::new(5, Duration::from_secs(2));
        assert_eq!(p.max_number_of_attempts, 5);
        assert_eq!(p.first_retry_interval, Duration::from_secs(2));
        assert!((p.backoff_coefficient - 1.0).abs() < f64::EPSILON);
        assert!(p.max_retry_interval.is_none());
        assert!(p.retry_timeout.is_none());
        assert!(p.handle.is_none());
    }

    #[test]
    fn with_backoff_coefficient() {
        let p = RetryPolicy::new(3, Duration::from_secs(1)).with_backoff_coefficient(2.5);
        assert!((p.backoff_coefficient - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn with_max_retry_interval() {
        let p = RetryPolicy::new(3, Duration::from_secs(1))
            .with_max_retry_interval(Duration::from_secs(60));
        assert_eq!(p.max_retry_interval, Some(Duration::from_secs(60)));
    }

    #[test]
    fn with_retry_timeout() {
        let p = RetryPolicy::new(3, Duration::from_secs(1))
            .with_retry_timeout(Duration::from_secs(300));
        assert_eq!(p.retry_timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn with_handle_is_callable() {
        let p = RetryPolicy::new(3, Duration::from_secs(1)).with_handle(|_details| true);
        let fd = FailureDetails {
            message: "err".into(),
            error_type: "E".into(),
            stack_trace: None,
        };
        assert!((p.handle.unwrap())(&fd));
    }

    #[test]
    fn debug_with_handle() {
        let p = RetryPolicy::new(1, Duration::from_secs(1)).with_handle(|_| false);
        let dbg = format!("{p:?}");
        assert!(dbg.contains(r#"Some("<fn>")"#));
    }

    #[test]
    fn debug_without_handle() {
        let p = RetryPolicy::new(1, Duration::from_secs(1));
        let dbg = format!("{p:?}");
        assert!(dbg.contains("handle: None"));
    }
}
