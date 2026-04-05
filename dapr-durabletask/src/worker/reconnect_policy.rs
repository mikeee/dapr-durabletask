use std::time::Duration;

/// Exponential-backoff policy used by [`TaskHubGrpcWorker`](super::TaskHubGrpcWorker) when the sidecar
/// is unavailable.
///
/// The worker applies this policy for both the initial connection attempt and
/// every subsequent reconnect after the gRPC stream drops. On a successful
/// connection the delay is reset to [`initial_delay`](Self::initial_delay).
///
/// # Delay calculation
///
/// ```text
/// delay[0] = initial_delay
/// delay[n] = min(delay[n-1] * multiplier, max_delay) ± jitter
/// ```
///
/// Jitter (when enabled) adds a uniformly-distributed ±10 % random offset to
/// each delay, which reduces "thundering herd" reconnects in environments
/// with many workers.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use dapr_durabletask::worker::ReconnectPolicy;
///
/// // Fast reconnect for development (no jitter, 2 max attempts):
/// let policy = ReconnectPolicy::new()
///     .with_initial_delay(Duration::from_millis(200))
///     .with_max_delay(Duration::from_secs(5))
///     .with_multiplier(2.0)
///     .with_max_attempts(2);
///
/// // Production policy with jitter:
/// let policy = ReconnectPolicy::new()
///     .with_initial_delay(Duration::from_secs(1))
///     .with_max_delay(Duration::from_secs(60))
///     .with_multiplier(1.5)
///     .with_jitter(true);
/// ```
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Delay before the first reconnect attempt. Defaults to 1 s.
    pub initial_delay: Duration,

    /// Upper bound on the delay between reconnect attempts. Defaults to 30 s.
    pub max_delay: Duration,

    /// Multiplier applied to the current delay after each failed attempt.
    /// Must be ≥ 1.0. Defaults to 1.5.
    pub multiplier: f64,

    /// Maximum number of reconnect attempts before `start()` returns an error.
    /// `None` means retry indefinitely. Defaults to `None`.
    pub max_attempts: Option<u32>,

    /// When `true`, add a uniformly-distributed ±10 % random offset to each
    /// delay. Defaults to `true`.
    pub jitter: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            multiplier: 1.5,
            max_attempts: None,
            jitter: true,
        }
    }
}

impl ReconnectPolicy {
    /// Create a policy with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the initial delay before the first reconnect attempt.
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set the maximum delay between reconnect attempts.
    pub fn with_max_delay(mut self, max: Duration) -> Self {
        self.max_delay = max;
        self
    }

    /// Set the backoff multiplier (must be ≥ 1.0).
    pub fn with_multiplier(mut self, mult: f64) -> Self {
        self.multiplier = mult.max(1.0);
        self
    }

    /// Set the maximum number of reconnect attempts.
    /// `None` retries indefinitely.
    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = Some(n);
        self
    }

    /// Enable or disable ±10 % random jitter on each delay.
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }
}

// ── Internal backoff iterator ─────────────────────────────────────────────────

/// Stateful iterator that computes successive backoff delays.
pub(crate) struct BackoffIter<'a> {
    policy: &'a ReconnectPolicy,
    current: Duration,
    attempts: u32,
}

impl<'a> BackoffIter<'a> {
    pub(crate) fn new(policy: &'a ReconnectPolicy) -> Self {
        Self {
            policy,
            current: policy.initial_delay,
            attempts: 0,
        }
    }

    /// Returns the next delay, or `None` if `max_attempts` has been reached.
    pub(crate) fn next_delay(&mut self) -> Option<Duration> {
        if let Some(max) = self.policy.max_attempts {
            if self.attempts >= max {
                return None;
            }
        }
        self.attempts += 1;

        let delay = self.current;

        let next_secs = (self.current.as_secs_f64() * self.policy.multiplier)
            .min(self.policy.max_delay.as_secs_f64());
        self.current = Duration::from_secs_f64(next_secs);

        if self.policy.jitter {
            Some(apply_jitter(delay))
        } else {
            Some(delay)
        }
    }

    /// Reset the delay back to `initial_delay` and clear the attempt counter.
    pub(crate) fn reset(&mut self) {
        self.current = self.policy.initial_delay;
        self.attempts = 0;
    }
}

/// Add a uniformly-distributed ±10 % offset to `d`.
fn apply_jitter(d: Duration) -> Duration {
    // Simple LCG-based random — avoids pulling in the `rand` crate.
    // Seeded from the lower bits of the current time.
    static SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seed = SEED.fetch_add(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(12345),
        std::sync::atomic::Ordering::Relaxed,
    );
    // LCG parameters (Knuth).
    let r = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    // Map to [-0.1, +0.1].
    let factor = (r % 201) as f64 / 1000.0 - 0.1;
    let adjusted = d.as_secs_f64() * (1.0 + factor);
    Duration::from_secs_f64(adjusted.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_grows_to_max() {
        let policy = ReconnectPolicy::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_millis(500))
            .with_multiplier(2.0)
            .with_jitter(false);

        let mut iter = BackoffIter::new(&policy);

        assert_eq!(iter.next_delay(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next_delay(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next_delay(), Some(Duration::from_millis(400)));
        // Capped at max_delay.
        assert_eq!(iter.next_delay(), Some(Duration::from_millis(500)));
        assert_eq!(iter.next_delay(), Some(Duration::from_millis(500)));
    }

    #[test]
    fn test_max_attempts_exhausted() {
        let policy = ReconnectPolicy::new()
            .with_initial_delay(Duration::from_millis(10))
            .with_max_attempts(3)
            .with_jitter(false);

        let mut iter = BackoffIter::new(&policy);

        assert!(iter.next_delay().is_some());
        assert!(iter.next_delay().is_some());
        assert!(iter.next_delay().is_some());
        assert_eq!(iter.next_delay(), None);
    }

    #[test]
    fn test_reset_restarts_delay() {
        let policy = ReconnectPolicy::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_multiplier(2.0)
            .with_max_attempts(10)
            .with_jitter(false);

        let mut iter = BackoffIter::new(&policy);
        iter.next_delay(); // 100 ms
        iter.next_delay(); // 200 ms
        iter.reset();

        // After reset, delay starts again from initial_delay and attempt
        // counter is cleared.
        assert_eq!(iter.next_delay(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_jitter_stays_within_bounds() {
        let policy = ReconnectPolicy::new()
            .with_initial_delay(Duration::from_millis(1000))
            .with_max_delay(Duration::from_millis(2000))
            .with_multiplier(1.0) // keep delay constant at 1000 ms
            .with_jitter(true);

        let mut iter = BackoffIter::new(&policy);
        for _ in 0..50 {
            let d = iter.next_delay().unwrap().as_secs_f64();
            // ±10 % around 1000 ms → [900, 1100] ms.
            assert!((0.9..=1.11).contains(&d), "jitter out of range: {d}");
        }
    }

    #[test]
    fn test_multiplier_below_one_clamped() {
        let policy = ReconnectPolicy::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_multiplier(0.5) // clamped to 1.0
            .with_jitter(false);
        let mut iter = BackoffIter::new(&policy);
        // With multiplier clamped to 1.0 the delay stays constant.
        assert_eq!(iter.next_delay(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next_delay(), Some(Duration::from_millis(100)));
    }
}
