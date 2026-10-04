use std::time::Duration;

use super::reconnect_policy::ReconnectPolicy;
use crate::internal::DEFAULT_MAX_IDENTIFIER_LENGTH;

/// Default cap on the number of per-instance histories the worker's
/// stateful-history cache retains on one work-item stream.
pub const DEFAULT_HISTORY_CACHE_MAX_INSTANCES: usize = 100_000;

/// Default sliding time-to-live of an instance's cached history after its
/// last turn.
pub const DEFAULT_HISTORY_CACHE_TTL: Duration = Duration::from_secs(60 * 60);

/// Default interval at which expired history cache entries are reclaimed.
pub const DEFAULT_HISTORY_CACHE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Tuning for the worker's stateful-history cache (see
/// [`WorkerOptions::stateful_history`]).
///
/// Every field is optional: `None` (or a zero value) selects the default, as in
/// durabletask-go's `WithWorkflowHistoryCache*` options.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistoryCacheOptions {
    /// How long an instance's history is kept after its last turn (a sliding
    /// window refreshed by every turn). Default: [`DEFAULT_HISTORY_CACHE_TTL`].
    pub ttl: Option<Duration>,
    /// How often expired entries are reclaimed.
    /// Default: [`DEFAULT_HISTORY_CACHE_SWEEP_INTERVAL`].
    pub sweep_interval: Option<Duration>,
    /// Maximum number of per-instance histories retained on one stream; the
    /// least recently used entry is evicted beyond it.
    /// Default: [`DEFAULT_HISTORY_CACHE_MAX_INSTANCES`].
    pub max_instances: Option<usize>,
    /// Budget in bytes (serialized history size) across all cached instances;
    /// least recently used entries are evicted beyond it. A single entry larger
    /// than the budget is kept. Default: unlimited.
    pub max_bytes: Option<u64>,
}

impl HistoryCacheOptions {
    /// The effective TTL (the configured value, or the default).
    pub fn effective_ttl(&self) -> Duration {
        self.ttl
            .filter(|d| !d.is_zero())
            .unwrap_or(DEFAULT_HISTORY_CACHE_TTL)
    }

    /// The effective sweep interval (the configured value, or the default).
    pub fn effective_sweep_interval(&self) -> Duration {
        self.sweep_interval
            .filter(|d| !d.is_zero())
            .unwrap_or(DEFAULT_HISTORY_CACHE_SWEEP_INTERVAL)
    }

    /// The effective instance cap (the configured value, or the default).
    pub fn effective_max_instances(&self) -> usize {
        self.max_instances
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_HISTORY_CACHE_MAX_INSTANCES)
    }

    /// The effective byte budget; `0` means unlimited.
    pub fn effective_max_bytes(&self) -> u64 {
        self.max_bytes.unwrap_or(0)
    }
}

/// Configuration options for [`TaskHubGrpcWorker`](super::TaskHubGrpcWorker).
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Maximum number of concurrent work items (orchestrations + activities)
    /// processed simultaneously. The worker stops accepting new work items
    /// until an in-flight task completes.
    pub max_concurrent_work_items: usize,

    /// Maximum number of distinct event names that can be buffered per
    /// orchestration. External events arriving before the orchestrator calls
    /// `wait_for_external_event` are held in a per-name buffer. This cap
    /// limits the number of unique event names to prevent memory exhaustion
    /// from a flood of differently-named events.
    pub max_event_names: usize,

    /// Maximum number of events buffered per event name. When an external
    /// event arrives but no orchestrator is waiting for it yet, the event
    /// payload is queued. This cap bounds the queue depth per event name —
    /// excess events are discarded with a warning.
    pub max_events_per_name: usize,

    /// Maximum number of pending `wait_for_external_event` tasks per event
    /// name. If an orchestrator issues more concurrent waits on the same
    /// event name than this limit, additional waits return an incomplete task.
    pub max_pending_tasks_per_name: usize,

    /// Maximum JSON payload size in bytes for deserialisation. Payloads
    /// exceeding this limit are rejected with an error.
    pub max_json_payload_size: usize,

    /// Maximum allowed length (in bytes) for identifiers such as orchestrator
    /// names, activity names, instance IDs, and event names.
    pub max_identifier_length: usize,

    /// Reconnection policy applied when the gRPC connection to the sidecar
    /// is unavailable or drops. The policy governs both the initial connection
    /// attempt and every subsequent reconnect.
    pub reconnect_policy: ReconnectPolicy,

    /// Whether the worker uses the stateful-history optimization (default
    /// `true`). When enabled the worker advertises
    /// `WORKER_CAPABILITY_STATEFUL_HISTORY`, keeps each instance's committed
    /// history between turns on the same work-item stream, and lets the
    /// sidecar send only the new events (a delta); the full history is
    /// rebuilt from the cache, or fetched with `GetInstanceHistory` on a cache
    /// miss. Disable to have the sidecar send the full history every turn.
    pub stateful_history: bool,

    /// Bounds and TTL of the stateful-history cache.
    pub history_cache: HistoryCacheOptions,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            max_concurrent_work_items: 10_000,
            max_event_names: 1_000,
            max_events_per_name: 10_000,
            max_pending_tasks_per_name: 10_000,
            max_json_payload_size: 64 * 1024 * 1024, // 64 MiB
            max_identifier_length: DEFAULT_MAX_IDENTIFIER_LENGTH,
            reconnect_policy: ReconnectPolicy::default(),
            stateful_history: true,
            history_cache: HistoryCacheOptions::default(),
        }
    }
}

impl WorkerOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of concurrent work items.
    pub fn with_max_concurrent_work_items(mut self, limit: usize) -> Self {
        self.max_concurrent_work_items = limit;
        self
    }

    /// Set the maximum number of distinct event names buffered per orchestration.
    pub fn with_max_event_names(mut self, limit: usize) -> Self {
        self.max_event_names = limit;
        self
    }

    /// Set the maximum number of events buffered per event name.
    pub fn with_max_events_per_name(mut self, limit: usize) -> Self {
        self.max_events_per_name = limit;
        self
    }

    /// Set the maximum number of pending wait tasks per event name.
    pub fn with_max_pending_tasks_per_name(mut self, limit: usize) -> Self {
        self.max_pending_tasks_per_name = limit;
        self
    }

    /// Set the maximum JSON payload size in bytes.
    pub fn with_max_json_payload_size(mut self, limit: usize) -> Self {
        self.max_json_payload_size = limit;
        self
    }

    /// Set the maximum identifier length in bytes.
    pub fn with_max_identifier_length(mut self, limit: usize) -> Self {
        self.max_identifier_length = limit;
        self
    }

    /// Set the reconnect backoff policy.
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    /// Enable or disable the stateful-history optimization (see
    /// [`stateful_history`](Self::stateful_history)).
    pub fn with_stateful_history(mut self, enabled: bool) -> Self {
        self.stateful_history = enabled;
        self
    }

    /// Opt out of the stateful-history optimization: the worker advertises no
    /// capability, keeps no history cache, and receives the full history on
    /// every turn (durabletask-go `WithStatefulHistoryDisabled`).
    pub fn with_stateful_history_disabled(self) -> Self {
        self.with_stateful_history(false)
    }

    /// Replace the whole history cache configuration.
    pub fn with_history_cache(mut self, cache: HistoryCacheOptions) -> Self {
        self.history_cache = cache;
        self
    }

    /// Set the sliding TTL of cached histories. Zero keeps the default.
    pub fn with_history_cache_ttl(mut self, ttl: Duration) -> Self {
        self.history_cache.ttl = Some(ttl);
        self
    }

    /// Set how often expired cached histories are reclaimed. Zero keeps the
    /// default.
    pub fn with_history_cache_sweep_interval(mut self, interval: Duration) -> Self {
        self.history_cache.sweep_interval = Some(interval);
        self
    }

    /// Set the maximum number of cached per-instance histories. Zero keeps the
    /// default.
    pub fn with_history_cache_max_instances(mut self, max_instances: usize) -> Self {
        self.history_cache.max_instances = Some(max_instances);
        self
    }

    /// Set the history cache byte budget (serialized size). Zero means
    /// unlimited.
    pub fn with_history_cache_max_bytes(mut self, max_bytes: u64) -> Self {
        self.history_cache.max_bytes = Some(max_bytes);
        self
    }

    /// Convenience: configure a fast reconnect policy suitable for tests.
    ///
    /// Sets a 50 ms initial delay, 500 ms maximum delay, ×2 multiplier, and
    /// disables jitter.
    pub fn with_fast_reconnect(self) -> Self {
        self.with_reconnect_policy(
            ReconnectPolicy::new()
                .with_initial_delay(std::time::Duration::from_millis(50))
                .with_max_delay(std::time::Duration::from_millis(500))
                .with_multiplier(2.0)
                .with_jitter(false),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stateful_history_options() {
        let opts = WorkerOptions::default();
        assert!(opts.stateful_history);
        assert_eq!(opts.history_cache, HistoryCacheOptions::default());
        assert_eq!(
            opts.history_cache.effective_ttl(),
            DEFAULT_HISTORY_CACHE_TTL
        );
        assert_eq!(
            opts.history_cache.effective_sweep_interval(),
            DEFAULT_HISTORY_CACHE_SWEEP_INTERVAL
        );
        assert_eq!(
            opts.history_cache.effective_max_instances(),
            DEFAULT_HISTORY_CACHE_MAX_INSTANCES
        );
        assert_eq!(opts.history_cache.effective_max_bytes(), 0);

        let opts = WorkerOptions::new()
            .with_stateful_history_disabled()
            .with_history_cache_ttl(Duration::from_secs(5))
            .with_history_cache_sweep_interval(Duration::from_secs(1))
            .with_history_cache_max_instances(7)
            .with_history_cache_max_bytes(1024);
        assert!(!opts.stateful_history);
        assert_eq!(opts.history_cache.effective_ttl(), Duration::from_secs(5));
        assert_eq!(
            opts.history_cache.effective_sweep_interval(),
            Duration::from_secs(1)
        );
        assert_eq!(opts.history_cache.effective_max_instances(), 7);
        assert_eq!(opts.history_cache.effective_max_bytes(), 1024);

        // Zero values fall back to the defaults (Go: non-positive keeps default).
        let opts = WorkerOptions::new()
            .with_history_cache_ttl(Duration::ZERO)
            .with_history_cache_max_instances(0);
        assert_eq!(
            opts.history_cache.effective_ttl(),
            DEFAULT_HISTORY_CACHE_TTL
        );
        assert_eq!(
            opts.history_cache.effective_max_instances(),
            DEFAULT_HISTORY_CACHE_MAX_INSTANCES
        );
    }

    #[test]
    fn worker_options_defaults() {
        let opts = WorkerOptions::default();
        assert_eq!(opts.max_concurrent_work_items, 10_000);
        assert_eq!(opts.max_event_names, 1_000);
        assert_eq!(opts.max_events_per_name, 10_000);
        assert_eq!(opts.max_pending_tasks_per_name, 10_000);
        assert_eq!(opts.max_json_payload_size, 64 * 1024 * 1024);
        assert_eq!(opts.max_identifier_length, 1_024);
    }

    #[test]
    fn with_max_concurrent_work_items() {
        let opts = WorkerOptions::new().with_max_concurrent_work_items(500);
        assert_eq!(opts.max_concurrent_work_items, 500);
    }

    #[test]
    fn with_max_event_names() {
        let opts = WorkerOptions::new().with_max_event_names(200);
        assert_eq!(opts.max_event_names, 200);
    }

    #[test]
    fn with_max_events_per_name() {
        let opts = WorkerOptions::new().with_max_events_per_name(5_000);
        assert_eq!(opts.max_events_per_name, 5_000);
    }

    #[test]
    fn with_max_pending_tasks_per_name() {
        let opts = WorkerOptions::new().with_max_pending_tasks_per_name(2_000);
        assert_eq!(opts.max_pending_tasks_per_name, 2_000);
    }

    #[test]
    fn with_max_json_payload_size() {
        let opts = WorkerOptions::new().with_max_json_payload_size(1024);
        assert_eq!(opts.max_json_payload_size, 1024);
    }

    #[test]
    fn with_max_identifier_length() {
        let opts = WorkerOptions::new().with_max_identifier_length(512);
        assert_eq!(opts.max_identifier_length, 512);
    }

    #[test]
    fn with_fast_reconnect() {
        let opts = WorkerOptions::new().with_fast_reconnect();
        let rp = &opts.reconnect_policy;
        assert_eq!(rp.initial_delay, Duration::from_millis(50));
        assert_eq!(rp.max_delay, Duration::from_millis(500));
        assert_eq!(rp.multiplier, 2.0);
        assert!(!rp.jitter);
    }

    #[test]
    fn builder_chaining() {
        let opts = WorkerOptions::new()
            .with_max_concurrent_work_items(100)
            .with_max_event_names(50)
            .with_max_events_per_name(200)
            .with_max_pending_tasks_per_name(300)
            .with_max_json_payload_size(4096)
            .with_max_identifier_length(128)
            .with_fast_reconnect();

        assert_eq!(opts.max_concurrent_work_items, 100);
        assert_eq!(opts.max_event_names, 50);
        assert_eq!(opts.max_events_per_name, 200);
        assert_eq!(opts.max_pending_tasks_per_name, 300);
        assert_eq!(opts.max_json_payload_size, 4096);
        assert_eq!(opts.max_identifier_length, 128);
        assert_eq!(
            opts.reconnect_policy.initial_delay,
            Duration::from_millis(50)
        );
    }
}
