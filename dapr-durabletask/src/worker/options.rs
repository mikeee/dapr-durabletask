use super::reconnect_policy::ReconnectPolicy;

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
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            max_concurrent_work_items: 10_000,
            max_event_names: 1_000,
            max_events_per_name: 10_000,
            max_pending_tasks_per_name: 10_000,
            max_json_payload_size: 64 * 1024 * 1024, // 64 MiB
            max_identifier_length: 1_024,
            reconnect_policy: ReconnectPolicy::default(),
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
    use std::time::Duration;

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
