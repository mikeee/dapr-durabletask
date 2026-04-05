use crate::internal;
use crate::proto;

use super::OrchestrationStatus;

/// Filter criteria for bulk-purging orchestration instances.
///
/// Build using the provided builder methods, then pass to
/// [`TaskHubGrpcClient::purge_orchestrations_by_filter`](crate::client::TaskHubGrpcClient::purge_orchestrations_by_filter).
///
/// An empty filter matches **all** instances. Narrow the scope with
/// [`with_created_time_from`](Self::with_created_time_from),
/// [`with_created_time_to`](Self::with_created_time_to), and
/// [`with_runtime_status`](Self::with_runtime_status).
///
/// # Examples
///
/// ```rust
/// use dapr_durabletask::api::{OrchestrationStatus, PurgeInstanceFilter};
///
/// let filter = PurgeInstanceFilter::new()
///     .with_created_time_from(chrono::Utc::now() - chrono::Duration::hours(24))
///     .with_runtime_status([OrchestrationStatus::Completed, OrchestrationStatus::Failed]);
/// ```
#[derive(Debug, Clone, Default)]
pub struct PurgeInstanceFilter {
    /// Only purge instances created at or after this time.
    pub created_time_from: Option<chrono::DateTime<chrono::Utc>>,

    /// Only purge instances created before or at this time.
    pub created_time_to: Option<chrono::DateTime<chrono::Utc>>,

    /// Only purge instances whose runtime status is in this list.
    /// An empty list matches all statuses.
    pub runtime_status: Vec<OrchestrationStatus>,
}

impl PurgeInstanceFilter {
    /// Create an empty filter that matches all instances.
    pub fn new() -> Self {
        Self::default()
    }

    /// Only purge instances created at or after the given time.
    pub fn with_created_time_from(mut self, from: chrono::DateTime<chrono::Utc>) -> Self {
        self.created_time_from = Some(from);
        self
    }

    /// Only purge instances created before or at the given time.
    pub fn with_created_time_to(mut self, to: chrono::DateTime<chrono::Utc>) -> Self {
        self.created_time_to = Some(to);
        self
    }

    /// Only purge instances whose runtime status is in the provided list.
    pub fn with_runtime_status(
        mut self,
        statuses: impl IntoIterator<Item = OrchestrationStatus>,
    ) -> Self {
        self.runtime_status = statuses.into_iter().collect();
        self
    }

    /// Convert to the proto [`PurgeInstanceFilter`](proto::PurgeInstanceFilter).
    pub(crate) fn into_proto(self) -> proto::PurgeInstanceFilter {
        proto::PurgeInstanceFilter {
            created_time_from: self.created_time_from.map(internal::to_timestamp),
            created_time_to: self.created_time_to.map(internal::to_timestamp),
            runtime_status: self.runtime_status.into_iter().map(i32::from).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    #[test]
    fn new_creates_empty_filter() {
        let f = PurgeInstanceFilter::new();
        assert!(f.created_time_from.is_none());
        assert!(f.created_time_to.is_none());
        assert!(f.runtime_status.is_empty());
    }

    #[test]
    fn builder_methods_set_fields() {
        let from = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let to = DateTime::from_timestamp(1_700_100_000, 0).unwrap();
        let f = PurgeInstanceFilter::new()
            .with_created_time_from(from)
            .with_created_time_to(to)
            .with_runtime_status([OrchestrationStatus::Completed, OrchestrationStatus::Failed]);
        assert_eq!(f.created_time_from, Some(from));
        assert_eq!(f.created_time_to, Some(to));
        assert_eq!(f.runtime_status.len(), 2);
    }

    #[test]
    fn into_proto_converts_correctly() {
        let from = DateTime::from_timestamp(1_700_000_000, 123_000_000).unwrap();
        let to = DateTime::from_timestamp(1_700_100_000, 0).unwrap();
        let f = PurgeInstanceFilter::new()
            .with_created_time_from(from)
            .with_created_time_to(to)
            .with_runtime_status([OrchestrationStatus::Failed]);
        let p = f.into_proto();
        let ts_from = p.created_time_from.unwrap();
        assert_eq!(ts_from.seconds, 1_700_000_000);
        assert_eq!(ts_from.nanos, 123_000_000);
        let ts_to = p.created_time_to.unwrap();
        assert_eq!(ts_to.seconds, 1_700_100_000);
        assert_eq!(ts_to.nanos, 0);
        // Failed == 3
        assert_eq!(p.runtime_status, vec![3]);
    }
}
