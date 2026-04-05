pub use dapr_durabletask_proto as proto;

pub mod api;
pub mod client;
pub(crate) mod internal;
pub mod task;
pub mod worker;

/// OpenTelemetry distributed tracing helpers.
///
/// Available when the `opentelemetry` feature is enabled.
#[cfg(feature = "opentelemetry")]
pub mod otel {
    pub use crate::internal::otel::*;
}
