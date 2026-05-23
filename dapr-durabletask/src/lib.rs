// Internal modules use `crate::proto::*`; this alias is always available.
pub(crate) use dapr_durabletask_proto as proto;

/// Re-export of the generated protobuf crate.
///
/// Available only with the `proto` feature, to avoid making protobuf schema
/// changes part of this crate's stable public API.
#[cfg(feature = "proto")]
pub use dapr_durabletask_proto as proto_public;

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
