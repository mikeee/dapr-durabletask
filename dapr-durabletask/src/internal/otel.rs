//! OpenTelemetry distributed tracing helpers for the Durable Task SDK.
//!
//! This module provides helpers that manage tracing including:
//! - W3C TraceContext propagation via proto `TraceContext` messages
//! - Span creation for orchestrations, activities, and client operations
//! - Consistent span naming and attribute conventions across SDKs
//!
//! Enabled only when the `opentelemetry` feature is active.

use opentelemetry::trace::{
    SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer,
};
use opentelemetry::{Context, KeyValue};

use crate::proto;

/// Tracer name used for all spans created by this SDK.
const TRACER_NAME: &str = "durabletask";

fn tracer() -> opentelemetry::global::BoxedTracer {
    opentelemetry::global::tracer(TRACER_NAME)
}

/// Build a span name following the Go SDK convention: `"{type}||{name}||{version}"`.
fn span_name(task_type: &str, task_name: &str, task_version: &str) -> String {
    if !task_version.is_empty() {
        format!("{task_type}||{task_name}||{task_version}")
    } else if !task_name.is_empty() {
        format!("{task_type}||{task_name}")
    } else {
        task_type.to_string()
    }
}

/// Parse a W3C `traceparent` header value into a remote [`SpanContext`].
///
/// Format: `"00-{traceID}-{spanID}-{traceFlags}"`
fn span_context_from_traceparent(
    trace_parent: &str,
    trace_state_str: Option<&str>,
) -> Option<SpanContext> {
    let parts: Vec<&str> = trace_parent.split('-').collect();
    if parts.len() != 4 {
        return None;
    }

    let trace_id = TraceId::from_hex(parts[1]).ok()?;
    let span_id = SpanId::from_hex(parts[2]).ok()?;
    let flags_byte = u8::from_str_radix(parts[3], 16).ok()?;
    let trace_flags = TraceFlags::new(flags_byte);

    let trace_state = trace_state_str
        .and_then(|s| TraceState::from_key_value(parse_tracestate_pairs(s)).ok())
        .unwrap_or_default();

    Some(SpanContext::new(
        trace_id,
        span_id,
        trace_flags,
        true, // remote
        trace_state,
    ))
}

/// Parse tracestate `key=value` pairs from a comma-separated string.
fn parse_tracestate_pairs(s: &str) -> Vec<(String, String)> {
    s.split(',')
        .take(32) // W3C Trace Context spec: max 32 list-members
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?.trim().to_string();
            let value = parts.next()?.trim().to_string();
            if key.is_empty() {
                None
            } else {
                Some((key, value))
            }
        })
        .collect()
}

/// Create an OpenTelemetry [`Context`] from a proto [`proto::TraceContext`].
///
/// Returns the parent context with the remote span context attached, or
/// the current context if the trace context is missing/invalid.
pub fn context_from_trace_context(tc: Option<&proto::TraceContext>) -> Context {
    let parent = Context::current();
    let Some(tc) = tc else {
        return parent;
    };

    let trace_state_str = tc.trace_state.as_deref();
    match span_context_from_traceparent(&tc.trace_parent, trace_state_str) {
        Some(sc) => parent.with_remote_span_context(sc),
        None => parent,
    }
}

/// Build a proto [`proto::TraceContext`] from the current span in the given context.
///
/// Returns `None` if the span is not sampled (enforcing parent-based sampling).
pub fn trace_context_from_span_context(sc: &SpanContext) -> Option<proto::TraceContext> {
    if !sc.is_sampled() || !sc.is_valid() {
        return None;
    }

    let trace_parent = format!(
        "00-{}-{}-{:02x}",
        sc.trace_id(),
        sc.span_id(),
        sc.trace_flags().to_u8(),
    );

    let trace_state = {
        let ts = sc.trace_state().header();
        if ts.is_empty() { None } else { Some(ts) }
    };

    #[allow(deprecated)]
    Some(proto::TraceContext {
        trace_parent,
        span_id: String::new(),
        trace_state,
    })
}

/// Start a new `create_orchestration` client span.
///
/// Returns the context with the new span and the span itself.
pub fn start_create_orchestration_span(ctx: &Context, name: &str, instance_id: &str) -> Context {
    let span_name = span_name("create_orchestration", name, "");
    let tracer = tracer();
    let span = tracer
        .span_builder(span_name)
        .with_kind(SpanKind::Client)
        .with_attributes([
            KeyValue::new("durabletask.type", "orchestration"),
            KeyValue::new("durabletask.task.name", name.to_string()),
            KeyValue::new("durabletask.task.instance_id", instance_id.to_string()),
        ])
        .start_with_context(&tracer, ctx);
    ctx.with_span(span)
}

/// Start a new `orchestration` server span for running an orchestration.
pub fn start_orchestration_span(ctx: &Context, name: &str, instance_id: &str) -> Context {
    let span_name = span_name("orchestration", name, "");
    let tracer = tracer();
    let span = tracer
        .span_builder(span_name)
        .with_kind(SpanKind::Server)
        .with_attributes([
            KeyValue::new("durabletask.type", "orchestration"),
            KeyValue::new("durabletask.task.name", name.to_string()),
            KeyValue::new("durabletask.task.instance_id", instance_id.to_string()),
        ])
        .start_with_context(&tracer, ctx);
    ctx.with_span(span)
}

/// Start a new `activity` server span.
pub fn start_activity_span(ctx: &Context, name: &str, instance_id: &str, task_id: i32) -> Context {
    let span_name = span_name("activity", name, "");
    let tracer = tracer();
    let span = tracer
        .span_builder(span_name)
        .with_kind(SpanKind::Server)
        .with_attributes([
            KeyValue::new("durabletask.type", "activity"),
            KeyValue::new("durabletask.task.name", name.to_string()),
            KeyValue::new("durabletask.task.instance_id", instance_id.to_string()),
            KeyValue::new("durabletask.task.task_id", task_id as i64),
        ])
        .start_with_context(&tracer, ctx);
    ctx.with_span(span)
}

/// End the current span in the context, optionally setting an error status.
pub fn end_span(ctx: &Context) {
    ctx.span().end();
}

/// Set the `durabletask.runtime_status` attribute on the current span.
pub fn set_span_status_attribute(ctx: &Context, status: &str) {
    ctx.span().set_attribute(KeyValue::new(
        "durabletask.runtime_status",
        status.to_string(),
    ));
}

/// Mark the span as errored with the given message.
pub fn set_span_error(ctx: &Context, error_message: &str) {
    use opentelemetry::trace::Status;
    ctx.span().set_status(Status::Error {
        description: std::borrow::Cow::Owned(error_message.to_string()),
    });
}

/// Add a named event to the current span.
pub fn add_span_event(ctx: &Context, event_name: &'static str, attributes: Vec<KeyValue>) {
    ctx.span().add_event(event_name, attributes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_name_with_version() {
        assert_eq!(
            span_name("orchestration", "my_orch", "v1"),
            "orchestration||my_orch||v1"
        );
    }

    #[test]
    fn test_span_name_without_version() {
        assert_eq!(span_name("activity", "my_act", ""), "activity||my_act");
    }

    #[test]
    fn test_span_name_type_only() {
        assert_eq!(span_name("timer", "", ""), "timer");
    }

    #[test]
    #[allow(deprecated)]
    fn test_span_context_roundtrip() {
        let tc = proto::TraceContext {
            trace_parent: "00-a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6-1a2b3c4d5e6f7a8b-01".to_string(),
            span_id: String::new(),
            trace_state: None,
        };

        let ctx = context_from_trace_context(Some(&tc));
        let sc = ctx.span().span_context().clone();
        assert_eq!(
            sc.trace_id().to_string(),
            "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"
        );
        assert_eq!(sc.span_id().to_string(), "1a2b3c4d5e6f7a8b");
        assert!(sc.trace_flags().is_sampled());
        assert!(sc.is_remote());
    }

    #[test]
    fn test_trace_context_from_span_context() {
        let sc = SpanContext::new(
            TraceId::from_hex("d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2").unwrap(),
            SpanId::from_hex("9c8b7a6f5e4d3c2b").unwrap(),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        );

        let tc = trace_context_from_span_context(&sc).unwrap();
        assert_eq!(
            tc.trace_parent,
            "00-d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2-9c8b7a6f5e4d3c2b-01"
        );
    }

    #[test]
    fn test_trace_context_unsampled_returns_none() {
        let sc = SpanContext::new(
            TraceId::from_hex("f0e1d2c3b4a5968778695a4b3c2d1e0f").unwrap(),
            SpanId::from_hex("ab12cd34ef56ab78").unwrap(),
            TraceFlags::default(), // not sampled
            false,
            TraceState::default(),
        );

        assert!(trace_context_from_span_context(&sc).is_none());
    }

    #[test]
    fn test_context_from_none_trace_context() {
        let ctx = context_from_trace_context(None);
        // Should return a valid context (just no remote span)
        assert!(!ctx.span().span_context().is_remote());
    }

    #[test]
    fn test_parse_tracestate_pairs() {
        let pairs = parse_tracestate_pairs("vendor1=value1,vendor2=value2");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("vendor1".to_string(), "value1".to_string()));
        assert_eq!(pairs[1], ("vendor2".to_string(), "value2".to_string()));
    }
}
