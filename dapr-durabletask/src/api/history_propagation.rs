//! Workflow history propagation: scope enum, propagated history struct, and
//! convenience filters.
//!
//! Dapr Workflows can propagate execution history from a parent workflow to
//! its child workflows and activities. Two scopes are supported:
//!
//! * [`HistoryPropagationScope::OwnHistory`] — only the caller's events are
//!   forwarded; ancestral history is dropped (a trust boundary).
//! * [`HistoryPropagationScope::Lineage`] — the caller's events plus the full
//!   ancestor chain are forwarded.
//!
//! Parent (schedule-side):
//! ```ignore
//! ctx.call_activity_with_options(
//!     "verify",
//!     input,
//!     ActivityOptions::new().with_history_propagation(HistoryPropagationScope::Lineage),
//! );
//! ```
//!
//! Child / activity (receive-side):
//! ```ignore
//! if let Some(history) = ctx.propagated_history() {
//!     for app in history.app_ids() {
//!         println!("ancestor app: {app}");
//!     }
//! }
//! ```

use crate::proto;
use crate::proto::prost::Message as _;

/// Controls how history flows from a calling workflow into a scheduled
/// activity or child workflow.
///
/// Mirrors the proto `HistoryPropagationScope` enum without exposing the raw
/// `i32` discriminants. The default `None` scope is intentionally not part of
/// the public surface — callers either pass an explicit scope or omit the
/// option entirely (which is equivalent to "no propagation").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HistoryPropagationScope {
    /// Forward only the caller's own history events.
    /// Ancestral history (anything the caller itself received from its
    /// parent) is dropped at this trust boundary.
    OwnHistory,

    /// Forward the caller's own history events and the full ancestor chain.
    /// Any propagated history the caller received is forwarded as additional
    /// chunks alongside the caller's own.
    Lineage,
}

impl From<HistoryPropagationScope> for proto::HistoryPropagationScope {
    fn from(scope: HistoryPropagationScope) -> Self {
        match scope {
            HistoryPropagationScope::OwnHistory => proto::HistoryPropagationScope::OwnHistory,
            HistoryPropagationScope::Lineage => proto::HistoryPropagationScope::Lineage,
        }
    }
}

impl TryFrom<proto::HistoryPropagationScope> for HistoryPropagationScope {
    type Error = ();

    fn try_from(scope: proto::HistoryPropagationScope) -> std::result::Result<Self, Self::Error> {
        match scope {
            proto::HistoryPropagationScope::OwnHistory => Ok(Self::OwnHistory),
            proto::HistoryPropagationScope::Lineage => Ok(Self::Lineage),
            proto::HistoryPropagationScope::None => Err(()),
        }
    }
}

impl HistoryPropagationScope {
    /// Convert to the wire-format proto enum value.
    pub(crate) fn to_proto(self) -> proto::HistoryPropagationScope {
        self.into()
    }

    /// Convert from a proto enum value, returning `None` for `SCOPE_NONE` or
    /// unknown variants.
    pub(crate) fn from_proto(scope: proto::HistoryPropagationScope) -> Option<Self> {
        Self::try_from(scope).ok()
    }
}

/// A single per-app slice of propagated history.
///
/// One chunk corresponds to all events produced by a single workflow
/// instance running on a single Dapr app. When `Lineage` is used, multiple
/// chunks describe the full ancestor chain in execution order.
#[derive(Clone, Debug)]
pub struct PropagatedHistoryChunk {
    /// The Dapr app ID that produced these events.
    pub app_id: String,
    /// The workflow instance ID that produced these events.
    pub instance_id: String,
    /// The workflow function name that produced these events.
    pub workflow_name: String,
    /// Index of the first event in this chunk relative to the propagated
    /// stream as a whole.
    pub start_event_index: i32,
    /// Number of events in this chunk.
    pub event_count: i32,
    /// Decoded history events for this chunk, in execution order.
    pub events: Vec<proto::HistoryEvent>,
}

/// Propagated execution history delivered to a child workflow or activity.
///
/// Use [`PropagatedHistory::events`] for the flat event stream, or any of the
/// `events_by_*` / `workflow_by_name` filters to slice it by chunk metadata.
#[derive(Clone, Debug)]
pub struct PropagatedHistory {
    /// The propagation scope the parent used when scheduling this work item.
    pub scope: HistoryPropagationScope,
    /// All propagated events flattened in execution order.
    pub events: Vec<proto::HistoryEvent>,
    /// Per-app/per-instance chunk metadata.
    pub chunks: Vec<PropagatedHistoryChunk>,
}

/// Returned when a `*_by_name` filter cannot find a matching workflow or app.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("propagated history: {kind} '{name}' not found")]
pub struct PropagationNotFoundError {
    /// Human-readable kind of the missing entity ("workflow", "app id", etc.).
    pub kind: &'static str,
    /// The name that was searched for.
    pub name: String,
}

impl PropagatedHistory {
    /// Build a `PropagatedHistory` from the wire-format proto.
    ///
    /// Returns `None` if the proto's scope is `SCOPE_NONE` (no propagation
    /// happened) or unknown. Each chunk's raw bytes are decoded once into
    /// typed [`HistoryEvent`](proto::HistoryEvent)s; malformed events are
    /// silently dropped (the chunk's `event_count` reflects the original wire
    /// count so the caller can detect drift).
    pub fn from_proto(p: proto::PropagatedHistory) -> Option<Self> {
        let scope = HistoryPropagationScope::from_proto(
            proto::HistoryPropagationScope::try_from(p.scope).ok()?,
        )?;

        let mut all_events = Vec::new();
        let mut chunks = Vec::with_capacity(p.chunks.len());

        for raw in p.chunks {
            let start_event_index = all_events.len() as i32;
            let mut decoded = Vec::with_capacity(raw.raw_events.len());
            for ev_bytes in &raw.raw_events {
                if let Ok(ev) = proto::HistoryEvent::decode(ev_bytes.as_slice()) {
                    decoded.push(ev);
                }
            }
            let event_count = raw.raw_events.len() as i32;
            // Clone each decoded event into the flat stream, then move the
            // owned Vec into the chunk — no double allocation of the Vec
            // backing storage.
            all_events.extend_from_slice(&decoded);
            chunks.push(PropagatedHistoryChunk {
                app_id: raw.app_id,
                instance_id: raw.instance_id,
                workflow_name: raw.workflow_name,
                start_event_index,
                event_count,
                events: decoded,
            });
        }

        Some(Self {
            scope,
            events: all_events,
            chunks,
        })
    }

    /// Deduplicated list of app IDs in the propagated chain, in chunk order
    /// (earliest ancestor first).
    pub fn app_ids(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        self.chunks
            .iter()
            .filter(|c| seen.insert(c.app_id.as_str()))
            .map(|c| c.app_id.clone())
            .collect()
    }

    /// Return the chunk produced by a workflow with the given function name.
    ///
    /// If multiple chunks share the same name (re-entrant ancestor calls) the
    /// first match in chain order is returned.
    pub fn workflow_by_name(
        &self,
        name: &str,
    ) -> Result<&PropagatedHistoryChunk, PropagationNotFoundError> {
        self.chunks
            .iter()
            .find(|c| c.workflow_name == name)
            .ok_or_else(|| PropagationNotFoundError {
                kind: "workflow",
                name: name.to_string(),
            })
    }

    /// All events from chunks tagged with the given Dapr app ID.
    pub fn events_by_app_id(
        &self,
        app_id: &str,
    ) -> Result<Vec<proto::HistoryEvent>, PropagationNotFoundError> {
        let mut out = Vec::new();
        let mut found = false;
        for c in &self.chunks {
            if c.app_id == app_id {
                found = true;
                out.extend(c.events.iter().cloned());
            }
        }
        if found {
            Ok(out)
        } else {
            Err(PropagationNotFoundError {
                kind: "app id",
                name: app_id.to_string(),
            })
        }
    }

    /// All events from the chunk with the given instance ID.
    pub fn events_by_instance_id(
        &self,
        instance_id: &str,
    ) -> Result<Vec<proto::HistoryEvent>, PropagationNotFoundError> {
        let mut out = Vec::new();
        let mut found = false;
        for c in &self.chunks {
            if c.instance_id == instance_id {
                found = true;
                out.extend(c.events.iter().cloned());
            }
        }
        if found {
            Ok(out)
        } else {
            Err(PropagationNotFoundError {
                kind: "instance id",
                name: instance_id.to_string(),
            })
        }
    }

    /// All events from chunks produced by a workflow with the given function
    /// name.
    pub fn events_by_workflow_name(
        &self,
        name: &str,
    ) -> Result<Vec<proto::HistoryEvent>, PropagationNotFoundError> {
        let mut out = Vec::new();
        let mut found = false;
        for c in &self.chunks {
            if c.workflow_name == name {
                found = true;
                out.extend(c.events.iter().cloned());
            }
        }
        if found {
            Ok(out)
        } else {
            Err(PropagationNotFoundError {
                kind: "workflow",
                name: name.to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::prost::Message;

    fn ev(id: i32) -> proto::HistoryEvent {
        proto::HistoryEvent {
            event_id: id,
            timestamp: None,
            router: None,
            event_type: None,
        }
    }

    fn raw_chunk(app: &str, inst: &str, wf: &str, n: i32) -> proto::PropagatedHistoryChunk {
        let raw_events = (0..n).map(|i| ev(i).encode_to_vec()).collect();
        proto::PropagatedHistoryChunk {
            raw_events,
            app_id: app.to_string(),
            instance_id: inst.to_string(),
            workflow_name: wf.to_string(),
            raw_signatures: vec![],
            signing_cert_chains: vec![],
        }
    }

    #[test]
    fn from_proto_none_returns_none() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::None as i32,
            chunks: vec![],
        };
        assert!(PropagatedHistory::from_proto(p).is_none());
    }

    #[test]
    fn from_proto_decodes_chunks_and_flattens_events() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::Lineage as i32,
            chunks: vec![
                raw_chunk("app-a", "inst-a", "WfA", 2),
                raw_chunk("app-b", "inst-b", "WfB", 3),
            ],
        };
        let h = PropagatedHistory::from_proto(p).expect("scope set");
        assert_eq!(h.scope, HistoryPropagationScope::Lineage);
        assert_eq!(h.events.len(), 5);
        assert_eq!(h.chunks.len(), 2);
        assert_eq!(h.chunks[0].start_event_index, 0);
        assert_eq!(h.chunks[0].event_count, 2);
        assert_eq!(h.chunks[1].start_event_index, 2);
        assert_eq!(h.chunks[1].event_count, 3);
    }

    #[test]
    fn app_ids_are_deduplicated_in_chain_order() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::Lineage as i32,
            chunks: vec![
                raw_chunk("app-a", "i1", "Wf1", 1),
                raw_chunk("app-b", "i2", "Wf2", 1),
                raw_chunk("app-a", "i3", "Wf3", 1),
            ],
        };
        let h = PropagatedHistory::from_proto(p).unwrap();
        assert_eq!(h.app_ids(), vec!["app-a".to_string(), "app-b".to_string()]);
    }

    #[test]
    fn filters_return_not_found_for_missing_names() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::OwnHistory as i32,
            chunks: vec![raw_chunk("app-a", "inst", "WfA", 1)],
        };
        let h = PropagatedHistory::from_proto(p).unwrap();
        assert!(h.workflow_by_name("missing").is_err());
        assert!(h.events_by_app_id("missing").is_err());
        assert!(h.events_by_instance_id("missing").is_err());
        assert!(h.events_by_workflow_name("missing").is_err());
    }

    #[test]
    fn filters_return_matching_events() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::Lineage as i32,
            chunks: vec![
                raw_chunk("app-a", "inst-a", "WfA", 2),
                raw_chunk("app-b", "inst-b", "WfB", 3),
            ],
        };
        let h = PropagatedHistory::from_proto(p).unwrap();
        assert_eq!(h.events_by_app_id("app-a").unwrap().len(), 2);
        assert_eq!(h.events_by_instance_id("inst-b").unwrap().len(), 3);
        assert_eq!(h.events_by_workflow_name("WfA").unwrap().len(), 2);
        assert_eq!(h.workflow_by_name("WfB").unwrap().instance_id, "inst-b");
    }
}
