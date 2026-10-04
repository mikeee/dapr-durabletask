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
use crate::proto::history_event::EventType as HistoryEventType;
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

/// Returned when a wire-format propagated history is malformed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("propagated history: {0}")]
pub struct InvalidPropagatedHistoryError(String);

impl PropagatedHistory {
    /// Build a `PropagatedHistory` from the wire-format proto.
    ///
    /// Returns `None` if the proto's scope is `SCOPE_NONE` (no propagation
    /// happened) or unknown, or if the proto is malformed (see
    /// [`try_from_proto`](Self::try_from_proto)); malformed input is logged.
    pub fn from_proto(p: proto::PropagatedHistory) -> Option<Self> {
        Self::try_from_proto(p).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Discarding malformed propagated history");
            None
        })
    }

    /// Build a `PropagatedHistory` from the wire-format proto, rejecting
    /// malformed input.
    ///
    /// Every chunk must carry a non-empty app ID and every raw event must
    /// decode, whatever the scope; otherwise the whole history is rejected,
    /// matching durabletask-go. A well-formed proto whose scope is
    /// `SCOPE_NONE` or unknown yields `Ok(None)`.
    pub fn try_from_proto(
        p: proto::PropagatedHistory,
    ) -> Result<Option<Self>, InvalidPropagatedHistoryError> {
        for (i, chunk) in p.chunks.iter().enumerate() {
            if chunk.app_id.is_empty() {
                return Err(InvalidPropagatedHistoryError(format!(
                    "chunk {i} has empty appId"
                )));
            }
        }
        let scope = proto::HistoryPropagationScope::try_from(p.scope)
            .ok()
            .and_then(HistoryPropagationScope::from_proto);

        let mut all_events = Vec::new();
        let mut chunks = Vec::with_capacity(p.chunks.len());

        for (i, raw) in p.chunks.into_iter().enumerate() {
            let start_event_index = all_events.len() as i32;
            let mut decoded = Vec::with_capacity(raw.raw_events.len());
            for (j, ev_bytes) in raw.raw_events.iter().enumerate() {
                let ev = proto::HistoryEvent::decode(ev_bytes.as_slice()).map_err(|e| {
                    InvalidPropagatedHistoryError(format!(
                        "chunk {i} (app {:?}): failed to decode rawEvent {j}: {e}",
                        raw.app_id
                    ))
                })?;
                decoded.push(ev);
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

        Ok(scope.map(|scope| Self {
            scope,
            events: all_events,
            chunks,
        }))
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
    /// last match in chain order (the nearest ancestor) is returned, matching
    /// durabletask-go's `GetLastWorkflowByName`.
    pub fn workflow_by_name(
        &self,
        name: &str,
    ) -> Result<&PropagatedHistoryChunk, PropagationNotFoundError> {
        self.chunks
            .iter()
            .rfind(|c| c.workflow_name == name)
            .ok_or_else(|| PropagationNotFoundError {
                kind: "workflow",
                name: name.to_string(),
            })
    }

    /// All chunks produced by a workflow with the given function name, in
    /// chain order (earliest ancestor first).
    ///
    /// Returns an empty `Vec` when nothing matches. The last element, if any,
    /// is the chunk [`workflow_by_name`](Self::workflow_by_name) returns.
    /// Mirrors durabletask-go's `GetWorkflowsByName`.
    pub fn workflows_by_name(&self, name: &str) -> Vec<&PropagatedHistoryChunk> {
        self.chunks
            .iter()
            .filter(|c| c.workflow_name == name)
            .collect()
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

/// The resolved state of one activity invocation recorded in a propagated
/// workflow chunk.
///
/// Built from a `TaskScheduled` event plus the `TaskCompleted` /
/// `TaskFailed` event (if any) whose `task_scheduled_id` equals the
/// scheduling event's ID. Matching is by scheduling event ID rather than by
/// `task_execution_id`, because SDK-driven retries reuse the same task
/// execution ID for every attempt. Mirrors durabletask-go's `ActivityResult`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ActivityResult {
    /// Activity function name.
    pub name: String,
    /// Event ID of the `TaskScheduled` event that started this attempt.
    pub scheduled_event_id: i32,
    /// Task execution ID; shared across retry attempts of the same activity.
    pub task_execution_id: String,
    /// Whether the activity was scheduled. Always `true` for a resolved
    /// result; kept for parity with durabletask-go.
    pub started: bool,
    /// Whether a matching `TaskCompleted` event was found.
    pub completed: bool,
    /// Whether a matching `TaskFailed` event was found.
    pub failed: bool,
    /// Serialized activity input, if any.
    pub input: Option<String>,
    /// Serialized activity output from the matching `TaskCompleted` event.
    pub output: Option<String>,
    /// Failure details from the matching `TaskFailed` event.
    pub failure: Option<proto::TaskFailureDetails>,
}

/// The resolved state of one child workflow invocation recorded in a
/// propagated workflow chunk.
///
/// Built from a `ChildWorkflowInstanceCreated` event plus the
/// `ChildWorkflowInstanceCompleted` / `ChildWorkflowInstanceFailed` event
/// (if any) whose `task_scheduled_id` equals the creation event's ID.
/// Mirrors durabletask-go's `ChildWorkflowResult`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ChildWorkflowResult {
    /// Child workflow function name.
    pub name: String,
    /// Instance ID assigned to the child workflow.
    pub instance_id: String,
    /// Event ID of the `ChildWorkflowInstanceCreated` event.
    pub scheduled_event_id: i32,
    /// Whether the child workflow was created. Always `true` for a resolved
    /// result; kept for parity with durabletask-go.
    pub started: bool,
    /// Whether a matching `ChildWorkflowInstanceCompleted` event was found.
    pub completed: bool,
    /// Whether a matching `ChildWorkflowInstanceFailed` event was found.
    pub failed: bool,
    /// Serialized child workflow input, if any.
    pub input: Option<String>,
    /// Serialized child workflow output from the completion event.
    pub output: Option<String>,
    /// Failure details from the failure event.
    pub failure: Option<proto::TaskFailureDetails>,
}

impl PropagatedHistoryChunk {
    fn resolve_activity(&self, event_id: i32, ts: &proto::TaskScheduledEvent) -> ActivityResult {
        let mut result = ActivityResult {
            name: ts.name.clone(),
            scheduled_event_id: event_id,
            task_execution_id: ts.task_execution_id.clone(),
            started: true,
            completed: false,
            failed: false,
            input: ts.input.clone(),
            output: None,
            failure: None,
        };
        for e in &self.events {
            match &e.event_type {
                Some(HistoryEventType::TaskCompleted(tc)) if tc.task_scheduled_id == event_id => {
                    result.completed = true;
                    result.output = tc.result.clone();
                }
                Some(HistoryEventType::TaskFailed(tf)) if tf.task_scheduled_id == event_id => {
                    result.failed = true;
                    result.failure = tf.failure_details.clone();
                }
                _ => {}
            }
        }
        result
    }

    fn resolve_child_workflow(
        &self,
        event_id: i32,
        cw: &proto::ChildWorkflowInstanceCreatedEvent,
    ) -> ChildWorkflowResult {
        let mut result = ChildWorkflowResult {
            name: cw.name.clone(),
            instance_id: cw.instance_id.clone(),
            scheduled_event_id: event_id,
            started: true,
            completed: false,
            failed: false,
            input: cw.input.clone(),
            output: None,
            failure: None,
        };
        for e in &self.events {
            match &e.event_type {
                Some(HistoryEventType::ChildWorkflowInstanceCompleted(cc))
                    if cc.task_scheduled_id == event_id =>
                {
                    result.completed = true;
                    result.output = cc.result.clone();
                }
                Some(HistoryEventType::ChildWorkflowInstanceFailed(cf))
                    if cf.task_scheduled_id == event_id =>
                {
                    result.failed = true;
                    result.failure = cf.failure_details.clone();
                }
                _ => {}
            }
        }
        result
    }

    /// All activity invocations with the given name scheduled in this
    /// workflow chunk, in execution order.
    ///
    /// Each `TaskScheduled` event yields one [`ActivityResult`], so repeated
    /// calls and retries (which share a task execution ID) appear as
    /// separate entries. Returns an empty `Vec` when nothing matches.
    /// Mirrors durabletask-go's `WorkflowResult.GetActivitiesByName`.
    pub fn activities_by_name(&self, name: &str) -> Vec<ActivityResult> {
        self.events
            .iter()
            .filter_map(|e| match &e.event_type {
                Some(HistoryEventType::TaskScheduled(ts)) if ts.name == name => {
                    Some(self.resolve_activity(e.event_id, ts))
                }
                _ => None,
            })
            .collect()
    }

    /// The most recent activity invocation with the given name in this
    /// workflow chunk — for retries, the final attempt.
    ///
    /// Equivalent to the last element of
    /// [`activities_by_name`](Self::activities_by_name). Returns a
    /// [`PropagationNotFoundError`] of kind `"activity"` when nothing
    /// matches. Mirrors durabletask-go's `WorkflowResult.GetLastActivityByName`.
    pub fn last_activity_by_name(
        &self,
        name: &str,
    ) -> Result<ActivityResult, PropagationNotFoundError> {
        self.activities_by_name(name)
            .pop()
            .ok_or_else(|| PropagationNotFoundError {
                kind: "activity",
                name: name.to_string(),
            })
    }

    /// All child workflow invocations with the given name created from this
    /// workflow chunk, in execution order. Returns an empty `Vec` when
    /// nothing matches. Mirrors durabletask-go's
    /// `WorkflowResult.GetChildWorkflowsByName`.
    pub fn child_workflows_by_name(&self, name: &str) -> Vec<ChildWorkflowResult> {
        self.events
            .iter()
            .filter_map(|e| match &e.event_type {
                Some(HistoryEventType::ChildWorkflowInstanceCreated(cw)) if cw.name == name => {
                    Some(self.resolve_child_workflow(e.event_id, cw))
                }
                _ => None,
            })
            .collect()
    }

    /// The most recent child workflow invocation with the given name created
    /// from this workflow chunk.
    ///
    /// Equivalent to the last element of
    /// [`child_workflows_by_name`](Self::child_workflows_by_name). Returns a
    /// [`PropagationNotFoundError`] of kind `"child workflow"` when nothing
    /// matches. Mirrors durabletask-go's
    /// `WorkflowResult.GetLastChildWorkflowByName`.
    pub fn last_child_workflow_by_name(
        &self,
        name: &str,
    ) -> Result<ChildWorkflowResult, PropagationNotFoundError> {
        self.child_workflows_by_name(name)
            .pop()
            .ok_or_else(|| PropagationNotFoundError {
                kind: "child workflow",
                name: name.to_string(),
            })
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

    #[test]
    fn scope_roundtrip_own_history() {
        let scope = HistoryPropagationScope::OwnHistory;
        let proto_scope: proto::HistoryPropagationScope = scope.into();
        assert_eq!(proto_scope, proto::HistoryPropagationScope::OwnHistory);
        let back = HistoryPropagationScope::try_from(proto_scope).unwrap();
        assert_eq!(back, scope);
    }

    #[test]
    fn scope_roundtrip_lineage() {
        let scope = HistoryPropagationScope::Lineage;
        let proto_scope: proto::HistoryPropagationScope = scope.into();
        assert_eq!(proto_scope, proto::HistoryPropagationScope::Lineage);
        let back = HistoryPropagationScope::try_from(proto_scope).unwrap();
        assert_eq!(back, scope);
    }

    #[test]
    fn scope_none_rejected() {
        let result = HistoryPropagationScope::try_from(proto::HistoryPropagationScope::None);
        assert!(result.is_err());
    }

    #[test]
    fn from_proto_own_history_scope() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::OwnHistory as i32,
            chunks: vec![raw_chunk("app", "inst", "Wf", 1)],
        };
        let h = PropagatedHistory::from_proto(p).unwrap();
        assert_eq!(h.scope, HistoryPropagationScope::OwnHistory);
    }

    #[test]
    fn from_proto_unknown_scope_returns_none() {
        let p = proto::PropagatedHistory {
            scope: 999,
            chunks: vec![],
        };
        assert!(PropagatedHistory::from_proto(p).is_none());
    }

    #[test]
    fn propagation_not_found_error_display() {
        let err = PropagationNotFoundError {
            kind: "workflow",
            name: "MyWf".into(),
        };
        assert_eq!(
            err.to_string(),
            "propagated history: workflow 'MyWf' not found"
        );
    }

    #[test]
    fn propagation_not_found_error_display_app_id() {
        let err = PropagationNotFoundError {
            kind: "app id",
            name: "my-app".into(),
        };
        assert_eq!(
            err.to_string(),
            "propagated history: app id 'my-app' not found"
        );
    }

    #[test]
    fn from_proto_empty_chunks() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::Lineage as i32,
            chunks: vec![],
        };
        let h = PropagatedHistory::from_proto(p).unwrap();
        assert_eq!(h.scope, HistoryPropagationScope::Lineage);
        assert!(h.events.is_empty());
        assert!(h.chunks.is_empty());
        assert!(h.app_ids().is_empty());
    }

    #[test]
    fn from_proto_malformed_event_bytes_rejected() {
        let bad_chunk = proto::PropagatedHistoryChunk {
            raw_events: vec![vec![0xFF, 0xFF, 0xFF]], // invalid protobuf
            app_id: "app".into(),
            instance_id: "inst".into(),
            workflow_name: "wf".into(),
            raw_signatures: vec![],
            signing_cert_chains: vec![],
        };
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::OwnHistory as i32,
            chunks: vec![bad_chunk],
        };
        let err = PropagatedHistory::try_from_proto(p.clone()).unwrap_err();
        assert!(err.to_string().contains("failed to decode rawEvent 0"));
        assert!(PropagatedHistory::from_proto(p).is_none());
    }

    #[test]
    fn from_proto_empty_app_id_rejected() {
        let p = proto::PropagatedHistory {
            scope: proto::HistoryPropagationScope::Lineage as i32,
            chunks: vec![proto::PropagatedHistoryChunk {
                raw_events: vec![],
                app_id: String::new(),
                instance_id: "inst".into(),
                workflow_name: "wf".into(),
                raw_signatures: vec![],
                signing_cert_chains: vec![],
            }],
        };
        let err = PropagatedHistory::try_from_proto(p).unwrap_err();
        assert!(err.to_string().contains("empty appId"));
    }

    mod propagation_api {
        //! Go's propagation tests build `PropagatedHistory` from unexported fields.
        //! Here every fixture goes through the SDK's wire decoder
        //! (`PropagatedHistory::try_from_proto`), and the decoded chunk layout is
        //! checked against Go's `historyChunk` layout, so the chunk list and
        //! per-chunk events the assertions read are SDK output rather than values
        //! the test wrote itself.

        use std::sync::Arc;

        use crate::api::{HistoryPropagationScope, PropagatedHistory, PropagationNotFoundError};
        use crate::proto;
        use crate::proto::history_event::EventType;
        use crate::proto::prost::Message as _;
        use crate::proto::workflow_action::WorkflowActionType;
        use crate::task::{ActivityOptions, SubOrchestratorOptions};
        use crate::worker::{OrchestrationExecutor, OrchestratorFn, WorkerOptions};

        // ---------------------------------------------------------------------------
        // api/propagation_test.go helpers
        // ---------------------------------------------------------------------------

        fn event(id: i32, et: EventType) -> proto::HistoryEvent {
            proto::HistoryEvent {
                event_id: id,
                timestamp: None,
                router: None,
                event_type: Some(et),
            }
        }

        fn bare_event(id: i32) -> proto::HistoryEvent {
            proto::HistoryEvent {
                event_id: id,
                timestamp: None,
                router: None,
                event_type: None,
            }
        }

        fn exec_started(name: &str) -> EventType {
            EventType::ExecutionStarted(proto::ExecutionStartedEvent {
                name: name.to_string(),
                ..Default::default()
            })
        }

        fn task_scheduled(name: &str, exec_id: &str, input: &str) -> EventType {
            EventType::TaskScheduled(proto::TaskScheduledEvent {
                name: name.to_string(),
                task_execution_id: exec_id.to_string(),
                input: Some(input.to_string()),
                ..Default::default()
            })
        }

        fn task_completed(scheduled_id: i32, exec_id: &str, result: &str) -> EventType {
            EventType::TaskCompleted(proto::TaskCompletedEvent {
                task_scheduled_id: scheduled_id,
                task_execution_id: exec_id.to_string(),
                result: Some(result.to_string()),
                ..Default::default()
            })
        }

        fn task_failed(scheduled_id: i32, exec_id: &str, msg: &str) -> EventType {
            EventType::TaskFailed(proto::TaskFailedEvent {
                task_scheduled_id: scheduled_id,
                task_execution_id: exec_id.to_string(),
                failure_details: Some(proto::TaskFailureDetails {
                    error_message: msg.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }

        fn child_created(name: &str, instance_id: &str) -> EventType {
            EventType::ChildWorkflowInstanceCreated(proto::ChildWorkflowInstanceCreatedEvent {
                name: name.to_string(),
                instance_id: instance_id.to_string(),
                ..Default::default()
            })
        }

        /// Build a `PropagatedHistory` through the SDK's wire decoder from a flat
        /// event list plus Go-style `(app_id, start, count, instance_id,
        /// workflow_name)` chunk descriptors (Go's `historyChunk`). Each chunk's
        /// `[start, start + count)` slice is encoded into its `raw_events`; the
        /// decoded result must reproduce exactly that layout, so the fixture itself
        /// is SDK output.
        ///
        /// Go leaves the scope unset in several fixtures because it fills the struct
        /// directly; the Rust decoder yields no history for `SCOPE_NONE`, so callers
        /// pass a real scope (none of those tests read it).
        fn from_wire(
            events: Vec<proto::HistoryEvent>,
            scope: proto::HistoryPropagationScope,
            chunks: &[(&str, usize, usize, &str, &str)],
        ) -> PropagatedHistory {
            let wire = proto::PropagatedHistory {
                scope: scope as i32,
                chunks: chunks
                    .iter()
                    .map(
                        |&(app, start, count, inst, wf)| proto::PropagatedHistoryChunk {
                            app_id: app.to_string(),
                            instance_id: inst.to_string(),
                            workflow_name: wf.to_string(),
                            raw_events: events[start..start + count]
                                .iter()
                                .map(|e| e.encode_to_vec())
                                .collect(),
                            ..Default::default()
                        },
                    )
                    .collect(),
            };
            let ph = PropagatedHistory::try_from_proto(wire)
                .expect("fixture must be well-formed")
                .expect("fixture scope is not NONE");

            assert_eq!(
                ph.events, events,
                "decoded flat stream must equal the fixture"
            );
            assert_eq!(ph.chunks.len(), chunks.len());
            for (c, &(app, start, count, inst, wf)) in ph.chunks.iter().zip(chunks) {
                assert_eq!(c.app_id, app);
                assert_eq!(c.instance_id, inst);
                assert_eq!(c.workflow_name, wf);
                assert_eq!(
                    c.start_event_index, start as i32,
                    "chunk {app}/{inst} start"
                );
                assert_eq!(c.event_count, count as i32, "chunk {app}/{inst} count");
                assert_eq!(c.events.as_slice(), &events[start..start + count]);
            }
            ph
        }

        /// Go's `makeTestHistory`.
        fn make_test_history() -> PropagatedHistory {
            let events = vec![
                // Chunk 0: MerchantCheckout (appA, wf-001), events[0..3]
                event(0, exec_started("MerchantCheckout")),
                event(
                    1,
                    task_scheduled("ValidateMerchant", "exec-1", r#"{"merchant":"abc"}"#),
                ),
                event(-1, task_completed(1, "exec-1", "true")),
                event(2, child_created("ProcessPayment", "wf-002")),
                // Chunk 1: ProcessPayment (appB, wf-002), events[4..9]
                event(0, exec_started("ProcessPayment")),
                event(
                    1,
                    task_scheduled("ValidateCard", "exec-2", r#"{"card":"4242"}"#),
                ),
                event(-1, task_completed(1, "exec-2", "true")),
                event(
                    2,
                    task_scheduled("ValidateCard", "exec-2", r#"{"card":"4242","retry":true}"#),
                ),
                event(-1, task_failed(2, "exec-2", "card declined")),
                event(3, child_created("FraudDetection", "wf-003")),
            ];
            from_wire(
                events,
                proto::HistoryPropagationScope::Lineage,
                &[
                    ("appA", 0, 4, "wf-001", "MerchantCheckout"),
                    ("appB", 4, 6, "wf-002", "ProcessPayment"),
                ],
            )
        }

        /// `n` chunks `app{i}` / `w-{i}` (1-based), each one `Worker` execution, as
        /// in Go's `_Duplicates` and `_EqualsPluralLast` fixtures.
        fn repeated_worker_history(n: usize) -> PropagatedHistory {
            let events = (0..n).map(|_| event(0, exec_started("Worker"))).collect();
            let descriptors: Vec<(String, String)> = (1..=n)
                .map(|i| (format!("app{i}"), format!("w-{i}")))
                .collect();
            let chunks: Vec<(&str, usize, usize, &str, &str)> = descriptors
                .iter()
                .enumerate()
                .map(|(i, (app, inst))| (app.as_str(), i, 1, inst.as_str(), "Worker"))
                .collect();
            from_wire(events, proto::HistoryPropagationScope::Lineage, &chunks)
        }

        fn assert_not_found(err: &PropagationNotFoundError, kind: &str, name: &str) {
            assert_eq!(err.kind, kind);
            assert_eq!(err.name, name);
        }

        // ---------------------------------------------------------------------------
        // api/propagation_test.go
        // ---------------------------------------------------------------------------

        #[test]
        fn test_get_workflows() {
            let ph = make_test_history();
            // Rust has no GetWorkflows(): the SDK exposes the decoded workflow
            // chunks, in execution order, as the public `chunks` list.
            let wfs = &ph.chunks;

            assert_eq!(wfs.len(), 2);
            assert_eq!(wfs[0].workflow_name, "MerchantCheckout");
            assert_eq!(wfs[0].app_id, "appA");
            assert_eq!(wfs[0].instance_id, "wf-001");
            // Go `Found == true`: the entry is a real chunk. There is no Found flag
            // in Rust; the SDK's by-name lookup must resolve to this very chunk.
            assert!(std::ptr::eq(
                ph.workflow_by_name("MerchantCheckout").expect("found"),
                &wfs[0]
            ));

            assert_eq!(wfs[1].workflow_name, "ProcessPayment");
            assert_eq!(wfs[1].app_id, "appB");
            assert_eq!(wfs[1].instance_id, "wf-002");
            assert!(std::ptr::eq(
                ph.workflow_by_name("ProcessPayment").expect("found"),
                &wfs[1]
            ));
        }

        #[test]
        fn test_get_last_workflow_by_name() {
            let ph = make_test_history();

            let wf = ph
                .workflow_by_name("ProcessPayment")
                .expect("workflow should be found");
            assert_eq!(wf.app_id, "appB");
            assert_eq!(wf.instance_id, "wf-002");

            // Go: require.ErrorIs(err, ErrPropagationNotFound).
            let err = ph.workflow_by_name("NonExistent").unwrap_err();
            assert_not_found(&err, "workflow", "NonExistent");
        }

        #[test]
        fn test_get_workflows_by_name() {
            let ph = make_test_history();

            let wfs = ph.workflows_by_name("ProcessPayment");
            assert_eq!(wfs.len(), 1);
            assert_eq!(wfs[0].instance_id, "wf-002");
            // The SDK's by-name event aggregation must span exactly that one chunk.
            assert_eq!(
                ph.events_by_workflow_name("ProcessPayment").expect("found"),
                wfs[0].events
            );

            // Go: GetWorkflowsByName("NonExistent") == nil.
            let none = ph.workflows_by_name("NonExistent");
            assert!(none.is_empty());
            assert_not_found(
                &ph.events_by_workflow_name("NonExistent").unwrap_err(),
                "workflow",
                "NonExistent",
            );
        }

        #[test]
        fn test_get_workflows_by_name_duplicates() {
            // Two instances of the same workflow name
            let ph = repeated_worker_history(2);

            // Singular returns last (most recent)
            let wf = ph.workflow_by_name("Worker").expect("found");
            assert_eq!(wf.instance_id, "w-2");
            assert_eq!(wf.app_id, "app2");

            // Plural returns all in order
            let wfs = ph.workflows_by_name("Worker");
            assert_eq!(wfs.len(), 2);
            assert_eq!(wfs[0].instance_id, "w-1");
            assert_eq!(wfs[1].instance_id, "w-2");
            // The SDK's by-name event aggregation must span both chunks, in order.
            let expected: Vec<_> = wfs.iter().flat_map(|c| c.events.clone()).collect();
            assert_eq!(expected.len(), 2);
            assert_eq!(
                ph.events_by_workflow_name("Worker").expect("found"),
                expected
            );
        }

        #[test]
        fn test_get_last_activity_by_name() {
            let ph = make_test_history();
            let wf = ph.workflow_by_name("MerchantCheckout").expect("found");

            let act = wf
                .last_activity_by_name("ValidateMerchant")
                .expect("activity should be found");
            assert_eq!(act.name, "ValidateMerchant");
            assert!(act.started);
            assert!(act.completed);
            assert!(!act.failed);
            assert_eq!(act.input.as_deref(), Some(r#"{"merchant":"abc"}"#));
            assert_eq!(act.output.as_deref(), Some("true"));
            assert!(act.failure.is_none());
            assert_eq!(act.task_execution_id, "exec-1");
            assert_eq!(act.scheduled_event_id, 1);

            // Go: require.ErrorIs(err, ErrPropagationNotFound).
            let err = wf.last_activity_by_name("NonExistent").unwrap_err();
            assert_not_found(&err, "activity", "NonExistent");
        }

        #[test]
        fn test_get_last_activity_by_name_returns_last() {
            let ph = make_test_history();
            let wf = ph.workflow_by_name("ProcessPayment").expect("found");

            // ValidateCard was scheduled twice (event ids 1 and 2, both with
            // taskExecutionId "exec-2"); the singular returns the LAST (failed retry).
            let act = wf
                .last_activity_by_name("ValidateCard")
                .expect("activity should be found");
            assert!(act.started);
            assert!(!act.completed);
            assert!(act.failed);
            assert_eq!(
                act.input.as_deref(),
                Some(r#"{"card":"4242","retry":true}"#)
            );
            assert_eq!(
                act.failure.as_ref().map(|f| f.error_message.as_str()),
                Some("card declined")
            );
            assert_eq!(act.scheduled_event_id, 2);
        }

        #[test]
        fn test_get_activities_by_name_retries() {
            let ph = make_test_history();
            let wf = ph.workflow_by_name("ProcessPayment").expect("found");

            let acts = wf.activities_by_name("ValidateCard");
            assert_eq!(acts.len(), 2);

            // Both attempts share taskExecutionId "exec-2"; completion/failure is
            // matched by the scheduling event id (taskScheduledId).
            assert_eq!(acts[0].task_execution_id, "exec-2");
            assert_eq!(acts[1].task_execution_id, "exec-2");

            assert!(acts[0].started);
            assert!(acts[0].completed);
            assert!(!acts[0].failed);
            assert_eq!(acts[0].input.as_deref(), Some(r#"{"card":"4242"}"#));
            assert_eq!(acts[0].output.as_deref(), Some("true"));
            assert!(acts[0].failure.is_none());

            assert!(acts[1].started);
            assert!(!acts[1].completed);
            assert!(acts[1].failed);
            assert_eq!(
                acts[1].input.as_deref(),
                Some(r#"{"card":"4242","retry":true}"#)
            );
            assert!(acts[1].output.is_none());
            assert_eq!(
                acts[1].failure.as_ref().map(|f| f.error_message.as_str()),
                Some("card declined")
            );
        }

        #[test]
        fn test_get_activities_by_name_not_found() {
            let ph = make_test_history();
            let wf = ph.workflow_by_name("ProcessPayment").expect("found");
            // Go: wf.GetActivitiesByName("NonExistent") == nil.
            assert!(wf.activities_by_name("NonExistent").is_empty());
        }

        #[test]
        fn test_get_activities_by_name_workflow_not_found() {
            let ph = make_test_history();
            // Go: _, err := ph.GetLastWorkflowByName("NonExistent") -> ErrorIs ErrPropagationNotFound.
            let err = ph.workflow_by_name("NonExistent").unwrap_err();
            assert_not_found(&err, "workflow", "NonExistent");
            // Go: a zero-valued WorkflowResult (Found == false) returns nil from
            // GetActivitiesByName("ValidateCard"). Rust has no "not found"
            // workflow value — the lookup is an Err — so the equivalent is that
            // no activities are reachable through the failed lookup.
            let acts: Vec<_> = ph
                .workflow_by_name("NonExistent")
                .map(|wf| wf.activities_by_name("ValidateCard"))
                .unwrap_or_default();
            assert!(acts.is_empty());
        }

        #[test]
        fn test_get_last_workflow_by_name_equals_plural_last() {
            let ph = repeated_worker_history(3);

            let singular = ph.workflow_by_name("Worker").expect("found");
            let plural = ph.workflows_by_name("Worker");

            assert_eq!(plural.len(), 3);
            let last = plural[plural.len() - 1];

            // Singular must match the last entry of plural on all observable fields
            // (Go's `Found` has no Rust counterpart; both are real chunks).
            assert_eq!(last.instance_id, singular.instance_id);
            assert_eq!(last.app_id, singular.app_id);
            assert_eq!(last.workflow_name, singular.workflow_name);
            assert!(
                std::ptr::eq(last, singular),
                "singular must be the last chunk"
            );

            // last differs from first
            assert_ne!(
                plural[0].instance_id, singular.instance_id,
                "singular must differ from plural[0]"
            );
        }

        #[test]
        fn test_get_last_activity_by_name_equals_plural_last() {
            let ph = make_test_history();
            let wf = ph.workflow_by_name("ProcessPayment").expect("found");

            let singular = wf.last_activity_by_name("ValidateCard").expect("found");
            let plural = wf.activities_by_name("ValidateCard");
            assert_eq!(plural.len(), 2);

            let last = &plural[1];
            assert_eq!(last.name, singular.name);
            assert_eq!(last.started, singular.started);
            assert_eq!(last.completed, singular.completed);
            assert_eq!(last.failed, singular.failed);
            assert_eq!(last.input, singular.input);
            assert_eq!(last.output, singular.output);
            assert_eq!(
                last.failure.as_ref().map(|f| &f.error_message),
                singular.failure.as_ref().map(|f| &f.error_message)
            );
            assert_eq!(*last, singular);

            // plural[0] (the earlier, successful attempt) differs from singular.
            assert_ne!(plural[0].failed, singular.failed);
        }

        #[test]
        fn test_get_last_child_workflow_by_name() {
            let ph = make_test_history();
            let wf = ph.workflow_by_name("MerchantCheckout").expect("found");

            let child = wf
                .last_child_workflow_by_name("ProcessPayment")
                .expect("child workflow should be found");
            assert!(child.started);
            assert_eq!(child.name, "ProcessPayment");
            assert_eq!(child.instance_id, "wf-002");
            assert_eq!(child.scheduled_event_id, 2);
            // No ChildWorkflowInstanceCompleted event in the fixture.
            assert!(!child.completed);
            assert!(!child.failed);
            assert!(child.output.is_none());

            let err = wf.last_child_workflow_by_name("NonExistent").unwrap_err();
            assert_not_found(&err, "child workflow", "NonExistent");
        }

        #[test]
        fn test_get_child_workflows_by_name() {
            let ph = make_test_history();
            let wf = ph.workflow_by_name("ProcessPayment").expect("found");

            let children = wf.child_workflows_by_name("FraudDetection");
            assert_eq!(children.len(), 1);
            assert!(children[0].started);
            assert_eq!(children[0].name, "FraudDetection");
            assert_eq!(children[0].instance_id, "wf-003");

            // Go: wf.GetChildWorkflowsByName("NonExistent") == nil.
            assert!(wf.child_workflows_by_name("NonExistent").is_empty());
        }

        #[test]
        fn test_get_app_ids() {
            let ph = make_test_history();
            let ids = ph.app_ids();
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0], "appA");
            assert_eq!(ids[1], "appB");
        }

        #[test]
        fn test_get_app_ids_deduplicated() {
            let ph = from_wire(
                vec![],
                proto::HistoryPropagationScope::Lineage,
                &[
                    ("app1", 0, 0, "", ""),
                    ("app1", 0, 0, "", ""),
                    ("app2", 0, 0, "", ""),
                ],
            );
            let ids = ph.app_ids();
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0], "app1");
            assert_eq!(ids[1], "app2");
        }

        #[test]
        fn test_get_events_by_app_id() {
            let ph = make_test_history();

            let app_a_events = ph.events_by_app_id("appA").expect("appA");
            assert_eq!(app_a_events.len(), 4);

            let app_b_events = ph.events_by_app_id("appB").expect("appB");
            assert_eq!(app_b_events.len(), 6);

            // Go returns nil; Rust returns a not-found error.
            let err = ph.events_by_app_id("nonExistent").unwrap_err();
            assert_not_found(&err, "app id", "nonExistent");
        }

        #[test]
        fn test_get_events_by_instance_id() {
            let ph = make_test_history();

            let events = ph.events_by_instance_id("wf-002").expect("wf-002");
            assert_eq!(events.len(), 6);

            // Go returns nil; Rust returns a not-found error.
            let err = ph.events_by_instance_id("nonExistent").unwrap_err();
            assert_not_found(&err, "instance id", "nonExistent");
        }

        #[test]
        fn test_get_events_by_workflow_name() {
            let ph = make_test_history();

            let events = ph
                .events_by_workflow_name("MerchantCheckout")
                .expect("MerchantCheckout");
            assert_eq!(events.len(), 4);

            // Go returns nil; Rust returns a not-found error.
            let err = ph.events_by_workflow_name("nonExistent").unwrap_err();
            assert_not_found(&err, "workflow", "nonExistent");
        }

        fn wire_chunk(app_id: &str, raw_events: Vec<Vec<u8>>) -> proto::PropagatedHistoryChunk {
            proto::PropagatedHistoryChunk {
                app_id: app_id.to_string(),
                raw_events,
                ..Default::default()
            }
        }

        #[test]
        fn test_propagated_history_from_proto() {
            // Each chunk carries its own raw event bytes; try_from_proto decodes them
            // into typed events for SDK consumption.
            let raw_events: Vec<Vec<u8>> = [bare_event(1), bare_event(2)]
                .iter()
                .map(|e| e.encode_to_vec())
                .collect();

            let wire = proto::PropagatedHistory {
                scope: proto::HistoryPropagationScope::Lineage as i32,
                chunks: vec![proto::PropagatedHistoryChunk {
                    app_id: "app1".into(),
                    instance_id: "wf-1".into(),
                    workflow_name: "MyWf".into(),
                    raw_events,
                    ..Default::default()
                }],
            };

            let ph = PropagatedHistory::try_from_proto(wire)
                .expect("require.NoError")
                .expect("require.NotNil");
            assert_eq!(ph.events.len(), 2);
            assert_eq!(ph.scope, HistoryPropagationScope::Lineage);

            let wfs = &ph.chunks;
            assert_eq!(wfs.len(), 1);
            assert_eq!(wfs[0].app_id, "app1");
            assert_eq!(wfs[0].instance_id, "wf-1");
            assert_eq!(wfs[0].workflow_name, "MyWf");
        }

        #[test]
        fn test_propagated_history_from_proto_nil() {
            // Go: PropagatedHistoryFromProto(nil) -> (nil, nil). A Rust message
            // cannot be nil (the worker maps an absent `propagated_history` field to
            // "no history" before decoding); the closest input the decoder can get is
            // an empty message, which must be accepted without error (Go:
            // require.NoError) and yield no history (Go: assert.Nil).
            let res = PropagatedHistory::try_from_proto(proto::PropagatedHistory::default());
            assert!(matches!(res, Ok(None)), "expected Ok(None), got {res:?}");
        }

        #[test]
        fn test_propagated_history_from_proto_empty_app_id() {
            // Go's exact payload: scope unset (NONE), one chunk with an empty appId.
            // ValidatePropagatedHistory rejects it before any decoding.
            let wire = proto::PropagatedHistory {
                scope: proto::HistoryPropagationScope::None as i32,
                chunks: vec![wire_chunk("", vec![])],
            };
            let err = PropagatedHistory::try_from_proto(wire)
                .expect_err("a chunk with an empty appId must be rejected (Go: ph == nil)");
            assert!(err.to_string().contains("empty appId"), "got: {err}");
        }

        #[test]
        fn test_propagated_history_from_proto_invalid_raw_event() {
            let bad = || vec![b"not-a-valid-protobuf-payload".to_vec()];
            let decode = |scope: proto::HistoryPropagationScope| {
                let wire = proto::PropagatedHistory {
                    scope: scope as i32,
                    chunks: vec![wire_chunk("app1", bad())],
                };
                std::panic::catch_unwind(|| PropagatedHistory::try_from_proto(wire))
                    .expect("must not panic (Go: assert.NotPanics)")
            };

            // Supplementary: with a real scope the SDK must reject the payload with
            // Go's message.
            let err = decode(proto::HistoryPropagationScope::Lineage)
                .expect_err("a malformed rawEvent must be rejected");
            assert!(
                err.to_string().contains("failed to decode rawEvent 0"),
                "got: {err}"
            );

            // Go's exact payload leaves the scope unset (NONE). Go decodes rawEvents
            // regardless of scope, so it errors with "failed to decode rawEvent 0"
            // and returns ph == nil.
            match decode(proto::HistoryPropagationScope::None) {
                Err(err) => assert!(
                    err.to_string().contains("failed to decode rawEvent 0"),
                    "got: {err}"
                ),
                Ok(ph) => panic!(
                    "a malformed rawEvent must be rejected even with an unset scope \
             (durabletask-go decodes rawEvents regardless of scope), but \
             try_from_proto returned Ok({})",
                    if ph.is_some() { "Some(..)" } else { "None" }
                ),
            }
        }

        fn scope_to_proto(
            scope: Option<HistoryPropagationScope>,
        ) -> proto::HistoryPropagationScope {
            scope
                .map(proto::HistoryPropagationScope::from)
                .unwrap_or(proto::HistoryPropagationScope::None)
        }

        fn test_timestamp() -> proto::prost_types::Timestamp {
            proto::prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }
        }

        /// Run one orchestrator turn in-process (no sidecar) and return the actions
        /// it emits.
        async fn run_turn(orch: OrchestratorFn) -> Vec<proto::WorkflowAction> {
            let old_events = vec![
                proto::HistoryEvent {
                    event_id: 1,
                    timestamp: Some(test_timestamp()),
                    router: None,
                    event_type: Some(EventType::WorkflowStarted(
                        proto::WorkflowStartedEvent::default(),
                    )),
                },
                proto::HistoryEvent {
                    event_id: 2,
                    timestamp: Some(test_timestamp()),
                    router: None,
                    event_type: Some(exec_started("Parent")),
                },
            ];
            OrchestrationExecutor::execute(
                &orch,
                "parent",
                old_events,
                vec![],
                String::new(),
                &WorkerOptions::default(),
                None,
            )
            .await
            .expect("orchestrator turn must succeed")
            .actions
        }

        #[tokio::test]
        async fn test_new_history_propagation_scope_nil() {
            // Go: NewHistoryPropagationScope(nil) == SCOPE_NONE, without panicking.
            // Rust has no nil option (HistoryPropagationScope has no NONE variant);
            // "no option" means never calling `with_history_propagation`, and the
            // SDK must then emit SCOPE_NONE on the scheduled action.
            const NONE: i32 = proto::HistoryPropagationScope::None as i32;
            let act = ActivityOptions::new();
            assert!(act.history_propagation_scope.is_none());
            let sub = SubOrchestratorOptions::new();
            assert!(sub.history_propagation_scope.is_none());

            let actions = run_turn(Arc::new(move |ctx| {
                let act = act.clone();
                Box::pin(async move {
                    let _ = ctx.call_activity_with_options("Act", "x", act).await;
                    Ok(None)
                })
            }))
            .await;
            let st = actions
                .iter()
                .find_map(|a| match &a.workflow_action_type {
                    Some(WorkflowActionType::ScheduleTask(st)) => Some(st),
                    _ => None,
                })
                .expect("ScheduleTask action");
            // Absent and explicit SCOPE_NONE are equivalent on the wire.
            assert_eq!(
                st.history_propagation_scope.unwrap_or(NONE),
                NONE,
                "expected SCOPE_NONE"
            );

            let actions = run_turn(Arc::new(move |ctx| {
                let sub = sub.clone();
                Box::pin(async move {
                    let _ = ctx
                        .call_sub_orchestrator_with_options("Child", "x", sub)
                        .await;
                    Ok(None)
                })
            }))
            .await;
            let cw = actions
                .iter()
                .find_map(|a| match &a.workflow_action_type {
                    Some(WorkflowActionType::CreateChildWorkflow(cw)) => Some(cw),
                    _ => None,
                })
                .expect("CreateChildWorkflow action");
            // Absent and explicit SCOPE_NONE are equivalent on the wire.
            assert_eq!(
                cw.history_propagation_scope.unwrap_or(NONE),
                NONE,
                "expected SCOPE_NONE"
            );
        }

        #[test]
        fn test_new_history_propagation_scope_options() {
            // Go: NewHistoryPropagationScope(PropagateOwnHistory()) == OWN_HISTORY and
            // NewHistoryPropagationScope(PropagateLineage()) == LINEAGE. Rust: the
            // option builders plus the SDK's scope -> wire conversion (the `From`
            // impl the context uses when emitting actions).
            assert_eq!(
                proto::HistoryPropagationScope::OwnHistory,
                scope_to_proto(
                    ActivityOptions::new()
                        .with_history_propagation(HistoryPropagationScope::OwnHistory)
                        .history_propagation_scope
                )
            );
            assert_eq!(
                proto::HistoryPropagationScope::Lineage,
                scope_to_proto(
                    ActivityOptions::new()
                        .with_history_propagation(HistoryPropagationScope::Lineage)
                        .history_propagation_scope
                )
            );
            assert_eq!(
                proto::HistoryPropagationScope::OwnHistory,
                scope_to_proto(
                    SubOrchestratorOptions::new()
                        .with_history_propagation(HistoryPropagationScope::OwnHistory)
                        .history_propagation_scope
                )
            );
            assert_eq!(
                proto::HistoryPropagationScope::Lineage,
                scope_to_proto(
                    SubOrchestratorOptions::new()
                        .with_history_propagation(HistoryPropagationScope::Lineage)
                        .history_propagation_scope
                )
            );
        }
    }
}
