use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::api::{DurableTaskError, Result};
use crate::internal;
use crate::proto;

/// TLS configuration for a gRPC connection.
///
/// # Examples
///
/// ```rust,no_run
/// use dapr_durabletask::client::{ClientOptions, TlsConfig};
///
/// // Use system CA certificates (no client cert).
/// let opts = ClientOptions::new().with_tls(TlsConfig::default());
///
/// // Mutual TLS with a custom CA and client certificate.
/// let tls = TlsConfig::new()
///     .with_ca_cert_pem(std::fs::read("ca.pem").unwrap())
///     .with_client_cert_pem(
///         std::fs::read("client.pem").unwrap(),
///         std::fs::read("client.key").unwrap(),
///     );
/// let opts = ClientOptions::new().with_tls(tls);
/// ```
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// PEM-encoded CA certificate to use for server certificate verification.
    /// If `None`, the system root CA store is used.
    pub ca_cert_pem: Option<Vec<u8>>,

    /// PEM-encoded client certificate for mutual TLS. Requires `client_key_pem`.
    pub client_cert_pem: Option<Vec<u8>>,

    /// PEM-encoded private key for the client certificate.
    pub client_key_pem: Option<Vec<u8>>,

    /// Skip server certificate verification. **Use only in development.**
    pub skip_verify: bool,

    /// Override the domain name used for TLS verification.
    /// Useful when the server's certificate CN differs from the host address.
    pub domain_name: Option<String>,
}

impl TlsConfig {
    /// Create a TLS config that uses the system CA store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a custom PEM-encoded CA certificate for server verification.
    pub fn with_ca_cert_pem(mut self, pem: Vec<u8>) -> Self {
        self.ca_cert_pem = Some(pem);
        self
    }

    /// Set a PEM-encoded client certificate and private key for mutual TLS.
    pub fn with_client_cert_pem(mut self, cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        self.client_cert_pem = Some(cert_pem);
        self.client_key_pem = Some(key_pem);
        self
    }

    /// Skip server certificate verification. **Use only in development.**
    pub fn with_skip_verify(mut self) -> Self {
        self.skip_verify = true;
        self
    }

    /// Override the domain name used for TLS server name indication (SNI).
    pub fn with_domain_name(mut self, name: impl Into<String>) -> Self {
        self.domain_name = Some(name.into());
        self
    }
}

/// Configuration options for [`TaskHubGrpcClient`](super::TaskHubGrpcClient).
///
/// All fields have sensible defaults. Use [`ClientOptions::default()`] or the
/// builder methods to customise.
///
/// # Examples
///
/// ```rust,no_run
/// use dapr_durabletask::client::{ClientOptions, TlsConfig};
/// use std::time::Duration;
///
/// let opts = ClientOptions::new()
///     .with_tls(TlsConfig::default())
///     .with_connect_timeout(Duration::from_secs(5))
///     .with_max_grpc_message_size(16 * 1024 * 1024);
/// ```
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Maximum JSON payload size in bytes for deserialisation.
    pub max_json_payload_size: usize,

    /// Maximum allowed length (in bytes) for identifiers such as orchestrator
    /// names, instance IDs, and event names.
    pub max_identifier_length: usize,

    /// TLS configuration. When `None` the connection is plaintext.
    pub tls: Option<TlsConfig>,

    /// Timeout for establishing the initial TCP connection to the sidecar.
    pub connect_timeout: Option<Duration>,

    /// Maximum size in bytes for inbound gRPC messages.
    /// Defaults to tonic's built-in 4 MiB limit when not set.
    pub max_grpc_message_size: Option<usize>,

    /// Duration between TCP keepalive probes sent to the sidecar.
    pub keepalive_interval: Option<Duration>,

    /// Duration to wait for a keepalive acknowledgement before closing the
    /// connection.
    pub keepalive_timeout: Option<Duration>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            max_json_payload_size: 64 * 1024 * 1024, // 64 MiB
            max_identifier_length: 1_024,
            tls: None,
            connect_timeout: None,
            max_grpc_message_size: None,
            keepalive_interval: None,
            keepalive_timeout: None,
        }
    }
}

impl ClientOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
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

    /// Enable TLS with the given configuration.
    /// Pass [`TlsConfig::default()`] to use system CA certificates.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the timeout for the initial TCP connection.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the maximum inbound gRPC message size in bytes.
    pub fn with_max_grpc_message_size(mut self, size: usize) -> Self {
        self.max_grpc_message_size = Some(size);
        self
    }

    /// Set the interval between TCP keepalive probes.
    pub fn with_keepalive_interval(mut self, interval: Duration) -> Self {
        self.keepalive_interval = Some(interval);
        self
    }

    /// Set the timeout for a keepalive acknowledgement.
    pub fn with_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.keepalive_timeout = Some(timeout);
        self
    }
}

// ─── Per-call request options ────────────────────────────────────────────────

/// Build a [`TaskRouter`](proto::TaskRouter) that targets `app_id`, or `None`
/// when no app ID is set (the request is served by the local app).
///
/// The source app ID is left empty: the sidecar stamps it, not the client.
fn router_for(app_id: Option<&str>) -> Option<proto::TaskRouter> {
    app_id.map(|id| proto::TaskRouter {
        source_app_id: String::new(),
        target_app_id: Some(id.to_string()),
        target_app_namespace: None,
    })
}

/// Validate a task router attached to a client request.
///
/// A target app namespace must be paired with a target app ID. A missing
/// router, an empty router, a router with only a target app ID, and a router
/// with both a target app ID and namespace are all valid. The client calls
/// this on every routed request after all options have been applied, because
/// the invariant spans several fields.
///
/// Mirrors durabletask-go's `api.ValidateTaskRouter`.
///
/// # Errors
/// Returns [`DurableTaskError::Other`] if the router has a non-empty target
/// app namespace but no (or an empty) target app ID.
pub fn validate_task_router(router: Option<&proto::TaskRouter>) -> Result<()> {
    let Some(r) = router else {
        return Ok(());
    };
    let has_ns = r
        .target_app_namespace
        .as_deref()
        .is_some_and(|s| !s.is_empty());
    let has_app = r.target_app_id.as_deref().is_some_and(|s| !s.is_empty());
    if has_ns && !has_app {
        return Err(DurableTaskError::Other(
            "a target app namespace requires a target app ID".into(),
        ));
    }
    Ok(())
}

/// Options for [`TaskHubGrpcClient::schedule_new_orchestration_with_options`](super::TaskHubGrpcClient::schedule_new_orchestration_with_options).
///
/// # Examples
///
/// ```rust
/// use dapr_durabletask::client::NewOrchestrationOptions;
///
/// let opts = NewOrchestrationOptions::new()
///     .with_input(r#""hello""#)
///     .with_instance_id("order-42")
///     .with_enforce_unique_instance_id()
///     .with_app_id("orders-app");
/// assert!(opts.enforce_unique_instance_id);
/// ```
#[derive(Debug, Clone, Default)]
pub struct NewOrchestrationOptions {
    /// JSON-serialized orchestration input.
    pub input: Option<String>,
    /// Instance ID to use. A random UUID is generated when `None`.
    pub instance_id: Option<String>,
    /// Delay the start of the orchestration until this time.
    pub start_at: Option<DateTime<Utc>>,
    /// Fail scheduling if an instance with the same ID already exists, whether
    /// it is still active or already completed. The sidecar reports this as a
    /// gRPC `ALREADY_EXISTS` status. Without it, an existing completed
    /// instance is replaced (restarted) and only an active one is rejected.
    pub enforce_unique_instance_id: bool,
    /// Schedule the orchestration on the app with this app ID rather than the
    /// local app. The target app's access policy governs whether this is
    /// permitted.
    pub app_id: Option<String>,
}

impl NewOrchestrationOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON-serialized orchestration input.
    pub fn with_input(mut self, input: impl Into<String>) -> Self {
        self.input = Some(input.into());
        self
    }

    /// Set the instance ID.
    pub fn with_instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.instance_id = Some(instance_id.into());
        self
    }

    /// Delay the start of the orchestration until `start_at`.
    pub fn with_start_time(mut self, start_at: DateTime<Utc>) -> Self {
        self.start_at = Some(start_at);
        self
    }

    /// Fail if an instance with the same ID already exists (active or
    /// completed). See [`Self::enforce_unique_instance_id`].
    pub fn with_enforce_unique_instance_id(mut self) -> Self {
        self.enforce_unique_instance_id = true;
        self
    }

    /// Target the app with the given app ID. See [`Self::app_id`].
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Build the proto request. `instance_id` is the resolved instance ID.
    pub(crate) fn to_request(
        &self,
        name: &str,
        instance_id: String,
        parent_trace_context: Option<proto::TraceContext>,
    ) -> proto::CreateInstanceRequest {
        proto::CreateInstanceRequest {
            instance_id,
            name: name.to_string(),
            input: self.input.clone(),
            scheduled_start_timestamp: self.start_at.map(internal::to_timestamp),
            version: None,
            execution_id: None,
            tags: std::collections::HashMap::new(),
            parent_trace_context,
            enforce_unique_instance_id: self.enforce_unique_instance_id,
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for fetching or waiting on orchestration state, used by
/// [`TaskHubGrpcClient::get_orchestration_state_with_options`](super::TaskHubGrpcClient::get_orchestration_state_with_options)
/// and the `wait_for_orchestration_*_with_options` methods.
#[derive(Debug, Clone)]
pub struct FetchOptions {
    /// Whether to load inputs, outputs and custom status (which may be
    /// large). Defaults to `true`.
    pub fetch_payloads: bool,
    /// Read the instance owned by the app with this app ID rather than the
    /// local app.
    pub app_id: Option<String>,
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self {
            fetch_payloads: true,
            app_id: None,
        }
    }
}

impl FetchOptions {
    /// Create options with default values (payloads fetched, local app).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to load inputs, outputs and custom status.
    pub fn with_fetch_payloads(mut self, fetch_payloads: bool) -> Self {
        self.fetch_payloads = fetch_payloads;
        self
    }

    /// Target the instance owned by the app with the given app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub(crate) fn to_request(&self, instance_id: &str) -> proto::GetInstanceRequest {
        proto::GetInstanceRequest {
            instance_id: instance_id.to_string(),
            get_inputs_and_outputs: self.fetch_payloads,
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for [`TaskHubGrpcClient::raise_orchestration_event_with_options`](super::TaskHubGrpcClient::raise_orchestration_event_with_options).
#[derive(Debug, Clone, Default)]
pub struct RaiseEventOptions {
    /// JSON-serialized event payload.
    pub data: Option<String>,
    /// Raise the event on the instance owned by the app with this app ID.
    pub app_id: Option<String>,
}

impl RaiseEventOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON-serialized event payload.
    pub fn with_data(mut self, data: impl Into<String>) -> Self {
        self.data = Some(data.into());
        self
    }

    /// Target the instance owned by the app with the given app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub(crate) fn to_request(
        &self,
        instance_id: &str,
        event_name: &str,
    ) -> proto::RaiseEventRequest {
        proto::RaiseEventRequest {
            instance_id: instance_id.to_string(),
            name: event_name.to_string(),
            input: self.data.clone(),
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for [`TaskHubGrpcClient::terminate_orchestration_with_options`](super::TaskHubGrpcClient::terminate_orchestration_with_options).
#[derive(Debug, Clone, Default)]
pub struct TerminateOptions {
    /// JSON-serialized output recorded for the terminated instance.
    pub output: Option<String>,
    /// Also terminate all child orchestrations.
    pub recursive: bool,
    /// Terminate the instance owned by the app with this app ID.
    pub app_id: Option<String>,
}

impl TerminateOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON-serialized output.
    pub fn with_output(mut self, output: impl Into<String>) -> Self {
        self.output = Some(output.into());
        self
    }

    /// Set whether child orchestrations are terminated too.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Target the instance owned by the app with the given app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub(crate) fn to_request(&self, instance_id: &str) -> proto::TerminateRequest {
        proto::TerminateRequest {
            instance_id: instance_id.to_string(),
            output: self.output.clone(),
            recursive: self.recursive,
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for [`TaskHubGrpcClient::suspend_orchestration_with_options`](super::TaskHubGrpcClient::suspend_orchestration_with_options).
#[derive(Debug, Clone, Default)]
pub struct SuspendOptions {
    /// Human-readable reason for the suspension.
    pub reason: Option<String>,
    /// Suspend the instance owned by the app with this app ID.
    pub app_id: Option<String>,
}

impl SuspendOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the suspension reason.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Target the instance owned by the app with the given app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub(crate) fn to_request(&self, instance_id: &str) -> proto::SuspendRequest {
        proto::SuspendRequest {
            instance_id: instance_id.to_string(),
            reason: self.reason.clone(),
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for [`TaskHubGrpcClient::resume_orchestration_with_options`](super::TaskHubGrpcClient::resume_orchestration_with_options).
#[derive(Debug, Clone, Default)]
pub struct ResumeOptions {
    /// Human-readable reason for the resumption.
    pub reason: Option<String>,
    /// Resume the instance owned by the app with this app ID.
    pub app_id: Option<String>,
}

impl ResumeOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the resumption reason.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Target the instance owned by the app with the given app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub(crate) fn to_request(&self, instance_id: &str) -> proto::ResumeRequest {
        proto::ResumeRequest {
            instance_id: instance_id.to_string(),
            reason: self.reason.clone(),
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for [`TaskHubGrpcClient::purge_orchestration_with_options`](super::TaskHubGrpcClient::purge_orchestration_with_options)
/// and [`TaskHubGrpcClient::purge_orchestrations_by_filter_with_options`](super::TaskHubGrpcClient::purge_orchestrations_by_filter_with_options).
///
/// # Examples
///
/// ```rust
/// use dapr_durabletask::client::PurgeOptions;
///
/// // Tear down a stuck subtree regardless of its runtime status.
/// let opts = PurgeOptions::new().with_recursive(true).with_force(true);
/// assert_eq!(opts.force, Some(true));
/// ```
#[derive(Debug, Clone, Default)]
pub struct PurgeOptions {
    /// Also purge all child orchestrations.
    pub recursive: bool,
    /// Purge regardless of the instance's state, even if it is still running
    /// or being processed. Highly discouraged unless you know what you are
    /// doing. `None` leaves the sidecar default (not forced).
    pub force: Option<bool>,
    /// Purge the instance owned by the app with this app ID. The sidecar
    /// delegates the whole purge (honouring `recursive`) to the target app
    /// without walking any local state.
    pub app_id: Option<String>,
}

impl PurgeOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether child orchestrations are purged too.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Set whether to force the purge. See [`Self::force`].
    pub fn with_force(mut self, force: bool) -> Self {
        self.force = Some(force);
        self
    }

    /// Target the instance owned by the app with the given app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub(crate) fn to_request(
        &self,
        request: proto::purge_instances_request::Request,
    ) -> proto::PurgeInstancesRequest {
        proto::PurgeInstancesRequest {
            request: Some(request),
            recursive: self.recursive,
            force: self.force,
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for [`TaskHubGrpcClient::rerun_orchestration_from_event`](super::TaskHubGrpcClient::rerun_orchestration_from_event).
#[derive(Debug, Clone, Default)]
pub struct RerunOptions {
    /// Instance ID for the new instance. A random ID is generated by the
    /// sidecar when `None`.
    pub new_instance_id: Option<String>,
    /// Replace the input of the event being rerun from with [`Self::input`].
    pub overwrite_input: bool,
    /// New JSON-serialized input, used when [`Self::overwrite_input`] is set.
    /// `None` with `overwrite_input` clears the input.
    pub input: Option<String>,
    /// Instance ID to use when rerunning from a child-workflow creation event.
    pub new_child_workflow_instance_id: Option<String>,
    /// Rerun the instance owned by the app with this app ID.
    pub app_id: Option<String>,
}

impl RerunOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the new instance ID.
    pub fn with_new_instance_id(mut self, id: impl Into<String>) -> Self {
        self.new_instance_id = Some(id.into());
        self
    }

    /// Overwrite the rerun event's input with `input` (`None` clears it).
    pub fn with_input(mut self, input: Option<String>) -> Self {
        self.overwrite_input = true;
        self.input = input;
        self
    }

    /// Set the instance ID for a rerun child workflow.
    pub fn with_new_child_workflow_instance_id(mut self, id: impl Into<String>) -> Self {
        self.new_child_workflow_instance_id = Some(id.into());
        self
    }

    /// Target the instance owned by the app with the given app ID.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub(crate) fn to_request(
        &self,
        source_instance_id: &str,
        event_id: u32,
    ) -> proto::RerunWorkflowFromEventRequest {
        proto::RerunWorkflowFromEventRequest {
            source_instance_id: source_instance_id.to_string(),
            event_id,
            new_instance_id: self.new_instance_id.clone(),
            input: self.input.clone(),
            overwrite_input: self.overwrite_input,
            new_child_workflow_instance_id: self.new_child_workflow_instance_id.clone(),
            router: router_for(self.app_id.as_deref()),
        }
    }
}

/// Options for [`TaskHubGrpcClient::list_instance_ids`](super::TaskHubGrpcClient::list_instance_ids).
#[derive(Debug, Clone)]
pub struct ListInstanceIdsOptions {
    /// Maximum number of instance IDs per page. Defaults to `Some(1024)`;
    /// `None` asks the sidecar to return all instances at once.
    pub page_size: Option<u32>,
    /// Continuation token from a previous page. `None` requests the first
    /// page.
    pub continuation_token: Option<String>,
}

impl Default for ListInstanceIdsOptions {
    fn default() -> Self {
        Self {
            page_size: Some(1024),
            continuation_token: None,
        }
    }
}

impl ListInstanceIdsOptions {
    /// Create options with default values (first page, 1024 per page).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the page size.
    pub fn with_page_size(mut self, page_size: u32) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Continue from the given token.
    pub fn with_continuation_token(mut self, token: impl Into<String>) -> Self {
        self.continuation_token = Some(token.into());
        self
    }

    pub(crate) fn to_request(&self) -> proto::ListInstanceIDsRequest {
        proto::ListInstanceIDsRequest {
            continuation_token: self.continuation_token.clone(),
            page_size: self.page_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rerun_request_carries_every_option() {
        let req = RerunOptions::new()
            .with_new_instance_id("new")
            .with_input(Some("\"in\"".into()))
            .with_new_child_workflow_instance_id("child")
            .with_app_id("app2")
            .to_request("source", 7);
        assert_eq!(req.source_instance_id, "source");
        assert_eq!(req.event_id, 7);
        assert_eq!(req.new_instance_id.as_deref(), Some("new"));
        assert!(req.overwrite_input);
        assert_eq!(req.input.as_deref(), Some("\"in\""));
        assert_eq!(req.new_child_workflow_instance_id.as_deref(), Some("child"));
        assert_eq!(
            req.router.and_then(|r| r.target_app_id).as_deref(),
            Some("app2")
        );

        // Without options the source input is kept and the sidecar picks IDs.
        let req = RerunOptions::new().to_request("source", 0);
        assert!(!req.overwrite_input);
        assert!(req.input.is_none() && req.new_instance_id.is_none() && req.router.is_none());

        // `with_input(None)` clears the input.
        let req = RerunOptions::new().with_input(None).to_request("source", 0);
        assert!(req.overwrite_input && req.input.is_none());
    }

    #[test]
    fn purge_by_filter_request_carries_filter_and_options() {
        let filter = crate::api::PurgeInstanceFilter::new()
            .with_runtime_status([crate::api::OrchestrationStatus::Completed]);
        let req = PurgeOptions::new()
            .with_recursive(true)
            .with_force(true)
            .with_app_id("app2")
            .to_request(
                proto::purge_instances_request::Request::PurgeInstanceFilter(filter.into_proto()),
            );
        assert!(req.recursive);
        assert_eq!(req.force, Some(true));
        assert_eq!(
            req.router.and_then(|r| r.target_app_id).as_deref(),
            Some("app2")
        );
        match req.request {
            Some(proto::purge_instances_request::Request::PurgeInstanceFilter(f)) => {
                assert_eq!(
                    f.runtime_status,
                    vec![proto::OrchestrationStatus::Completed as i32]
                );
            }
            other => panic!("expected a filter, got {other:?}"),
        }
        let req = PurgeOptions::new().to_request(
            proto::purge_instances_request::Request::InstanceId("i".into()),
        );
        assert!(!req.recursive && req.force.is_none() && req.router.is_none());
    }

    #[test]
    fn tls_config_defaults() {
        let tls = TlsConfig::new();
        assert!(tls.ca_cert_pem.is_none());
        assert!(tls.client_cert_pem.is_none());
        assert!(tls.client_key_pem.is_none());
        assert!(!tls.skip_verify);
        assert!(tls.domain_name.is_none());
    }

    #[test]
    fn tls_config_with_ca_cert_pem() {
        let tls = TlsConfig::new().with_ca_cert_pem(b"ca-data".to_vec());
        assert_eq!(tls.ca_cert_pem.as_deref(), Some(b"ca-data".as_slice()));
    }

    #[test]
    fn tls_config_with_client_cert_pem() {
        let tls = TlsConfig::new().with_client_cert_pem(b"cert".to_vec(), b"key".to_vec());
        assert_eq!(tls.client_cert_pem.as_deref(), Some(b"cert".as_slice()));
        assert_eq!(tls.client_key_pem.as_deref(), Some(b"key".as_slice()));
    }

    #[test]
    fn tls_config_with_skip_verify() {
        let tls = TlsConfig::new().with_skip_verify();
        assert!(tls.skip_verify);
    }

    #[test]
    fn tls_config_with_domain_name() {
        let tls = TlsConfig::new().with_domain_name("example.com");
        assert_eq!(tls.domain_name.as_deref(), Some("example.com"));
    }

    #[test]
    fn client_options_defaults() {
        let opts = ClientOptions::default();
        assert_eq!(opts.max_json_payload_size, 64 * 1024 * 1024);
        assert_eq!(opts.max_identifier_length, 1024);
        assert!(opts.tls.is_none());
        assert!(opts.connect_timeout.is_none());
        assert!(opts.max_grpc_message_size.is_none());
        assert!(opts.keepalive_interval.is_none());
        assert!(opts.keepalive_timeout.is_none());
    }

    #[test]
    fn client_options_with_max_json_payload_size() {
        let opts = ClientOptions::new().with_max_json_payload_size(1024);
        assert_eq!(opts.max_json_payload_size, 1024);
    }

    #[test]
    fn client_options_with_max_identifier_length() {
        let opts = ClientOptions::new().with_max_identifier_length(256);
        assert_eq!(opts.max_identifier_length, 256);
    }

    #[test]
    fn client_options_with_tls() {
        let opts = ClientOptions::new().with_tls(TlsConfig::default());
        assert!(opts.tls.is_some());
    }

    #[test]
    fn client_options_with_connect_timeout() {
        let opts = ClientOptions::new().with_connect_timeout(Duration::from_secs(5));
        assert_eq!(opts.connect_timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    fn client_options_with_max_grpc_message_size() {
        let opts = ClientOptions::new().with_max_grpc_message_size(16 * 1024 * 1024);
        assert_eq!(opts.max_grpc_message_size, Some(16 * 1024 * 1024));
    }

    #[test]
    fn client_options_with_keepalive_interval() {
        let opts = ClientOptions::new().with_keepalive_interval(Duration::from_secs(30));
        assert_eq!(opts.keepalive_interval, Some(Duration::from_secs(30)));
    }

    #[test]
    fn client_options_with_keepalive_timeout() {
        let opts = ClientOptions::new().with_keepalive_timeout(Duration::from_secs(10));
        assert_eq!(opts.keepalive_timeout, Some(Duration::from_secs(10)));
    }

    #[test]
    fn client_options_builder_chaining() {
        let opts = ClientOptions::new()
            .with_max_json_payload_size(2048)
            .with_max_identifier_length(512)
            .with_tls(TlsConfig::new().with_skip_verify())
            .with_connect_timeout(Duration::from_secs(3))
            .with_max_grpc_message_size(8 * 1024 * 1024)
            .with_keepalive_interval(Duration::from_secs(60))
            .with_keepalive_timeout(Duration::from_secs(20));

        assert_eq!(opts.max_json_payload_size, 2048);
        assert_eq!(opts.max_identifier_length, 512);
        assert!(opts.tls.as_ref().unwrap().skip_verify);
        assert_eq!(opts.connect_timeout, Some(Duration::from_secs(3)));
        assert_eq!(opts.max_grpc_message_size, Some(8 * 1024 * 1024));
        assert_eq!(opts.keepalive_interval, Some(Duration::from_secs(60)));
        assert_eq!(opts.keepalive_timeout, Some(Duration::from_secs(20)));
    }

    mod router_options {

        use super::super::*;

        fn target(r: Option<&proto::TaskRouter>) -> Option<&str> {
            r.and_then(|r| r.target_app_id.as_deref())
        }

        #[test]
        fn test_with_app_id_options_set_target_on_router() {
            // schedule
            let req = NewOrchestrationOptions::new()
                .with_app_id("app2")
                .to_request("wf", "id".into(), None);
            assert!(validate_task_router(req.router.as_ref()).is_ok());
            assert_eq!(target(req.router.as_ref()), Some("app2"));
            let ns = req
                .router
                .as_ref()
                .and_then(|r| r.target_app_namespace.as_deref());
            assert!(ns.is_none_or(str::is_empty));
            // The sidecar stamps the source app ID, not the client.
            assert!(req.router.as_ref().unwrap().source_app_id.is_empty());

            // schedule composes with other options
            let opts = NewOrchestrationOptions::new()
                .with_instance_id("iid")
                .with_app_id("app2");
            let iid = opts.instance_id.clone().unwrap();
            let req = opts.to_request("wf", iid, None);
            assert_eq!(req.instance_id, "iid");
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // fetch
            let req = FetchOptions::new().with_app_id("app2").to_request("i");
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // raise event
            let req = RaiseEventOptions::new()
                .with_app_id("app2")
                .to_request("i", "e");
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // terminate
            let req = TerminateOptions::new().with_app_id("app2").to_request("i");
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // suspend
            let req = SuspendOptions::new().with_app_id("app2").to_request("i");
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // resume
            let req = ResumeOptions::new().with_app_id("app2").to_request("i");
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // purge
            let req = PurgeOptions::new().with_app_id("app2").to_request(
                proto::purge_instances_request::Request::InstanceId("i".into()),
            );
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // rerun
            let req = RerunOptions::new().with_app_id("app2").to_request("i", 0);
            assert_eq!(target(req.router.as_ref()), Some("app2"));

            // Without an app ID no router is sent (served by the local app).
            assert!(FetchOptions::new().to_request("i").router.is_none());
            assert!(
                NewOrchestrationOptions::new()
                    .to_request("wf", "i".into(), None)
                    .router
                    .is_none()
            );
        }

        #[test]
        fn test_validate_task_router() {
            let router = |app: Option<&str>, ns: Option<&str>| proto::TaskRouter {
                source_app_id: String::new(),
                target_app_id: app.map(str::to_string),
                target_app_namespace: ns.map(str::to_string),
            };
            assert!(validate_task_router(None).is_ok());
            assert!(validate_task_router(Some(&router(None, None))).is_ok());
            assert!(validate_task_router(Some(&router(Some("app2"), None))).is_ok());
            assert!(validate_task_router(Some(&router(Some("app2"), Some("ns2")))).is_ok());
            let err = validate_task_router(Some(&router(None, Some("ns2"))))
                .expect_err("namespace without app id must be rejected");
            assert!(err.to_string().contains("requires a target app ID"));
            // An empty target app ID counts as missing.
            assert!(validate_task_router(Some(&router(Some(""), Some("ns2")))).is_err());
        }
    }

    #[test]
    fn new_orchestration_options_to_request() {
        let start = chrono::Utc::now();
        let req = NewOrchestrationOptions::new()
            .with_input("1")
            .with_start_time(start)
            .with_enforce_unique_instance_id()
            .to_request("wf", "id".into(), None);
        assert_eq!(req.name, "wf");
        assert_eq!(req.instance_id, "id");
        assert_eq!(req.input.as_deref(), Some("1"));
        assert!(req.enforce_unique_instance_id);
        assert_eq!(
            req.scheduled_start_timestamp.unwrap().seconds,
            start.timestamp()
        );
        let req = NewOrchestrationOptions::new().to_request("wf", "id".into(), None);
        assert!(!req.enforce_unique_instance_id);
    }

    #[test]
    fn purge_options_force_and_recursive() {
        let req = PurgeOptions::new()
            .with_recursive(true)
            .with_force(true)
            .to_request(proto::purge_instances_request::Request::InstanceId(
                "i".into(),
            ));
        assert!(req.recursive);
        assert_eq!(req.force, Some(true));
        assert!(
            PurgeOptions::new()
                .to_request(proto::purge_instances_request::Request::InstanceId(
                    "i".into()
                ))
                .force
                .is_none()
        );
    }

    #[test]
    fn fetch_and_list_defaults() {
        assert!(FetchOptions::default().fetch_payloads);
        let req = ListInstanceIdsOptions::new().to_request();
        assert_eq!(req.page_size, Some(1024));
        assert!(req.continuation_token.is_none());
        let req = ListInstanceIdsOptions::new()
            .with_page_size(2)
            .with_continuation_token("t")
            .to_request();
        assert_eq!(req.page_size, Some(2));
        assert_eq!(req.continuation_token.as_deref(), Some("t"));
    }

    #[test]
    fn rerun_options_with_input_sets_overwrite() {
        let req = RerunOptions::new().with_input(None).to_request("i", 3);
        assert!(req.overwrite_input);
        assert!(req.input.is_none());
        assert_eq!(req.event_id, 3);
        assert!(!RerunOptions::new().to_request("i", 3).overwrite_input);
    }
}
