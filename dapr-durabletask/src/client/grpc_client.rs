use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

use crate::api::{DurableTaskError, OrchestrationState, PurgeInstanceFilter, Result};
use crate::internal;
use crate::proto;
use crate::proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient;

use super::options::ClientOptions;

/// Client for managing orchestrations via a gRPC connection to a sidecar.
pub struct TaskHubGrpcClient {
    inner: TaskHubSidecarServiceClient<Channel>,
    options: ClientOptions,
}

// ─── Channel construction ────────────────────────────────────────────────────

/// Build a tonic [`Channel`] from the host address and client options,
/// applying TLS, keepalive, connect timeout, and message-size limits.
async fn build_channel(host_address: &str, options: &ClientOptions) -> Result<Channel> {
    let mut builder = Channel::from_shared(host_address.to_string())
        .map_err(|e| DurableTaskError::Other(e.to_string()))?;

    if let Some(tls) = &options.tls {
        if tls.skip_verify {
            return Err(DurableTaskError::Other(
                "skip_verify is not supported; connect without TLS for development".into(),
            ));
        }

        let mut tls_config = ClientTlsConfig::new();

        if let Some(ca_pem) = &tls.ca_cert_pem {
            tls_config = tls_config.ca_certificate(Certificate::from_pem(ca_pem));
        }

        match (&tls.client_cert_pem, &tls.client_key_pem) {
            (Some(cert), Some(key)) => {
                tls_config = tls_config.identity(Identity::from_pem(cert, key));
            }
            (None, None) => {}
            _ => {
                return Err(DurableTaskError::Other(
                    "client_cert_pem and client_key_pem must both be set for mutual TLS".into(),
                ));
            }
        }

        if let Some(domain) = &tls.domain_name {
            tls_config = tls_config.domain_name(domain.clone());
        }

        builder = builder
            .tls_config(tls_config)
            .map_err(|e| DurableTaskError::Other(e.to_string()))?;
    }

    if let Some(timeout) = options.connect_timeout {
        builder = builder.connect_timeout(timeout);
    }

    if let Some(interval) = options.keepalive_interval {
        builder = builder.tcp_keepalive(Some(interval));
    }

    builder
        .connect()
        .await
        .map_err(|e| DurableTaskError::Other(e.to_string()))
}

/// Wrap a channel in the gRPC client stub, applying the max-message-size limit.
fn make_stub(channel: Channel, options: &ClientOptions) -> TaskHubSidecarServiceClient<Channel> {
    let mut stub = TaskHubSidecarServiceClient::new(channel);
    if let Some(size) = options.max_grpc_message_size {
        stub = stub.max_decoding_message_size(size);
    }
    stub
}

// ─── TaskHubGrpcClient ───────────────────────────────────────────────────────

impl TaskHubGrpcClient {
    /// Create a new client connected to the given host address.
    ///
    /// The default address is `http://localhost:4001`.
    pub async fn new(host_address: &str) -> Result<Self> {
        Self::with_options(host_address, ClientOptions::default()).await
    }

    /// Create a new client connected to the given host address with custom options.
    pub async fn with_options(host_address: &str, options: ClientOptions) -> Result<Self> {
        tracing::info!(address = %host_address, "Connecting to sidecar");
        let channel = build_channel(host_address, &options).await?;
        tracing::info!(address = %host_address, "Client connected");
        let inner = make_stub(channel, &options);
        Ok(Self { inner, options })
    }

    /// Create a client from an existing tonic [`Channel`].
    pub fn from_channel(channel: Channel) -> Self {
        let options = ClientOptions::default();
        let inner = make_stub(channel, &options);
        Self { inner, options }
    }

    /// Create a client from an existing tonic [`Channel`] with custom options.
    pub fn from_channel_with_options(channel: Channel, options: ClientOptions) -> Self {
        let inner = make_stub(channel, &options);
        Self { inner, options }
    }

    /// Close the client, releasing the underlying gRPC channel.
    ///
    /// The channel is also released when the client is dropped. This method
    /// provides an explicit, named alternative.
    pub fn close(self) {
        drop(self);
    }

    /// Schedule a new orchestration instance and return its instance ID.
    pub async fn schedule_new_orchestration(
        &mut self,
        orchestrator_name: &str,
        input: Option<String>,
        instance_id: Option<String>,
        start_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
        internal::validate_identifier(
            orchestrator_name,
            "orchestrator name",
            self.options.max_identifier_length,
        )?;
        let instance_id = instance_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        internal::validate_identifier(
            &instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;

        tracing::info!(
            instance_id = %instance_id,
            orchestrator = %orchestrator_name,
            "Scheduling new orchestration"
        );

        #[cfg(feature = "opentelemetry")]
        let (parent_trace_context, _otel_ctx) = {
            let parent_ctx = opentelemetry::Context::current();
            let ctx = internal::otel::start_create_orchestration_span(
                &parent_ctx,
                orchestrator_name,
                &instance_id,
            );
            let sc = opentelemetry::trace::TraceContextExt::span(&ctx)
                .span_context()
                .clone();
            let tc = internal::otel::trace_context_from_span_context(&sc);
            (tc, ctx)
        };
        #[cfg(not(feature = "opentelemetry"))]
        let parent_trace_context: Option<proto::TraceContext> = None;

        let request = proto::CreateInstanceRequest {
            instance_id: instance_id.clone(),
            name: orchestrator_name.to_string(),
            input,
            scheduled_start_timestamp: start_at.map(internal::to_timestamp),
            version: None,
            execution_id: None,
            tags: std::collections::HashMap::new(),
            parent_trace_context,
        };

        let response = self.inner.start_instance(request).await?;
        let result_id = response.into_inner().instance_id;

        #[cfg(feature = "opentelemetry")]
        internal::otel::end_span(&_otel_ctx);

        tracing::debug!(instance_id = %result_id, "Orchestration scheduled");
        Ok(result_id)
    }

    /// Get the current state of an orchestration.
    pub async fn get_orchestration_state(
        &mut self,
        instance_id: &str,
        fetch_payloads: bool,
    ) -> Result<Option<OrchestrationState>> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        let request = proto::GetInstanceRequest {
            instance_id: instance_id.to_string(),
            get_inputs_and_outputs: fetch_payloads,
        };
        let response = self.inner.get_instance(request).await?;
        Ok(OrchestrationState::from_proto(&response.into_inner()))
    }

    /// Wait for an orchestration to start running.
    pub async fn wait_for_orchestration_start(
        &mut self,
        instance_id: &str,
        fetch_payloads: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<OrchestrationState>> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::debug!(instance_id = %instance_id, "Waiting for orchestration to start");

        let request = proto::GetInstanceRequest {
            instance_id: instance_id.to_string(),
            get_inputs_and_outputs: fetch_payloads,
        };

        let fut = self.inner.wait_for_instance_start(request);

        let response = if let Some(timeout_dur) = timeout {
            tokio::time::timeout(timeout_dur, fut)
                .await
                .map_err(|_| DurableTaskError::Timeout)??
        } else {
            fut.await?
        };

        let state = OrchestrationState::from_proto(&response.into_inner());
        tracing::debug!(
            instance_id = %instance_id,
            status = ?state.as_ref().map(|s| &s.runtime_status),
            "Orchestration started"
        );
        Ok(state)
    }

    /// Wait for an orchestration to reach a terminal state.
    pub async fn wait_for_orchestration_completion(
        &mut self,
        instance_id: &str,
        fetch_payloads: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<OrchestrationState>> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::debug!(instance_id = %instance_id, "Waiting for orchestration completion");

        let request = proto::GetInstanceRequest {
            instance_id: instance_id.to_string(),
            get_inputs_and_outputs: fetch_payloads,
        };

        let fut = self.inner.wait_for_instance_completion(request);

        let response = if let Some(timeout_dur) = timeout {
            tokio::time::timeout(timeout_dur, fut)
                .await
                .map_err(|_| DurableTaskError::Timeout)??
        } else {
            fut.await?
        };

        let state = OrchestrationState::from_proto(&response.into_inner());
        tracing::debug!(
            instance_id = %instance_id,
            status = ?state.as_ref().map(|s| &s.runtime_status),
            "Orchestration completed"
        );
        Ok(state)
    }

    /// Raise an event to an orchestration instance.
    pub async fn raise_orchestration_event(
        &mut self,
        instance_id: &str,
        event_name: &str,
        data: Option<String>,
    ) -> Result<()> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        internal::validate_identifier(
            event_name,
            "event name",
            self.options.max_identifier_length,
        )?;
        tracing::info!(
            instance_id = %instance_id,
            event_name = %event_name,
            "Raising orchestration event"
        );
        let request = proto::RaiseEventRequest {
            instance_id: instance_id.to_string(),
            name: event_name.to_string(),
            input: data,
        };
        self.inner.raise_event(request).await?;
        Ok(())
    }

    /// Terminate a running orchestration.
    pub async fn terminate_orchestration(
        &mut self,
        instance_id: &str,
        output: Option<String>,
        recursive: bool,
    ) -> Result<()> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(
            instance_id = %instance_id,
            recursive = recursive,
            "Terminating orchestration"
        );
        let request = proto::TerminateRequest {
            instance_id: instance_id.to_string(),
            output,
            recursive,
        };
        self.inner.terminate_instance(request).await?;
        Ok(())
    }

    /// Suspend a running orchestration.
    pub async fn suspend_orchestration(
        &mut self,
        instance_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(instance_id = %instance_id, "Suspending orchestration");
        let request = proto::SuspendRequest {
            instance_id: instance_id.to_string(),
            reason,
        };
        self.inner.suspend_instance(request).await?;
        Ok(())
    }

    /// Resume a suspended orchestration.
    pub async fn resume_orchestration(
        &mut self,
        instance_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(instance_id = %instance_id, "Resuming orchestration");
        let request = proto::ResumeRequest {
            instance_id: instance_id.to_string(),
            reason,
        };
        self.inner.resume_instance(request).await?;
        Ok(())
    }

    /// Purge an orchestration's history and state by instance ID.
    ///
    /// Returns the number of deleted instances.
    pub async fn purge_orchestration(&mut self, instance_id: &str, recursive: bool) -> Result<i32> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(instance_id = %instance_id, "Purging orchestration");
        let request = proto::PurgeInstancesRequest {
            request: Some(proto::purge_instances_request::Request::InstanceId(
                instance_id.to_string(),
            )),
            recursive,
            force: None,
        };
        let response = self.inner.purge_instances(request).await?;
        let count = response.into_inner().deleted_instance_count;
        tracing::debug!(instance_id = %instance_id, deleted = count, "Purge complete");
        Ok(count)
    }

    /// Purge orchestrations matching the given filter criteria.
    ///
    /// Returns the number of deleted instances.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use dapr_durabletask::api::{OrchestrationStatus, PurgeInstanceFilter};
    ///
    /// # async fn example(mut client: dapr_durabletask::client::TaskHubGrpcClient) {
    /// let filter = PurgeInstanceFilter::new()
    ///     .with_created_time_from(chrono::Utc::now() - chrono::Duration::hours(24))
    ///     .with_runtime_status([OrchestrationStatus::Completed, OrchestrationStatus::Failed]);
    ///
    /// let deleted = client.purge_orchestrations_by_filter(filter, false).await.unwrap();
    /// println!("Deleted {deleted} orchestrations");
    /// # }
    /// ```
    pub async fn purge_orchestrations_by_filter(
        &mut self,
        filter: PurgeInstanceFilter,
        recursive: bool,
    ) -> Result<i32> {
        tracing::info!(?filter, "Purging orchestrations by filter");
        let request = proto::PurgeInstancesRequest {
            request: Some(
                proto::purge_instances_request::Request::PurgeInstanceFilter(filter.into_proto()),
            ),
            recursive,
            force: None,
        };
        let response = self.inner.purge_instances(request).await?;
        let count = response.into_inner().deleted_instance_count;
        tracing::debug!(deleted = count, "Purge by filter complete");
        Ok(count)
    }
}
