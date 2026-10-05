use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

use crate::api::{DurableTaskError, OrchestrationState, PurgeInstanceFilter, Result};
use crate::internal;
use crate::proto;
use crate::proto::task_hub_sidecar_service_client::TaskHubSidecarServiceClient;

use super::options::{
    ClientOptions, FetchOptions, ListInstanceIdsOptions, NewOrchestrationOptions, PurgeOptions,
    RaiseEventOptions, RerunOptions, ResumeOptions, SuspendOptions, TerminateOptions,
    validate_task_router,
};

/// One page of instance IDs returned by
/// [`TaskHubGrpcClient::list_instance_ids`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstanceIdPage {
    /// The instance IDs on this page.
    pub instance_ids: Vec<String>,
    /// Token to pass to [`ListInstanceIdsOptions::with_continuation_token`] to
    /// fetch the next page, or `None` if this is the last page.
    pub continuation_token: Option<String>,
}

/// Client for managing orchestrations via a gRPC connection to a sidecar.
pub struct TaskHubGrpcClient {
    inner: TaskHubSidecarServiceClient<Channel>,
    options: ClientOptions,
}

// ─── Channel construction ────────────────────────────────────────────────────

/// Build a tonic [`Channel`] from the host address and client options,
/// applying TLS, keepalive, connect timeout, and message-size limits.
async fn build_channel(host_address: &str, options: &ClientOptions) -> Result<Channel> {
    const USER_AGENT: &str = concat!("dapr-durabletask/rust/", env!("CARGO_PKG_VERSION"));

    let mut builder = Channel::from_shared(host_address.to_string())
        .map_err(|e| DurableTaskError::InvalidAddress(e.to_string()))?
        .user_agent(USER_AGENT)
        .map_err(|e| DurableTaskError::InvalidAddress(e.to_string()))?;

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
            .map_err(|e| DurableTaskError::ConnectionFailed(e.to_string()))?;
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
        .map_err(|e| DurableTaskError::ConnectionFailed(e.to_string()))
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
    ///
    /// # Errors
    /// Returns [`DurableTaskError::InvalidAddress`] if `host_address` is not a
    /// valid URI, or [`DurableTaskError::ConnectionFailed`] / [`DurableTaskError::GrpcError`]
    /// if the underlying transport cannot be established.
    pub async fn new(host_address: &str) -> Result<Self> {
        Self::with_options(host_address, ClientOptions::default()).await
    }

    /// Create a new client connected to the given host address with custom options.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::InvalidAddress`] if `host_address` is not a
    /// valid URI, [`DurableTaskError::Other`] if TLS options are inconsistent
    /// (e.g. only one of `client_cert_pem` / `client_key_pem` is set, or
    /// `skip_verify` is requested), or [`DurableTaskError::ConnectionFailed`] /
    /// [`DurableTaskError::GrpcError`] if the underlying transport cannot be
    /// established.
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
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `orchestrator_name` or
    /// `instance_id` is empty, exceeds the configured identifier length, or
    /// contains control characters. Returns [`DurableTaskError::GrpcError`] if
    /// the sidecar RPC fails.
    pub async fn schedule_new_orchestration(
        &mut self,
        orchestrator_name: &str,
        input: Option<String>,
        instance_id: Option<String>,
        start_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
        let options = NewOrchestrationOptions {
            input,
            instance_id,
            start_at,
            ..Default::default()
        };
        self.schedule_new_orchestration_with_options(orchestrator_name, options)
            .await
    }

    /// Schedule a new orchestration instance with the given options and
    /// return its instance ID.
    ///
    /// Beyond [`schedule_new_orchestration`](Self::schedule_new_orchestration)
    /// this supports enforcing a unique instance ID and targeting another app.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use dapr_durabletask::client::NewOrchestrationOptions;
    ///
    /// # async fn example(mut client: dapr_durabletask::client::TaskHubGrpcClient) {
    /// let id = client
    ///     .schedule_new_orchestration_with_options(
    ///         "ProcessOrder",
    ///         NewOrchestrationOptions::new()
    ///             .with_instance_id("order-42")
    ///             .with_enforce_unique_instance_id(),
    ///     )
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `orchestrator_name` or the
    /// instance ID is invalid. Returns [`DurableTaskError::GrpcError`] if the
    /// sidecar RPC fails; with
    /// [`enforce_unique_instance_id`](NewOrchestrationOptions::enforce_unique_instance_id)
    /// set, an existing instance (active or completed) yields a status with
    /// code [`tonic::Code::AlreadyExists`].
    pub async fn schedule_new_orchestration_with_options(
        &mut self,
        orchestrator_name: &str,
        options: NewOrchestrationOptions,
    ) -> Result<String> {
        internal::validate_identifier(
            orchestrator_name,
            "orchestrator name",
            self.options.max_identifier_length,
        )?;
        let instance_id = options
            .instance_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
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

        let request = options.to_request(orchestrator_name, instance_id, parent_trace_context);
        validate_task_router(request.router.as_ref())?;

        let response = self.inner.start_instance(request).await?;
        let result_id = response.into_inner().instance_id;

        #[cfg(feature = "opentelemetry")]
        internal::otel::end_span(&_otel_ctx);

        tracing::debug!(instance_id = %result_id, "Orchestration scheduled");
        Ok(result_id)
    }

    /// Get the current state of an orchestration.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails. The successful
    /// result is `Ok(None)` if the instance does not exist.
    pub async fn get_orchestration_state(
        &mut self,
        instance_id: &str,
        fetch_payloads: bool,
    ) -> Result<Option<OrchestrationState>> {
        let options = FetchOptions::new().with_fetch_payloads(fetch_payloads);
        self.get_orchestration_state_with_options(instance_id, options)
            .await
    }

    /// Get the current state of an orchestration with the given options
    /// (payload fetching and app-id routing).
    ///
    /// # Errors
    /// Same as [`get_orchestration_state`](Self::get_orchestration_state).
    pub async fn get_orchestration_state_with_options(
        &mut self,
        instance_id: &str,
        options: FetchOptions,
    ) -> Result<Option<OrchestrationState>> {
        let request = self.get_instance_request(instance_id, &options)?;
        let response = self.inner.get_instance(request).await?;
        Ok(OrchestrationState::try_from(&response.into_inner()).ok())
    }

    /// Wait for an orchestration to start running.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid,
    /// [`DurableTaskError::Timeout`] if `timeout` elapses before the instance
    /// starts, or [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn wait_for_orchestration_start(
        &mut self,
        instance_id: &str,
        fetch_payloads: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<OrchestrationState>> {
        let options = FetchOptions::new().with_fetch_payloads(fetch_payloads);
        self.wait_for_orchestration_start_with_options(instance_id, options, timeout)
            .await
    }

    /// Like [`wait_for_orchestration_start`](Self::wait_for_orchestration_start),
    /// with [`FetchOptions`] for payload fetching and app-id routing.
    ///
    /// # Errors
    /// Same as [`wait_for_orchestration_start`](Self::wait_for_orchestration_start).
    pub async fn wait_for_orchestration_start_with_options(
        &mut self,
        instance_id: &str,
        options: FetchOptions,
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<OrchestrationState>> {
        let request = self.get_instance_request(instance_id, &options)?;
        tracing::debug!(instance_id = %instance_id, "Waiting for orchestration to start");

        let fut = self.inner.wait_for_instance_start(request);

        let response = if let Some(timeout_dur) = timeout {
            tokio::time::timeout(timeout_dur, fut)
                .await
                .map_err(|_| DurableTaskError::Timeout)??
        } else {
            fut.await?
        };

        let state = OrchestrationState::try_from(&response.into_inner()).ok();
        tracing::debug!(
            instance_id = %instance_id,
            status = ?state.as_ref().map(|s| &s.runtime_status),
            "Orchestration started"
        );
        Ok(state)
    }

    /// Wait for an orchestration to reach a terminal state.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid,
    /// [`DurableTaskError::Timeout`] if `timeout` elapses before completion, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn wait_for_orchestration_completion(
        &mut self,
        instance_id: &str,
        fetch_payloads: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<OrchestrationState>> {
        let options = FetchOptions::new().with_fetch_payloads(fetch_payloads);
        self.wait_for_orchestration_completion_with_options(instance_id, options, timeout)
            .await
    }

    /// Like [`wait_for_orchestration_completion`](Self::wait_for_orchestration_completion),
    /// with [`FetchOptions`] for payload fetching and app-id routing.
    ///
    /// # Errors
    /// Same as [`wait_for_orchestration_completion`](Self::wait_for_orchestration_completion).
    pub async fn wait_for_orchestration_completion_with_options(
        &mut self,
        instance_id: &str,
        options: FetchOptions,
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<OrchestrationState>> {
        let request = self.get_instance_request(instance_id, &options)?;
        tracing::debug!(instance_id = %instance_id, "Waiting for orchestration completion");

        let fut = self.inner.wait_for_instance_completion(request);

        let response = if let Some(timeout_dur) = timeout {
            tokio::time::timeout(timeout_dur, fut)
                .await
                .map_err(|_| DurableTaskError::Timeout)??
        } else {
            fut.await?
        };

        let state = OrchestrationState::try_from(&response.into_inner()).ok();
        tracing::debug!(
            instance_id = %instance_id,
            status = ?state.as_ref().map(|s| &s.runtime_status),
            "Orchestration completed"
        );
        Ok(state)
    }

    /// Raise an event to an orchestration instance.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` or `event_name` is
    /// invalid, or [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn raise_orchestration_event(
        &mut self,
        instance_id: &str,
        event_name: &str,
        data: Option<String>,
    ) -> Result<()> {
        let options = RaiseEventOptions { data, app_id: None };
        self.raise_orchestration_event_with_options(instance_id, event_name, options)
            .await
    }

    /// Raise an event to an orchestration instance with the given options
    /// (payload and app-id routing).
    ///
    /// # Errors
    /// Same as [`raise_orchestration_event`](Self::raise_orchestration_event).
    pub async fn raise_orchestration_event_with_options(
        &mut self,
        instance_id: &str,
        event_name: &str,
        options: RaiseEventOptions,
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
        let request = options.to_request(instance_id, event_name);
        validate_task_router(request.router.as_ref())?;
        self.inner.raise_event(request).await?;
        Ok(())
    }

    /// Terminate a running orchestration.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn terminate_orchestration(
        &mut self,
        instance_id: &str,
        output: Option<String>,
        recursive: bool,
    ) -> Result<()> {
        let options = TerminateOptions {
            output,
            recursive,
            app_id: None,
        };
        self.terminate_orchestration_with_options(instance_id, options)
            .await
    }

    /// Terminate a running orchestration with the given options (output,
    /// recursion and app-id routing).
    ///
    /// # Errors
    /// Same as [`terminate_orchestration`](Self::terminate_orchestration).
    pub async fn terminate_orchestration_with_options(
        &mut self,
        instance_id: &str,
        options: TerminateOptions,
    ) -> Result<()> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(
            instance_id = %instance_id,
            recursive = options.recursive,
            "Terminating orchestration"
        );
        let request = options.to_request(instance_id);
        validate_task_router(request.router.as_ref())?;
        self.inner.terminate_instance(request).await?;
        Ok(())
    }

    /// Suspend a running orchestration.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn suspend_orchestration(
        &mut self,
        instance_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        let options = SuspendOptions {
            reason,
            app_id: None,
        };
        self.suspend_orchestration_with_options(instance_id, options)
            .await
    }

    /// Like [`suspend_orchestration`](Self::suspend_orchestration), with
    /// [`SuspendOptions`] for the reason and app-id routing.
    ///
    /// # Errors
    /// Same as [`suspend_orchestration`](Self::suspend_orchestration).
    pub async fn suspend_orchestration_with_options(
        &mut self,
        instance_id: &str,
        options: SuspendOptions,
    ) -> Result<()> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(instance_id = %instance_id, "Suspending orchestration");
        let request = options.to_request(instance_id);
        validate_task_router(request.router.as_ref())?;
        self.inner.suspend_instance(request).await?;
        Ok(())
    }

    /// Resume a suspended orchestration.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn resume_orchestration(
        &mut self,
        instance_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        let options = ResumeOptions {
            reason,
            app_id: None,
        };
        self.resume_orchestration_with_options(instance_id, options)
            .await
    }

    /// Like [`resume_orchestration`](Self::resume_orchestration), with
    /// [`ResumeOptions`] for the reason and app-id routing.
    ///
    /// # Errors
    /// Same as [`resume_orchestration`](Self::resume_orchestration).
    pub async fn resume_orchestration_with_options(
        &mut self,
        instance_id: &str,
        options: ResumeOptions,
    ) -> Result<()> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(instance_id = %instance_id, "Resuming orchestration");
        let request = options.to_request(instance_id);
        validate_task_router(request.router.as_ref())?;
        self.inner.resume_instance(request).await?;
        Ok(())
    }

    /// Purge an orchestration's history and state by instance ID.
    ///
    /// Returns the number of deleted instances.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn purge_orchestration(&mut self, instance_id: &str, recursive: bool) -> Result<i32> {
        let options = PurgeOptions::new().with_recursive(recursive);
        self.purge_orchestration_with_options(instance_id, options)
            .await
    }

    /// Purge an orchestration's history and state with the given options
    /// (recursion, force and app-id routing).
    ///
    /// Returns the number of deleted instances.
    ///
    /// With [`PurgeOptions::with_force`] the purge proceeds even if the
    /// instance (or, when recursive, any descendant) has not completed. With
    /// [`PurgeOptions::with_app_id`] the sidecar delegates the purge to the
    /// target app without walking local state.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn purge_orchestration_with_options(
        &mut self,
        instance_id: &str,
        options: PurgeOptions,
    ) -> Result<i32> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        tracing::info!(instance_id = %instance_id, "Purging orchestration");
        let request = options.to_request(proto::purge_instances_request::Request::InstanceId(
            instance_id.to_string(),
        ));
        validate_task_router(request.router.as_ref())?;
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
    ///
    /// # Errors
    /// Returns [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn purge_orchestrations_by_filter(
        &mut self,
        filter: PurgeInstanceFilter,
        recursive: bool,
    ) -> Result<i32> {
        let options = PurgeOptions::new().with_recursive(recursive);
        self.purge_orchestrations_by_filter_with_options(filter, options)
            .await
    }

    /// Purge orchestrations matching the given filter with the given options
    /// (recursion, force and app-id routing).
    ///
    /// Returns the number of deleted instances.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn purge_orchestrations_by_filter_with_options(
        &mut self,
        filter: PurgeInstanceFilter,
        options: PurgeOptions,
    ) -> Result<i32> {
        tracing::info!(?filter, "Purging orchestrations by filter");
        let request = options.to_request(
            proto::purge_instances_request::Request::PurgeInstanceFilter(filter.into_proto()),
        );
        validate_task_router(request.router.as_ref())?;
        let response = self.inner.purge_instances(request).await?;
        let count = response.into_inner().deleted_instance_count;
        tracing::debug!(deleted = count, "Purge by filter complete");
        Ok(count)
    }

    /// Rerun an orchestration from a specific history event ID of the source
    /// instance, creating a new instance. Returns the new instance's ID.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `source_instance_id` (or a
    /// provided new instance ID) is invalid, or [`DurableTaskError::GrpcError`]
    /// if the sidecar RPC fails.
    pub async fn rerun_orchestration_from_event(
        &mut self,
        source_instance_id: &str,
        event_id: u32,
        options: RerunOptions,
    ) -> Result<String> {
        internal::validate_identifier(
            source_instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        if let Some(id) = &options.new_instance_id {
            internal::validate_identifier(id, "instance ID", self.options.max_identifier_length)?;
        }
        tracing::info!(
            instance_id = %source_instance_id,
            event_id,
            "Rerunning orchestration from event"
        );
        let request = options.to_request(source_instance_id, event_id);
        validate_task_router(request.router.as_ref())?;
        let response = self.inner.rerun_workflow_from_event(request).await?;
        Ok(response.into_inner().new_instance_id)
    }

    /// List the IDs of orchestration instances known to the sidecar, one page
    /// at a time.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use dapr_durabletask::client::ListInstanceIdsOptions;
    ///
    /// # async fn example(mut client: dapr_durabletask::client::TaskHubGrpcClient) {
    /// let mut options = ListInstanceIdsOptions::new().with_page_size(100);
    /// loop {
    ///     let page = client.list_instance_ids(options.clone()).await.unwrap();
    ///     for id in &page.instance_ids {
    ///         println!("{id}");
    ///     }
    ///     match page.continuation_token {
    ///         Some(token) => options = options.with_continuation_token(token),
    ///         None => break,
    ///     }
    /// }
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns [`DurableTaskError::GrpcError`] if the sidecar RPC fails.
    pub async fn list_instance_ids(
        &mut self,
        options: ListInstanceIdsOptions,
    ) -> Result<InstanceIdPage> {
        let response = self
            .inner
            .list_instance_i_ds(options.to_request())
            .await?
            .into_inner();
        Ok(InstanceIdPage {
            instance_ids: response.instance_ids,
            continuation_token: response.continuation_token.filter(|t| !t.is_empty()),
        })
    }

    /// Get the full persisted history of an orchestration instance, in order.
    ///
    /// # Errors
    /// Returns [`DurableTaskError::Other`] if `instance_id` is invalid, or
    /// [`DurableTaskError::GrpcError`] if the sidecar RPC fails (including
    /// when the instance does not exist).
    pub async fn get_instance_history(
        &mut self,
        instance_id: &str,
    ) -> Result<Vec<proto::HistoryEvent>> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        let request = proto::GetInstanceHistoryRequest {
            instance_id: instance_id.to_string(),
        };
        let response = self.inner.get_instance_history(request).await?;
        Ok(response.into_inner().events)
    }

    /// Validate `instance_id` and build a routed `GetInstanceRequest`.
    fn get_instance_request(
        &self,
        instance_id: &str,
        options: &FetchOptions,
    ) -> Result<proto::GetInstanceRequest> {
        internal::validate_identifier(
            instance_id,
            "instance ID",
            self.options.max_identifier_length,
        )?;
        let request = options.to_request(instance_id);
        validate_task_router(request.router.as_ref())?;
        Ok(request)
    }
}
