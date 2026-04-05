use std::time::Duration;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
