use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::api::Result;
use crate::task::{ActivityContext, OrchestrationContext};

/// Type alias for orchestrator function return type.
pub type OrchestratorResult = Result<Option<String>>;

/// Type alias for boxed orchestrator functions.
///
/// Orchestrator functions are async functions that take an `OrchestrationContext`
/// and return an optional JSON-serialised result string.
pub type OrchestratorFn = Arc<
    dyn Fn(OrchestrationContext) -> Pin<Box<dyn Future<Output = OrchestratorResult> + Send>>
        + Send
        + Sync,
>;

/// Type alias for activity function return type.
pub type ActivityResult = Result<Option<String>>;

/// Type alias for boxed activity functions.
///
/// Activity functions are async functions that take an `ActivityContext` and
/// optional JSON-serialised input string.
pub type ActivityFn = Arc<
    dyn Fn(ActivityContext, Option<String>) -> Pin<Box<dyn Future<Output = ActivityResult> + Send>>
        + Send
        + Sync,
>;

/// An entry in the orchestrator registry.
///
/// Holds the function pointer and an optional version label. The sidecar
/// dispatches work items by name; the registry resolves the correct function
/// using the version selection rules (exact match → latest → unversioned).
#[derive(Clone)]
struct OrchestratorEntry {
    f: OrchestratorFn,
    /// `None` means the entry is unversioned (registered with
    /// [`Registry::add_named_orchestrator`]).
    version: Option<String>,
    is_latest: bool,
}

/// Registry for orchestrator and activity functions.
///
/// Functions must be registered before the worker is started. Versioned
/// orchestrators can be registered with [`add_versioned_orchestrator`] and a
/// specific version string, or with [`add_latest_orchestrator`] to mark a
/// version as the default when no exact version match is found.
///
/// [`add_versioned_orchestrator`]: Registry::add_versioned_orchestrator
/// [`add_latest_orchestrator`]: Registry::add_latest_orchestrator
pub struct Registry {
    /// Map of orchestrator name → list of registered entries.
    orchestrators: HashMap<String, Vec<OrchestratorEntry>>,
    activities: HashMap<String, ActivityFn>,
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            orchestrators: HashMap::new(),
            activities: HashMap::new(),
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────

    fn push_orchestrator_entry(&mut self, name: &str, entry: OrchestratorEntry) {
        self.orchestrators
            .entry(name.to_string())
            .or_default()
            .push(entry);
    }

    // ─── Registration ─────────────────────────────────────────────────────

    /// Register an unversioned orchestrator function with the given name.
    ///
    /// This is the simplest registration path. When versioning is not
    /// required, prefer this method.
    pub fn add_named_orchestrator<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn(OrchestrationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = OrchestratorResult> + Send + 'static,
    {
        tracing::info!(orchestrator = %name, "Registering orchestrator");
        let f: OrchestratorFn = Arc::new(move |ctx| {
            Box::pin(f(ctx)) as Pin<Box<dyn Future<Output = OrchestratorResult> + Send>>
        });
        self.push_orchestrator_entry(
            name,
            OrchestratorEntry {
                f,
                version: None,
                is_latest: false,
            },
        );
    }

    /// Register a versioned orchestrator function.
    ///
    /// The `version` is matched against the `version` field of incoming
    /// `ExecutionStarted` history events. Use this when you need to run
    /// multiple versions of the same orchestrator simultaneously.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use dapr_durabletask::worker::Registry;
    ///
    /// let mut reg = Registry::new();
    /// reg.add_versioned_orchestrator("my_orch", "v1", |ctx| async move { Ok(None) });
    /// reg.add_versioned_orchestrator("my_orch", "v2", |ctx| async move { Ok(None) });
    /// ```
    pub fn add_versioned_orchestrator<F, Fut>(&mut self, name: &str, version: &str, f: F)
    where
        F: Fn(OrchestrationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = OrchestratorResult> + Send + 'static,
    {
        tracing::info!(orchestrator = %name, version = %version, "Registering versioned orchestrator");
        let f: OrchestratorFn = Arc::new(move |ctx| {
            Box::pin(f(ctx)) as Pin<Box<dyn Future<Output = OrchestratorResult> + Send>>
        });
        self.push_orchestrator_entry(
            name,
            OrchestratorEntry {
                f,
                version: Some(version.to_string()),
                is_latest: false,
            },
        );
    }

    /// Register a versioned orchestrator and mark it as the *latest*.
    ///
    /// The latest entry is selected when no exact version match is found. If
    /// multiple entries are marked as latest for the same name, the last one
    /// registered wins.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use dapr_durabletask::worker::Registry;
    ///
    /// let mut reg = Registry::new();
    /// reg.add_versioned_orchestrator("my_orch", "v1", |ctx| async move { Ok(None) });
    /// reg.add_latest_orchestrator("my_orch", "v2", |ctx| async move { Ok(None) });
    /// // Requests for "v1" → v1 handler; any other version → v2 handler.
    /// ```
    pub fn add_latest_orchestrator<F, Fut>(&mut self, name: &str, version: &str, f: F)
    where
        F: Fn(OrchestrationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = OrchestratorResult> + Send + 'static,
    {
        tracing::info!(orchestrator = %name, version = %version, "Registering latest orchestrator");
        let f: OrchestratorFn = Arc::new(move |ctx| {
            Box::pin(f(ctx)) as Pin<Box<dyn Future<Output = OrchestratorResult> + Send>>
        });
        self.push_orchestrator_entry(
            name,
            OrchestratorEntry {
                f,
                version: Some(version.to_string()),
                is_latest: true,
            },
        );
    }

    /// Register an activity function with the given name.
    pub fn add_named_activity<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn(ActivityContext, Option<String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ActivityResult> + Send + 'static,
    {
        tracing::info!(activity = %name, "Registering activity");
        let f: ActivityFn = Arc::new(move |ctx, input| {
            Box::pin(f(ctx, input)) as Pin<Box<dyn Future<Output = ActivityResult> + Send>>
        });
        self.activities.insert(name.to_string(), f);
    }

    // ─── Lookup ───────────────────────────────────────────────────────────

    /// Look up a registered orchestrator by name and optional version.
    ///
    /// Resolution order:
    /// 1. Exact version match (when `version` is `Some`).
    /// 2. The entry marked as `is_latest` for this name.
    /// 3. The single unversioned entry (registered via
    ///    [`add_named_orchestrator`]).
    ///
    /// Returns `None` if no suitable entry is found.
    ///
    /// [`add_named_orchestrator`]: Registry::add_named_orchestrator
    pub fn get_orchestrator(&self, name: &str) -> Option<&OrchestratorFn> {
        self.get_orchestrator_version(name, None)
    }

    /// Look up a registered orchestrator by name, optionally constraining the
    /// version. See [`get_orchestrator`](Self::get_orchestrator) for the
    /// resolution rules.
    pub fn get_orchestrator_version(
        &self,
        name: &str,
        version: Option<&str>,
    ) -> Option<&OrchestratorFn> {
        let entries = self.orchestrators.get(name)?;

        // 1. Exact version match.
        if let Some(v) = version {
            if let Some(entry) = entries.iter().find(|e| e.version.as_deref() == Some(v)) {
                return Some(&entry.f);
            }
        }

        // 2. Latest-flagged entry (last registered wins).
        if let Some(entry) = entries.iter().rev().find(|e| e.is_latest) {
            return Some(&entry.f);
        }

        // 3. Unversioned entry.
        if let Some(entry) = entries.iter().find(|e| e.version.is_none()) {
            return Some(&entry.f);
        }

        None
    }

    /// Look up a registered activity by name.
    pub fn get_activity(&self, name: &str) -> Option<&ActivityFn> {
        self.activities.get(name)
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn dummy_orchestrator(_ctx: OrchestrationContext) -> OrchestratorResult {
        Ok(Some("\"done\"".to_string()))
    }

    async fn dummy_activity(_ctx: ActivityContext, _input: Option<String>) -> ActivityResult {
        Ok(Some("\"result\"".to_string()))
    }

    #[test]
    fn test_register_and_lookup_orchestrator() {
        let mut reg = Registry::new();
        reg.add_named_orchestrator("my_orch", dummy_orchestrator);
        assert!(reg.get_orchestrator("my_orch").is_some());
        assert!(reg.get_orchestrator("missing").is_none());
    }

    #[test]
    fn test_register_and_lookup_activity() {
        let mut reg = Registry::new();
        reg.add_named_activity("my_act", dummy_activity);
        assert!(reg.get_activity("my_act").is_some());
        assert!(reg.get_activity("missing").is_none());
    }

    #[tokio::test]
    async fn test_invoke_orchestrator() {
        let mut reg = Registry::new();
        reg.add_named_orchestrator("orch", dummy_orchestrator);

        let f = reg.get_orchestrator("orch").unwrap();
        let ctx = OrchestrationContext::new(
            "test".to_string(),
            "orch".to_string(),
            None,
            chrono::Utc::now(),
            false,
            &crate::worker::WorkerOptions::default(),
            0,
        );
        let result = (f)(ctx).await;
        assert_eq!(result.unwrap(), Some("\"done\"".to_string()));
    }

    #[tokio::test]
    async fn test_invoke_activity() {
        let mut reg = Registry::new();
        reg.add_named_activity("act", dummy_activity);

        let f = reg.get_activity("act").unwrap();
        let ctx = ActivityContext::new("test".to_string(), 0, String::new());
        let result = (f)(ctx, None).await;
        assert_eq!(result.unwrap(), Some("\"result\"".to_string()));
    }

    // ─── Versioned orchestrator tests ──────────────────────────────────────

    #[test]
    fn test_versioned_exact_match() {
        let mut reg = Registry::new();
        reg.add_versioned_orchestrator("orch", "v1", |_| async move { Ok(Some("v1".to_string())) });
        reg.add_versioned_orchestrator("orch", "v2", |_| async move { Ok(Some("v2".to_string())) });

        // Exact version match should resolve to the registered function.
        assert!(reg.get_orchestrator_version("orch", Some("v1")).is_some());
        assert!(reg.get_orchestrator_version("orch", Some("v2")).is_some());
        assert!(reg.get_orchestrator_version("orch", Some("v3")).is_none());
    }

    #[test]
    fn test_latest_is_fallback() {
        let mut reg = Registry::new();
        reg.add_versioned_orchestrator("orch", "v1", |_| async move { Ok(Some("v1".to_string())) });
        reg.add_latest_orchestrator("orch", "v2", |_| async move { Ok(Some("v2".to_string())) });

        // Unknown version should fall back to the latest-flagged entry.
        assert!(reg.get_orchestrator_version("orch", Some("v99")).is_some());
        // No version should also resolve to latest.
        assert!(reg.get_orchestrator("orch").is_some());
    }

    #[test]
    fn test_unversioned_fallback() {
        let mut reg = Registry::new();
        reg.add_named_orchestrator("orch", dummy_orchestrator);

        // Requesting a specific version when only unversioned exists falls
        // back to the unversioned entry.
        assert!(reg.get_orchestrator_version("orch", Some("any")).is_some());
    }
}
