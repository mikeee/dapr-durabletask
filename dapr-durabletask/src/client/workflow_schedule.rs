use chrono::{DateTime, Utc};

use crate::api::WorkflowSchedule;
use crate::internal;
use crate::proto;

/// Options for [`TaskHubGrpcClient::create_workflow_schedule`](super::TaskHubGrpcClient::create_workflow_schedule).
///
/// # Examples
///
/// ```rust
/// use dapr_durabletask::client::CreateWorkflowScheduleOptions;
///
/// let opts = CreateWorkflowScheduleOptions::new()
///     .with_input(r#"{"region":"eu"}"#)
///     .with_repeats(10)
///     .with_overwrite();
/// assert!(opts.overwrite);
/// ```
#[derive(Debug, Clone, Default)]
pub struct CreateWorkflowScheduleOptions {
    /// JSON-serialized input given to every run.
    pub input: Option<String>,
    /// Time of the first run. Otherwise the first run is at the first time
    /// the schedule matches after it is created.
    pub due_time: Option<DateTime<Utc>>,
    /// Time after which no run starts. The schedule completes once its next
    /// run would be after it.
    pub expiration_time: Option<DateTime<Utc>>,
    /// Number of runs after which the schedule completes. Must be at least 1.
    pub repeats: Option<u32>,
    /// App ID hosting the workflow each run starts. Defaults to the app that
    /// owns the schedule.
    pub target_app_id: Option<String>,
    /// Namespace of [`Self::target_app_id`]. Requires `target_app_id`.
    pub target_app_namespace: Option<String>,
    /// Replace an existing schedule of the same name. Runs it already started
    /// are not affected. Without it, creating a schedule whose name is taken
    /// fails with gRPC status `ALREADY_EXISTS`.
    pub overwrite: bool,
    /// Start one run for the times the schedule missed while the app was down,
    /// then carry on from the next time after now. Without it the schedule
    /// catches up: every missed time starts its own run, one after another, as
    /// with Dapr jobs and actor reminders.
    pub skip_missed: bool,
}

impl CreateWorkflowScheduleOptions {
    /// Create options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON-serialized input given to every run.
    pub fn with_input(mut self, input: impl Into<String>) -> Self {
        self.input = Some(input.into());
        self
    }

    /// Set the time of the first run.
    pub fn with_due_time(mut self, due_time: DateTime<Utc>) -> Self {
        self.due_time = Some(due_time);
        self
    }

    /// Set the time after which no run starts.
    pub fn with_expiration_time(mut self, expiration_time: DateTime<Utc>) -> Self {
        self.expiration_time = Some(expiration_time);
        self
    }

    /// Set the number of runs after which the schedule completes.
    pub fn with_repeats(mut self, repeats: u32) -> Self {
        self.repeats = Some(repeats);
        self
    }

    /// Start each run on the app with the given app ID.
    pub fn with_target_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.target_app_id = Some(app_id.into());
        self
    }

    /// Set the namespace of the target app ID.
    pub fn with_target_app_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.target_app_namespace = Some(namespace.into());
        self
    }

    /// Replace an existing schedule of the same name. See [`Self::overwrite`].
    pub fn with_overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    /// Start one run for missed times instead of catching up. See
    /// [`Self::skip_missed`].
    pub fn with_skip_missed(mut self) -> Self {
        self.skip_missed = true;
        self
    }

    pub(crate) fn to_request(
        &self,
        name: &str,
        workflow_name: &str,
        schedule: &str,
    ) -> proto::CreateWorkflowScheduleRequest {
        proto::CreateWorkflowScheduleRequest {
            schedule: Some(proto::WorkflowSchedule {
                name: name.to_string(),
                workflow_name: workflow_name.to_string(),
                input: self.input.clone(),
                schedule: schedule.to_string(),
                due_time: self.due_time.map(internal::to_timestamp),
                expiration_time: self.expiration_time.map(internal::to_timestamp),
                repeats: self.repeats,
                target_app_id: self.target_app_id.clone(),
                target_app_namespace: self.target_app_namespace.clone(),
                status: None,
                skip_missed: self.skip_missed.then_some(true),
            }),
            overwrite: self.overwrite,
        }
    }
}

/// Options for [`TaskHubGrpcClient::list_workflow_schedules`](super::TaskHubGrpcClient::list_workflow_schedules).
#[derive(Debug, Clone, Default)]
pub struct ListWorkflowSchedulesOptions {
    /// Maximum number of schedules per page. `None` asks the sidecar to
    /// return all schedules at once.
    pub page_size: Option<u32>,
    /// Continuation token from a previous page. `None` requests the first
    /// page.
    pub continuation_token: Option<String>,
}

impl ListWorkflowSchedulesOptions {
    /// Create options with default values (first page, no page size).
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

    pub(crate) fn to_request(&self) -> proto::ListWorkflowSchedulesRequest {
        proto::ListWorkflowSchedulesRequest {
            continuation_token: self.continuation_token.clone(),
            page_size: self.page_size,
        }
    }
}

/// One page of schedules returned by
/// [`TaskHubGrpcClient::list_workflow_schedules`](super::TaskHubGrpcClient::list_workflow_schedules).
#[derive(Debug, Clone, Default)]
pub struct WorkflowSchedulePage {
    /// The schedules on this page, with their status. A page can hold fewer
    /// schedules than the page size, even none, when more follow.
    pub schedules: Vec<WorkflowSchedule>,
    /// Token to pass to [`ListWorkflowSchedulesOptions::with_continuation_token`]
    /// to fetch the next page, or `None` if this is the last page.
    pub continuation_token: Option<String>,
}

impl From<proto::ListWorkflowSchedulesResponse> for WorkflowSchedulePage {
    fn from(resp: proto::ListWorkflowSchedulesResponse) -> Self {
        Self {
            schedules: resp.schedules.into_iter().map(Into::into).collect(),
            continuation_token: resp.continuation_token.filter(|t| !t.is_empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_request_carries_every_option() {
        let due = chrono::Utc::now();
        let expiry = due + chrono::Duration::hours(1);
        let req = CreateWorkflowScheduleOptions::new()
            .with_input("1")
            .with_due_time(due)
            .with_expiration_time(expiry)
            .with_repeats(3)
            .with_target_app_id("app2")
            .with_target_app_namespace("ns2")
            .with_overwrite()
            .with_skip_missed()
            .to_request("nightly", "report", "0 30 9 * * MON");
        assert!(req.overwrite);
        let ws = req.schedule.expect("schedule");
        assert_eq!(ws.name, "nightly");
        assert_eq!(ws.workflow_name, "report");
        assert_eq!(ws.schedule, "0 30 9 * * MON");
        assert_eq!(ws.input.as_deref(), Some("1"));
        assert_eq!(ws.due_time.unwrap().seconds, due.timestamp());
        assert_eq!(ws.expiration_time.unwrap().seconds, expiry.timestamp());
        assert_eq!(ws.repeats, Some(3));
        assert_eq!(ws.target_app_id.as_deref(), Some("app2"));
        assert_eq!(ws.target_app_namespace.as_deref(), Some("ns2"));
        assert_eq!(ws.skip_missed, Some(true));
        assert!(ws.status.is_none());

        let req = CreateWorkflowScheduleOptions::new().to_request("n", "w", "@daily");
        assert!(!req.overwrite);
        let ws = req.schedule.expect("schedule");
        assert!(ws.input.is_none() && ws.due_time.is_none() && ws.expiration_time.is_none());
        assert!(ws.repeats.is_none() && ws.target_app_id.is_none());
        assert!(ws.target_app_namespace.is_none());
        // Unset, so the runtime default (catch up on every missed time) applies.
        assert!(ws.skip_missed.is_none());
    }

    #[test]
    fn list_request_and_page() {
        let req = ListWorkflowSchedulesOptions::new().to_request();
        assert!(req.page_size.is_none() && req.continuation_token.is_none());
        let req = ListWorkflowSchedulesOptions::new()
            .with_page_size(2)
            .with_continuation_token("t")
            .to_request();
        assert_eq!(req.page_size, Some(2));
        assert_eq!(req.continuation_token.as_deref(), Some("t"));

        let page = WorkflowSchedulePage::from(proto::ListWorkflowSchedulesResponse {
            schedules: vec![proto::WorkflowSchedule {
                name: "a".into(),
                ..Default::default()
            }],
            continuation_token: Some("next".into()),
        });
        assert_eq!(page.schedules.len(), 1);
        assert_eq!(page.schedules[0].name, "a");
        assert_eq!(page.continuation_token.as_deref(), Some("next"));

        // An empty token means the last page.
        let page = WorkflowSchedulePage::from(proto::ListWorkflowSchedulesResponse {
            schedules: vec![],
            continuation_token: Some(String::new()),
        });
        assert!(page.continuation_token.is_none());
    }
}
