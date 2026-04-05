use crate::proto;
use crate::task::ActivityContext;

use super::registry::ActivityFn;

pub(crate) struct ActivityExecutor;

impl ActivityExecutor {
    /// Execute an activity function and return the proto response.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute(
        activity_fn: &ActivityFn,
        name: &str,
        instance_id: &str,
        task_id: i32,
        task_execution_id: String,
        encoded_input: Option<String>,
        parent_trace_context: Option<&proto::TraceContext>,
        completion_token: String,
    ) -> proto::ActivityResponse {
        let ctx = ActivityContext::new(instance_id.to_string(), task_id, task_execution_id);

        tracing::info!(
            instance_id = %instance_id,
            activity = %name,
            task_id = task_id,
            "Executing activity"
        );

        // Create an OTel activity span
        #[cfg(feature = "opentelemetry")]
        let otel_ctx = {
            let parent_ctx =
                crate::internal::otel::context_from_trace_context(parent_trace_context);
            crate::internal::otel::start_activity_span(&parent_ctx, name, instance_id, task_id)
        };
        #[cfg(not(feature = "opentelemetry"))]
        let _ = parent_trace_context;

        let response = match (activity_fn)(ctx, encoded_input).await {
            Ok(result) => {
                tracing::info!(
                    instance_id = %instance_id,
                    activity = %name,
                    task_id = task_id,
                    "Activity completed successfully"
                );
                proto::ActivityResponse {
                    instance_id: instance_id.to_string(),
                    task_id,
                    result,
                    failure_details: None,
                    completion_token,
                }
            }
            Err(e) => {
                tracing::warn!(
                    instance_id = %instance_id,
                    activity = %name,
                    task_id = task_id,
                    error = %e,
                    "Activity failed"
                );
                #[cfg(feature = "opentelemetry")]
                crate::internal::otel::set_span_error(&otel_ctx, &e.to_string());
                proto::ActivityResponse {
                    instance_id: instance_id.to_string(),
                    task_id,
                    result: None,
                    failure_details: Some(proto::TaskFailureDetails {
                        error_type: "ActivityError".to_string(),
                        error_message: e.to_string(),
                        stack_trace: None,
                        inner_failure: None,
                        is_non_retriable: false,
                    }),
                    completion_token,
                }
            }
        };

        #[cfg(feature = "opentelemetry")]
        crate::internal::otel::end_span(&otel_ctx);

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    #[tokio::test]
    async fn test_activity_success() {
        let f: ActivityFn = Arc::new(|_ctx, input| Box::pin(async move { Ok(input) }));

        let resp = ActivityExecutor::execute(
            &f,
            "greet",
            "inst-1",
            42,
            String::new(),
            Some("\"hello\"".to_string()),
            None,
            "token".to_string(),
        )
        .await;

        assert_eq!(resp.instance_id, "inst-1");
        assert_eq!(resp.task_id, 42);
        assert_eq!(resp.result, Some("\"hello\"".to_string()));
        assert!(resp.failure_details.is_none());
        assert_eq!(resp.completion_token, "token");
    }

    #[tokio::test]
    async fn test_activity_failure() {
        let f: ActivityFn = Arc::new(|_ctx, _input| {
            Box::pin(async move { Err(crate::api::DurableTaskError::Other("boom".to_string())) })
        });

        let resp = ActivityExecutor::execute(
            &f,
            "greet",
            "inst-1",
            42,
            String::new(),
            None,
            None,
            String::new(),
        )
        .await;

        assert!(resp.failure_details.is_some());
        let fd = resp.failure_details.unwrap();
        assert_eq!(fd.error_type, "ActivityError");
        assert!(fd.error_message.contains("boom"));
    }

    #[tokio::test]
    async fn test_activity_context_task_execution_id() {
        let f: ActivityFn = Arc::new(|ctx, _input| {
            let exec_id = ctx.task_execution_id().to_string();
            Box::pin(async move { Ok(Some(exec_id)) })
        });

        let resp = ActivityExecutor::execute(
            &f,
            "greet",
            "inst-1",
            42,
            "exec-xyz".to_string(),
            None,
            None,
            String::new(),
        )
        .await;

        assert_eq!(resp.result, Some("exec-xyz".to_string()));
    }
}
