//! Performance / throughput tests for the durabletask worker.
//!
//! Gated behind `#[ignore]` — never run in normal CI.  Run explicitly:
//!
//! ```sh
//! cargo nextest run --workspace --all-features -E 'test(perf_)' --run-ignored all
//! # or with plain cargo test:
//! cargo test --all-features --test perf -- --ignored
//! ```
//!
//! Nextest serialisation is configured in `.config/nextest.toml` (test-group
//! `perf`, max-threads = 1).
//!
//! ## Tuning
//!
//! All counts and durations are overridable via environment variables so you
//! can run a quick smoke-check or an overnight soak without recompiling:
//!
//! | Variable              | Affects                                      | Default |
//! |-----------------------|----------------------------------------------|---------|
//! | `PERF_COUNT`          | batch tests (empty, single-activity)         | varies  |
//! | `PERF_ORCH_COUNT`     | batch fan-out / chain tests                  | 100     |
//! | `PERF_FAN_WIDTH`      | fan-out parallelism                          | 50      |
//! | `PERF_CHAIN_LEN`      | sequential chain length                      | 10      |
//! | `PERF_DURATION_SECS`  | sustained-load test duration                 | 30/60   |

mod harness;

use std::time::{Duration, Instant};

use dapr_durabletask::api::OrchestrationStatus;
use dapr_durabletask::client::TaskHubGrpcClient;
use dapr_durabletask::task::{ActivityContext, when_all, when_any};
use dapr_durabletask::worker::TaskHubGrpcWorker;

use harness::{WorkerGuard, env_or};

const COMPLETION_TIMEOUT: Duration = Duration::from_secs(120);

// ── Batch runner ─────────────────────────────────────────────────────────────

/// Schedule `count` orchestrations, then wait for all to complete.
///
/// Latency is measured server-side (`created_at` → `last_updated_at`) to
/// reflect actual orchestration execution time rather than client-side
/// queueing artefacts.
async fn run_batch(
    client: &mut TaskHubGrpcClient,
    name: &str,
    input: Option<String>,
    count: usize,
) -> PerfResult {
    let mut ids = Vec::with_capacity(count);
    let wall_start = Instant::now();

    // Phase 1: schedule all orchestrations as fast as possible.
    for _ in 0..count {
        let id = client
            .schedule_new_orchestration(name, input.clone(), None, None)
            .await
            .unwrap();
        ids.push(id);
    }
    let schedule_elapsed = wall_start.elapsed();

    // Phase 2: wait for every orchestration to complete and collect
    // server-side execution durations.
    let mut latencies = Vec::with_capacity(count);
    for id in &ids {
        let state = client
            .wait_for_orchestration_completion(id, true, Some(COMPLETION_TIMEOUT))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("no state for {id}"));
        assert_eq!(
            state.runtime_status,
            OrchestrationStatus::Completed,
            "workflow {id} failed: {:?}",
            state.failure_details
        );
        if let (Some(created), Some(updated)) = (state.created_at, state.last_updated_at) {
            let dur = (updated - created).to_std().unwrap_or(Duration::ZERO);
            latencies.push(dur);
        }
    }

    PerfResult {
        executions: count,
        elapsed: wall_start.elapsed(),
        schedule_elapsed: Some(schedule_elapsed),
        latencies,
    }
}

// ── Sustained runner ─────────────────────────────────────────────────────────

/// Schedule orchestrations continuously for `duration`, draining completions
/// periodically to bound memory.  Returns stats for the full run including
/// the drain phase.
///
/// Latency is measured server-side where possible.
async fn run_sustained(
    client: &mut TaskHubGrpcClient,
    name: &str,
    input: Option<String>,
    duration: Duration,
) -> PerfResult {
    let wall_start = Instant::now();
    let mut scheduled: usize = 0;
    let mut latencies: Vec<Duration> = Vec::new();
    let mut pending: Vec<String> = Vec::new();

    // Phase 1: schedule as fast as possible for the target duration.
    while wall_start.elapsed() < duration {
        let id = client
            .schedule_new_orchestration(name, input.clone(), None, None)
            .await
            .unwrap();
        pending.push(id);
        scheduled += 1;

        // Every 64 schedules, sweep for completions to bound memory.
        if scheduled % 64 == 0 {
            drain_completed(client, &mut pending, &mut latencies).await;
        }
    }

    // Phase 2: drain all remaining in-flight orchestrations.
    for id in pending {
        let s = client
            .wait_for_orchestration_completion(&id, true, Some(COMPLETION_TIMEOUT))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("no state for {id}"));
        assert_eq!(
            s.runtime_status,
            OrchestrationStatus::Completed,
            "workflow {id} failed: {:?}",
            s.failure_details
        );
        if let (Some(created), Some(updated)) = (s.created_at, s.last_updated_at) {
            let dur = (updated - created).to_std().unwrap_or(Duration::ZERO);
            latencies.push(dur);
        }
    }

    PerfResult {
        executions: scheduled,
        elapsed: wall_start.elapsed(),
        schedule_elapsed: None,
        latencies,
    }
}

/// Sweep `pending` for completed orchestrations; collect server-side
/// latencies and retain only still-running IDs.
async fn drain_completed(
    client: &mut TaskHubGrpcClient,
    pending: &mut Vec<String>,
    latencies: &mut Vec<Duration>,
) {
    let mut still_pending = Vec::with_capacity(pending.len());
    for id in pending.drain(..) {
        match client.get_orchestration_state(&id, true).await.unwrap() {
            Some(s) if s.runtime_status == OrchestrationStatus::Completed => {
                if let (Some(created), Some(updated)) = (s.created_at, s.last_updated_at) {
                    let dur = (updated - created).to_std().unwrap_or(Duration::ZERO);
                    latencies.push(dur);
                }
            }
            Some(s) if s.runtime_status == OrchestrationStatus::Failed => {
                panic!("workflow {id} failed: {:?}", s.failure_details);
            }
            _ => still_pending.push(id),
        }
    }
    *pending = still_pending;
}

// ── Result + reporting ───────────────────────────────────────────────────────

struct PerfResult {
    executions: usize,
    /// Wall-clock time from first schedule to last completion drained.
    elapsed: Duration,
    /// Time spent in the scheduling phase only (batch tests).
    schedule_elapsed: Option<Duration>,
    /// Server-side execution durations (`created_at` → `last_updated_at`).
    latencies: Vec<Duration>,
}

impl PerfResult {
    fn throughput(&self) -> f64 {
        self.executions as f64 / self.elapsed.as_secs_f64()
    }

    fn percentile(&self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.latencies.clone();
        sorted.sort();
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn print(&self, label: &str) {
        let secs = self.elapsed.as_secs_f64();
        let p50 = self.percentile(50.0);
        let p95 = self.percentile(95.0);
        let p99 = self.percentile(99.0);
        eprintln!();
        eprintln!("┌─────────────────────────────────────────────────────────┐");
        eprintln!("│ {:<55} │", label);
        eprintln!("├─────────────────────────────────────────────────────────┤");
        eprintln!(
            "│  Executions:  {:<8}                                 │",
            self.executions
        );
        if let Some(sched) = self.schedule_elapsed {
            eprintln!(
                "│  Schedule:    {:>8.2}s ({:>6.0} sched/s)               │",
                sched.as_secs_f64(),
                self.executions as f64 / sched.as_secs_f64()
            );
        }
        eprintln!(
            "│  Wall time:   {:>8.2}s                                 │",
            secs
        );
        eprintln!(
            "│  Throughput:  {:>8.1} workflows/s                     │",
            self.throughput()
        );
        eprintln!("├─────────────────────────────────────────────────────────┤");
        eprintln!(
            "│  Latency p50: {:>8.1}ms  (server-side)                │",
            p50.as_secs_f64() * 1000.0
        );
        eprintln!(
            "│  Latency p95: {:>8.1}ms                               │",
            p95.as_secs_f64() * 1000.0
        );
        eprintln!(
            "│  Latency p99: {:>8.1}ms                               │",
            p99.as_secs_f64() * 1000.0
        );
        eprintln!("└─────────────────────────────────────────────────────────┘");
        eprintln!();
    }

    fn print_with_activities(&self, label: &str, total_activities: usize) {
        let secs = self.elapsed.as_secs_f64();
        let p50 = self.percentile(50.0);
        let p95 = self.percentile(95.0);
        let p99 = self.percentile(99.0);
        eprintln!();
        eprintln!("┌─────────────────────────────────────────────────────────┐");
        eprintln!("│ {:<55} │", label);
        eprintln!("├─────────────────────────────────────────────────────────┤");
        eprintln!(
            "│  Orchestrations: {:<8}                              │",
            self.executions
        );
        eprintln!(
            "│  Activities:     {:<8}                              │",
            total_activities
        );
        if let Some(sched) = self.schedule_elapsed {
            eprintln!(
                "│  Schedule:       {:>8.2}s ({:>6.0} sched/s)            │",
                sched.as_secs_f64(),
                self.executions as f64 / sched.as_secs_f64()
            );
        }
        eprintln!(
            "│  Wall time:      {:>8.2}s                              │",
            secs
        );
        eprintln!(
            "│  Orch/s:         {:>8.1}                              │",
            self.throughput()
        );
        eprintln!(
            "│  Act/s:          {:>8.1}                              │",
            total_activities as f64 / secs
        );
        eprintln!("├─────────────────────────────────────────────────────────┤");
        eprintln!(
            "│  Latency p50:    {:>8.1}ms  (server-side)              │",
            p50.as_secs_f64() * 1000.0
        );
        eprintln!(
            "│  Latency p95:    {:>8.1}ms                             │",
            p95.as_secs_f64() * 1000.0
        );
        eprintln!(
            "│  Latency p99:    {:>8.1}ms                             │",
            p99.as_secs_f64() * 1000.0
        );
        eprintln!("└─────────────────────────────────────────────────────────┘");
        eprintln!();
    }
}

// ── Worker builder helpers ───────────────────────────────────────────────────

fn worker_empty(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_empty", |_ctx| async move { Ok(None) });
    (w, "perf_empty")
}

fn worker_single_activity(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_single", |ctx| async move {
            ctx.call_activity("noop", ()).await?;
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "noop",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_single")
}

fn worker_fanout(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_fanout", |ctx| async move {
            let width: i32 = ctx.get_input()?;
            let tasks: Vec<_> = (0..width)
                .map(|_| ctx.call_activity("noop_fan", ()))
                .collect();
            when_all(tasks).await?;
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "noop_fan",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_fanout")
}

fn worker_chain(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_chain", |ctx| async move {
            let steps: i32 = ctx.get_input()?;
            for i in 0..steps {
                ctx.call_activity("chain_step", i).await?;
            }
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "chain_step",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_chain")
}

fn worker_mixed(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_mixed", |ctx| async move {
            // Sequential phase: 3 activities in series.
            for i in 0..3_i32 {
                ctx.call_activity("step_mx", i).await?;
            }
            // Parallel phase: fan-out to 5 activities.
            let tasks: Vec<_> = (0..5).map(|_| ctx.call_activity("step_mx", ())).collect();
            when_all(tasks).await?;
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "step_mx",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_mixed")
}

fn worker_sub_orchestration(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_child", |ctx| async move {
            ctx.call_activity("child_work", ()).await?;
            Ok(None)
        });
    w.registry_mut()
        .add_named_orchestrator("perf_parent", |ctx| async move {
            let count: i32 = ctx.get_input()?;
            let tasks: Vec<_> = (0..count)
                .map(|_| ctx.call_sub_orchestrator("perf_child", (), None))
                .collect();
            when_all(tasks).await?;
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "child_work",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_parent")
}

fn worker_continue_as_new(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_continue", |ctx| async move {
            let iteration: i32 = ctx.get_input()?;
            ctx.call_activity("iter_work", iteration).await?;
            let target: i32 = 10;
            if iteration < target {
                ctx.continue_as_new(iteration + 1, false);
            }
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "iter_work",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_continue")
}

fn worker_when_any_race(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_race", |ctx| async move {
            let width: i32 = ctx.get_input()?;
            let tasks: Vec<_> = (0..width)
                .map(|i| ctx.call_activity("race_work", i))
                .collect();
            // Complete as soon as the first activity finishes.
            when_any(tasks).await?;
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "race_work",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_race")
}

fn worker_deep_chain(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_deep_chain", |ctx| async move {
            let steps: i32 = ctx.get_input()?;
            let mut acc = 0_i64;
            for i in 0..steps {
                let result = ctx.call_activity("deep_step", i).await?;
                let val: i64 = serde_json::from_str(result.as_deref().unwrap_or("0")).unwrap_or(0);
                acc += val;
            }
            Ok(Some(serde_json::to_string(&acc).unwrap()))
        });
    w.registry_mut().add_named_activity(
        "deep_step",
        |_ctx: ActivityContext, input: Option<String>| async move {
            let i: i64 = serde_json::from_str(input.as_deref().unwrap_or("0")).unwrap_or(0);
            Ok(Some(serde_json::to_string(&(i + 1)).unwrap()))
        },
    );
    (w, "perf_deep_chain")
}

fn worker_wide_fanout(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_wide_fan", |ctx| async move {
            let width: i32 = ctx.get_input()?;
            let tasks: Vec<_> = (0..width)
                .map(|i| ctx.call_activity("wide_work", i))
                .collect();
            when_all(tasks).await?;
            Ok(None)
        });
    w.registry_mut().add_named_activity(
        "wide_work",
        |_ctx: ActivityContext, _input: Option<String>| async move { Ok(None) },
    );
    (w, "perf_wide_fan")
}

fn worker_payload(env: &harness::TestEnv) -> (TaskHubGrpcWorker, &str) {
    let mut w = env.new_worker();
    w.registry_mut()
        .add_named_orchestrator("perf_payload", |ctx| async move {
            let input: String = ctx.get_input()?;
            let result = ctx.call_activity("echo", &input).await?;
            Ok(result)
        });
    w.registry_mut().add_named_activity(
        "echo",
        |_ctx: ActivityContext, input: Option<String>| async move { Ok(input) },
    );
    (w, "perf_payload")
}

// ===========================================================================
// Batch tests — schedule N workflows, wait for all, report.
// ===========================================================================

/// Batch: 1 000 no-op orchestrations (no activities).
///
/// Measures raw scheduling → dispatch → completion throughput.
/// Override count with `PERF_COUNT`.
#[tokio::test]
#[ignore]
async fn perf_1000_empty_orchestrations() {
    setup!(env);
    let count = env_or("PERF_COUNT", 1_000);
    let (worker, name) = worker_empty(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let result = run_batch(&mut client, name, None, count).await;
    result.print("Batch — empty orchestrations (no activities)");

    guard.stop().await;
}

/// Batch: 500 orchestrations × 1 activity each.
///
/// Exercises the full orchestration → activity → completion round-trip.
/// Override count with `PERF_COUNT`.
#[tokio::test]
#[ignore]
async fn perf_500_single_activity_orchestrations() {
    setup!(env);
    let count = env_or("PERF_COUNT", 500);
    let (worker, name) = worker_single_activity(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let result = run_batch(&mut client, name, None, count).await;
    result.print("Batch — 1 activity per orchestration");

    guard.stop().await;
}

/// Batch: 100 orchestrations × 50 parallel activities (5 000 total), fan-in
/// via `when_all`.
///
/// Stresses concurrent activity dispatch and the join path.
/// Override with `PERF_ORCH_COUNT` and `PERF_FAN_WIDTH`.
#[tokio::test]
#[ignore]
async fn perf_100_orchestrations_fan_out_50_activities() {
    setup!(env);
    let orch_count = env_or("PERF_ORCH_COUNT", 100);
    let fan_width: i32 = env_or("PERF_FAN_WIDTH", 50);
    let (worker, name) = worker_fanout(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&fan_width).unwrap());
    let result = run_batch(&mut client, name, input, orch_count).await;
    result.print_with_activities("Batch — fan-out / fan-in", orch_count * fan_width as usize);

    guard.stop().await;
}

/// Batch: 100 orchestrations × 10 sequential activities (1 000 total).
///
/// Measures replay-loop overhead for chained activities.
/// Override with `PERF_ORCH_COUNT` and `PERF_CHAIN_LEN`.
#[tokio::test]
#[ignore]
async fn perf_100_orchestrations_chain_10_activities() {
    setup!(env);
    let orch_count = env_or("PERF_ORCH_COUNT", 100);
    let chain_len: i32 = env_or("PERF_CHAIN_LEN", 10);
    let (worker, name) = worker_chain(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&chain_len).unwrap());
    let result = run_batch(&mut client, name, input, orch_count).await;
    result.print_with_activities(
        "Batch — sequential activity chains",
        orch_count * chain_len as usize,
    );

    guard.stop().await;
}

// ===========================================================================
// Sustained load tests — schedule continuously for a fixed duration.
//
// Each workflow runs to completion (it does NOT stay alive waiting for
// external input).  The tests measure steady-state throughput when the
// pipeline is kept full.
// ===========================================================================

/// Sustained 30 s: fire-and-forget empty orchestrations.
///
/// Measures raw scheduling+completion overhead under continuous pressure.
/// Override with `PERF_DURATION_SECS`.
#[tokio::test]
#[ignore]
async fn perf_sustained_30s_empty_orchestrations() {
    setup!(env);
    let secs = env_or("PERF_DURATION_SECS", 30);
    let (worker, name) = worker_empty(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let result = run_sustained(&mut client, name, None, Duration::from_secs(secs)).await;
    result.print(&format!("Sustained {secs}s — empty orchestrations"));

    guard.stop().await;
}

/// Sustained 30 s: each orchestration calls one activity.
///
/// Full round-trip under continuous scheduling pressure.
/// Override with `PERF_DURATION_SECS`.
#[tokio::test]
#[ignore]
async fn perf_sustained_30s_single_activity_orchestrations() {
    setup!(env);
    let secs = env_or("PERF_DURATION_SECS", 30);
    let (worker, name) = worker_single_activity(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let result = run_sustained(&mut client, name, None, Duration::from_secs(secs)).await;
    result.print(&format!("Sustained {secs}s — 1 activity per orch"));

    guard.stop().await;
}

/// Sustained 30 s: each orchestration fans out to 10 parallel activities.
///
/// Concurrent dispatch + join under continuous load.
/// Override with `PERF_DURATION_SECS` and `PERF_FAN_WIDTH`.
#[tokio::test]
#[ignore]
async fn perf_sustained_30s_fan_out_10_activities() {
    setup!(env);
    let secs = env_or("PERF_DURATION_SECS", 30);
    let fan_width: i32 = env_or("PERF_FAN_WIDTH", 10);
    let (worker, name) = worker_fanout(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&fan_width).unwrap());
    let result = run_sustained(&mut client, name, input, Duration::from_secs(secs)).await;
    result.print(&format!(
        "Sustained {secs}s — fan-out {fan_width} activities"
    ));

    guard.stop().await;
}

/// Sustained 30 s: each orchestration chains 5 sequential activities.
///
/// Replay-loop overhead under continuous pressure.
/// Override with `PERF_DURATION_SECS` and `PERF_CHAIN_LEN`.
#[tokio::test]
#[ignore]
async fn perf_sustained_30s_chain_5_activities() {
    setup!(env);
    let secs = env_or("PERF_DURATION_SECS", 30);
    let chain_len: i32 = env_or("PERF_CHAIN_LEN", 5);
    let (worker, name) = worker_chain(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&chain_len).unwrap());
    let result = run_sustained(&mut client, name, input, Duration::from_secs(secs)).await;
    result.print(&format!(
        "Sustained {secs}s — chain of {chain_len} activities"
    ));

    guard.stop().await;
}

/// Sustained 60 s: mixed workload — 3 sequential + 5 parallel activities.
///
/// Realistic workflow shape under continuous load.
/// Override with `PERF_DURATION_SECS`.
#[tokio::test]
#[ignore]
async fn perf_sustained_60s_mixed_sequential_and_parallel() {
    setup!(env);
    let secs = env_or("PERF_DURATION_SECS", 60);
    let (worker, name) = worker_mixed(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let result = run_sustained(&mut client, name, None, Duration::from_secs(secs)).await;
    result.print(&format!("Sustained {secs}s — 3 sequential + 5 parallel"));

    guard.stop().await;
}

// ===========================================================================
// Additional workflow-pattern tests
// ===========================================================================

/// Batch: 50 parent orchestrations each spawning 5 sub-orchestrations that
/// each call 1 activity (250 child workflows, 250 activities total).
///
/// Exercises the sub-orchestration dispatch and join path.
/// Override with `PERF_ORCH_COUNT` and `PERF_FAN_WIDTH`.
#[tokio::test]
#[ignore]
async fn perf_50_orchestrations_with_5_sub_orchestrations() {
    setup!(env);
    let orch_count = env_or("PERF_ORCH_COUNT", 50);
    let children: i32 = env_or("PERF_FAN_WIDTH", 5);
    let (worker, name) = worker_sub_orchestration(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&children).unwrap());
    let result = run_batch(&mut client, name, input, orch_count).await;
    result.print_with_activities(
        "Batch — sub-orchestration fan-out",
        orch_count * children as usize,
    );

    guard.stop().await;
}

/// Batch: 200 orchestrations using `continue_as_new` to loop 10 times each,
/// calling 1 activity per iteration (2 000 total activities).
///
/// Measures the overhead of the continue-as-new replay mechanism.
/// Override with `PERF_COUNT`.
#[tokio::test]
#[ignore]
async fn perf_200_orchestrations_continue_as_new_10_iterations() {
    setup!(env);
    let count = env_or("PERF_COUNT", 200);
    let (worker, name) = worker_continue_as_new(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&1_i32).unwrap());
    let result = run_batch(&mut client, name, input, count).await;
    result.print_with_activities("Batch — continue-as-new (10 iterations)", count * 10);

    guard.stop().await;
}

/// Batch: 500 orchestrations using `when_any` to race 5 parallel activities,
/// completing as soon as the first finishes.
///
/// Exercises the when_any early-completion path under load.
/// Override with `PERF_COUNT` and `PERF_FAN_WIDTH`.
#[tokio::test]
#[ignore]
async fn perf_500_orchestrations_when_any_race_5_activities() {
    setup!(env);
    let count = env_or("PERF_COUNT", 500);
    let width: i32 = env_or("PERF_FAN_WIDTH", 5);
    let (worker, name) = worker_when_any_race(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&width).unwrap());
    let result = run_batch(&mut client, name, input, count).await;
    result.print("Batch — when_any race (first-activity-wins)");

    guard.stop().await;
}

/// Batch: 20 orchestrations each chaining 50 sequential activities with
/// input/output passing (1 000 total activities).
///
/// Stresses the replay loop with a deep history and per-step serialisation.
/// Override with `PERF_ORCH_COUNT` and `PERF_CHAIN_LEN`.
#[tokio::test]
#[ignore]
async fn perf_20_orchestrations_deep_chain_50_activities() {
    setup!(env);
    let orch_count = env_or("PERF_ORCH_COUNT", 20);
    let chain_len: i32 = env_or("PERF_CHAIN_LEN", 50);
    let (worker, name) = worker_deep_chain(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&chain_len).unwrap());
    let result = run_batch(&mut client, name, input, orch_count).await;
    result.print_with_activities(
        "Batch — deep chain (50 sequential w/ serde)",
        orch_count * chain_len as usize,
    );

    guard.stop().await;
}

/// Batch: 10 orchestrations each fanning out to 500 parallel activities
/// (5 000 total).
///
/// Tests extreme concurrency within a single orchestration.
/// Override with `PERF_ORCH_COUNT` and `PERF_FAN_WIDTH`.
#[tokio::test]
#[ignore]
async fn perf_10_orchestrations_wide_fan_out_500_activities() {
    setup!(env);
    let orch_count = env_or("PERF_ORCH_COUNT", 10);
    let width: i32 = env_or("PERF_FAN_WIDTH", 500);
    let (worker, name) = worker_wide_fanout(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&width).unwrap());
    let result = run_batch(&mut client, name, input, orch_count).await;
    result.print_with_activities(
        "Batch — extreme fan-out (500 activities/orch)",
        orch_count * width as usize,
    );

    guard.stop().await;
}

/// Batch: 200 orchestrations passing a 4 KB JSON payload through an echo
/// activity.
///
/// Measures serialisation/deserialisation overhead with non-trivial payloads.
/// Override with `PERF_COUNT`.
#[tokio::test]
#[ignore]
async fn perf_200_orchestrations_4kb_payload_echo() {
    setup!(env);
    let count = env_or("PERF_COUNT", 200);
    let payload = "x".repeat(4 * 1024);
    let (worker, name) = worker_payload(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&payload).unwrap());
    let result = run_batch(&mut client, name, input, count).await;
    result.print("Batch — 4 KB payload echo");

    guard.stop().await;
}

/// Sustained 30 s: sub-orchestration workload — each parent fans out to 3
/// child orchestrations that each call 1 activity.
///
/// Override with `PERF_DURATION_SECS` and `PERF_FAN_WIDTH`.
#[tokio::test]
#[ignore]
async fn perf_sustained_30s_sub_orchestrations() {
    setup!(env);
    let secs = env_or("PERF_DURATION_SECS", 30);
    let children: i32 = env_or("PERF_FAN_WIDTH", 3);
    let (worker, name) = worker_sub_orchestration(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&children).unwrap());
    let result = run_sustained(&mut client, name, input, Duration::from_secs(secs)).await;
    result.print(&format!(
        "Sustained {secs}s — {children} sub-orchestrations"
    ));

    guard.stop().await;
}

/// Sustained 30 s: continue-as-new loop — each orchestration iterates 10
/// times with 1 activity per iteration.
///
/// Override with `PERF_DURATION_SECS`.
#[tokio::test]
#[ignore]
async fn perf_sustained_30s_continue_as_new() {
    setup!(env);
    let secs = env_or("PERF_DURATION_SECS", 30);
    let (worker, name) = worker_continue_as_new(&env);
    let guard = WorkerGuard::start(worker);
    let mut client = env.new_client().await;

    let input = Some(serde_json::to_string(&1_i32).unwrap());
    let result = run_sustained(&mut client, name, input, Duration::from_secs(secs)).await;
    result.print(&format!(
        "Sustained {secs}s — continue-as-new (10 iterations)"
    ));

    guard.stop().await;
}
