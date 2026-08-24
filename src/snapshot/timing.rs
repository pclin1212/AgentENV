use std::future::Future;
use std::time::Instant;

use super::SnapshotId;

/// Times one bounded snapshot-publish stage and emits both a structured log
/// and a Prometheus histogram sample.
pub(crate) async fn time_publish_stage<F, T, E>(
    snapshot_id: &SnapshotId,
    component: &'static str,
    stage: &'static str,
    future: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let start = Instant::now();
    let result = future.await;
    let success = result.is_ok();
    let status = if success { "success" } else { "error" };
    let elapsed = start.elapsed();

    metrics::histogram!(
        "agentenv_snapshot_publish_stage_duration_seconds",
        "component" => component,
        "stage" => stage,
        "status" => status,
    )
    .record(elapsed.as_secs_f64());
    tracing::info!(
        %snapshot_id,
        operation = "publish",
        component,
        stage,
        elapsed_ms = elapsed.as_millis() as u64,
        success,
        "snapshot publish stage elapsed"
    );

    result
}
