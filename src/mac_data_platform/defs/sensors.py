"""재실행까지 실패한 run 을 logs/errors.log 에 기록."""

import dagster as dg

from mac_data_platform.pipeline import write_error_log


@dg.run_failure_sensor(name="log_failed_runs", default_status=dg.DefaultSensorStatus.RUNNING, minimum_interval_seconds=30, monitor_all_code_locations=True)
def log_failed_runs(context: dg.RunFailureSensorContext) -> None:
    run = context.dagster_run
    steps = context.get_step_failure_events()
    if not steps:
        write_error_log("RUN_FAILED", job=run.job_name, run_id=run.run_id[:8], error=context.failure_event.message)
    for step in steps:
        err = step.event_specific_data.error if step.event_specific_data else None
        write_error_log("RUN_FAILED", job=run.job_name, run_id=run.run_id[:8], step=step.step_key, error=err.message.strip() if err else step.message)
