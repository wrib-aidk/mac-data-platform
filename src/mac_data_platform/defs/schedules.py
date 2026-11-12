"""config/pipelines.yaml 의 jobs → Dagster job + schedule."""

import dagster as dg

from mac_data_platform import settings


def _op_name(asset_key: str) -> str:
    return asset_key.replace("/", "__")


def _build() -> tuple[list, list[dg.ScheduleDefinition]]:
    cfg = settings.config()
    named = cfg.get("schedules", {})
    jobs, schedules = [], []
    for name, job_cfg in (cfg.get("jobs") or {}).items():
        selection = dg.AssetSelection.assets(*(dg.AssetKey(k.split("/")) for k in job_cfg["assets"] if not k.endswith("/*")))
        for k in job_cfg["assets"]:
            if k.endswith("/*"):  # "db/*" → 접두어가 db 인 asset 전부
                selection |= dg.AssetSelection.key_prefixes(k[:-2].split("/"))
        run_config = {"ops": {_op_name(k): {"config": v} for k, v in (job_cfg.get("config") or {}).items()}}
        job = dg.define_asset_job(
            name=name,
            selection=selection,
            description=job_cfg.get("description"),
            config=run_config if run_config["ops"] else None,
            tags={"stage": "collect"},
        )
        jobs.append(job)
        if schedule := job_cfg.get("schedule"):
            schedules.append(
                dg.ScheduleDefinition(
                    name=f"{name}_schedule",
                    job=job,
                    cron_schedule=named.get(schedule, schedule),
                    description=f"{name} 수집 주기: {schedule} ({named.get(schedule, schedule)}) — config/pipelines.yaml 에서 변경"
                    + ("" if job_cfg.get("enabled", True) else " / 꺼짐(enabled: false)"),
                    execution_timezone=cfg.get("timezone", "Asia/Seoul"),
                    default_status=dg.DefaultScheduleStatus.RUNNING if job_cfg.get("enabled", True) else dg.DefaultScheduleStatus.STOPPED,
                ),
            )
    return jobs, schedules


_jobs, _schedules = _build()
defs = dg.Definitions(jobs=_jobs, schedules=_schedules)
