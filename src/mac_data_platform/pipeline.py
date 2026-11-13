"""수집 asset 공통: 재시도 정책, 실패 기록(logs/errors.log), 결과 메타데이터."""

import os
import textwrap
from datetime import date, datetime, timedelta
from pathlib import Path

import dagster as dg

from . import settings
from .collectors.common import ItemResult

_r = settings.retry_settings()
COLLECT_RETRY = dg.RetryPolicy(max_retries=_r["max_retries"], delay=_r["delay_seconds"], backoff=dg.Backoff.EXPONENTIAL, jitter=dg.Jitter.PLUS_MINUS)
ITEM_RETRIES = _r["item_retries"]


def _stage_metadata(site: str | None, output: str | None, metadata: dict | None) -> dict:
    metadata = dict(metadata or {})
    if site:
        metadata["출처"] = dg.MetadataValue.url(site)
    if output:
        metadata["저장 위치"] = dg.MetadataValue.text(output)
    return metadata


def collect_asset(source: str, name: str, description: str, *, site: str | None = None, output: str | None = None, **kwargs: object):  # noqa: ANN201
    """수집 단계 asset. key = <source>/<name>, group = collect_<source>.

    description: Markdown. Dagster UI 의 asset 상세 화면에 그대로 보인다 (들여쓰기는 자동 제거).
    site / output: UI 메타데이터 표에 "출처" 링크와 "저장 위치" 로 표시된다.
    """
    return dg.asset(
        key=dg.AssetKey([source, name]),
        group_name=f"collect_{source}",
        description=textwrap.dedent(description).strip(),
        metadata=_stage_metadata(site, output, kwargs.pop("metadata", None)),
        retry_policy=kwargs.pop("retry_policy", COLLECT_RETRY),
        tags={"stage": "collect", "source": source},
        kinds={"python"},
        **kwargs,
    )


def extract_asset(source: str, name: str, description: str, *, output: str | None = None, **kwargs: object):  # noqa: ANN201
    """추출 단계 asset. key = <source>/<name>, group = extract_<source>. 입력은 data/raw, 결과는 data/extracted."""
    return dg.asset(
        key=dg.AssetKey([source, name]),
        group_name=f"extract_{source}",
        description=textwrap.dedent(description).strip(),
        metadata=_stage_metadata(None, output, kwargs.pop("metadata", None)),
        retry_policy=kwargs.pop("retry_policy", COLLECT_RETRY),
        tags={"stage": "extract", "source": source},
        kinds=kwargs.pop("kinds", {"python"}),
        **kwargs,
    )


def db_asset(schema: str, table: str, source: str, description: str, **kwargs: object):  # noqa: ANN201
    """적재 단계 asset = PostgreSQL 테이블 1개. key = db/<schema>/<table>, group = db_<source>.

    다른 asset(추출 파이프라인 등)은 deps=[dg.AssetKey(["db", "raw", "seibro_fund_nav"])] 로 이 테이블에 연결하고,
    함수 인자 `postgres: PostgresResource` 로 읽고 쓰면 된다. 결과는 table_result() 로 돌려주면 UI 에 행 수·컬럼·미리보기가 보인다.
    """
    return dg.asset(
        key=dg.AssetKey(["db", schema, table]),
        group_name=f"db_{source}",
        description=textwrap.dedent(description).strip(),
        metadata={"테이블": dg.MetadataValue.text(f"{schema}.{table}"), **kwargs.pop("metadata", {})},
        retry_policy=kwargs.pop("retry_policy", COLLECT_RETRY),
        tags={"stage": "load" if schema == "raw" else "extract", "source": source},
        kinds={"postgres"},
        **kwargs,
    )


def table_result(conn, table: str, stats: dict | None = None, order_by: str | None = None, **extra: object) -> dg.MaterializeResult:  # noqa: ANN001
    """테이블 상태를 UI 메타데이터로: 행 수, 컬럼(설명·예시값), 앞 5행 미리보기 + 적재 통계."""
    from . import db

    cols = db.table_columns(conn, table)
    comments = db.column_comments(conn, table)
    examples = db.column_examples(conn, table, [c for c, _ in cols])

    def describe(c: str) -> str | None:
        parts = [comments.get(c), f"예: {examples[c]}" if c in examples else None]
        return " · ".join(p for p in parts if p) or None

    return dg.MaterializeResult(
        metadata={
            **(stats or {}),
            "dagster/row_count": db.table_count(conn, table),
            "dagster/column_schema": dg.TableSchema(columns=[dg.TableColumn(name=c, type=t, description=describe(c)) for c, t in cols]),
            "dagster/table_name": table,
            "preview": md_table(db.table_head(conn, table, order_by=order_by), max_cell=30),
            **extra,
        },
    )


# ---------------------------------------------------------------------------
# UI 미리보기: MaterializeResult metadata 에 넣으면 asset 화면에서 결과 예시를 볼 수 있음
# ---------------------------------------------------------------------------
def md_table(rows: list[dict], max_cell: int = 40) -> dg.MetadataValue:
    """dict 목록 → Markdown 표."""
    if not rows:
        return dg.MetadataValue.md("_(비어 있음)_")
    cols = list(rows[0])

    def cell(v: object) -> str:
        t = "" if v is None else str(v).replace("|", "\\|").replace("\n", " ")
        return t if len(t) <= max_cell else t[: max_cell - 1] + "…"

    lines = ["| " + " | ".join(cols) + " |", "|" + " --- |" * len(cols)]
    lines += ["| " + " | ".join(cell(r.get(c)) for c in cols) + " |" for r in rows]
    return dg.MetadataValue.md("\n".join(lines))


def csv_preview(path: Path, n: int = 5, columns: list[str] | None = None) -> dg.MetadataValue:
    """CSV 앞 n행 미리보기."""
    import csv
    import itertools

    if not path.exists():
        return dg.MetadataValue.md("_(파일 없음)_")
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(itertools.islice(csv.DictReader(f), n))
    if columns:
        rows = [{c: r.get(c) for c in columns} for r in rows]
    return md_table(rows)


def text_preview(path: Path, chars: int = 1500) -> dg.MetadataValue:
    """텍스트 파일 앞부분 미리보기."""
    if not path.exists():
        return dg.MetadataValue.md("_(파일 없음)_")
    text = path.read_text(encoding="utf-8", errors="replace")[:chars]
    return dg.MetadataValue.md(f"`{path.parent.name}/{path.name}`\n\n```text\n{text}\n```")


def write_error_log(kind: str, **fields: object) -> None:
    """logs/errors.log 에 한 줄 기록: 시각 [종류] key=value ..."""
    settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(settings.tz()).strftime("%Y-%m-%d %H:%M:%S")
    body = " ".join(f"{k}={str(v).replace(chr(10), ' ')}" for k, v in fields.items())
    with settings.ERROR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{ts} [{kind}] {body}\n")


def require_env(*names: str) -> list[str]:
    """인증정보 확인. 없으면 재시도 없이 바로 실패."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise dg.Failure(f"환경변수 {missing} 가 없습니다. 프로젝트 루트의 .env 에 넣어주세요 (.env.example 참고).", allow_retries=False)
    return [os.environ[n] for n in names]


def source_dir(source: str) -> Path:
    """수집 결과 폴더: data/raw/<source>/ (항상 최신본 유지, 내용이 바뀌면 덮어씀)"""
    return settings.RAW_DIR / source


def date_range(start: str, end: str, lookback_days: int) -> tuple[date, date]:
    """start/end 가 비어 있으면 end=어제, start=end-lookback_days."""
    end_d = date.fromisoformat(end) if end else settings.today() - timedelta(days=1)
    start_d = date.fromisoformat(start) if start else end_d - timedelta(days=lookback_days)
    return start_d, end_d


def item_result(context: dg.AssetExecutionContext, result: ItemResult, out: Path, **extra: object) -> dg.MaterializeResult:
    """항목별 수집 결과를 기록. 재시도해도 실패한 항목은 errors.log 에 남기고, 전부 실패했으면 asset 실패(→ 재실행)."""
    asset = context.asset_key.to_user_string()
    for item, error in result.failed.items():
        write_error_log("ITEM_FAILED", asset=asset, run_id=context.run_id[:8], item=item, error=error)
    summary = result.summary()
    context.log.info(f"{asset}: {summary}")
    if result.total and result.failed and not (result.ok or result.skipped or result.empty):
        raise dg.Failure(f"{asset}: {len(result.failed)}건 전부 실패", metadata=summary)
    return dg.MaterializeResult(metadata={**summary, **result.counters, "path": dg.MetadataValue.path(str(out)), **extra})
