"""UI 에서 SQL 조회: Jobs → sql_query → Launchpad 에 query 를 적고 Launch.

읽기 전용 트랜잭션이라 INSERT/UPDATE/DELETE/DDL 은 실패한다(데이터 변경 없음).
결과는 run 화면의 Output 이벤트에 표로, 전체 결과는 data/query/<run_id>.csv 로 저장.
"""

import csv
import datetime as dt
from decimal import Decimal

import dagster as dg

from mac_data_platform import settings
from mac_data_platform.defs.resources import PostgresResource


class SqlQueryConfig(dg.Config):
    query: str = "select table_schema, table_name from information_schema.tables where table_schema in ('raw','extracted','etl') order by 1, 2"
    limit: int = 100          # 화면에 보여줄 행 수 (CSV 는 최대 max_rows 행)
    max_rows: int = 100_000   # CSV 로 저장할 최대 행 수
    timeout_seconds: int = 300


def _cell(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (dt.date, dt.datetime, Decimal)):
        v = str(v)
    s = str(v).replace("|", "\\|").replace("\n", " ")
    return s if len(s) <= 80 else s[:77] + "..."


@dg.op(description="읽기 전용으로 SQL 을 실행하고 결과를 표/CSV 로 남김")
def sql_query(context: dg.OpExecutionContext, config: SqlQueryConfig, postgres: PostgresResource) -> dg.Output[None]:
    with postgres.connect() as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute(f"SET LOCAL statement_timeout = {int(config.timeout_seconds) * 1000}")
        with conn.cursor() as cur:
            cur.execute(config.query)
            if cur.description is None:
                raise dg.Failure("결과를 돌려주는 SELECT 문만 실행할 수 있습니다.")
            columns = [d.name for d in cur.description]
            rows = cur.fetchmany(config.max_rows + 1)
        conn.rollback()

    truncated = len(rows) > config.max_rows
    rows = rows[: config.max_rows]

    out_dir = settings.DATA_DIR / "query"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{context.run_id}.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(columns)
        w.writerows(rows)

    shown = rows[: config.limit]
    md = "| " + " | ".join(columns) + " |\n|" + "---|" * len(columns) + "\n"
    md += "".join("| " + " | ".join(_cell(v) for v in r) + " |\n" for r in shown)
    note = f"{len(rows):,}행" + (f" (max_rows {config.max_rows:,} 에서 잘림)" if truncated else "")
    if len(rows) > len(shown):
        note += f", 화면엔 앞 {len(shown)}행"
    context.log.info(f"{note} → {csv_path}")
    return dg.Output(
        None,
        metadata={
            "rows": len(rows),
            "result": dg.MetadataValue.md(f"{note}\n\n{md}"),
            "csv": dg.MetadataValue.path(str(csv_path)),
            "query": dg.MetadataValue.md(f"```sql\n{config.query}\n```"),
        },
    )


@dg.job(description="DB 를 SQL 로 조회 (읽기 전용). Launchpad 에서 ops.sql_query.config.query 를 바꿔 실행", tags={"stage": "query"})
def sql_query_job():
    sql_query()


defs = dg.Definitions(jobs=[sql_query_job])
