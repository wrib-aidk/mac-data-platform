"""KOFIA 파일 → PostgreSQL."""

import dagster as dg

from mac_data_platform.loaders import kofia as load
from mac_data_platform.pipeline import db_asset, source_dir, table_result

from ..collect.kofia import fund_holdings, fund_nav
from ..resources import PostgresResource
from ._config import LoadConfig


@db_asset(
    "raw",
    "kofia_fund_nav",
    "kofia",
    """
    ### KOFIA 일별 NAV (`raw.kofia_fund_nav`)
    `funds/<fund_code>/nav.csv` → (`fund_code`, `date`) 1행. 바뀐 펀드만 교체합니다.
    """,
    deps=[fund_nav],
)
def kofia_fund_nav(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_nav(conn, source_dir("kofia") / "funds", log=context.log, force=config.force)
        return table_result(conn, "raw.kofia_fund_nav", stats, order_by="fund_code, date")


@db_asset(
    "raw",
    "kofia_holdings",
    "kofia",
    """
    ### KOFIA 보유종목 (`raw.kofia_holdings`, `raw.kofia_funds`)
    `kofia_holdings.db`(SQLite)의 `holdings`·`funds` 테이블을 그대로 옮깁니다. SQLite 파일이 바뀌면 통째로 교체.
    (수집기의 이어받기 큐 `jobs`·`company_filings` 는 수집 관리용이라 SQLite 에만 둠)
    """,
    deps=[fund_holdings],
)
def kofia_holdings(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_holdings(conn, source_dir("kofia") / "kofia_holdings.db", log=context.log, force=config.force)
        return table_result(conn, "raw.kofia_holdings", stats, order_by="std_dt DESC, standard_cd, line_no")
