"""KOFIA 파일 → PostgreSQL."""

import dagster as dg

from mac_data_platform.loaders import kofia as load
from mac_data_platform.pipeline import db_asset, source_dir, table_result

from ..collect.kofia import fees_dir, fund_fees, fund_holdings, fund_nav
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
    ### KOFIA 보유종목 (`raw.kofia_holdings`)
    `kofia_holdings.db`(SQLite)의 `holdings` 테이블을 그대로 옮깁니다. SQLite 파일이 바뀌면 통째로 교체.
    펀드 1개 = 여러 행, 펀드 정보는 `raw.kofia_funds` 와 `standard_cd` 로 조인.
    (수집기의 이어받기 큐 `jobs`·`company_filings` 는 수집 관리용이라 SQLite 에만 둠)
    """,
    deps=[fund_holdings],
)
def kofia_holdings(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_holdings(conn, source_dir("kofia") / "kofia_holdings.db", log=context.log, force=config.force)
        return table_result(conn, "raw.kofia_holdings", stats, order_by="std_dt DESC, standard_cd, line_no")


@db_asset(
    "raw",
    "kofia_funds",
    "kofia",
    """
    ### KOFIA 펀드 목록 (`raw.kofia_funds`)
    `kofia_holdings.db`(SQLite)의 `funds` 테이블을 그대로 옮깁니다. SQLite 파일이 바뀌면 통째로 교체.
    영업보고서를 낸 운용사의 운용 중 펀드 전체(사모·클래스 포함). `is_etf`·`is_private`·`is_class` 로 수집 대상을 구분합니다.
    """,
    deps=[fund_holdings],
)
def kofia_funds(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_funds(conn, source_dir("kofia") / "kofia_holdings.db", log=context.log, force=config.force)
        return table_result(conn, "raw.kofia_funds", stats, order_by="standard_cd")


@db_asset(
    "raw",
    "kofia_fund_fees",
    "kofia",
    """
    ### KOFIA 펀드별 보수비용 (`raw.kofia_fund_fees`)
    `fees/<기준일>.csv` → (`standard_dt`, `standard_cd`) 1행. 펀드·ETF 전체. 바뀐 기준일 파일만 교체합니다.
    ETF 만 보려면 `fund_nm LIKE '%상장지수%'`.
    """,
    deps=[fund_fees],
)
def kofia_fund_fees(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_fees(conn, fees_dir(), log=context.log, force=config.force)
        return table_result(conn, "raw.kofia_fund_fees", stats, order_by="standard_dt DESC, standard_cd")
