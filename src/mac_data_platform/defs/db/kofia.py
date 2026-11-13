"""KOFIA 파일 → PostgreSQL."""

import dagster as dg

from mac_data_platform.extractors import kofia_holdings as extract_holdings
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
    "extracted",
    "kofia_fund_links",
    "kofia",
    """
    ### KOFIA 펀드 수익증권·ETF 연결표 (`extracted.kofia_fund_links`)
    보유종목에 적힌 펀드 수익증권·ETF 이름 → 보유내역이 있는 KOFIA 펀드. `extracted.kofia_fund_holdings` 에서 펼칠 때 씁니다.
    보유종목에는 이름이 줄여 쓰이거나(`IT코리아증권모[주식]`) 30자에서 잘리고, ETF 는 KRX 이름(`KODEX 삼성그룹`)으로 적혀 있어 규칙으로 맞춥니다.
    ETF 는 먼저 KRX 종목코드 → ISIN → SEIBro 공식명으로 KOFIA ETF명과 정확히 맞추고(`etf_seibro`), 안 되면 이름 규칙으로.
    `method` 로 어떻게 맞췄는지 확인할 수 있습니다 (`etf_seibro`, `etf_exact`, `etf_subseq`, `exact`, `subseq`, `subseq_any`, `+class`, `+feeder`).
    """,
    deps=[kofia_holdings, kofia_funds, dg.AssetKey(["db", "raw", "krx_etf_daily"]), dg.AssetKey(["db", "raw", "seibro_funds"])],
)
def kofia_fund_links(context: dg.AssetExecutionContext, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = extract_holdings.build_links(conn, log=context.log)
        return table_result(conn, "extracted.kofia_fund_links", stats, order_by="std_dt DESC, company_cd, item_nm")


@db_asset(
    "extracted",
    "kofia_fund_holdings",
    "kofia",
    """
    ### KOFIA 펀드별 실제 보유종목 (`extracted.kofia_fund_holdings`)
    `raw.kofia_holdings` 에서 펀드마다 실제로 들고 있는 종목을 뽑습니다.
    - 모투자신탁·ETF 등 다른 펀드는 연결표(`extracted.kofia_fund_links`)로 찾은 그 펀드의 종목으로 펼쳐 보유 비율만큼 나눔 (최대 3단계, `via` 에 거쳐 온 펀드)
    - 해외 펀드·해외 ETF 등 KOFIA 에 보유내역이 없는 것은 수익증권 그대로 남음
    - 같은 종목은 합치고, `weight` = 펀드 보유자산 시가평가액 합계 대비 비중(%), `rank` = 펀드 안 순위
    - 국내 주식·ETF 는 기준일 KRX 종목명으로 `isu_cd`(종목코드)를 붙임 (해외주식·채권은 빈 값)

    ```sql
    -- 펀드 보유종목 상위
    select rank, item_nm, isu_cd, weight, via from extracted.kofia_fund_holdings
    where std_dt = '2026-06-30' and fund_nm = '삼성KODEX200증권상장지수투자신탁[주식]' order by rank;
    -- 삼성전자를 들고 있는 펀드
    select fund_nm, weight, market_amt from extracted.kofia_fund_holdings
    where std_dt = '2026-06-30' and isu_cd = '005930' order by market_amt desc;
    ```
    """,
    deps=[kofia_fund_links, dg.AssetKey(["db", "raw", "krx_stock_daily"]), dg.AssetKey(["db", "raw", "krx_etf_daily"])],
)
def kofia_fund_holdings(context: dg.AssetExecutionContext, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = extract_holdings.build(conn, log=context.log)
        return table_result(conn, "extracted.kofia_fund_holdings", stats, order_by="std_dt DESC, standard_cd, rank")


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
