"""KRX 파일 → PostgreSQL."""

import dagster as dg

from mac_data_platform.loaders import krx as load
from mac_data_platform.pipeline import db_asset, source_dir, table_result

from ..collect.krx import etf_constituents, etf_daily_quotes, stock_daily_quotes, stock_list
from ..resources import PostgresResource
from ._config import LoadConfig


@db_asset(
    "raw",
    "krx_stock_list",
    "krx",
    """
    ### KRX 종목기본정보 (`raw.krx_stock_list`)
    `isu_cd` 별 API 응답 한 행을 `data`(jsonb) 로. 파일이 바뀌면 통째로 교체합니다.
    """,
    deps=[stock_list],
)
def krx_stock_list(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_stock_list(conn, source_dir("krx") / "stock_list.csv", log=context.log, force=config.force)
        return table_result(conn, "raw.krx_stock_list", stats, order_by="isu_cd")


def _daily(kind: str, label: str, upstream: dg.AssetsDefinition) -> dg.AssetsDefinition:
    @db_asset(
        "raw",
        f"krx_{kind}_daily",
        "krx",
        f"""
        ### KRX {label} 일별 시세 (`raw.krx_{kind}_daily`)
        `quotes/<YYYYMMDD>_{kind}.csv` → (`bas_dd`, `isu_cd`) 1행. 컬럼은 API 출력명 소문자 (`tdd_clsprc` 종가, `acc_trdvol` 거래량 …).
        새로 받은(바뀐) 날짜 파일만 그 날짜 행을 교체합니다.
        """,
        deps=[upstream],
    )
    def _asset(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
        with postgres.connect() as conn:
            stats = load.load_daily(conn, source_dir("krx") / "quotes", kind, log=context.log, force=config.force)
            return table_result(conn, f"raw.krx_{kind}_daily", stats, order_by="bas_dd DESC, isu_cd")

    return _asset


krx_stock_daily = _daily("stock", "코스피·코스닥", stock_daily_quotes)
krx_etf_daily = _daily("etf", "ETF", etf_daily_quotes)


@db_asset(
    "raw",
    "krx_etf_constituents",
    "krx",
    """
    ### KRX ETF 구성종목 (`raw.krx_etf_constituents`)
    `etf_constituents/<YYYYMMDD>/<ETF ISIN>.csv` → (날짜, ETF, 순번) 1행, 웹 응답 필드는 `data`(jsonb).
    """,
    deps=[etf_constituents],
)
def krx_etf_constituents(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_etf_constituents(conn, source_dir("krx") / "etf_constituents", log=context.log, force=config.force)
        return table_result(conn, "raw.krx_etf_constituents", stats, order_by="trade_date DESC, etf_isin, line_no")


# KRX 정제본(extracted.krx_*) 적재는 sql/003 에서 제거함 — 정제 파이프라인이 정해지면 그 asset 이 직접 extracted 테이블에 쓰도록 추가
