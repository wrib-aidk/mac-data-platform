"""DART 파일 → PostgreSQL."""

import dagster as dg

from mac_data_platform.loaders import dart as load
from mac_data_platform.pipeline import db_asset, source_dir, table_result

from ..collect.dart import corp_filings, corp_list, list_path
from ..resources import PostgresResource
from ._config import LoadConfig


@db_asset(
    "raw",
    "dart_corps",
    "dart",
    """
    ### DART 회사 목록 (`raw.dart_corps`)
    `corp_code` 별 회사명·영문명·종목코드·수정일. 목록이 바뀌면 통째로 교체합니다.
    """,
    deps=[corp_list],
)
def dart_corps(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_corps(conn, list_path(), log=context.log, force=config.force)
        return table_result(conn, "raw.dart_corps", stats, order_by="corp_code")


@db_asset(
    "raw",
    "dart_filings",
    "dart",
    """
    ### DART 정기보고서 목록 (`raw.dart_filings`)
    (`corp_code`, 결산기, 보고서 종류) 별 최신 접수번호와 XML 파일 위치. **본문 XML 은 파일로 둡니다.** 매번 파일 목록으로 다시 만듭니다.
    """,
    deps=[corp_filings],
)
def dart_filings(context: dg.AssetExecutionContext, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_filings(conn, source_dir("dart") / "filings", log=context.log)
        return table_result(conn, "raw.dart_filings", stats, order_by="corp_code, fiscal_period DESC")
