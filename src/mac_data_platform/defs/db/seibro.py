"""SEIBro 파일 → PostgreSQL."""

import dagster as dg

from mac_data_platform.loaders import seibro as load
from mac_data_platform.pipeline import db_asset, source_dir, table_result

from ..collect.seibro import fund_list, fund_nav
from ..resources import PostgresResource
from ._config import LoadConfig


@db_asset(
    "raw",
    "seibro_funds",
    "seibro",
    """
    ### SEIBro 펀드 목록 (`raw.seibro_funds`)
    `isin` 별 펀드명·클래스 코드. 목록 파일이 바뀌면 통째로 교체합니다.
    """,
    deps=[fund_list],
)
def seibro_funds(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_funds(conn, source_dir("seibro") / "fund_list.csv", log=context.log, force=config.force)
        return table_result(conn, "raw.seibro_funds", stats, order_by="isin")


@db_asset(
    "raw",
    "seibro_fund_nav",
    "seibro",
    """
    ### SEIBro 일별 NAV (`raw.seibro_fund_nav`)
    `funds/<isin>/nav.csv` → (`isin`, `date`) 1행. 기준가·전일대비·등락률·과표기준가·설정원본·순자산·분배금.
    `nav.csv` 가 바뀐 펀드만 그 펀드 행 전체를 교체합니다 (첫 적재는 약 3,700만 행).

    ```sql
    SELECT date, nav FROM raw.seibro_fund_nav WHERE isin = 'KR5101888398' AND date >= '2026-01-01' ORDER BY date;
    ```
    """,
    deps=[fund_nav],
)
def seibro_fund_nav(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_nav(conn, source_dir("seibro") / "funds", log=context.log, force=config.force)
        return table_result(conn, "raw.seibro_fund_nav", stats, order_by="isin, date")
