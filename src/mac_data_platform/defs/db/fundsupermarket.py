"""펀드슈퍼마켓 파일 → PostgreSQL."""

import dagster as dg

from mac_data_platform import settings
from mac_data_platform.loaders import fundsupermarket as load
from mac_data_platform.pipeline import db_asset, source_dir, table_result

from ..collect.fundsupermarket import fund_details, fund_list
from ..extract.fundsupermarket import OUT_DIR as TEXT_DIR
from ..extract.fundsupermarket import fund_documents_text
from ..resources import PostgresResource
from ._config import LoadConfig

RETENTION = settings.db_settings()["retention_days"]


@db_asset(
    "raw",
    "fundsupermarket_fund_list",
    "fundsupermarket",
    f"""
    ### 펀드 목록 스냅샷 (`raw.fundsupermarket_fund_list`)
    `fund_list.csv` 를 **수집일(snapshot_date)별**로 보관합니다. CSV 한 행이 `data`(jsonb)에 그대로 들어갑니다 (값은 화면 표시용 문자열).
    - 같은 날 다시 수집하면 그날 스냅샷을 교체
    - **{RETENTION}일**이 지난 스냅샷은 자동 삭제 (`config/pipelines.yaml` 의 `db.retention_days`)

    ```sql
    SELECT fund_code, data->>'total_fee', data->>'return_1y' FROM raw.fundsupermarket_fund_list WHERE snapshot_date = (SELECT max(snapshot_date) FROM raw.fundsupermarket_fund_list);
    ```
    """,
    deps=[fund_list],
)
def fundsupermarket_fund_list(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_fund_list(conn, source_dir("fundsupermarket") / "fund_list.csv", log=context.log, retention_days=RETENTION, force=config.force)
        return table_result(conn, "raw.fundsupermarket_fund_list", stats, order_by="snapshot_date DESC, fund_code")


@db_asset(
    "raw",
    "fundsupermarket_fund_details",
    "fundsupermarket",
    f"""
    ### 펀드 상세 스냅샷 (`raw.fundsupermarket_fund_details`)
    `funds/<fund_code>/details.json` 을 **펀드 × 수집일** 1행으로 보관합니다 (`data` jsonb: 보수, 수익률, 위험지표, 자산구성 등 약 100개 항목).
    - details.json 이 바뀐(다시 수집된) 펀드만 적재
    - **{RETENTION}일**이 지난 스냅샷은 자동 삭제

    ```sql
    SELECT snapshot_date, data->>'total_fee', data->'risk_metrics'->'sharpe_ratio' FROM raw.fundsupermarket_fund_details WHERE fund_code = 'K55101B38300' ORDER BY snapshot_date;
    ```
    """,
    deps=[fund_details],
)
def fundsupermarket_fund_details(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_fund_details(conn, source_dir("fundsupermarket") / "funds", log=context.log, retention_days=RETENTION, force=config.force)
        return table_result(conn, "raw.fundsupermarket_fund_details", stats, order_by="snapshot_date DESC, fund_code")


@db_asset(
    "raw",
    "fundsupermarket_documents",
    "fundsupermarket",
    """
    ### 공시 문서 목록 (`raw.fundsupermarket_documents`)
    펀드별 PDF 6종의 위치(`path`, data/ 기준), 크기, 수정시각, 문서 기준일(`doc_date`), 본문 해시. **PDF 자체는 파일로 둡니다.**
    PDF 가 바뀐 펀드만 다시 적재합니다.
    """,
    deps=[fund_details],
)
def fundsupermarket_documents(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_documents(conn, source_dir("fundsupermarket") / "funds", log=context.log, force=config.force)
        return table_result(conn, "raw.fundsupermarket_documents", stats, order_by="fund_code, doc_type")


@db_asset(
    "extracted",
    "fundsupermarket_document_text",
    "fundsupermarket",
    """
    ### 공시 문서 텍스트 (`extracted.fundsupermarket_document_text`)
    `fundsupermarket/fund_documents_text` 가 만든 `.md` 를 **펀드 × 문서종류** 1행으로 넣습니다 (`text`, `chars`).
    텍스트가 다시 변환된 펀드만 교체합니다. 문서 내용 기반 추출 파이프라인의 입력으로 쓰면 됩니다.

    ```sql
    SELECT fund_code, left(text, 300) FROM extracted.fundsupermarket_document_text WHERE doc_type = 'summary_prospectus' AND text LIKE '%투자목적%';
    ```
    """,
    deps=[fund_documents_text],
)
def fundsupermarket_document_text(context: dg.AssetExecutionContext, config: LoadConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        stats = load.load_document_text(conn, TEXT_DIR, log=context.log, force=config.force)
        return table_result(conn, "extracted.fundsupermarket_document_text", stats, order_by="fund_code, doc_type")
