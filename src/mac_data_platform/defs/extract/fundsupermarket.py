"""펀드슈퍼마켓 추출 파이프라인: 공시 PDF → 텍스트(.md).

원본: wrib-crawler/scripts/fundsupermarket/convert_pdf_to_txt.py
흐름: fundsupermarket/fund_details (PDF 수집) → fundsupermarket/fund_documents_text (텍스트 변환)
"""

import dagster as dg

from mac_data_platform import settings
from mac_data_platform.extractors import fund_documents
from mac_data_platform.pipeline import extract_asset, item_result, md_table, text_preview, source_dir

from ..collect.fundsupermarket import fund_details

OUT_DIR = settings.DATA_DIR / "extracted" / "fundsupermarket" / "funds"


class FundDocumentsTextConfig(dg.Config):
    workers: int = 0  # 동시 변환 프로세스 수, 0 이면 CPU 코어 수
    force: bool = False  # true 면 바뀌지 않은 PDF 도 전부 다시 변환
    limit: int | None = None  # 테스트용: 앞에서부터 N개 펀드만


@extract_asset(
    "fundsupermarket",
    "fund_documents_text",
    """
    ### 공시 PDF → 텍스트
    `data/raw/fundsupermarket/funds/<fund_code>/*.pdf` 를 pypdfium2로 텍스트만 뽑아
    `data/extracted/fundsupermarket/funds/<fund_code>/<문서>.md` 로 저장합니다.
    (확장자는 `.md`지만 내용은 레이아웃 분석 없는 순수 텍스트, 쪽 사이는 빈 줄 하나)

    | 파일 | 원본 문서 |
    | --- | --- |
    | `collective_investment_agreement.md` | 집합투자규약 |
    | `statutory_prospectus.md` | 투자설명서 |
    | `summary_prospectus.md` | 간이투자설명서 |
    | `key_summary_explanation.md` | 핵심(요약)상품설명서 |
    | `asset_management_report.md` | 운용보고서 |
    | `asset_safekeeping_and_management_report.md` | 자산관리보고서 |

    **갱신 규칙**
    - 변환할 때의 PDF 크기·수정시각을 `<fund_code>/.text.json` 에 기록하고, **PDF가 바뀐 문서만** 다시 변환합니다.
      (원본 스크립트는 텍스트가 이미 있으면 건너뛰어서 공시가 바뀌어도 예전 판 텍스트가 남았음)
    - 기록이 없는 문서(HF에서 가져온 것 포함)는 처음 한 번 다시 변환해 기준을 잡습니다.

    결과 화면의 **preview** 에서 변환된 문서 예시를 볼 수 있습니다.
    설정: `workers`(0 = CPU 코어 수), `force`(전부 다시 변환), `limit`(테스트용)
    """,
    output="data/extracted/fundsupermarket/funds/<fund_code>/<문서>.md",
    deps=[fund_details],
)
def fund_documents_text(context: dg.AssetExecutionContext, config: FundDocumentsTextConfig) -> dg.MaterializeResult:
    raw_dir = source_dir("fundsupermarket") / "funds"
    result = fund_documents.extract_fund_documents(raw_dir, OUT_DIR, log=context.log, workers=config.workers or None, limit=config.limit, force=config.force)
    sample_dir = next((d for d in sorted(OUT_DIR.iterdir()) if (d / "summary_prospectus.md").exists()), None) if OUT_DIR.exists() else None
    preview = {}
    if sample_dir:
        files = sorted(sample_dir.glob("*.md"))
        preview = {
            "preview_files": md_table([{"fund_code": sample_dir.name, "file": f.name, "chars": f"{len(f.read_text(encoding='utf-8')):,}"} for f in files]),
            "preview": text_preview(sample_dir / "summary_prospectus.md"),
        }
    return item_result(context, result, OUT_DIR, **preview)
