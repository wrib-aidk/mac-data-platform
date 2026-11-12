"""펀드슈퍼마켓(우리투자증권) 수집 파이프라인.

원본: wrib-crawler/scripts/fundsupermarket/ (README "펀드슈퍼마켓 크롤러")
흐름: fund_list (전체 펀드 목록) → fund_details (펀드별 상세 JSON + 공시 PDF)
"""

import dagster as dg

from mac_data_platform.collectors import fundsupermarket
from mac_data_platform.collectors.common import read_csv_column
from mac_data_platform.pipeline import ITEM_RETRIES, collect_asset, csv_preview, item_result, source_dir
from mac_data_platform.settings import raw_postfix


def list_path():  # noqa: ANN201
    return source_dir("fundsupermarket") / f"fund_list.csv{raw_postfix()}"


@collect_asset(
    "fundsupermarket",
    "fund_list",
    """
    ### 펀드슈퍼마켓 전체 펀드 목록
    우리투자증권 펀드슈퍼마켓의 **전체펀드 > 펀드검색** 화면에 나오는 펀드 전체를 받습니다.
    `fund_code`(KOFIA 펀드코드, `K55...` 등)가 들어 있어 다음 단계의 입력이 됩니다.

    - 사용처: `fundsupermarket/fund_details`, `kofia/fund_nav`
    - 갱신: 실행할 때마다 **전체를 새로 받아 덮어씀**
    """,
    site="https://fundsupermarket.wooriib.com/fmm/FMM1010301/main.do",
    output="data/raw/fundsupermarket/fund_list.csv",
)
def fund_list(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    out = list_path()
    n = fundsupermarket.fetch_fund_list(out)
    context.log.info(f"펀드 {n}개 → {out}")
    return dg.MaterializeResult(metadata={"rows": n, "path": dg.MetadataValue.path(str(out)), "preview": csv_preview(out, columns=["fund_code", "fund_name", "fund_class", "fund_type", "total_fee", "aum", "nav", "risk_grade", "return_1y"])})


class FundDetailsConfig(dg.Config):
    workers: int = 8
    limit: int | None = None  # 테스트용: 앞에서부터 N개 펀드만


@collect_asset(
    "fundsupermarket",
    "fund_details",
    """
    ### 펀드별 상세 정보 + 공시 문서(PDF)
    펀드 목록의 `fund_code`마다 `data/raw/fundsupermarket/funds/<fund_code>/`에 아래 파일을 저장합니다.

    | 파일 | 내용 |
    | --- | --- |
    | `details.json` | **펀드 메타데이터** (유형, 보수, 설정일, 운용사 등) |
    | `collective_investment_agreement.pdf` | 집합투자규약 |
    | `statutory_prospectus.pdf` | 투자설명서 (투자목적·투자전략 포함) |
    | `summary_prospectus.pdf` | **간이투자설명서** (투자목적·투자전략 포함) |
    | `key_summary_explanation.pdf` | 핵심(요약)상품설명서 |
    | `asset_management_report.pdf` | 운용보고서 (운용사마다 형식이 다름) |
    | `asset_safekeeping_and_management_report.pdf` | **자산관리보고서** |

    **갱신 규칙**
    - `details.json`은 매번 새로 받아 덮어씁니다.
    - PDF도 매번 받되 **본문 내용이 바뀐 경우에만** 덮어씁니다 (`.docs.json`에 해시·문서 날짜 기록).
    - 서버가 가끔 **예전 판 문서**를 돌려주기 때문에, 문서 안의 날짜가 기존 파일보다 오래됐으면 무시합니다 (`docs_stale_ignored`).

    설정: `workers`(동시 요청 수), `limit`(테스트용, 앞에서 N개 펀드만)
    """,
    site="https://fundsupermarket.wooriib.com/fmm/FMM1010301/main.do",
    output="data/raw/fundsupermarket/funds/<fund_code>/",
    deps=[fund_list],
)
def fund_details(context: dg.AssetExecutionContext, config: FundDetailsConfig) -> dg.MaterializeResult:
    out_dir = source_dir("fundsupermarket") / "funds"
    result = fundsupermarket.fetch_fund_details(list_path(), out_dir, log=context.log, pdf_postfix=raw_postfix(), workers=config.workers, limit=config.limit, retries=ITEM_RETRIES)
    sample = next((out_dir / c / "details.json" for c in read_csv_column(list_path(), "fund_code") if (out_dir / c / "details.json").exists()), None)
    preview = {}
    if sample:
        files = sorted(p.name for p in sample.parent.iterdir() if not p.name.startswith("."))
        text = sample.read_text(encoding="utf-8")
        preview = {"preview": dg.MetadataValue.md(f"`{sample.parent.name}/`: {', '.join(files)}\n\n```json\n{text[:1500]}{'…' if len(text) > 1500 else ''}\n```")}
    return item_result(context, result, out_dir, **preview)
