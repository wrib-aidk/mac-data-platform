"""SEIBro(한국예탁결제원 증권정보포털) 펀드 NAV 수집 파이프라인.

원본: wrib-crawler/scripts/seibro/ (README "SEIBro 펀드 NAV 크롤러")
흐름: fund_list (전체 펀드 ISIN 목록) → fund_nav (ISIN별 일별 기준가)
주의: KOFIA 펀드코드(K55..., K5M..., KR5..., KRM...)가 아니라 ISIN(KR...)을 키로 씀.
"""

import dagster as dg

from mac_data_platform.collectors import seibro
from mac_data_platform.pipeline import ITEM_RETRIES, collect_asset, csv_preview, date_range, item_result, source_dir
from mac_data_platform.settings import raw_postfix


def list_path():  # noqa: ANN201
    return source_dir("seibro") / f"fund_list.csv{raw_postfix()}"


@collect_asset(
    "seibro",
    "fund_list",
    """
    ### SEIBro 전체 펀드 목록
    SEIBro 증권정보포털에 등록된 펀드 전체의 목록을 `isin`과 함께 받습니다.

    > KOFIA 펀드코드(`K55...`, `K5M...`, `KR5...`, `KRM...`)와 **다른 ISIN(`KR...`)** 을 씁니다.

    - 사용처: `seibro/fund_nav`
    - 갱신: 실행할 때마다 **전체를 새로 받아 덮어씀**
    """,
    site="https://seibro.or.kr/",
    output="data/raw/seibro/fund_list.csv",
)
def fund_list(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    out = list_path()
    n = seibro.fetch_fund_list(out)
    context.log.info(f"펀드 {n}개 → {out}")
    return dg.MaterializeResult(metadata={"rows": n, "path": dg.MetadataValue.path(str(out)), "preview": csv_preview(out)})


class FundNavConfig(dg.Config):
    start: str = "2007-01-01"  # 처음 받는 펀드의 시작일. 이미 받은 펀드는 마지막 날짜 7일 전부터만 받아 합침
    end: str = ""  # YYYY-MM-DD, 비우면 어제
    lookback_days: int = 100
    workers: int = 8
    limit: int | None = None  # 테스트용


@collect_asset(
    "seibro",
    "fund_nav",
    """
    ### ISIN별 일별 NAV(기준가)
    펀드 목록의 ISIN마다 일별 기준가를 받아 `data/raw/seibro/funds/<isin>/nav.csv`에 저장합니다.

    | 컬럼 | 설명 |
    | --- | --- |
    | `date` | 기준일자 (`YYYYMMDD`) |
    | `nav` | 기준가격 |
    | `nav_change` | 전일대비 (원) |
    | `nav_change_pct` | 전일대비 등락률 (%) |
    | `tax_nav` | 과표기준가격 |
    | `principal` | 설정원본 |

    **갱신 규칙** (원본 크롤러는 이미 받은 펀드를 건너뛰었지만 여기서는 이어 받음)
    - 처음 받는 펀드: `start`(기본 2007-01-01)부터 어제까지
    - 이미 받은 펀드: 마지막 날짜 **7일 전부터** 다시 받아 날짜 기준으로 합침 → 새 날짜 추가 + 정정된 값 덮어씀

    설정: `start`, `end`, `workers`, `limit`(테스트용)
    """,
    site="https://seibro.or.kr/",
    output="data/raw/seibro/funds/<isin>/nav.csv",
    deps=[fund_list],
)
def fund_nav(context: dg.AssetExecutionContext, config: FundNavConfig) -> dg.MaterializeResult:
    start, end = date_range(config.start, config.end, config.lookback_days)
    out_dir = source_dir("seibro") / "funds"
    result = seibro.fetch_fund_nav(list_path(), out_dir, log=context.log, start=start, end=end, postfix=raw_postfix(), workers=config.workers, limit=config.limit, retries=ITEM_RETRIES)
    sample = next((p for p in sorted(out_dir.glob("*/nav.csv"))), None)
    preview = {"preview": dg.MetadataValue.md(f"`{sample.parent.name}/nav.csv` 앞 5행\n\n" + csv_preview(sample).value)} if sample else {}
    return item_result(context, result, out_dir, start=str(start), end=str(end), **preview)
