"""Open DART(금융감독원 전자공시) 정기공시 수집 파이프라인. .env 에 DART_API_KEY(40자리) 필요.

원본: wrib-crawler/scripts/dart/ (README "DART 정기공시 크롤러")
흐름: corp_list (상장·상장폐지 회사 목록) → corp_filings (회사별 사업·반기·분기보고서 XML)
"""

import dagster as dg

from mac_data_platform import settings
from mac_data_platform.collectors import dart
from mac_data_platform.pipeline import ITEM_RETRIES, collect_asset, csv_preview, date_range, item_result, require_env, source_dir
from mac_data_platform.settings import raw_postfix


def list_path():  # noqa: ANN201
    return source_dir("dart") / f"corp_list.csv{raw_postfix()}"


@collect_asset(
    "dart",
    "corp_list",
    """
    ### Open DART 공시대상 회사 목록
    Open DART API로 공시대상 회사 전체를 받고, **종목코드가 있는 회사(상장폐지 포함)만** 저장합니다. 비상장 법인은 제외.

    - 필요: `.env`의 `DART_API_KEY` (Open DART에서 발급, 40자리)
    - 사용처: `dart/corp_filings`
    - 갱신: 실행할 때마다 **전체를 새로 받아 덮어씀**
    """,
    site="https://opendart.fss.or.kr/",
    output="data/raw/dart/corp_list.csv",
)
def corp_list(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    (api_key,) = require_env("DART_API_KEY")
    out = list_path()
    n = dart.fetch_corp_list(api_key, out)
    context.log.info(f"회사 {n}개 → {out}")
    return dg.MaterializeResult(metadata={"rows": n, "path": dg.MetadataValue.path(str(out)), "preview": csv_preview(out)})


class CorpFilingsConfig(dg.Config):
    start: str = ""  # 공시일 YYYY-MM-DD, 비우면 end - lookback_days (전체 이력은 "2010-01-01")
    end: str = ""  # 비우면 어제
    lookback_days: int = 100
    workers: int = 4
    limit: int | None = None  # 테스트용: 앞에서부터 N개 회사만


@collect_asset(
    "dart",
    "corp_filings",
    """
    ### 회사별 정기보고서 본문(XML)
    회사 목록의 회사마다 `start`~`end` 기간에 공시된 **사업보고서 / 반기보고서 / 분기보고서** 본문을 UTF-8 XML로 저장합니다.
    파일명: `data/raw/dart/filings/<corp_code>_<결산기 YYYYMM>_<보고서>_<접수번호>.xml` (`stock_code`가 아니라 `corp_code` 기준!)

    **갱신 규칙**
    - 보고서마다 **최신본 하나만** 유지합니다.
    - 원본 크롤러는 정정본이 나와도 다시 받지 않았지만, 여기서는 정정본(새 접수번호)이 나오면 받아서 **이전 판을 지웁니다**.

    설정: `start`/`end`(공시일, 비우면 최근 `lookback_days`일, 전체 이력은 `start: 2010-01-01`), `workers`, `limit`(테스트용)
    """,
    site="https://opendart.fss.or.kr/",
    output="data/raw/dart/filings/",
    deps=[corp_list],
)
def corp_filings(context: dg.AssetExecutionContext, config: CorpFilingsConfig) -> dg.MaterializeResult:
    (api_key,) = require_env("DART_API_KEY")
    start, end = date_range(config.start, config.end, config.lookback_days)
    out_dir = settings.RAW_DIR / "dart" / "filings"
    result = dart.fetch_corp_filings(api_key, list_path(), out_dir, log=context.log, start=start, end=end, postfix=raw_postfix(), workers=config.workers, limit=config.limit, retries=ITEM_RETRIES)
    return item_result(context, result, out_dir, start=str(start), end=str(end))
