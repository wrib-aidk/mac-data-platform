"""KRX(한국거래소) 수집 파이프라인.

원본: wrib-crawler/scripts/krx/ (README "KRX 크롤러"), scripts/krx_web/ (README "KRX 웹 크롤러")
- OPEN API (.env 의 KRX_AUTH_KEY): stock_list, stock_daily_quotes, etf_daily_quotes
  인증키 발급 후 API 별로 '서비스 이용' 신청·승인이 필요 (안 하면 401)
- 웹 (.env 의 KRX_AUTH_ID / KRX_AUTH_PW): etf_constituents — 과도하게 요청하면 IP 차단됨
"""

from datetime import date, timedelta

import dagster as dg

from mac_data_platform import settings
from mac_data_platform.collectors import krx
from mac_data_platform.pipeline import ITEM_RETRIES, collect_asset, csv_preview, date_range, item_result, require_env, source_dir
from mac_data_platform.settings import raw_postfix


class AsOfConfig(dg.Config):
    date: str = ""  # YYYY-MM-DD, 비우면 어제 (당일 데이터는 익일 08시부터)


def _as_of(value: str) -> date:
    return date.fromisoformat(value) if value else settings.today() - timedelta(days=1)


@collect_asset(
    "krx",
    "stock_list",
    """
    ### 코스피·코스닥 종목 목록
    KRX OPEN API의 **유가증권·코스닥 종목기본정보**를 합쳐 저장합니다.

    - 필요: `.env`의 `KRX_AUTH_KEY` + 해당 API '서비스 이용' 승인 (없으면 401)
    - 기준일: `date` (비우면 어제. 당일 데이터는 익일 오전 8시부터, 2010-01-04부터 제공)
    - 갱신: 실행할 때마다 덮어씀
    """,
    site="https://openapi.krx.co.kr/",
    output="data/raw/krx/stock_list.csv",
)
def stock_list(context: dg.AssetExecutionContext, config: AsOfConfig) -> dg.MaterializeResult:
    (auth_key,) = require_env("KRX_AUTH_KEY")
    out = source_dir("krx") / f"stock_list.csv{raw_postfix()}"
    n = krx.fetch_stock_list(auth_key, _as_of(config.date), out)
    return dg.MaterializeResult(metadata={"rows": n, "path": dg.MetadataValue.path(str(out)), "preview": csv_preview(out)})


class QuotesConfig(dg.Config):
    start: str = ""  # YYYY-MM-DD, 비우면 end - lookback_days
    end: str = ""  # 비우면 어제
    lookback_days: int = 100
    workers: int = 8


def _quotes_asset(kind: str, label: str, detail: str) -> dg.AssetsDefinition:
    @collect_asset(
        "krx",
        f"{kind}_daily_quotes",
        f"""
        ### {label} 일별 시세
        KRX OPEN API의 {detail}를 일자별 파일로 저장합니다: `data/raw/krx/quotes/<YYYYMMDD>_{kind}.csv`
        컬럼은 API 출력명(`BAS_DD`, `ISU_CD`, `TDD_CLSPRC`, `ACC_TRDVOL`, `MKTCAP` 등) 그대로입니다.

        - 필요: `.env`의 `KRX_AUTH_KEY` + 해당 API '서비스 이용' 승인
        - 기간: `start`~`end` (비우면 최근 `lookback_days`일 ~ 어제). 휴장일은 파일을 만들지 않음
        - 갱신: **이미 받은 날짜는 건너뜀** (2010-01-04~ 이력은 Hugging Face `krx-quotes-raw`에서 이관됨)
        """,
        site="https://openapi.krx.co.kr/",
        output=f"data/raw/krx/quotes/<YYYYMMDD>_{kind}.csv",
    )
    def _asset(context: dg.AssetExecutionContext, config: QuotesConfig) -> dg.MaterializeResult:
        (auth_key,) = require_env("KRX_AUTH_KEY")
        start, end = date_range(config.start, config.end, config.lookback_days)
        out_dir = settings.RAW_DIR / "krx" / "quotes"
        result = krx.fetch_daily_quotes(kind, auth_key, out_dir, log=context.log, start=start, end=end, postfix=raw_postfix(), workers=config.workers, retries=ITEM_RETRIES)
        return item_result(context, result, out_dir, start=str(start), end=str(end))

    return _asset


stock_daily_quotes = _quotes_asset("stock", "코스피·코스닥", "유가증권·코스닥 일별매매정보(두 시장 합침)")
etf_daily_quotes = _quotes_asset("etf", "ETF", "ETF 일별매매정보(NAV, 순자산총액, 기초지수 포함)")


class ConstituentsConfig(AsOfConfig):
    limit: int | None = None  # 테스트용: 앞에서부터 N개 ETF만


@collect_asset(
    "krx",
    "etf_constituents",
    """
    ### ETF 구성종목  ⚠️ IP 차단 주의
    KRX 정보데이터시스템 **웹사이트**에 로그인해 현재 상장된 ETF 전체의 구성종목을 받습니다.
    `data/raw/krx/etf_constituents/<YYYYMMDD>/<ETF ISIN>.csv`

    - 필요: `.env`의 `KRX_AUTH_ID`, `KRX_AUTH_PW` (KRX 회원 ID/PW)
    - 웹 크롤링이라 과도하게 요청하면 **IP가 차단**되므로 1개씩 순서대로 받고, 실패 시 재실행도 30분 뒤 1번만 합니다.
    - 설정: `date`(비우면 어제), `limit`(테스트용)
    """,
    site="https://data.krx.co.kr/",
    output="data/raw/krx/etf_constituents/<YYYYMMDD>/<ETF ISIN>.csv",
    retry_policy=dg.RetryPolicy(max_retries=1, delay=1800),
)
def etf_constituents(context: dg.AssetExecutionContext, config: ConstituentsConfig) -> dg.MaterializeResult:
    login_id, login_pw = require_env("KRX_AUTH_ID", "KRX_AUTH_PW")
    out_dir = settings.RAW_DIR / "krx" / "etf_constituents"
    result = krx.fetch_etf_constituents(login_id, login_pw, _as_of(config.date), out_dir, log=context.log, postfix=raw_postfix(), limit=config.limit, retries=ITEM_RETRIES)
    return item_result(context, result, out_dir)
