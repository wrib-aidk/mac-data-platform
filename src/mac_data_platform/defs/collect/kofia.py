"""KOFIA(금융투자협회 전자공시서비스) 수집 파이프라인. ⚠️ 외부망 PC에서는 접속 불가.

원본: wrib-crawler/scripts/kofia_dis/ (README "KOFIA 펀드 NAV 크롤러", "KOFIA 펀드 보유종목 크롤러")
- fund_nav: 펀드슈퍼마켓 펀드 목록의 fund_code 별 일별 NAV
- fund_holdings: 분기영업보고서의 펀드·ETF 전체 보유종목 → SQLite (차단 회피 로직 포함)
- fund_fees: 펀드별 보수비용비교 (펀드·ETF 전체, 기준일별 CSV)
- etf_prospectus: ETF 투자설명서 PDF (fund_fees 의 ETF 목록 기준)
"""

import logging

import dagster as dg

from mac_data_platform import settings
from mac_data_platform.collectors import kofia_fees, kofia_holdings, kofia_nav
from mac_data_platform.pipeline import ITEM_RETRIES, collect_asset, csv_preview, date_range, item_result, source_dir, write_error_log
from mac_data_platform.settings import raw_postfix

from .fundsupermarket import fund_list as fsm_fund_list
from .fundsupermarket import list_path as fsm_list_path


class FundNavConfig(dg.Config):
    start: str = "2019-01-01"  # 처음 받는 펀드의 시작일. 이미 받은 펀드는 마지막 날짜 7일 전부터만 받아 합침
    end: str = ""  # 비우면 어제
    lookback_days: int = 100
    workers: int = 4
    limit: int | None = None  # 테스트용


@collect_asset(
    "kofia",
    "fund_nav",
    """
    ### KOFIA 펀드별 일별 NAV  ⚠️ 외부망 PC에서 사용 불가
    **펀드슈퍼마켓 펀드 목록**(`fundsupermarket/fund_list`)의 `fund_code`마다 금융투자협회 전자공시서비스에서
    일별 기준가를 받아 `data/raw/kofia/funds/<fund_code>/nav.csv`에 저장합니다.

    | 컬럼 | 설명 |
    | --- | --- |
    | `date` | 기준일자 (`YYYYMMDD`) |
    | `nav` | 기준가격 |
    | `nav_change` | 전일대비 (원 단위 차이) |
    | `tax_nav` | 과표기준가격 (비과세 손익 제외, 배당소득세 계산 기준) |
    | `principal` | 설정원본 (백만원) |

    **갱신 규칙**: 처음 받는 펀드는 `start`(기본 2019-01-01)부터, 이미 받은 펀드는 마지막 날짜 7일 전부터 받아 합칩니다.

    설정: `start`, `end`, `workers`, `limit`(테스트용)
    """,
    site="https://dis.kofia.or.kr/",
    output="data/raw/kofia/funds/<fund_code>/nav.csv",
    deps=[fsm_fund_list],
)
def fund_nav(context: dg.AssetExecutionContext, config: FundNavConfig) -> dg.MaterializeResult:
    start, end = date_range(config.start, config.end, config.lookback_days)
    out_dir = source_dir("kofia") / "funds"
    result = kofia_nav.fetch_fund_nav(fsm_list_path(), out_dir, log=context.log, start=start, end=end, postfix=raw_postfix(), workers=config.workers, limit=config.limit, retries=ITEM_RETRIES)
    return item_result(context, result, out_dir, start=str(start), end=str(end))


class FundHoldingsConfig(dg.Config):
    command: str = "daily"  # daily = 요청 예산(budget)만큼 한 묶음 / backfill = 큐가 빌 때까지 "묶음 → 1~2시간 휴식" 반복
    target: str = "all"  # all / etf / fund
    include_private: bool = False
    quarters: list[str] = []  # 분기말 YYYYMMDD, 비우면 최근 lookback_days 안의 분기말
    lookback_days: int = 100
    companies: list[str] = []  # 테스트용: 운용사 코드 (예: A01031)
    budget: int = 1300  # 한 묶음 최대 요청 수 (±20% 무작위)
    daily_cap: int = 1300  # 하루(그날 실행 합계) 최대 요청 수. 다 쓰면 그날은 건너뜀, backfill 은 다음날 09:00~11:30 까지 대기
    include_class: bool = False  # True 면 클래스 펀드(A, C-Pe 등)도 수집. 기본은 클래스 펀드 제외
    force: bool = False  # 차단 쿨다운 무시


class _ToDagsterLog(logging.Handler):
    def __init__(self, context: dg.AssetExecutionContext) -> None:
        super().__init__()
        self.context = context

    def emit(self, record: logging.LogRecord) -> None:
        getattr(self.context.log, record.levelname.lower(), self.context.log.info)(record.getMessage())


@collect_asset(
    "kofia",
    "fund_holdings",
    """
    ### KOFIA 펀드·ETF 보유종목 (분기영업보고서)
    전자공시서비스 **펀드공시 > 분기영업보고서 > 운용현황 > 세부현황(유가증권내역)** 에서 펀드·ETF별 **전체 보유종목**
    (종목명, 유가증권 종류, 수량, 취득·시가·장부평가액, 발행지, 상장여부)을 받아 SQLite에 적재합니다.
    원본 작성자: 박현석

    - 보유내역은 **분기말 기준**(3/31, 6/30, 9/30, 12/31)으로만 제공, 운용사가 분기 종료 후 약 2개월 안에 제출
    - 종목코드는 없고 종목명만 제공. 클래스 펀드·자투자신탁은 모펀드 수익증권 1~2행만 나옴 (실제 종목은 `모투자신탁`/`(운용)` 펀드에 있음)
    - **클래스 펀드 제외**: 클래스(A, C-Pe, 종류S, Class P1 …)로 판별된 펀드는 `skipped` 로 건너뜀 (`funds.is_class`). 이미 받은 클래스 데이터는 그대로 둠

    | 테이블 | 설명 |
    | --- | --- |
    | `holdings` | 보유종목 (`std_dt` 기준일, `standard_cd` 펀드코드, `item_nm`, `security_type`, `quantity`, `market_amt` 등, 금액 단위 원), 원본은 `raw_json` |
    | `funds` | 펀드 목록 (`fund_nm`, `is_etf`, `is_private`, `is_class`, `start_dt` 등) |
    | `jobs` | 펀드별 수집 상태 (`pending` / `done` / `empty` / `error` / `skipped`) — 다음 실행 때 남은 것만 이어 받음 |
    | `company_filings` | 발견한 운용사별 영업보고서 제출 기록 (정정 보고서가 올라오면 해당 운용사 펀드를 다시 받음) |

    **차단 회피**: 요청 간격 2.5~25초 불규칙(중앙값 5.5초), 50~120건마다 1~3분 휴식, 한 번에 최대 `budget`(±20%)건, 하루 합계 `daily_cap`건.
    시작 전 메인 페이지 GET 으로 접속 확인. 403/429 또는 연결 실패 3회 연속이면 즉시 멈추고 24~48시간 쉽니다 (`BLOCKED` 로그, 재실행 안 함).

    설정
    - `command`: `daily`(요청 예산만큼 한 묶음) / `backfill`(큐가 빌 때까지 "묶음 → 1~2시간 휴식" 반복, 분기당 2만 개 이상일 때)
    - `target`: `all` / `etf` / `fund`, `quarters`: 분기말 `YYYYMMDD` (비우면 최근 `lookback_days` 안의 분기말)
    - `budget`, `daily_cap`, `include_class`, `include_private`, `companies`(테스트용 운용사 코드), `force`(쿨다운 무시)

    로그: `logs/kofia_holdings.log`
    """,
    site="https://dis.kofia.or.kr/",
    output="data/raw/kofia/kofia_holdings.db",
    retry_policy=dg.RetryPolicy(max_retries=1, delay=1800),  # 사이트 차단 위험이 있어 재실행을 줄임. 403/429 차단은 재실행 안 함
)
def fund_holdings(context: dg.AssetExecutionContext, config: FundHoldingsConfig) -> dg.MaterializeResult:
    db = settings.RAW_DIR / "kofia" / "kofia_holdings.db"
    logger = kofia_holdings.log
    logger.setLevel(logging.INFO)
    settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
    handlers = [_ToDagsterLog(context), logging.FileHandler(settings.LOG_DIR / "kofia_holdings.log", encoding="utf-8")]
    handlers[1].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    for h in handlers:
        logger.addHandler(h)
    try:
        code, last = kofia_holdings.run(
            db, command=config.command, target=config.target, include_private=config.include_private, quarters=config.quarters,
            lookback_days=config.lookback_days, companies=config.companies, budget=config.budget, daily_cap=config.daily_cap,
            include_class=config.include_class, force=config.force,
        )  # fmt: skip
    finally:
        for h in handlers:
            logger.removeHandler(h)
    outcome = last.get("outcome") or ""
    if code != 0:
        if outcome.startswith("blocked"):
            write_error_log("BLOCKED", asset="kofia/fund_holdings", run_id=context.run_id[:8], error=outcome, cooldown_until=kofia_holdings.cooldown_until(db))
            raise dg.Failure(f"KOFIA 차단 신호 — 쿨다운 {kofia_holdings.cooldown_until(db)}까지 수집 중단 ({outcome})", allow_retries=False)
        raise dg.Failure(f"KOFIA 보유종목 수집 실패: {outcome}")
    return dg.MaterializeResult(metadata={"outcome": outcome or "skipped (cooldown)", "requests": last.get("requests") or 0, "stats": last.get("stats_json") or "", "path": dg.MetadataValue.path(str(db))})


def fees_dir():  # noqa: ANN201
    return source_dir("kofia") / "fees"


def latest_fee_csv():  # noqa: ANN201
    """가장 최근 기준일의 보수비용 CSV (파일명 = 기준일이라 이름순 마지막)."""
    files = sorted(fees_dir().glob(f"*.csv{raw_postfix()}"))
    if not files:
        raise dg.Failure("보수비용 CSV 가 없습니다. kofia/fund_fees 를 먼저 실행하세요.", allow_retries=False)
    return files[-1]


def _blocked(context: dg.AssetExecutionContext, error: str) -> dg.Failure:
    write_error_log("BLOCKED", asset=context.asset_key.to_user_string(), run_id=context.run_id[:8], error=error)
    return dg.Failure(f"KOFIA 차단 신호 — 재실행하지 않고 멈춤 ({error}). 하루 이상 지난 뒤 다시 실행하세요.", allow_retries=False)


class FundFeesConfig(dg.Config):
    standard_dt: str = ""  # 기준일 YYYYMMDD. 비우면 협회 최신 기준일 (보통 직전 월말 영업일)


@collect_asset(
    "kofia",
    "fund_fees",
    """
    ### KOFIA 펀드별 보수비용 (펀드·ETF 전체)
    전자공시서비스 **펀드공시 > 펀드 보수 및 비용 > 펀드별 보수비용비교** 를 기준일 하나로 한 번에 받아
    `data/raw/kofia/fees/<기준일>.csv` 에 저장합니다 (약 2만 개 클래스, ETF 약 1,200개).

    | 컬럼 | 설명 |
    | --- | --- |
    | `standard_cd` | 펀드 표준코드 (`K55…`, `KR5…`. KRX 단축코드·ISIN 과 다름) |
    | `company_nm`, `fund_nm`, `fund_type`, `start_dt` | 운용회사, 펀드명, 펀드유형, 설정일 (`YYYYMMDD`) |
    | `manage_fee`, `sale_fee`, `trust_fee`, `admin_fee` | 보수율 운용·판매·수탁·사무관리 (%) |
    | `total_fee` | 보수율 합계 (A) |
    | `peer_avg_fee` | 유사유형 평균보수율 |
    | `other_expense` | 기타비용 (B) |
    | `ter` | 총보수·비용비율 TER (A+B) |
    | `front_load`, `back_load` | 판매수수료 선취·후취 (%) |
    | `trading_fee` | 매매·중개수수료율 (D) |

    **기준일**: 협회가 정한 공시 기준일만 조회됩니다 (보통 월말 영업일, 다른 날짜는 0건). 비우면 최신 기준일.
    이미 받은 기준일은 건너뜁니다 (기준일별 파일이 쌓임).

    설정: `standard_dt`
    """,
    site="https://dis.kofia.or.kr/",
    output="data/raw/kofia/fees/<기준일>.csv",
)
def fund_fees(context: dg.AssetExecutionContext, config: FundFeesConfig) -> dg.MaterializeResult:
    try:
        stats = kofia_fees.fetch_fees(fees_dir(), log=context.log, standard_dt=config.standard_dt, postfix=raw_postfix())
    except kofia_fees.Blocked as e:
        raise _blocked(context, str(e)) from e
    path = stats.pop("path")
    return dg.MaterializeResult(metadata={**stats, "path": dg.MetadataValue.path(str(path)), "preview": csv_preview(path, columns=["standard_cd", "fund_nm", "total_fee", "ter", "trading_fee"])})


class EtfProspectusConfig(dg.Config):
    workers: int = 1  # 동시 요청 수. 차단 위험이 있어 1 권장
    delay_seconds: float = 1.0  # 요청 사이 간격(초, ±30% 무작위)
    limit: int | None = None  # 테스트용: 앞에서부터 N개만


@collect_asset(
    "kofia",
    "etf_prospectus",
    """
    ### KOFIA ETF 투자설명서 PDF
    `kofia/fund_fees` 최신 CSV 에서 **ETF**(펀드명에 `상장지수`)만 골라, 표준코드마다 펀드 다모아 상세 팝업의
    투자설명서 파일을 받아 `data/raw/kofia/etf_prospectus/<standard_cd>/prospectus.pdf` 에 저장합니다.
    일반 펀드 투자설명서는 받지 않습니다 (펀드슈퍼마켓 수집에 있음).

    같은 폴더 `prospectus.json`: 원래 파일명(`original_filename`), 서버 파일명(`server_file`), 크기, 받은 시각.

    **갱신 규칙**: 서버 파일명이 지난번과 같으면 건너뛰고(`skipped`), 바뀌었으면 새 판으로 덮어씁니다(`docs_updated`).
    협회에 투자설명서가 등록되지 않은 ETF 는 `empty`. 403/429 를 받으면 나머지는 요청하지 않고 멈춥니다 (`BLOCKED`, 재실행 안 함).
    ETF 전체 약 1,200개 기준 처음 한 번은 약 1시간·2GB.

    설정: `workers`, `delay_seconds`, `limit`(테스트용)
    """,
    site="https://dis.kofia.or.kr/",
    output="data/raw/kofia/etf_prospectus/<standard_cd>/prospectus.pdf",
    deps=[fund_fees],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=1800),  # 사이트 차단 위험이 있어 재실행을 줄임
)
def etf_prospectus(context: dg.AssetExecutionContext, config: EtfProspectusConfig) -> dg.MaterializeResult:
    fee_csv = latest_fee_csv()
    out_dir = source_dir("kofia") / "etf_prospectus"
    result = kofia_fees.fetch_etf_prospectus(
        fee_csv, out_dir, log=context.log, postfix=raw_postfix(), workers=config.workers, delay=config.delay_seconds, limit=config.limit, retries=ITEM_RETRIES,
    )  # fmt: skip
    if result.counters.get("blocked"):
        raise _blocked(context, f"투자설명서 수집 중 403/429 ({result.summary()})")
    return item_result(context, result, out_dir, fee_csv=fee_csv.name)
