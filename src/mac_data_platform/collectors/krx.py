"""KRX: OPEN API(종목 목록, 일별 시세) + 정보데이터시스템 웹(ETF 구성종목).

원본: wrib-crawler/scripts/krx/*.py, scripts/krx_web/fetch_etf_constituents.py
- OPEN API: 인증키(KRX_AUTH_KEY) + API별 이용신청 필요
- 웹: 회원 ID/PW(KRX_AUTH_ID/KRX_AUTH_PW) 필요, 과도하게 요청하면 IP 차단됨
"""

import logging
import time
from datetime import date, timedelta
from pathlib import Path

import requests

from .common import Empty, ItemResult, Skipped, run_items, write_csv

OPEN_API = "https://data-dbg.krx.co.kr/svc/apis"
QUOTE_APIS = {  # 종류 → (API 경로들, 파일 접미사)
    "stock": ["sto/stk_bydd_trd", "sto/ksq_bydd_trd"],  # 코스피, 코스닥 일별매매정보
    "etf": ["etp/etf_bydd_trd"],  # ETF 일별매매정보
}


def _open_api(path: str, auth_key: str, bas_dd: str) -> list[dict]:
    for attempt in range(5):  # KRX sometimes returns 200 with a non-JSON body under load
        r = requests.get(f"{OPEN_API}/{path}", params={"basDd": bas_dd}, headers={"AUTH_KEY": auth_key}, timeout=60)
        r.raise_for_status()
        try:
            return r.json()["OutBlock_1"]
        except requests.JSONDecodeError:
            time.sleep(2**attempt)
    raise RuntimeError(f"{path} {bas_dd}: non-JSON response {r.text[:200]!r}")


def fetch_stock_list(auth_key: str, as_of: date, out: Path) -> int:
    """코스피·코스닥 종목기본정보."""
    rows = []
    for path in ("sto/stk_isu_base_info", "sto/ksq_isu_base_info"):
        rows += _open_api(path, auth_key, f"{as_of:%Y%m%d}")
    if not rows:
        raise RuntimeError(f"no data for {as_of}")
    write_csv(out, rows)
    return len(rows)


def fetch_daily_quotes(kind: str, auth_key: str, out_dir: Path, *, log: logging.Logger, start: date, end: date, postfix: str = "", workers: int = 8, retries: int = 2) -> ItemResult:
    """start~end 일자별로 out_dir/<YYYYMMDD>_<kind>.csv<postfix> 저장. 휴장일은 파일을 만들지 않음."""
    days = [f"{start + timedelta(days=i):%Y%m%d}" for i in range((end - start).days + 1)]

    def _fetch(bas_dd: str) -> None:
        out = out_dir / f"{bas_dd}_{kind}.csv{postfix}"
        if out.exists():
            raise Skipped
        rows = [row for path in QUOTE_APIS[kind] for row in _open_api(path, auth_key, bas_dd)]
        if not rows or (kind == "etf" and not any(row.get("TDD_CLSPRC") for row in rows)):  # ETF API는 휴장일에도 가격 없이 종목만 줌
            raise Empty("휴장일")
        write_csv(out, rows)

    return run_items(days, _fetch, log=log, workers=workers, retries=retries)


# ---------------------------------------------------------------------------- KRX 정보데이터시스템 웹

WEB_BASE = "https://data.krx.co.kr/contents/MDC/COMS/client"


class KrxWebClient:
    def __init__(self, login_id: str, login_pw: str) -> None:
        self.login_id, self.login_pw = login_id, login_pw
        self.session: requests.Session | None = None
        self.expiry = 0.0

    def _login(self) -> None:
        login_page, login_api = f"{WEB_BASE}/MDCCOMS001.cmd", f"{WEB_BASE}/MDCCOMS001D1.cmd"
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        s.get(login_page, timeout=15)
        s.get(f"{WEB_BASE}/view/login.jsp?site=mdc", headers={"Referer": login_page}, timeout=15)
        payload = {"mbrId": self.login_id, "pw": self.login_pw}
        code = s.post(login_api, data=payload, headers={"Referer": login_page}, timeout=15).json().get("_error_code", "")
        if code == "CD010":
            raise RuntimeError("KRX login failed: password change required.")
        if code == "CD011":  # 중복 로그인 → 기존 세션 무시하고 재전송
            payload["skipDup"] = "Y"
            code = s.post(login_api, data=payload, headers={"Referer": login_page}, timeout=15).json().get("_error_code", "")
        if code != "CD001":
            raise RuntimeError(f"KRX login failed ({code}): check credentials.")
        self.session, self.expiry = s, time.time() + 10800  # TTL 3시간

    def api(self, bld: str, **params: str) -> list[dict]:
        for attempt in range(3):
            if self.session is None or time.time() > self.expiry - 300:
                self._login()
            r = self.session.post(
                "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
                headers={"Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd", "X-Requested-With": "XMLHttpRequest"},
                data={**params, "bld": bld},
                timeout=60,
            )
            if r.text.strip() != "LOGOUT":  # 서버가 TTL 전에 세션을 끊는 경우가 있음
                r.raise_for_status()
                return r.json()["output"]
            self.session = None
            time.sleep(2**attempt)
        raise RuntimeError(f"{bld} {params}: still LOGOUT after re-login")


def fetch_etf_constituents(login_id: str, login_pw: str, trade_date: date, out_dir: Path, *, log: logging.Logger, postfix: str = "", limit: int | None = None, retries: int = 2) -> ItemResult:
    """현재 상장 ETF 전체의 구성종목을 out_dir/<YYYYMMDD>/<ETF ISIN>.csv<postfix> 로 저장. IP 차단 방지를 위해 순차 실행."""
    client = KrxWebClient(login_id, login_pw)
    etfs = client.api("dbms/MDC/STAT/standard/MDCSTAT04601")[:limit]
    day_dir = out_dir / f"{trade_date:%Y%m%d}"

    def _fetch(etf: dict) -> None:
        out = day_dir / f"{etf['ISU_CD']}.csv{postfix}"
        if out.exists():
            raise Skipped
        rows = client.api("dbms/MDC/STAT/standard/MDCSTAT05001", trdDd=f"{trade_date:%Y%m%d}", isuCd=etf["ISU_CD"])
        if not rows:
            raise Empty("휴장일 또는 미상장")
        write_csv(out, rows)

    return run_items(etfs, _fetch, log=log, key=lambda e: e["ISU_CD"], workers=1, retries=retries)
