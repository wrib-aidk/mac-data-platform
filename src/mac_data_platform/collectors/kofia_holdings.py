"""(원본: wrib-crawler/scripts/kofia_dis/fetch_fund_holdings.py, ⚠️ 외부망 PC에서 접속 불가)
KOFIA 전자공시 분기영업보고서의 펀드별 보유종목(세부현황 > 유가증권내역)을 SQLite에 적재한다.

  1. 운용사별 영업보고서 제출 목록   DISFundFTimeAnnSO.selectAnn   (uGb=R, tsCd=2RF00)
  2. 운용사의 펀드 목록(표준코드)     DISFundStandardCdSO.selectExcel
  3. 기준일별 보고서 양식            COMRptGridTreeSO.selectOutFrmWithInquiryGb (reportCd=DA010030020), DISComMetaRptGridSO.selRptGridInfo
  4. 펀드별 보유종목                 COMMeta{10,20,40}OutputSO.select

4번은 화면 JS가 SQL WHERE 조각(uWhereCond)을 직접 만들어 보내는 구조다.
화면과 똑같은 형태만 만들고, 끼워 넣는 값은 모두 정규식으로 검증한다.
"""

import argparse
import csv
import json
import logging
import math
import random
import re
import sqlite3
import ssl
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

import requests
from requests.adapters import HTTPAdapter

URL = "https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/"
REFERER = "https://dis.kofia.or.kr/websquare/index.jsp?w2xPath=/wq/fundann/DISFundAnnSrch.xml"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
HOLDINGS_REPORT_CD = "DA010030020"  # 분기영업보고서 > 운용현황 > 세부현황(유가증권내역)

RNG = random.Random()  # noqa: S311 - 요청 간격 지터용, 보안 용도 아님
log = logging.getLogger("kofia_holdings")


# ---------------------------------------------------------------------------- HTTP


class BlockedError(Exception):
    """서버가 차단/속도제한 신호(403/429)를 보냄. 그날 수집은 중단해야 한다."""


class BudgetExhaustedError(Exception):
    """이번 실행의 요청 예산을 다 썼음 (정상 종료)."""


class ServiceError(Exception):
    """ProFrame 서비스가 오류 헤더를 돌려줌 (요청 형식 문제 등)."""


class KofiaTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args: object, **kwargs: object) -> None:
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")  # SSLError - [SSL: DH_KEY_TOO_SMALL] dh key too small
        super().init_poolmanager(*args, **kwargs, ssl_context=ctx)


class Pacer:
    """불규칙한 요청 간격(로그정규, 중앙값 3.5초)과 50~120건마다 45~180초 휴식."""

    def __init__(self, median_delay: float = 3.5, min_delay: float = 1.5, max_delay: float = 20.0) -> None:
        self.mu = math.log(median_delay)
        self.min_delay, self.max_delay = min_delay, max_delay
        self._next_break = RNG.randint(50, 120)
        self._count = 0
        self._last = 0.0

    def wait(self) -> None:
        self._count += 1
        if self._count >= self._next_break:
            pause = RNG.uniform(45, 180)
            log.info("휴식 %.0f초 (요청 %d건 후)", pause, self._count)
            time.sleep(pause)
            self._count, self._next_break = 0, RNG.randint(50, 120)
        delay = min(self.max_delay, max(self.min_delay, RNG.lognormvariate(self.mu, 0.5)))
        if (gap := time.monotonic() - self._last) < delay:
            time.sleep(delay - gap)
        self._last = time.monotonic()


class KofiaClient:
    """요청 예산을 넘지 않고, 403/429를 받으면 즉시 멈춘다. IP/UA 교체 같은 우회는 하지 않는다."""

    def __init__(self, budget: int, max_retries: int = 3) -> None:
        self.budget, self.used, self.max_retries = budget, 0, max_retries
        self.pacer = Pacer()
        self.session = requests.Session()
        self.session.mount("https://dis.kofia.or.kr", KofiaTLSAdapter())
        self.session.headers.update({"User-Agent": USER_AGENT, "Referer": REFERER, "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"})

    @staticmethod
    def build_body(params: dict[str, Any]) -> str:
        """dict → XML. 값이 list면 같은 태그를 반복한다 (ProFrame 리스트 DTO)."""
        parts = []
        for key, value in params.items():
            for item in value if isinstance(value, list) else [value]:
                inner = KofiaClient.build_body(item) if isinstance(item, dict) else escape(str(item))
                parts.append(f"<{key}>{inner}</{key}>")
        return "".join(parts)

    def call(self, app: str, svc: str, fn: str, dto_name: str, params: dict[str, Any]) -> ET.Element:
        if self.used >= self.budget:
            raise BudgetExhaustedError(f"요청 예산 {self.budget}건 소진")
        body = (
            '<?xml version="1.0" encoding="utf-8"?><message><proframeHeader>'
            f"<pfmAppName>{app}</pfmAppName><pfmSvcName>{svc}</pfmSvcName><pfmFnName>{fn}</pfmFnName>"
            f"</proframeHeader><systemHeader></systemHeader><{dto_name}>{self.build_body(params)}</{dto_name}></message>"
        ).encode()
        for attempt in range(1, self.max_retries + 1):
            self.pacer.wait()
            self.used += 1
            try:
                r = self.session.post(URL, data=body, headers={"Content-Type": "text/xml; charset=utf-8"}, timeout=90)
            except (requests.ConnectionError, requests.Timeout) as e:
                self._backoff(attempt, f"네트워크 오류: {e}")
                continue
            if r.status_code in {403, 429}:
                raise BlockedError(f"HTTP {r.status_code} from {svc}.{fn}")
            if r.status_code >= 500:
                self._backoff(attempt, f"HTTP {r.status_code}")
                continue
            r.raise_for_status()
            if (message := ET.fromstring(r.content).find("message")) is None:  # noqa: S314
                raise ServiceError(f"unexpected response: {r.text[:200]!r}")
            header = message.find("proframeHeader")
            if header is not None and (header.findtext("pfmResponseType") == "S" or header.findtext("pfmResponseCode")):
                raise ServiceError(f"{svc}.{fn}: {header.findtext('pfmResponseCode')} {header.findtext('pfmResponseBasc')}")
            return message
        raise ServiceError(f"{svc}.{fn}: 재시도 {self.max_retries}회 실패")

    def _backoff(self, attempt: int, reason: str) -> None:
        wait = min(600, 15 * 2**attempt) * RNG.uniform(0.7, 1.3)
        log.warning("%s → %.0f초 후 재시도 (%d/%d)", reason, wait, attempt, self.max_retries)
        if attempt < self.max_retries:
            time.sleep(wait)


# ---------------------------------------------------------------------------- KOFIA API

_PATTERNS = {
    "company_cd": re.compile(r"^[A-Z][0-9]{5}$"),
    "standard_cd": re.compile(r"^[A-Z0-9]{12}$"),
    "std_dt": re.compile(r"^[0-9]{8}$"),
    "tx_cd": re.compile(r"^[0-9A-Z]{7}$"),
    "group_cd": re.compile(r"^[A-Z0-9]{1,10}$"),
    "col": re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$"),
}


def check(kind: str, value: str) -> str:
    if not _PATTERNS[kind].match(value or ""):
        raise ValueError(f"잘못된 {kind}: {value!r}")
    return value


def _text(node: ET.Element | None, tag: str) -> str:
    return "" if node is None else (node.findtext(tag) or "").strip()


def list_company_filings(client: KofiaClient, std_dt: str) -> list[dict[str, Any]]:
    """기준일(분기말)에 영업보고서를 제출한 운용사 목록."""
    check("std_dt", std_dt)
    params = {"uGb": "R", "vStrtDt": std_dt, "vEndDt": std_dt, "uCdList": "", "gbOption": "S", "uRptList": "2RF00", "tsCd": "2RF00", "uRptAllYN": "0", "companyCd": ""}
    msg = client.call("FS-DIS2", "DISFundFTimeAnnSO", "selectAnn", "DISFTimeAnnInsDTO", params)
    return [
        {"std_dt": _text(r, "standardDt"), "company_cd": _text(r, "companyCd"), "company_nm": _text(r, "koreanNm"), "seq": int(_text(r, "seq") or 1), "edit_yn": _text(r, "editYN")}
        for r in msg.findall("DISFTimeAnnListDTO/list")
    ]


def list_funds(client: KofiaClient, company_cd: str) -> list[dict[str, Any]]:
    """운용사의 현재 운용중인 펀드 목록 (펀드표준코드조회)."""
    msg = client.call("FS-DIS2", "DISFundStandardCdSO", "selectExcel", "DISStdCdPageDTO", {"companyCd": check("company_cd", company_cd)})
    funds = []
    for r in msg.iter("stdCdPage"):
        name = _text(r, "koreanCdtNm")
        funds.append(
            {
                "standard_cd": _text(r, "standardCd"),
                "company_cd": company_cd,
                "fund_nm": name,
                "fund_type": _text(r, "fundNm"),
                "short_cd": _text(r, "shortCd"),
                "start_dt": _text(r, "startDt"),
                "class_cd": _text(r, "classCd"),
                "is_etf": int("상장지수" in name.replace(" ", "")),
                "is_private": int("사모" in name),
            },
        )
    return funds


def resolve_form(client: KofiaClient, std_dt: str) -> dict[str, Any]:
    """기준일에 적용되는 '유가증권내역' 보고서 양식(컬럼/서비스명)."""
    check("std_dt", std_dt)
    msg = client.call("FS-COM", "COMRptGridTreeSO", "selectOutFrmWithInquiryGb", "COMOutFrmListDTO", {"standardDt": std_dt, "reportCd": HOLDINGS_REPORT_CD})
    if (item := msg.find("COMOutFrmListDTO/list")) is None:
        raise LookupError(f"{std_dt} 유가증권내역 양식 없음")
    form_no, tx_cd = _text(item, "outputFormNo"), check("tx_cd", _text(item, "txCd"))

    info = client.call("FS-DIS2", "DISComMetaRptGridSO", "selRptGridInfo", "DISComMetaRptFormInfoDTO", {"standardDt": std_dt, "outputFormNo": form_no}).find("DISComMetaRptFormInfoDTO")
    if info is None:
        raise LookupError(f"양식 {form_no} 메타 없음")
    form = info.find(f"DISComOutFormListDTO/DISComOutFormDTO[outputFormNo='{form_no}']")
    if form is None or _text(form, "outputPrcGb") not in {"01", "1"}:
        raise NotImplementedError(f"양식 {form_no}: 동적 조회형이 아님")
    headers = [h for h in info.findall("DISComOutColHdrListDTO/DISComOutColHdrDTO") if _text(h, "outputFormNo") == form_no]
    if any(_text(h, "columnHdrgb") == "R" for h in headers):
        raise NotImplementedError(f"양식 {form_no}: 행 헤더형은 미지원")
    data_cols = info.findall("DISComOutDataColListDTO/DISComOutDataColDTO")
    columns = []
    for i, h in enumerate(h for h in headers if _text(h, "columnHdrgb") == "C"):
        d = data_cols[i] if i < len(data_cols) else None
        columns.append(
            {"label": _text(h, "headerNm"), "header_typ": _text(h, "headerTyp"), "col": check("col", _text(d, "outputColCd")), "cd_gb": _text(d, "outputCdGb"), "group_cd": _text(d, "groupCd")},
        )
    width = next((w for w in (10, 20, 40) if len(columns) <= w), None)
    if width is None:
        raise NotImplementedError(f"양식 {form_no}: 컬럼 {len(columns)}개")
    return {"std_dt": std_dt, "output_form_no": form_no, "tx_cd": tx_cd, "target_tab": check("col", _text(form, "objNm")), "so_name": f"COMMeta{width}OutputSO", "width": width, "columns": columns}


def _db_func(col: dict[str, str]) -> str:
    """disMeta.js getDBFunc() 재현."""
    name, gb = col["col"], col["cd_gb"]
    if gb == "01":
        return f" FC_Get_CompanyNm({name}) "
    if gb == "02":
        return f" FC_Get_StandardNm({name}) "
    if gb == "03":
        return f"(SELECT CODE_NM FROM TC_COMMONCODE WHERE GROUP_CD = '{check('group_cd', col['group_cd'])}' AND COMMON_CD =  {name})"  # noqa: S608 - 화면 JS와 같은 형태, 값은 정규식 검증
    return name


def fetch_holdings(client: KofiaClient, form: dict[str, Any], company_cd: str, standard_cd: str) -> list[dict[str, str]]:
    """펀드 하나의 보유종목. 양식 컬럼 라벨을 키로 하는 dict 목록."""
    check("company_cd", company_cd)
    check("standard_cd", standard_cd)
    std_dt, tx_cd = check("std_dt", form["std_dt"]), check("tx_cd", form["tx_cd"])
    where = f"company_cd='{company_cd}' AND standard_cd='{standard_cd}' AND standard_dt='{std_dt}' AND tx_cd='{tx_cd}' "
    extlist = [{"extType": "om", "ext1": "^", "ext2": f"{c['col']}^-1", "ext3": f"{c['header_typ']}†{_db_func(c)}"} for c in form["columns"]]
    params = {
        "uRowHdrCnt": 0,
        "targetTabId": form["target_tab"],
        "vGridMaxCol": form["width"],
        "uWhereCond": where,
        "dbio_total_count_": len(form["columns"]),
        "extlist": extlist,
        "standardDt": std_dt,
        "standardCd": standard_cd,
        "txCd": tx_cd,
        "vStrtDt": "",
        "vEndDt": "",
    }
    msg = client.call("FS-COM", form["so_name"], "select", "COMMetaInputDTO", params)
    return [{c["label"]: _text(item, f"val{i + 1}") for i, c in enumerate(form["columns"])} for item in msg.findall(f"COMMeta{form['width']}OutputListDTO/list")]


# ---------------------------------------------------------------------------- SQLite

SCHEMA = """
CREATE TABLE IF NOT EXISTS company_filings (std_dt TEXT, company_cd TEXT, seq INTEGER, company_nm TEXT, edit_yn TEXT, first_seen TEXT, PRIMARY KEY (std_dt, company_cd, seq));
CREATE TABLE IF NOT EXISTS funds (standard_cd TEXT PRIMARY KEY, company_cd TEXT, fund_nm TEXT, fund_type TEXT, short_cd TEXT, start_dt TEXT, class_cd TEXT, is_etf INTEGER, is_private INTEGER, updated_at TEXT);
CREATE TABLE IF NOT EXISTS forms (std_dt TEXT PRIMARY KEY, form_json TEXT, fetched_at TEXT);
CREATE TABLE IF NOT EXISTS jobs (
    std_dt TEXT, standard_cd TEXT, company_cd TEXT, filing_seq INTEGER,
    status TEXT DEFAULT 'pending',  -- pending | done | empty | error
    attempts INTEGER DEFAULT 0, n_rows INTEGER, last_error TEXT, updated_at TEXT,
    PRIMARY KEY (std_dt, standard_cd)
);
CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status);
CREATE TABLE IF NOT EXISTS holdings (
    std_dt TEXT, standard_cd TEXT, line_no INTEGER, company_cd TEXT,
    item_nm TEXT, security_type TEXT, quantity REAL, acquisition_amt REAL, market_amt REAL, book_amt REAL,
    issue_region TEXT, listing TEXT, issue_dt TEXT, coupon_rate REAL, raw_json TEXT, loaded_at TEXT,
    PRIMARY KEY (std_dt, standard_cd, line_no)
);
CREATE INDEX IF NOT EXISTS holdings_item ON holdings(std_dt, item_nm);
CREATE TABLE IF NOT EXISTS runs (run_id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, finished_at TEXT, requests INTEGER, outcome TEXT, stats_json TEXT);
CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
"""
# 양식 라벨 → holdings 컬럼. 양식이 바뀌어도 원본은 raw_json에 남는다.
LABEL_MAP = {
    "종목명": "item_nm",
    "유가증권 종류": "security_type",
    "수량": "quantity",
    "취득가액": "acquisition_amt",
    "시가평가액": "market_amt",
    "장부평가액": "book_amt",
    "발행지": "issue_region",
    "상장등록여부": "listing",
    "발행일": "issue_dt",
    "발행이율": "coupon_rate",
}
NUMERIC = {"quantity", "acquisition_amt", "market_amt", "book_amt", "coupon_rate"}
TARGET_SQL = {"all": "1=1", "etf": "f.is_etf=1", "fund": "f.is_etf=0"}


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def target_sql(target: str, include_private: bool) -> str:
    """funds(별칭 f) 기준 대상 필터. target은 argparse choices로 검증된 값만 들어온다."""
    return TARGET_SQL[target] + ("" if include_private else " AND f.is_private=0")


def _num(v: str) -> float | None:
    try:
        return float(s) if (s := re.sub(r"[,\s]", "", v or "")) else None
    except ValueError:
        return None


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    def get(self, k: str) -> str | None:
        row = self.conn.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return row["v"] if row else None

    def put(self, k: str, v: str) -> None:
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO kv VALUES (?,?)", (k, v))

    def known_filing_seq(self, std_dt: str, company_cd: str) -> int | None:
        return self.conn.execute("SELECT MAX(seq) FROM company_filings WHERE std_dt=? AND company_cd=?", (std_dt, company_cd)).fetchone()[0]

    def add_filing(self, f: dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO company_filings VALUES (?,?,?,?,?,?)", (f["std_dt"], f["company_cd"], f["seq"], f["company_nm"], f["edit_yn"], now()))

    def upsert_funds(self, funds: list[dict[str, Any]]) -> None:
        with self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO funds VALUES (:standard_cd,:company_cd,:fund_nm,:fund_type,:short_cd,:start_dt,:class_cd,:is_etf,:is_private,:updated_at)",
                [{**f, "updated_at": now()} for f in funds],
            )

    def enqueue(self, std_dt: str, company_cd: str, filing_seq: int, standard_cds: list[str]) -> None:
        """새 작업 추가. 정정(seq 증가)이면 기존 작업도 다시 pending으로."""
        with self.conn:
            self.conn.executemany(
                "INSERT INTO jobs (std_dt, standard_cd, company_cd, filing_seq, status, updated_at) VALUES (?,?,?,?,'pending',?) "
                "ON CONFLICT(std_dt, standard_cd) DO UPDATE SET status='pending', attempts=0, filing_seq=excluded.filing_seq, updated_at=excluded.updated_at "
                "WHERE excluded.filing_seq > jobs.filing_seq",
                [(std_dt, cd, company_cd, filing_seq, now()) for cd in standard_cds],
            )

    def get_form(self, std_dt: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT form_json FROM forms WHERE std_dt=?", (std_dt,)).fetchone()
        return json.loads(row["form_json"]) if row else None

    def put_form(self, form: dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO forms VALUES (?,?,?)", (form["std_dt"], json.dumps(form, ensure_ascii=False), now()))

    def pending_jobs(self, target: str, include_private: bool, quarters: list[str] | None = None, max_attempts: int = 3) -> list[dict[str, Any]]:
        sql = f"SELECT j.*, f.start_dt FROM jobs j JOIN funds f USING (standard_cd) WHERE (j.status='pending' OR (j.status='error' AND j.attempts < ?)) AND {target_sql(target, include_private)}"  # noqa: S608
        params: list[Any] = [max_attempts]
        if quarters:
            sql += f" AND j.std_dt IN ({','.join('?' * len(quarters))})"
            params += quarters
        return [dict(r) for r in self.conn.execute(sql, params)]

    def save_holdings(self, job: dict[str, Any], rows: list[dict[str, str]]) -> None:
        ts, records = now(), []
        for i, raw in enumerate(rows, 1):
            rec: dict[str, Any] = dict.fromkeys(LABEL_MAP.values())
            for label, val in raw.items():
                if col := LABEL_MAP.get(label):
                    rec[col] = _num(val) if col in NUMERIC else (val or None)
            records.append((job["std_dt"], job["standard_cd"], i, job["company_cd"], *(rec[c] for c in LABEL_MAP.values()), json.dumps(raw, ensure_ascii=False), ts))
        with self.conn:  # 한 펀드 단위로 교체 (정정 재수집 대비)
            self.conn.execute("DELETE FROM holdings WHERE std_dt=? AND standard_cd=?", (job["std_dt"], job["standard_cd"]))
            self.conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", records)
            self.conn.execute(
                "UPDATE jobs SET status=?, n_rows=?, attempts=attempts+1, last_error=NULL, updated_at=? WHERE std_dt=? AND standard_cd=?",
                ("done" if records else "empty", len(records), ts, job["std_dt"], job["standard_cd"]),
            )

    def fail_job(self, job: dict[str, Any], err: Exception) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE jobs SET status='error', attempts=attempts+1, last_error=?, updated_at=? WHERE std_dt=? AND standard_cd=?",
                (str(err)[:500], now(), job["std_dt"], job["standard_cd"]),
            )

    def start_run(self) -> int:
        with self.conn:
            return self.conn.execute("INSERT INTO runs (started_at) VALUES (?)", (now(),)).lastrowid or 0

    def finish_run(self, run_id: int, requests_used: int, outcome: str, stats: dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute("UPDATE runs SET finished_at=?, requests=?, outcome=?, stats_json=? WHERE run_id=?", (now(), requests_used, outcome, json.dumps(stats, ensure_ascii=False), run_id))


# ---------------------------------------------------------------------------- pipeline


def recent_quarter_ends(today: date, lookback_days: int) -> list[str]:
    """lookback_days 안의 분기말들 (영업보고서 제출 기한은 분기 종료 후 약 2개월)."""
    quarters = (date(y, m, d) for y in (today.year - 1, today.year) for m, d in ((3, 31), (6, 30), (9, 30), (12, 31)))
    return [f"{q:%Y%m%d}" for q in quarters if timedelta(0) <= today - q <= timedelta(days=lookback_days)]


def discover(client: KofiaClient, store: Store, args: argparse.Namespace, stats: dict[str, Any]) -> None:
    """새로 올라온(또는 정정된) 운용사 영업보고서 → 그 운용사 펀드 전부를 큐에 넣는다.

    대상(ETF/펀드/사모) 필터는 work 단계에서 건다. 여기서 거르면 나중에 대상을 바꿨을 때
    이미 '발견됨'으로 기록된 보고서의 펀드들이 큐에 안 들어온다.
    """
    stats.update(quarters=[], new_filings=0, enqueued=0)
    for std_dt in args.quarter or recent_quarter_ends(date.today(), args.lookback_days):
        stats["quarters"].append(std_dt)
        filings = [f for f in list_company_filings(client, std_dt) if not args.company or f["company_cd"] in args.company]
        log.info("[발견] %s 영업보고서 제출 운용사 %d곳", std_dt, len(filings))
        RNG.shuffle(filings)
        for f in filings:
            known = store.known_filing_seq(std_dt, f["company_cd"])
            if known is not None and f["seq"] <= known:
                continue
            funds = [x for x in list_funds(client, f["company_cd"]) if x["standard_cd"]]
            store.upsert_funds(funds)
            store.enqueue(std_dt, f["company_cd"], f["seq"], [x["standard_cd"] for x in funds])
            store.add_filing(f)  # 큐 적재가 끝난 뒤 기록해야 중간 실패 시 다음 실행에서 다시 잡힌다
            stats["new_filings"] += 1
            stats["enqueued"] += len(funds)
            log.info("[발견] %s %s(seq %d%s) → 펀드 %d개 적재", std_dt, f["company_nm"], f["seq"], ", 정정" if known else "", len(funds))


def work(client: KofiaClient, store: Store, args: argparse.Namespace, stats: dict[str, Any]) -> None:
    """큐의 작업을 무작위 순서로, 예산이 남는 동안 처리한다."""
    jobs = store.pending_jobs(args.target, args.include_private, args.quarter)
    RNG.shuffle(jobs)
    stats.update(target=args.target, pending=len(jobs), done=0, empty=0, error=0)
    forms: dict[str, dict[str, Any]] = {}
    consecutive_errors = 0
    for job in jobs:
        if job["start_dt"] and job["start_dt"] > job["std_dt"]:  # 기준일 이후 설정된 펀드는 그 분기 보유내역이 없다
            store.save_holdings(job, [])
            stats["empty"] += 1
            continue
        try:
            if (form := forms.get(job["std_dt"]) or store.get_form(job["std_dt"])) is None:
                form = resolve_form(client, job["std_dt"])
                store.put_form(form)
            forms[job["std_dt"]] = form
            rows = fetch_holdings(client, form, job["company_cd"], job["standard_cd"])
            store.save_holdings(job, rows)
            stats["done" if rows else "empty"] += 1
            consecutive_errors = 0
        except (ServiceError, ValueError, LookupError, NotImplementedError) as e:
            store.fail_job(job, e)
            stats["error"] += 1
            consecutive_errors += 1
            log.warning("[수집] %s %s 실패: %s", job["std_dt"], job["standard_cd"], e)
            if consecutive_errors >= 5:
                raise ServiceError("연속 5건 실패 — 요청 형식이 바뀌었을 수 있어 중단") from e
        if (n := stats["done"] + stats["empty"]) % 50 == 0 and n:
            log.info("[수집] %d/%d (요청 %d/%d)", n, len(jobs), client.used, client.budget)


def run_once(args: argparse.Namespace, steps: tuple[str, ...]) -> int:
    """discover/work 한 묶음. 예산 소진은 정상 종료, 차단이면 24~48시간 쿨다운을 기록한다."""
    store = Store(args.db)
    if (cooldown := store.get("cooldown_until")) and datetime.fromisoformat(cooldown) > datetime.now() and not args.force:
        log.warning("차단 감지 후 쿨다운 중 (%s까지) — 건너뜀", cooldown)
        return 0
    client = KofiaClient(budget=int(args.budget * RNG.uniform(0.8, 1.2)))  # 매일 요청량도 조금씩 다르게
    run_id, stats, outcome = store.start_run(), {}, "ok"
    try:
        for step in steps:  # 중간에 멈춰도 그때까지의 통계가 남도록 dict를 넘긴다
            stats[step] = {}
            {"discover": discover, "work": work}[step](client, store, args, stats[step])
    except BudgetExhaustedError:
        outcome = "budget_exhausted"  # 남은 작업은 다음 실행에서 이어서
    except BlockedError as e:
        until = datetime.now() + timedelta(hours=RNG.uniform(24, 48))
        store.put("cooldown_until", until.isoformat(timespec="seconds"))
        outcome = f"blocked: {e}"
        log.error("차단 신호 %s — %s까지 수집 중단", e, until)
    except ServiceError as e:
        outcome = f"error: {e}"
        log.error("%s", e)
    except (SystemExit, KeyboardInterrupt):
        outcome = "interrupted"
        raise
    finally:
        store.finish_run(run_id, client.used, outcome, stats)
        log.info("[종료] %s, 요청 %d건, %s", outcome, client.used, stats)
    return 0 if outcome in {"ok", "budget_exhausted"} else 1


def backfill(args: argparse.Namespace) -> int:
    """'묶음 → 1~2시간 휴식'을 큐가 빌 때까지 반복하고, 끝나면 macOS 알림을 띄운다."""
    store = Store(args.db)
    batch = 0
    while left := len(store.pending_jobs(args.target, args.include_private, args.quarter)):
        if (cooldown := store.get("cooldown_until")) and (until := datetime.fromisoformat(cooldown)) > datetime.now():
            wait = (until - datetime.now()).total_seconds() + RNG.uniform(300, 1800)
            log.info("[백필] 차단 쿨다운 중 → %.1f시간 대기", wait / 3600)
            time.sleep(wait)
            continue
        batch += 1
        log.info("[백필] 묶음 %d 시작 (남은 %d)", batch, left)
        code = run_once(args, ("discover", "work"))
        after = len(store.pending_jobs(args.target, args.include_private, args.quarter))
        log.info("[백필] 묶음 %d 종료 (이번에 %d건 처리, 남은 %d)", batch, left - after, after)
        if code != 0 and left == after and not store.get("cooldown_until"):
            notify("KOFIA 백필 중단", f"{args.target}: 진척 없이 실패, 남은 {after}건")
            return 1
        if after:
            gap = RNG.uniform(args.gap_min, args.gap_max) * 60
            log.info("[백필] %.0f분 휴식", gap / 60)
            time.sleep(gap)
    funds, rows = store.conn.execute(
        f"SELECT COUNT(DISTINCT h.standard_cd), COUNT(*) FROM holdings h JOIN funds f USING (standard_cd) WHERE {target_sql(args.target, args.include_private)}",  # noqa: S608
    ).fetchone()
    msg = f"{args.target} 완료: 펀드 {funds}개, {rows}행"
    log.info("[백필] %s", msg)
    args.db.with_name(f"{args.db.stem}_backfill_{args.target}.done").write_text(msg + "\n", encoding="utf-8")
    notify("KOFIA 백필 완료", msg)
    return 0


def notify(title: str, text: str) -> None:
    if sys.platform == "darwin":
        subprocess.run(["/usr/bin/osascript", "-e", f'display notification "{text}" with title "{title}"'], check=False)  # noqa: S603


def status(args: argparse.Namespace) -> int:
    store, where = Store(args.db), target_sql(args.target, args.include_private)
    kind = "CASE f.is_etf WHEN 1 THEN 'ETF' ELSE 'fund' END"
    sections = {
        "jobs": f"SELECT j.std_dt, {kind} kind, j.status, COUNT(*) n FROM jobs j JOIN funds f USING (standard_cd) WHERE {where} GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",  # noqa: S608
        "holdings": f"SELECT h.std_dt, {kind} kind, COUNT(DISTINCT h.standard_cd) funds, COUNT(*) n_rows FROM holdings h JOIN funds f USING (standard_cd) WHERE {where} GROUP BY 1, 2",  # noqa: S608
        "recent runs": "SELECT * FROM runs ORDER BY run_id DESC LIMIT 5",
    }
    out = [f"[target: {args.target}{', private' if args.include_private else ''}]"]
    for title, sql in sections.items():
        out += [f"== {title}", *(f"  {dict(r)}" for r in store.conn.execute(sql))]
    sys.stdout.write("\n".join(out) + "\n")
    return 0


def export(args: argparse.Namespace) -> int:
    store = Store(args.db)
    out = args.out or Path(f"raw/fund_holdings_{args.std_dt}.csv{args.postfix}")
    cur = store.conn.execute(
        f"SELECT h.*, f.fund_nm, f.is_etf FROM holdings h LEFT JOIN funds f USING (standard_cd) WHERE h.std_dt=? AND {target_sql(args.target, args.include_private)} ORDER BY h.standard_cd, h.line_no",  # noqa: S608
        (args.std_dt,),
    )
    cols = [d[0] for d in cur.description if d[0] != "raw_json"]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as f:  # 엑셀에서 바로 열리도록 BOM
        w = csv.writer(f, lineterminator="\n")
        w.writerow(cols)
        rows = [[row[c] for c in cols] for row in cur]
        w.writerows(rows)
    log.info("%d행 → %s", len(rows), out)
    return 0


# ---------------------------------------------------------------------------- Dagster 실행부 (원본 main() 대체)


def run(db: Path, *, command: str = "daily", target: str = "all", include_private: bool = False, quarters: list[str] | None = None, lookback_days: int = 100, companies: list[str] | None = None, budget: int = 1200, gap_min: int = 60, gap_max: int = 120, force: bool = False) -> tuple[int, dict[str, Any]]:
    """daily(=discover+work 한 묶음) 또는 backfill(큐가 빌 때까지 반복). (종료코드, 마지막 실행 기록)을 돌려줌."""
    args = argparse.Namespace(
        db=db, target=target, include_private=include_private, quarter=quarters or None, lookback_days=lookback_days,
        company=companies or None, budget=budget, gap_min=gap_min, gap_max=gap_max, force=force,
    )  # fmt: skip
    code = backfill(args) if command == "backfill" else run_once(args, ("discover", "work"))
    last = Store(db).conn.execute("SELECT * FROM runs ORDER BY run_id DESC LIMIT 1").fetchone()
    return code, dict(last) if last else {}


def cooldown_until(db: Path) -> str | None:
    return Store(db).get("cooldown_until") if db.exists() else None
