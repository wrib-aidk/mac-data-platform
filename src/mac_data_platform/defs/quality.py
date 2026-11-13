"""데이터 품질 체크 (Dagster asset check). DB 적재 asset 이 실행될 때마다 같이 돌고, UI 의 asset → Checks 탭에 결과가 쌓인다.

  freshness   마지막 날짜가 quality.freshness_days 보다 오래됐나            (ERROR)
  row_count   최근 날짜의 행 수가 이전 날짜들 중앙값의 row_count_ratio 미만인가 (최근 settle_dates 개는 WARN, 그 이전은 ERROR)
  null_spike  컬럼별 빈 값 비율이 평소보다 null_increase 이상 늘었나 / jsonb 키가 사라졌나 (WARN)
  price_rules KRX 시세 값끼리 맞나: 종가>0, 음수 없음, 저가≤종가≤고가, 등락률=대비/전일종가 (ERROR)

기준값은 config/pipelines.yaml 의 quality: 에서 바꾼다. 실패하면 logs/errors.log 에도 [CHECK_FAILED] 로 남는다.
"""

import statistics
from dataclasses import dataclass

import dagster as dg
from psycopg import sql

from mac_data_platform import settings
from mac_data_platform.pipeline import md_table, write_error_log

from .resources import PostgresResource


@dataclass(frozen=True)
class Table:
    name: str            # raw.<name>
    date_col: str        # 날짜(또는 스냅샷 날짜) 컬럼
    daily: bool = True   # 영업일마다 쌓이면 True, 분기 스냅샷이면 False
    price: bool = False  # KRX 시세 규칙 적용


TABLES = [
    Table("krx_stock_daily", "bas_dd", price=True),
    Table("krx_etf_daily", "bas_dd", price=True),
    Table("krx_etf_constituents", "trade_date"),
    Table("seibro_fund_nav", "date"),
    Table("kofia_fund_nav", "date"),
    Table("kofia_holdings", "std_dt", daily=False),
    Table("kofia_fund_fees", "standard_dt", daily=False),
    Table("fundsupermarket_fund_list", "snapshot_date", daily=False),
    Table("fundsupermarket_fund_details", "snapshot_date", daily=False),
]

DEFAULTS = {
    "freshness_days": 100,   # 수집 주기(3개월) + 여유. 주기를 줄이면 같이 줄일 것
    "row_count_ratio": 0.9,  # 이전 날짜 중앙값의 90% 미만이면 실패
    "scan_dates": 10,        # 최근 몇 개 날짜를 검사할지
    "baseline_dates": 20,    # 비교 기준으로 쓸 이전 날짜 수
    "settle_dates": 3,       # 가장 최근 몇 개 날짜는 아직 덜 들어왔을 수 있어 WARN 으로만
    "null_increase": 0.2,    # 빈 값 비율이 20%p 이상 늘면 경고
    "tables": {},            # 테이블별 덮어쓰기. 예) tables: {seibro_fund_nav: {freshness_days: 10}}
}


def _cfg(table: str) -> dict:
    q = {**DEFAULTS, **(settings.config().get("quality") or {})}
    return {**q, **(q.get("tables") or {}).get(table, {})}


def _ident(*parts: str) -> sql.Identifier:
    return sql.Identifier(*parts)


def _result(t: Table, check: str, passed: bool, severity: dg.AssetCheckSeverity, summary: str, **metadata: object) -> dg.AssetCheckResult:
    if not passed:
        write_error_log("CHECK_FAILED", table=f"raw.{t.name}", check=check, severity=severity.value, detail=summary)
    return dg.AssetCheckResult(passed=passed, severity=severity, description=summary, metadata={"요약": summary, **metadata})


def _date_counts(conn, t: Table, n: int) -> list[tuple]:  # noqa: ANN001
    """최근 n 개 날짜의 (날짜, 행 수), 최신순. 일별 테이블은 날짜 인덱스로 최근 구간만 읽는다."""
    tbl, d = _ident("raw", t.name), _ident(t.date_col)
    where = sql.SQL("WHERE {d} >= (SELECT max({d}) FROM {tbl}) - {days}").format(d=d, tbl=tbl, days=sql.Literal(n * 3)) if t.daily else sql.SQL("")
    q = sql.SQL("SELECT {d}, count(*) FROM {tbl} {where} GROUP BY 1 ORDER BY 1 DESC LIMIT {n}").format(d=d, tbl=tbl, where=where, n=sql.Literal(n))
    return conn.execute(q).fetchall()


# ---------------------------------------------------------------------------
def _freshness(t: Table, conn) -> dg.AssetCheckResult:  # noqa: ANN001
    c = _cfg(t.name)
    latest = conn.execute(sql.SQL("SELECT max({}) FROM {}").format(_ident(t.date_col), _ident("raw", t.name))).fetchone()[0]
    if latest is None:
        return _result(t, "freshness", False, dg.AssetCheckSeverity.WARN, "테이블이 비어 있음 (수집 job 이 꺼져 있는지 확인)")
    age = (settings.today() - latest).days
    ok = age <= c["freshness_days"]
    return _result(
        t, "freshness", ok, dg.AssetCheckSeverity.ERROR,
        f"마지막 날짜 {latest} ({age}일 전, 기준 {c['freshness_days']}일)" + ("" if ok else " — 수집이 멈췄는지 확인"),
        latest=str(latest), age_days=age,
    )


def _row_count(t: Table, conn) -> dg.AssetCheckResult:  # noqa: ANN001
    c = _cfg(t.name)
    counts = _date_counts(conn, t, c["scan_dates"] + c["baseline_dates"])
    if len(counts) < 2:
        return _result(t, "row_count", True, dg.AssetCheckSeverity.WARN, f"날짜가 {len(counts)}개뿐이라 비교 생략")
    rows, bad = [], []
    for i, (day, n) in enumerate(counts[: c["scan_dates"]]):
        prev = [m for _, m in counts[i + 1 : i + 1 + c["baseline_dates"]]]
        if not prev:
            break
        base = statistics.median(prev)
        ratio = n / base if base else 1.0
        rows.append({"날짜": day, "행 수": n, "기준(중앙값)": int(base), "비율": f"{ratio:.0%}"})
        if ratio < c["row_count_ratio"]:
            bad.append((i, day, n, int(base)))
    if not bad:
        return _result(t, "row_count", True, dg.AssetCheckSeverity.ERROR, f"최근 {len(rows)}개 날짜 행 수 정상", 날짜별=md_table(rows))
    old = [b for b in bad if b[0] >= c["settle_dates"]]
    detail = ", ".join(f"{day} {n:,}행(기준 {base:,})" for _, day, n, base in bad)
    if old:
        return _result(t, "row_count", False, dg.AssetCheckSeverity.ERROR, f"행 수가 적은 날짜: {detail} — 일부만 수집됐을 수 있음", 날짜별=md_table(rows))
    return _result(
        t, "row_count", False, dg.AssetCheckSeverity.WARN,
        f"최근 날짜 행 수가 적음: {detail} — 일부 항목만 수집됐거나 원천 반영이 늦은 경우. 원인은 수집 run 기록으로 확인", 날짜별=md_table(rows),
    )


def _null_spike(t: Table, conn) -> dg.AssetCheckResult:  # noqa: ANN001
    c = _cfg(t.name)
    counts = _date_counts(conn, t, c["baseline_dates"] + 1)
    if len(counts) < 2:
        return _result(t, "null_spike", True, dg.AssetCheckSeverity.WARN, f"날짜가 {len(counts)}개뿐이라 비교 생략")
    # 가장 최근의 '다 들어온' 날짜를 검사 대상으로 (덜 들어온 날은 비율이 흔들림)
    med = statistics.median(n for _, n in counts)
    target = next((d for d, n in counts if n >= c["row_count_ratio"] * med), counts[0][0])
    prev_days = [d for d, _ in counts if d < target][: c["baseline_dates"]]
    if not prev_days:
        return _result(t, "null_spike", True, dg.AssetCheckSeverity.WARN, "이전 날짜가 없어 비교 생략")

    cols = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = %s ORDER BY ordinal_position",
        (t.name,),
    ).fetchall()
    plain = [n for n, ty in cols if ty != "jsonb" and n != t.date_col]
    jsonb = [n for n, ty in cols if ty == "jsonb"]
    tbl, d = _ident("raw", t.name), _ident(t.date_col)

    ratios = sql.SQL(", ").join(sql.SQL("avg(({} IS NULL)::int)").format(_ident(n)) for n in plain)
    q = sql.SQL("SELECT {d}, {r} FROM {tbl} WHERE {d} = ANY(%s) GROUP BY 1").format(d=d, r=ratios, tbl=tbl)
    by_day = {r[0]: r[1:] for r in conn.execute(q, ([target, *prev_days],)).fetchall()}
    rows, bad = [], []
    for j, name in enumerate(plain):
        now = float(by_day[target][j])
        base = statistics.median(float(by_day[p][j]) for p in prev_days if p in by_day)
        rows.append({"컬럼": name, f"{target}": f"{now:.1%}", "평소": f"{base:.1%}"})
        if now - base >= c["null_increase"]:
            bad.append(f"{name} {base:.0%}→{now:.0%}")

    for name in jsonb:  # 원천 응답 구조 변화: 이전 날짜에 있던 키가 사라졌나
        kq = sql.SQL("SELECT DISTINCT jsonb_object_keys({c}) FROM {tbl} WHERE {d} = %s AND jsonb_typeof({c}) = 'object'").format(c=_ident(name), tbl=tbl, d=d)
        now_keys = {r[0] for r in conn.execute(kq, (target,))}
        prev_keys = {r[0] for r in conn.execute(kq, (prev_days[0],))}
        if missing := sorted(prev_keys - now_keys):
            bad.append(f"{name} 키 사라짐: {', '.join(missing[:10])}")
        if added := sorted(now_keys - prev_keys):
            rows.append({"컬럼": f"{name} (새 키)", f"{target}": ", ".join(added[:10]), "평소": ""})

    summary = f"{target} 기준, 이전 {len(prev_days)}개 날짜와 비교: " + ("; ".join(bad) if bad else "변화 없음")
    return _result(t, "null_spike", not bad, dg.AssetCheckSeverity.WARN, summary, 컬럼별=md_table(rows, max_cell=60))


def _price_rules(t: Table, conn) -> dg.AssetCheckResult:  # noqa: ANN001
    c = _cfg(t.name)
    days = [d for d, _ in _date_counts(conn, t, c["scan_dates"])]
    if not days:
        return _result(t, "price_rules", True, dg.AssetCheckSeverity.ERROR, "비어 있음")
    rules = {
        "종가 0 이하": "tdd_clsprc <= 0",
        "음수 값": "least(tdd_opnprc, tdd_hgprc, tdd_lwprc, acc_trdvol, acc_trdval, list_shrs) < 0",
        "저가≤종가≤고가 아님": "tdd_lwprc > 0 AND (tdd_hgprc < tdd_lwprc OR tdd_clsprc > tdd_hgprc OR tdd_clsprc < tdd_lwprc)",
        "등락률≠대비/전일종가": "tdd_clsprc - cmpprevdd_prc > 0 AND abs(fluc_rt - cmpprevdd_prc / (tdd_clsprc - cmpprevdd_prc) * 100) > 0.1",
    }
    tbl = _ident("raw", t.name)
    found, samples = [], []
    for label, cond in rules.items():
        q = sql.SQL("SELECT bas_dd, isu_cd, isu_nm, tdd_opnprc, tdd_hgprc, tdd_lwprc, tdd_clsprc, cmpprevdd_prc, fluc_rt FROM {} WHERE bas_dd = ANY(%s) AND ({})").format(tbl, sql.SQL(cond))
        hits = conn.execute(q, (days,)).fetchall()
        if hits:
            found.append(f"{label} {len(hits)}건")
            samples += [{"규칙": label, "날짜": h[0], "코드": h[1], "종목": h[2], "시/고/저/종": f"{h[3]}/{h[4]}/{h[5]}/{h[6]}", "대비": h[7], "등락률": h[8]} for h in hits[:5]]
    summary = f"최근 {len(days)}개 날짜({days[-1]}~{days[0]}): " + (", ".join(found) if found else "규칙 위반 없음")
    return _result(t, "price_rules", not found, dg.AssetCheckSeverity.ERROR, summary, **({"위반 예시": md_table(samples)} if samples else {}))


# ---------------------------------------------------------------------------
CHECKS = {
    "freshness": ("마지막 날짜가 quality.freshness_days 이내인가", _freshness),
    "row_count": ("최근 날짜별 행 수가 평소(이전 날짜 중앙값)만큼 들어왔나", _row_count),
    "null_spike": ("컬럼별 빈 값 비율이 갑자기 늘거나 jsonb 키가 사라졌나 (원천 구조 변경 감지)", _null_spike),
    "price_rules": ("KRX 시세 값끼리 맞나: 종가>0, 음수 없음, 저가≤종가≤고가, 등락률=대비/전일종가", _price_rules),
}


def _make(t: Table, name: str) -> dg.AssetChecksDefinition:
    desc, fn = CHECKS[name]

    @dg.asset_check(asset=dg.AssetKey(["db", "raw", t.name]), name=name, description=desc)
    def _check(postgres: PostgresResource) -> dg.AssetCheckResult:
        with postgres.connect() as conn:
            return fn(t, conn)

    return _check


_checks = [_make(t, name) for t in TABLES for name in ("freshness", "row_count", "null_spike") + (("price_rules",) if t.price else ())]
defs = dg.Definitions(asset_checks=_checks)
