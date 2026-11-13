"""raw.kofia_holdings → extracted.kofia_fund_links, extracted.kofia_fund_holdings (펀드별 실제 보유종목).

1. 연결표: 보유종목에 적힌 펀드 수익증권·ETF 이름을 보유내역이 있는 KOFIA 펀드와 맞춘다.
   - ETF: 보유종목 이름 = KRX ETF명 (`KODEX 삼성그룹`) → extracted.kofia_etf_codes (build_etf_codes) 로 표준코드.
     KRX ETF 전체를 KOFIA ETF 전체와 맞춘 표: 먼저 KRX 종목코드 → ISIN(KR7+코드) → SEIBro 공식명으로
     KOFIA ETF명과 정확히 맞추고(`etf_seibro`), 안 되면 KOFIA ETF명 (`삼성KODEX삼성그룹주증권상장지수투자신탁[주식]`)
     에서 브랜드(KODEX…) 뒤 부분을 비교. 브랜드가 바뀐 ETF(KOSEF→KIWOOM 등)는 옛 브랜드도 봄
   - 펀드: 이름이 같거나, 줄여 쓴 이름(`IT코리아증권모[주식]` → `하나IT코리아증권모투자신탁[주식]`)이 순서대로
     다 들어 있고 숫자가 같으면 연결. 같은 운용사 펀드를 먼저 보고, 후보가 여럿이면 덜 붙은 이름 하나만 허용
   - 클래스 접미사(`C-F`, `(종)`)를 떼고 다시, 클래스라 안 받은 자투자신탁은 모투자신탁으로
2. 펼치기: 연결된 펀드의 보유종목으로 보유 비율(들고 있는 시가평가액 / 그 펀드 보유자산 합계)만큼 나눔. 최대 3단계
3. 펀드 안 같은 종목은 합치고, 국내 주식·ETF 는 KRX 종목명으로 종목코드를 붙인다
"""

import difflib
import re
from collections import defaultdict
from datetime import date

import psycopg

MAX_DEPTH = 3
BRAND_ALIAS = {"KIWOOM": ["KOSEF"], "RISE": ["KBSTAR"], "PLUS": ["ARIRANG"], "TIME": ["TIMEFOLIO"]}
_CLASS_SUFFIX = re.compile(r"(\(종류?[^)]*\)|\s*(종류|Class|클래스)\s*[A-Za-z0-9\-]{1,6}|\s+[A-Z][A-Za-z0-9]{0,2}(-[A-Za-z0-9]{1,3})?)\s*$")


# ---------------------------------------------------------------------------- 이름 정규화
def fund_key(name: str) -> str:
    """펀드명 비교용: 모투자신탁→모, 자투자신탁→자, '증권'·'투자신탁'·'운용'·'제N호' 의 군더더기와 기호 제거."""
    s = name.upper().replace("엑티브", "액티브")
    s = re.sub(r"\(모\)\s*$", "", s)  # '…자투자신탁(주식)(모)' 의 (모)는 모투자신탁이 아니라 클래스 이름
    s = re.sub(r"모투자신탁|모펀드|모신탁", "모", s)
    s = re.sub(r"자투자신탁", "자", s)
    s = re.sub(r"모투자신?$|모투$", "모", s)  # 30자에서 잘린 이름
    s = re.sub(r"자투자신?$", "자", s)
    s = re.sub(r"투자신?$", "", s)
    s = re.sub(r"투자신탁|증권|\(운용\)|_?운용$|제(?=\d)|(?<=\d)호", "", s)
    return re.sub(r"모+", "모", re.sub(r"[^0-9A-Z가-힣&]", "", s))  # '증권모(모펀드)' → 모 하나


def etf_key(name: str) -> str:
    s = name.upper().replace("엑티브", "액티브")
    s = re.sub(r"증권|상장지수|투자신탁|자투자|모투자|\[[^\]]*\]|\([^)]*\)", "", s)
    return re.sub(r"[^0-9A-Z가-힣&]", "", s)


def official_key(name: str) -> str:
    """공식 펀드명 비교용 (SEIBro ↔ KOFIA): 괄호 모양·띄어쓰기·기호만 무시."""
    return re.sub(r"[^0-9A-Z가-힣&]", "", name.upper())


def _flags(name: str) -> frozenset[str]:
    u = name.upper().replace(" ", "")
    return frozenset(f for f, pat in (("H", r"[\(\[](합성)?H[\)\]]"), ("합성", "합성")) if re.search(pat, u))


def _subseq(a: str, b: str) -> bool:
    it = iter(b)
    return all(ch in it for ch in a)


def _digits(s: str) -> list[str]:
    return re.findall(r"\d+", s)


# ---------------------------------------------------------------------------- 연결
def _match_etf(isu_nm: str, etf_funds: list[tuple[str, str]]) -> tuple[str, str] | None:
    if not (m := re.match(r"[^\s]+", isu_nm)):
        return None
    brand = m.group(0).upper() if " " in isu_nm else (re.match(r"[A-Za-z0-9]+", isu_nm) or m).group(0).upper()
    k, fl = etf_key(isu_nm), _flags(isu_nm)
    cands = []
    for cd, nm in etf_funds:
        fk = etf_key(nm)
        for b in [brand, *BRAND_ALIAS.get(brand, [])]:
            if (i := fk.find(b)) >= 0:
                cands.append((brand + fk[i + len(b) :], cd, _flags(nm)))
                break
    exact = [c for c in cands if c[0] == k and c[2] == fl] or [c for c in cands if c[0] == k]
    if len(exact) == 1:
        return exact[0][1], "etf_exact"
    sub = sorted((len(c[0]) - len(k), c[1]) for c in cands if c[2] == fl and _subseq(k, c[0]) and _digits(k) == _digits(c[0]))
    if sub and (len(sub) == 1 or sub[0][0] < sub[1][0]):
        return sub[0][1], "etf_subseq"
    return None


def match_etfs(etf_funds: list[tuple[str, str]], krx_etfs: list[tuple[str, str, str | None]]) -> dict[str, tuple[str, str]]:
    """KRX ETF → KOFIA ETF. etf_funds: (standard_cd, fund_nm), krx_etfs: (isu_cd, isu_nm, SEIBro 공식명) → {isu_cd: (standard_cd, method)}"""
    by_official: dict[str, list[str]] = defaultdict(list)
    for cd, nm in etf_funds:
        by_official[official_key(nm)].append(cd)
    out = {}
    for isu_cd, isu_nm, official_nm in krx_etfs:
        if official_nm and len(cds := by_official.get(official_key(official_nm), [])) == 1:
            out[isu_cd] = (cds[0], "etf_seibro")
        elif m := _match_etf(isu_nm, etf_funds):
            out[isu_cd] = m
    return out


class Linker:
    def __init__(self, funds: list[tuple[str, str, str, bool, bool]], etf_codes: list[tuple[str, str, str, str, str]]) -> None:
        """funds: (standard_cd, company_cd, fund_nm, is_etf, is_class) 보유내역이 있는 펀드,
        etf_codes: (isu_cd, 기준일 KRX 종목명, standard_cd, fund_nm, method) — extracted.kofia_etf_codes

        클래스 펀드는 본 펀드 수익증권만 들고 있어 연결 대상에서 뺀다 (예전에 받아 둔 클래스 데이터가 있어도).
        """
        self.name = {cd: nm for cd, _, nm, _, _ in funds}
        self.key = {cd: fund_key(nm) for cd, _, nm, _, is_class in funds if not is_class}
        self.by_key: dict[str, list[tuple[str, str]]] = defaultdict(list)
        self.by_co: dict[str, list[str]] = defaultdict(list)
        for cd, co, _, _, is_class in funds:
            if not is_class:
                self.by_key[self.key[cd]].append((cd, co))
                self.by_co[co].append(cd)
        self.etf_by_name = {}
        for isu_cd, isu_nm, cd, fund_nm, method in etf_codes:
            self.etf_by_name[isu_nm.replace(" ", "")] = (isu_cd, cd, method)
            self.name.setdefault(cd, fund_nm)

    def _pick(self, k: str, pool: list[str], min_len: int, same_co: bool, skip: set[str]) -> str | None:
        cands = sorted(
            ((difflib.SequenceMatcher(None, k, self.key[cd]).ratio(), cd) for cd in pool
             if cd not in skip and len(k) >= min_len and len(k) >= 0.5 * len(self.key[cd]) and _subseq(k, self.key[cd])
             and _digits(k) == _digits(self.key[cd])[: len(_digits(k))]),
            key=lambda x: (-x[0], x[1]),
        )  # fmt: skip
        if not cands:
            return None
        if len({self.key[cd] for r, cd in cands if cands[0][0] - r < 0.05}) == 1:  # 공백 등만 다른 같은 이름
            return cands[0][1]
        if same_co:  # 클래스 접미사 붙은 형제 펀드보다 본 펀드: 덜 붙은 이름이 하나뿐이면
            extra = sorted((len(self.key[cd]) - len(k), cd) for _, cd in cands)
            if len(extra) == 1 or extra[0][0] < extra[1][0]:
                return extra[0][1]
        return None

    def _match_key(self, co: str, k: str, skip: set[str]) -> tuple[str, str] | None:
        if len(k) < 4:
            return None
        exact = [(cd, c) for cd, c in self.by_key.get(k, []) if cd not in skip]
        if len(exact) == 1:
            return exact[0][0], "exact"
        if len(same := [cd for cd, c in exact if c == co]) == 1:
            return same[0], "exact"
        if cd := self._pick(k, self.by_co[co], 5, same_co=True, skip=skip):
            return cd, "subseq"
        if cd := self._pick(k, list(self.key), 8, same_co=False, skip=skip):
            return cd, "subseq_any"
        return None

    def link(self, co: str, item_nm: str, skip: set[str] = frozenset()) -> tuple[str, str | None, str] | None:
        """(standard_cd, isu_cd, method) 또는 None. skip: 이 이름을 들고 있는 펀드 (자기 자신을 가리키면 펼쳐지지 않으므로 제외)"""
        if e := self.etf_by_name.get(item_nm.replace(" ", "")):
            return e[1], e[0], e[2]
        if m := self._match_key(co, fund_key(item_nm), skip):
            return m[0], None, m[1]
        base = _CLASS_SUFFIX.sub("", item_nm).strip()
        if base != item_nm and (m := self._match_key(co, fund_key(base), skip)):
            return m[0], None, m[1] + "+class"
        k = fund_key(base)
        for i in reversed([m.start() for m in re.finditer("자", k)]):  # 자투자신탁(클래스라 안 받음) → 모투자신탁
            if m := self._match_key(co, k[:i] + "모" + k[i + 1 :], skip):
                return m[0], None, m[1] + "+feeder"
        return None


# ---------------------------------------------------------------------------- 펼치기
# 펼칠 연결: 수익증권 행이거나 ETF 연결 (ETF 는 '주식 - KSE' 로 적히기도 함). 계산 전에 임시 테이블로 만들어 인덱스를 건다
LINK_TMP_SQL = """
CREATE TEMP TABLE _link ON COMMIT DROP AS
SELECT l.company_cd, l.item_nm, l.standard_cd, l.method LIKE 'etf%%' AS is_etf
FROM extracted.kofia_fund_links l
WHERE l.std_dt = %(d)s AND l.standard_cd IN (SELECT standard_cd FROM raw.kofia_holdings WHERE std_dt = %(d)s GROUP BY 1 HAVING sum(market_amt) <> 0);
CREATE INDEX ON _link (company_cd, item_nm);
CREATE TEMP TABLE _total ON COMMIT DROP AS
SELECT standard_cd, sum(market_amt) AS t FROM raw.kofia_holdings WHERE std_dt = %(d)s GROUP BY 1;
CREATE INDEX ON _total (standard_cd);
ANALYZE _link; ANALYZE _total;
"""

EXPAND_SQL = """
WITH RECURSIVE
x AS (  -- tgt: 이 행을 펼칠 펀드 (없으면 실제 종목)
    SELECT h.standard_cd AS root, h.item_nm, h.security_type, h.quantity, h.market_amt, NULL::text AS via, 0 AS depth, l.standard_cd AS tgt
    FROM raw.kofia_holdings h
    LEFT JOIN _link l ON l.company_cd = h.company_cd AND l.item_nm = h.item_nm AND l.standard_cd <> h.standard_cd
                     AND (h.security_type LIKE '%%수익증권%%' OR l.is_etf)
    WHERE h.std_dt = %(d)s
  UNION ALL
    SELECT x.root, m.item_nm, m.security_type, m.quantity * x.market_amt / t.t, m.market_amt * x.market_amt / t.t,
           coalesce(x.via || ' > ', '') || x.item_nm, x.depth + 1, l.standard_cd
    FROM x
    JOIN _total t ON t.standard_cd = x.tgt
    JOIN raw.kofia_holdings m ON m.std_dt = %(d)s AND m.standard_cd = x.tgt
    LEFT JOIN _link l ON l.company_cd = m.company_cd AND l.item_nm = m.item_nm AND l.standard_cd <> m.standard_cd
                     AND (m.security_type LIKE '%%수익증권%%' OR l.is_etf)
    WHERE x.tgt IS NOT NULL AND x.depth < %(max_depth)s
),
agg AS (  -- 펼친 행은 빼고 실제 종목만 (마지막 단계에서도 못 펼친 건 수익증권 그대로)
    SELECT root AS standard_cd, coalesce(item_nm, '') AS item_nm, coalesce(security_type, '') AS security_type,
           sum(quantity) AS quantity, sum(market_amt) AS market_amt,
           CASE WHEN bool_or(via IS NOT NULL) THEN string_agg(DISTINCT coalesce(via, '(직접)'), ' / ') END AS via
    FROM x WHERE tgt IS NULL OR depth = %(max_depth)s GROUP BY 1, 2, 3
),
krx AS (  -- 기준일 직전 거래일의 KRX 종목명 → 종목코드 (주식 우선, 그다음 ETF)
    SELECT DISTINCT ON (k) k, isu_cd FROM (
        SELECT replace(isu_nm, ' ', '') AS k, isu_cd, 0 AS pri FROM raw.krx_stock_daily
        WHERE bas_dd = (SELECT max(bas_dd) FROM raw.krx_stock_daily WHERE bas_dd <= %(d)s)
        UNION ALL
        SELECT replace(isu_nm, ' ', ''), isu_cd, 1 FROM raw.krx_etf_daily
        WHERE bas_dd = (SELECT max(bas_dd) FROM raw.krx_etf_daily WHERE bas_dd <= %(d)s)
    ) s ORDER BY k, pri
)
INSERT INTO extracted.kofia_fund_holdings
SELECT %(d)s, a.standard_cd,
       row_number() OVER (PARTITION BY a.standard_cd ORDER BY a.market_amt DESC NULLS LAST, a.item_nm),
       f.fund_nm, f.company_cd, f.is_etf, a.item_nm, a.security_type,
       CASE WHEN a.security_type LIKE '주식%%' OR a.security_type LIKE '%%수익증권%%' THEN k.isu_cd END,
       a.quantity, a.market_amt, round(100 * a.market_amt / nullif(t.t, 0), 4), a.via
FROM agg a
JOIN _total t ON t.standard_cd = a.standard_cd
LEFT JOIN raw.kofia_funds f ON f.standard_cd = a.standard_cd
LEFT JOIN krx k ON k.k = replace(a.item_nm, ' ', '')
"""


def build_etf_codes(conn: psycopg.Connection, *, log) -> dict:  # noqa: ANN001
    """KRX ETF 전체(지금까지 나온 종목코드, 이름은 가장 최근 것) → KOFIA ETF 표준코드. 통째로 다시 만든다.

    후보는 KOFIA ETF 전체 (최신 보수비용 + kofia_funds, 이름은 보수비용 우선). ISIN = KR7 + 종목코드 6자리 + 00 + 검증숫자
    """
    etf_funds = conn.execute(
        "SELECT DISTINCT ON (standard_cd) standard_cd, fund_nm FROM ("
        "  SELECT standard_cd, fund_nm, 0 AS pri FROM raw.kofia_fund_fees"
        "  WHERE fund_nm LIKE '%%상장지수%%' AND standard_dt = (SELECT max(standard_dt) FROM raw.kofia_fund_fees)"
        "  UNION ALL"
        "  SELECT standard_cd, fund_nm, 1 FROM raw.kofia_funds"
        "  WHERE (is_etf OR fund_nm LIKE '%%상장지수%%') AND NOT coalesce(is_class, false) AND fund_nm <> ''"
        ") u ORDER BY standard_cd, pri"
    ).fetchall()
    krx = conn.execute(  # 최근까지 거래된 종목부터
        "SELECT e.isu_cd, e.isu_nm, s.fund_name FROM (SELECT DISTINCT ON (isu_cd) isu_cd, isu_nm, bas_dd FROM raw.krx_etf_daily ORDER BY isu_cd, bas_dd DESC) e "
        "LEFT JOIN raw.seibro_funds s ON s.isin LIKE 'KR7' || e.isu_cd || '%%' ORDER BY e.bas_dd DESC, e.isu_cd"
    ).fetchall()
    matched, taken = {}, set()
    # 표준코드 하나엔 종목코드 하나: etf_seibro 먼저, 그다음 최근까지 거래된 종목 (상장폐지된 옛 ETF 가 이름 규칙으로 새 ETF 에 붙는 것 방지)
    for isu_cd, (cd, method) in sorted(match_etfs(etf_funds, krx).items(), key=lambda kv: kv[1][1] != "etf_seibro"):
        if cd not in taken:
            matched[isu_cd] = (cd, method)
            taken.add(cd)
    names, isu_nm = dict(etf_funds), {c: n for c, n, _ in krx}
    conn.execute("DELETE FROM extracted.kofia_etf_codes")
    with conn.cursor() as cur, cur.copy("COPY extracted.kofia_etf_codes FROM STDIN") as copy:
        for isu_cd, (cd, method) in matched.items():
            copy.write_row((isu_cd, isu_nm[isu_cd], cd, names[cd], method))
    conn.commit()
    log.info(f"extracted.kofia_etf_codes: KRX ETF {len(krx):,}개 중 {len(matched):,}개 연결 (KOFIA ETF 후보 {len(etf_funds):,}개)")
    return {"krx_etfs": len(krx), "linked": len(matched), "kofia_etfs": len(etf_funds)}


def _krx_etfs(conn: psycopg.Connection, d: date) -> list[tuple[str, str, str, str, str]]:
    """기준일 직전 거래일의 KRX ETF 중 연결된 것 (종목코드, 그날 종목명, standard_cd, fund_nm, method)."""
    return conn.execute(
        "SELECT e.isu_cd, e.isu_nm, c.standard_cd, c.fund_nm, c.method FROM raw.krx_etf_daily e "
        "JOIN extracted.kofia_etf_codes c USING (isu_cd) "
        "WHERE e.bas_dd = (SELECT max(bas_dd) FROM raw.krx_etf_daily WHERE bas_dd <= %s)",
        (d,),
    ).fetchall()


def _dates(conn: psycopg.Connection, table: str) -> list[date]:
    dates = [r[0] for r in conn.execute("SELECT DISTINCT std_dt FROM raw.kofia_holdings ORDER BY 1")]
    conn.execute(f"DELETE FROM {table} WHERE std_dt <> ALL(%s)", (dates,))  # noqa: S608 - 고정 테이블명
    return dates


def _links_for(conn: psycopg.Connection, d: date) -> dict[str, int]:
    funds = conn.execute(
        "SELECT f.standard_cd, f.company_cd, f.fund_nm, coalesce(f.is_etf, false), coalesce(f.is_class, false) FROM raw.kofia_funds f "
        "WHERE f.fund_nm <> '' AND f.standard_cd IN (SELECT standard_cd FROM raw.kofia_holdings WHERE std_dt = %s)",
        (d,),
    ).fetchall()
    linker = Linker(funds, _krx_etfs(conn, d))
    # 링크 후보: 수익증권 행 전부 + 다른 종류(주식 - KSE 등)로 적힌 ETF
    items = conn.execute(
        "SELECT company_cd, item_nm, bool_or(security_type LIKE '%%수익증권%%'), array_agg(DISTINCT standard_cd) "
        "FROM raw.kofia_holdings WHERE std_dt = %s AND item_nm <> '' GROUP BY 1, 2",
        (d,),
    ).fetchall()
    rows = {}
    for co, item, is_unit, holders in items:
        if (is_unit or item.replace(" ", "") in linker.etf_by_name) and (m := linker.link(co, item, set(holders))) is not None:
            rows[(co, item)] = (d, co, item, m[0], linker.name[m[0]], m[1], m[2])
    conn.execute("DELETE FROM extracted.kofia_fund_links WHERE std_dt = %s", (d,))
    with conn.cursor() as cur, cur.copy("COPY extracted.kofia_fund_links FROM STDIN") as copy:
        for r in rows.values():
            copy.write_row(r)
    (n_krx,) = conn.execute("SELECT count(*) FROM raw.krx_etf_daily WHERE bas_dd = (SELECT max(bas_dd) FROM raw.krx_etf_daily WHERE bas_dd <= %s)", (d,)).fetchone()
    return {"krx_etfs": n_krx, "krx_etfs_linked": len(linker.etf_by_name), "links": len(rows)}


def build_links(conn: psycopg.Connection, *, log) -> dict:  # noqa: ANN001
    """기준일마다 연결표를 통째로 다시 만든다."""
    stats: dict[str, int] = defaultdict(int)
    for d in _dates(conn, "extracted.kofia_fund_links"):
        s = _links_for(conn, d)
        conn.commit()
        log.info(f"extracted.kofia_fund_links {d}: 연결 {s['links']:,}건, KRX ETF {s['krx_etfs']:,}개 중 {s['krx_etfs_linked']:,}개 연결")
        for k, v in s.items():
            stats[k] += v
    return dict(stats)


def build(conn: psycopg.Connection, *, log) -> dict:  # noqa: ANN001
    """기준일마다 펼친 보유종목을 통째로 다시 만든다 (extracted.kofia_fund_links 를 먼저 만들어야 함, 분기당 1~2분)."""
    stats: dict[str, float] = defaultdict(int)
    for d in _dates(conn, "extracted.kofia_fund_holdings"):
        conn.execute("DELETE FROM extracted.kofia_fund_holdings WHERE std_dt = %s", (d,))
        for stmt in filter(str.strip, LINK_TMP_SQL.split(";")):  # 파라미터가 있으면 한 번에 한 문장만 보낼 수 있음
            conn.execute(stmt, {"d": d} if "%(d)s" in stmt else None)
        n = conn.execute(EXPAND_SQL, {"d": d, "max_depth": MAX_DEPTH}).rowcount
        funds, via, code, left = conn.execute(
            "SELECT count(DISTINCT standard_cd), count(*) FILTER (WHERE via IS NOT NULL), count(isu_cd), "
            "coalesce(sum(market_amt) FILTER (WHERE security_type LIKE '%%수익증권%%'), 0) / nullif(sum(market_amt), 0) "
            "FROM extracted.kofia_fund_holdings WHERE std_dt = %s",
            (d,),
        ).fetchone()
        conn.commit()
        log.info(f"extracted.kofia_fund_holdings {d}: 펀드 {funds:,}개, {n:,}행 (펼친 행 {via:,}, 종목코드 {code:,}, 남은 수익증권 비중 {float(left or 0):.1%})")
        for k, v in {"rows": n, "funds": funds, "looked_through": via, "with_isu_cd": code}.items():
            stats[k] += v
    return dict(stats)
