"""raw.kofia_holdings → extracted.kofia_fund_holdings (펀드별 실제 보유종목).

1. 펀드가 다른 펀드의 수익증권(모투자신탁 등)을 들고 있고, 그 펀드의 보유내역도 받아 둔 경우
   (종목명 = 펀드명, 공백 무시) 그 펀드의 보유종목으로 펼친다. 금액·수량은 보유 비율
   (들고 있는 수익증권 시가평가액 / 그 펀드 보유자산 합계)만큼 나눈다. 최대 3단계.
2. 펀드 안에서 같은 종목(종목명 + 유가증권 종류)은 합친다.
3. 국내 주식·ETF 는 기준일(또는 그 직전 거래일) KRX 시세의 종목명으로 종목코드를 붙인다.
"""

import psycopg

MAX_DEPTH = 3

BUILD_SQL = """
WITH RECURSIVE
fund_key AS (  -- 이 기준일에 보유내역이 있는 펀드: 공백 뺀 펀드명 → 코드
    SELECT DISTINCT ON (replace(f.fund_nm, ' ', '')) replace(f.fund_nm, ' ', '') AS k, f.standard_cd
    FROM raw.kofia_funds f
    WHERE f.fund_nm <> '' AND f.standard_cd IN (SELECT standard_cd FROM raw.kofia_holdings WHERE std_dt = %(d)s)
    ORDER BY replace(f.fund_nm, ' ', ''), f.standard_cd
),
total AS (
    SELECT standard_cd, sum(market_amt) AS t FROM raw.kofia_holdings WHERE std_dt = %(d)s GROUP BY 1
),
x AS (
    SELECT h.standard_cd AS root, h.standard_cd AS src, h.item_nm, h.security_type, h.quantity, h.market_amt,
           NULL::text AS via, 0 AS depth
    FROM raw.kofia_holdings h WHERE h.std_dt = %(d)s
  UNION ALL
    SELECT x.root, m.standard_cd, m.item_nm, m.security_type, m.quantity * x.market_amt / t.t, m.market_amt * x.market_amt / t.t,
           coalesce(x.via || ' > ', '') || x.item_nm, x.depth + 1
    FROM x
    JOIN fund_key fk ON fk.k = replace(x.item_nm, ' ', '') AND fk.standard_cd <> x.src
    JOIN total t ON t.standard_cd = fk.standard_cd AND t.t <> 0
    JOIN raw.kofia_holdings m ON m.std_dt = %(d)s AND m.standard_cd = fk.standard_cd
    WHERE x.depth < %(max_depth)s AND x.security_type LIKE '%%수익증권%%'
),
leaf AS (  -- 펼친 수익증권 행은 빼고 실제 종목만
    SELECT x.* FROM x
    WHERE NOT (x.depth < %(max_depth)s AND x.security_type LIKE '%%수익증권%%' AND EXISTS (
        SELECT 1 FROM fund_key fk JOIN total t ON t.standard_cd = fk.standard_cd AND t.t <> 0
        WHERE fk.k = replace(x.item_nm, ' ', '') AND fk.standard_cd <> x.src))
),
agg AS (
    SELECT root AS standard_cd, coalesce(item_nm, '') AS item_nm, coalesce(security_type, '') AS security_type,
           sum(quantity) AS quantity, sum(market_amt) AS market_amt, string_agg(DISTINCT via, ' / ') AS via
    FROM leaf GROUP BY 1, 2, 3
),
krx AS (  -- 기준일 직전 거래일의 KRX 종목명 → 종목코드 (주식 + ETF)
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
       a.quantity, a.market_amt,
       round(100 * a.market_amt / nullif(t.t, 0), 4),
       a.via
FROM agg a
JOIN total t ON t.standard_cd = a.standard_cd
LEFT JOIN raw.kofia_funds f ON f.standard_cd = a.standard_cd
LEFT JOIN krx k ON k.k = replace(a.item_nm, ' ', '')
"""


def build(conn: psycopg.Connection, *, log) -> dict:  # noqa: ANN001
    """기준일마다 통째로 다시 만든다 (분기당 수십만 행, 수 초)."""
    dates = [r[0] for r in conn.execute("SELECT DISTINCT std_dt FROM raw.kofia_holdings ORDER BY 1")]
    stats: dict[str, int] = {"quarters": len(dates), "rows": 0, "funds": 0, "looked_through": 0, "with_isu_cd": 0}
    conn.execute("DELETE FROM extracted.kofia_fund_holdings WHERE std_dt <> ALL(%s)", (dates,))
    for d in dates:
        conn.execute("DELETE FROM extracted.kofia_fund_holdings WHERE std_dt = %s", (d,))
        n = conn.execute(BUILD_SQL, {"d": d, "max_depth": MAX_DEPTH}).rowcount
        funds, via, code = conn.execute(
            "SELECT count(DISTINCT standard_cd), count(*) FILTER (WHERE via IS NOT NULL), count(isu_cd) FROM extracted.kofia_fund_holdings WHERE std_dt = %s",
            (d,),
        ).fetchone()
        conn.commit()
        log.info(f"extracted.kofia_fund_holdings {d}: 펀드 {funds:,}개, {n:,}행 (모투자신탁을 거친 행 {via:,}, 종목코드 {code:,})")
        stats["rows"] += n
        stats["funds"] += funds
        stats["looked_through"] += via
        stats["with_isu_cd"] += code
    return stats
