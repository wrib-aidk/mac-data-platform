-- 007: 펀드별 실제 보유종목 (raw.kofia_holdings 에서 추출)
--      모투자신탁·다른 펀드 수익증권을 들고 있으면 그 펀드의 보유종목으로 펼치고(최대 3단계), 같은 종목은 합침.
--      국내 주식·ETF 는 같은 날 KRX 시세의 종목명으로 종목코드를 붙임.
CREATE TABLE extracted.kofia_fund_holdings (
    std_dt        date    NOT NULL,
    standard_cd   text    NOT NULL,
    rank          integer NOT NULL,
    fund_nm       text,
    company_cd    text,
    is_etf        boolean,
    item_nm       text    NOT NULL,
    security_type text    NOT NULL,
    isu_cd        text,
    quantity      numeric,
    market_amt    numeric,
    weight        numeric,
    via           text,
    PRIMARY KEY (std_dt, standard_cd, rank)
);
CREATE INDEX ON extracted.kofia_fund_holdings (std_dt, isu_cd);
CREATE INDEX ON extracted.kofia_fund_holdings (std_dt, item_nm);

COMMENT ON TABLE extracted.kofia_fund_holdings IS '펀드별 실제 보유종목: 모투자신탁 등 펀드 수익증권은 그 펀드의 보유종목으로 펼쳐 비율대로 나눔, 같은 종목은 합침';
COMMENT ON COLUMN extracted.kofia_fund_holdings.std_dt IS '기준일 (분기말)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.standard_cd IS '펀드 표준코드 (raw.kofia_funds.standard_cd)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.rank IS '펀드 안에서 시가평가액 순위 (1 = 가장 큼)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.fund_nm IS '펀드명';
COMMENT ON COLUMN extracted.kofia_fund_holdings.company_cd IS '운용사 코드';
COMMENT ON COLUMN extracted.kofia_fund_holdings.is_etf IS 'ETF 여부';
COMMENT ON COLUMN extracted.kofia_fund_holdings.item_nm IS '종목명 (KOFIA 표기 그대로)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.security_type IS '유가증권 종류 (주식 - KSE, 채권 - 국채, 유동자산 - 보통예금 등)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.isu_cd IS 'KRX 종목코드 (기준일 KRX 주식·ETF 종목명과 같을 때만, 해외주식·채권 등은 빈 값)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.quantity IS '수량 (모투자신탁을 거친 분은 보유 비율만큼 나눈 값)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.market_amt IS '시가평가액 (원, 모투자신탁을 거친 분은 보유 비율만큼 나눈 값)';
COMMENT ON COLUMN extracted.kofia_fund_holdings.weight IS '비중 (%) = 시가평가액 / 펀드 보유자산 시가평가액 합계. 순자산 대비가 아님';
COMMENT ON COLUMN extracted.kofia_fund_holdings.via IS '거쳐 온 펀드 (모투자신탁명, 여러 단계면 > 로 연결, 여러 경로면 / 로 구분). 직접 보유만이면 빈 값';
