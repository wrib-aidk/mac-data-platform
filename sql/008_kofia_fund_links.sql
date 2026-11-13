-- 008: 보유종목에 적힌 펀드 수익증권·ETF 이름 → 실제 펀드(표준코드) 연결표. extracted.kofia_fund_holdings 를 펼칠 때 씀
CREATE TABLE extracted.kofia_fund_links (
    std_dt      date NOT NULL,
    company_cd  text NOT NULL,
    item_nm     text NOT NULL,
    standard_cd text NOT NULL,
    fund_nm     text,
    isu_cd      text,
    method      text NOT NULL,
    PRIMARY KEY (std_dt, company_cd, item_nm)
);

COMMENT ON TABLE extracted.kofia_fund_links IS '보유종목에 적힌 펀드 수익증권·ETF 이름 → 보유내역이 있는 KOFIA 펀드. 이름이 줄여 쓰이거나 잘려 있어 규칙으로 맞춤';
COMMENT ON COLUMN extracted.kofia_fund_links.std_dt IS '기준일 (분기말)';
COMMENT ON COLUMN extracted.kofia_fund_links.company_cd IS '이 이름을 보유종목에 적은 펀드의 운용사 코드 (같은 이름도 운용사마다 가리키는 펀드가 다를 수 있음)';
COMMENT ON COLUMN extracted.kofia_fund_links.item_nm IS '보유종목에 적힌 이름 (raw.kofia_holdings.item_nm)';
COMMENT ON COLUMN extracted.kofia_fund_links.standard_cd IS '연결된 펀드 표준코드';
COMMENT ON COLUMN extracted.kofia_fund_links.fund_nm IS '연결된 펀드명';
COMMENT ON COLUMN extracted.kofia_fund_links.isu_cd IS 'ETF 면 KRX 종목코드';
COMMENT ON COLUMN extracted.kofia_fund_links.method IS '맞춘 방법: etf_exact/etf_subseq (KRX ETF명 ↔ KOFIA ETF명), exact (이름 같음), subseq (줄인 이름, 같은 운용사), subseq_any (줄인 이름, 다른 운용사), +class (클래스 접미사 떼고), +feeder (클래스라 안 받은 자투자신탁 → 모투자신탁)';

COMMENT ON COLUMN extracted.kofia_fund_holdings.via IS '거쳐 온 펀드 (모투자신탁·ETF 등 보유종목에 적힌 이름, 여러 단계면 > 로 연결, 여러 경로면 / 로 구분). 직접 보유만이면 빈 값';
