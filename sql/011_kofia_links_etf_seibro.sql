-- 011: kofia_fund_links.method 에 etf_seibro 추가 (KRX 종목코드 → ISIN → SEIBro 공식명 = KOFIA ETF명)
COMMENT ON COLUMN extracted.kofia_fund_links.method IS '맞춘 방법: etf_seibro (KRX 종목코드 → ISIN → SEIBro 공식명 = KOFIA ETF명), etf_exact/etf_subseq (KRX ETF명 ↔ KOFIA ETF명), exact (이름 같음), subseq (줄인 이름, 같은 운용사), subseq_any (줄인 이름, 다른 운용사), +class (클래스 접미사 떼고), +feeder (클래스라 안 받은 자투자신탁 → 모투자신탁)';
