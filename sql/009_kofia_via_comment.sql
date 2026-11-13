-- 009: via 설명 보강 (직접 보유분이 함께 있으면 '(직접)' 표시)
COMMENT ON COLUMN extracted.kofia_fund_holdings.via IS '거쳐 온 펀드 (모투자신탁·ETF 등 보유종목에 적힌 이름, 여러 단계면 > 로 연결, 여러 경로면 / 로 구분, 직접 보유분도 있으면 (직접) 포함). 직접 보유만이면 빈 값';
