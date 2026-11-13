-- kofia_holdings.db funds.is_class (클래스 펀드 제외용 판별 결과)
ALTER TABLE raw.kofia_funds ADD COLUMN IF NOT EXISTS is_class boolean;
