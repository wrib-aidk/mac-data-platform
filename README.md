# mac-data-platform

[wrib-crawler](https://github.com/wrib-aidk/wrib-crawler) 크롤러를 Dagster로 관리하는 프로젝트.

```
수집 (collect_*) → data/raw/ 파일 ─┬→ DB 적재 (db_*) → PostgreSQL raw.*  ─┐
                                  └→ 추출 (extract_*) → data/extracted/ ─┴→ PostgreSQL extracted.* → 다음 추출 …
```
PDF·XML 원본은 파일로 두고, 표 데이터·문서 목록·텍스트는 PostgreSQL(Docker)에 넣습니다.

## 실행

```bash
./start-dagster.sh        # http://127.0.0.1:3000/dagster/  (UI + 스케줄러 데몬)
```

`start-dagster.sh` 가 PostgreSQL 컨테이너(`compose.yaml`)도 같이 켭니다 (Docker Desktop 이 켜져 있어야 함).
인증키(DART, KRX)와 DB 접속 정보는 `.env` 에 있습니다 (`.env.example` 참고, `.env` 는 git 에 올리지 않음).

## 설정 — `config/pipelines.yaml`

수집 주기·재시도·수집 범위는 전부 이 파일에서 바꿉니다. 고친 뒤 UI에서 **Deployment → Reload**.

| 항목 | 설명 |
| --- | --- |
| `schedules` | 주기 이름 → cron. 기본 `every_3_months` = 1·4·7·10월 1일 03:00 |
| `jobs.<이름>.schedule` | 주기 이름 또는 cron 직접 입력 |
| `jobs.<이름>.enabled` | `true` 면 스케줄 자동 실행 |
| `jobs.<이름>.config` | asset 별 옵션 (workers, start, lookback_days 등). UI Launchpad 기본값으로도 쓰임 |
| `retry.item_retries` | 펀드/종목 1건 실패 시 즉시 재시도 횟수 |
| `retry.max_retries`, `delay_seconds` | 수집 단계 전체 실패 시 Dagster 재실행 횟수·대기 |
| `raw_postfix` | 원본의 DRM 회피용 `.raw` 접미사 (이 PC에서는 `""`) |

## 오류 처리

1. 항목(펀드·회사·날짜) 1건 실패 → 지수 백오프로 `item_retries` 번 재시도, 나머지 항목은 계속 수집
2. 그래도 실패한 항목 → `logs/errors.log` 에 `[ITEM_FAILED]` 기록 (asset 메타데이터 `failed` 에도 표시)
3. 전부 실패 / 목록 조회 실패 등 단계 전체 실패 → Dagster가 `max_retries` 번 재실행 (이미 받은 파일은 건너뛰고 이어받음)
4. 재실행까지 실패 → `logs/errors.log` 에 `[RUN_FAILED]` 기록 (센서 `log_failed_runs`)
- 인증키 누락은 재시도 없이 바로 실패, KOFIA 403/429 차단은 재시도 없이 24~48시간 쿨다운 (`[BLOCKED]`)

## 수집 파이프라인

| job | asset | 저장 위치 | 필요 |
| --- | --- | --- | --- |
| fundsupermarket | `fundsupermarket/fund_list` → `fund_details` | `data/raw/fundsupermarket/` (`fund_list.csv`, `funds/<코드>/details.json` + PDF 6종) | |
| seibro | `seibro/fund_list` → `fund_nav` | `data/raw/seibro/` (`fund_list.csv`, `funds/<ISIN>/nav.csv`) | |
| kofia_nav | `fundsupermarket/fund_list` → `kofia/fund_nav` | `data/raw/kofia/funds/<코드>/nav.csv` | 내부망 |
| kofia_holdings | `kofia/fund_holdings` | `data/raw/kofia/kofia_holdings.db` (이어받기 큐), 로그 `logs/kofia_holdings.log` | 내부망 |
| dart | `dart/corp_list` → `corp_filings` | `data/raw/dart/corp_list.csv`, `data/raw/dart/filings/*.xml` | `DART_API_KEY` |
| krx_open_api | `krx/stock_list`, `stock_daily_quotes`, `etf_daily_quotes` | `data/raw/krx/stock_list.csv`, `data/raw/krx/quotes/` | `KRX_AUTH_KEY` |
| krx_web | `krx/etf_constituents` | `data/raw/krx/etf_constituents/<YYYYMMDD>/` | `KRX_AUTH_ID/PW` |

### 갱신 규칙 — 파일은 최신본만, 이력은 DB 에

다시 수집하면 `data/raw/` 의 파일을 새로 만들지 않고 기존 파일을 고칩니다. 덮어쓰기 전 값은 같은 job 안에서 먼저 DB 에 들어가 있습니다.

| 데이터 | 파일 (`data/raw/`) | DB |
| --- | --- | --- |
| 목록·펀드 `details.json` | 매번 덮어씀 (최신본만) | 수집일별 스냅샷으로 쌓음, `db.retention_days`(365일) 지나면 삭제 |
| NAV `nav.csv` | 날짜가 계속 쌓임. 마지막 날짜 7일 전부터 다시 받아 정정값 반영 | 전체 이력 |
| KRX 일별 시세 | 날짜별 파일이 쌓임. 이미 받은 날은 건너뜀 | 전체 이력 |
| 공시 PDF 6종 | 본문이 바뀌었을 때만 덮어씀. 문서 날짜가 기존보다 이르면 무시 | 최신 판만 |
| DART 정기보고서 | 정정본이 나오면 교체 | 최신 판만 |
| KOFIA 보유종목 | 정정(seq 증가) 시 해당 펀드 교체 | 최신 판만 |

문서별 날짜·본문 해시·링크 파일명·갱신일은 `funds/<코드>/.docs.json` 에 기록되고, 실행 메타데이터에 `docs_new`/`docs_updated`/`docs_stale_ignored` 건수가 표시됩니다.

> ⚠️ 펀드슈퍼마켓 서버는 같은 요청에도 **가끔 예전 판 문서를 돌려줍니다** (예: K55105DN1861 간이투자설명서가 2026.01.02판과 2021.09.03판을 번갈아 응답).
> 그래서 링크 파일명이 같다고 건너뛰지 않고 매번 받아서 비교하며, 날짜가 더 이른 판으로는 덮어쓰지 않습니다.
> HF 에서 가져온 문서 중에도 예전 판이 섞여 있을 수 있는데, 다음 수집에서 새 판을 받으면 자동으로 교체됩니다.

## 추출 파이프라인

| asset | 내용 | 상태 |
| --- | --- | --- |
| `fundsupermarket/fund_documents_text` | 공시 PDF → 텍스트 `.md` (원본 `convert_pdf_to_txt.py`, 결과가 HF 의 `.md` 와 같음) | `fundsupermarket` job 에서 `fund_details` 다음에 실행. 바뀐 PDF 만 다시 변환 (`.text.json`) |

### UI 에서 결과 미리보기

asset 화면의 메타데이터에 **preview** 항목이 있습니다.
- 목록 asset(`fund_list`, `corp_list`, `stock_list`): CSV 앞 5행
- `fundsupermarket/fund_details`: 펀드 1개의 파일 목록과 `details.json` 앞부분
- `seibro/fund_nav`: 펀드 1개의 `nav.csv` 앞 5행
- `fundsupermarket/fund_documents_text`: 펀드 1개의 문서별 글자 수와 간이투자설명서 앞부분

미리보기는 실행할 때 만들어지므로 한 번이라도 실행한 asset 에만 보입니다.

## PostgreSQL

| 스키마 | 내용 |
| --- | --- |
| `raw` | 수집 파일을 그대로 옮긴 테이블 (숫자·날짜만 타입 변환, 구조가 자주 바뀌는 응답은 `data jsonb`) |
| `extracted` | 추출 결과 (공시 텍스트 …) |
| `etl` | `schema_migrations`(적용된 sql 파일), `loaded_files`(파일별 적재 지문) |

- **증분 적재**: 파일 크기·수정시각을 `etl.loaded_files` 에 기록해 바뀐 파일만 그 단위(펀드·날짜)로 지우고 다시 넣습니다. 중간에 끊겨도 다음 실행 때 이어서 넣음.
- 테이블 추가/변경: `sql/<다음 번호>_xxx.sql` **새 파일**을 추가 (다음 연결 때 번호 순으로 한 번만 실행). 기존 sql 파일은 고치지 않음
- 접속: `docker compose exec postgres psql -U platform -d platform` 또는 DB 툴로 `127.0.0.1:5432`
- 백업/이전: `./scripts/db_backup.sh dump` → 새 PC 에서 `docker compose up -d` 후 `./scripts/db_backup.sh restore <파일>`

## 추출 파이프라인 추가하기 (처음 하는 사람용)

> 만든 파이프라인 코드는 **그대로** 두고, 그 위에 Dagster 용 "껍데기(asset)" 파일 하나만 얹는 방식입니다.
> 기존 코드는 고칠 필요가 없고, `defs/` 폴더의 파일은 자동으로 인식되므로 따로 등록하는 곳도 없습니다.

아래는 **"KRX 종가로 일간 수익률 계산"** 을 예로 든 전체 과정입니다 (실제로 실행해 검증한 코드).
이름만 바꿔서 그대로 따라 하면 됩니다.

### 한눈에 보기

| 단계 | 만들/고칠 파일 | 하는 일 |
| --- | --- | --- |
| 1 | `src/mac_data_platform/extractors/krx_returns.py` | **파이프라인 로직** — 순수 Python 함수 (Dagster·DB 몰라도 됨) |
| 2 | `sql/004_krx_stock_returns.sql` | **결과 테이블** 만들기 (마이그레이션) |
| 3 | `src/mac_data_platform/defs/extract/krx_returns.py` | **asset 껍데기** — DB 에서 읽고 → 1번 함수 호출 → DB 에 쓰기 |
| 4 | `config/pipelines.yaml` | 어느 job(주기)에서 돌릴지 한 줄 추가 |
| 5 | (터미널 / UI) | 확인하고 한 번 실행 |

```
data/raw (파일) ──► db/raw/krx_stock_daily ──► db/extracted/krx_stock_returns   ← 새로 붙는 부분
  수집 asset          적재 asset (이미 있음)       3번에서 만드는 asset (deps 로 연결)
```

### 1. 파이프라인 로직 — `src/mac_data_platform/extractors/krx_returns.py`

입력을 받아 결과를 돌려주는 함수면 됩니다. 이미 만든 코드가 있으면 이 폴더에 그대로 넣으세요.
(Dagster 없이 `uv run python` 으로 따로 테스트할 수 있게 두는 것이 핵심)

```python
"""예시 추출 로직: 종가 → 일간 수익률. Dagster 도 DB 도 모르는 순수 Python 함수."""

from collections.abc import Iterable, Iterator
from datetime import date
from decimal import Decimal

def daily_returns(rows: Iterable[tuple[str, date, Decimal | None]]) -> Iterator[tuple]:
    """rows: (종목코드, 날짜, 종가) — 종목코드·날짜 순으로 정렬돼 있어야 함.

    돌려주는 값: (종목코드, 날짜, 종가, 일간 수익률). 종목의 첫날은 수익률 None.
    """
    prev_code, prev_close = None, None
    for code, day, close in rows:
        ret = close / prev_close - 1 if code == prev_code and prev_close and close is not None else None
        yield code, day, close, ret
        prev_code, prev_close = code, close
```

pandas DataFrame 을 돌려주는 함수여도 됩니다 → 3번에서 `list(df.columns), df.itertuples(index=False)` 로 넘기면 됨.

### 2. 결과 테이블 (마이그레이션) — `sql/004_krx_stock_returns.sql`

**위치**: 프로젝트 루트의 `sql/` 폴더. **파일 이름**: `<다음 번호>_<설명>.sql` (지금 마지막이 `003` 이므로 `004`).

```sql
-- 004: KRX 주식 일간 수익률 (defs/extract/krx_returns.py 가 채움)
CREATE TABLE extracted.krx_stock_returns (
    isu_cd       text    NOT NULL,      -- 종목코드
    bas_dd       date    NOT NULL,      -- 기준일
    close        numeric,               -- 종가
    daily_return numeric,               -- 전일 대비 수익률 (0.01 = 1%)
    PRIMARY KEY (isu_cd, bas_dd)
);
```

- 직접 실행할 필요 없음. asset 이 처음 DB 에 연결할 때 **아직 안 돌린 sql 파일만 번호 순으로 한 번** 자동 실행됩니다.
- 실행된 파일은 `etl.schema_migrations` 에 기록됩니다: `SELECT * FROM etl.schema_migrations;`
- ⚠️ **이미 적용된 sql 파일은 고치지 마세요** (다시 실행되지 않음). 컬럼 추가 등 변경은 새 파일로:
  ```sql
  -- sql/005_krx_stock_returns_add_volume.sql
  ALTER TABLE extracted.krx_stock_returns ADD COLUMN volume numeric;
  ```
- 결과는 `extracted` 스키마, 수집 원본은 `raw` 스키마에 둡니다.
- 입력으로 쓸 수 있는 테이블·컬럼은 `sql/001_init.sql` 참고 (또는 UI 에서 `db/raw/...` asset 의 **Columns** 탭).

### 3. asset 껍데기 — `src/mac_data_platform/defs/extract/krx_returns.py`

"DB 에서 읽기 → 1번 함수 호출 → DB 에 쓰기 → UI 에 결과 표시" 네 줄짜리 틀입니다. `# ←` 표시만 바꾸면 됩니다.

```python
"""raw.krx_stock_daily → extracted.krx_stock_returns. 로직은 extractors/krx_returns.py."""

import dagster as dg

from mac_data_platform import db
from mac_data_platform.extractors.krx_returns import daily_returns     # ← 1번 함수
from mac_data_platform.pipeline import db_asset, table_result

from ..resources import PostgresResource

TABLE = "extracted.krx_stock_returns"                                  # ← 2번 테이블
COLUMNS = ["isu_cd", "bas_dd", "close", "daily_return"]                # ← 1번 함수가 돌려주는 순서

class ReturnsConfig(dg.Config):
    days: int = 30  # 실행 옵션 (UI Launchpad·pipelines.yaml 에서 바꿀 수 있음). 필요 없으면 삭제

@db_asset(
    "extracted",                 # 스키마
    "krx_stock_returns",         # 테이블 이름 → asset key 는 db/extracted/krx_stock_returns
    "krx",                       # 소스 → UI 에서 db_krx 그룹에 표시
    """
    ### KRX 주식 일간 수익률 (`extracted.krx_stock_returns`)
    `raw.krx_stock_daily` 의 종가로 최근 N일 일간 수익률을 계산합니다. 실행할 때마다 통째로 다시 만듭니다.
    """,                         # ← UI 에 보이는 설명 (Markdown)
    deps=[dg.AssetKey(["db", "raw", "krx_stock_daily"])],   # ← 입력 테이블의 asset (이게 끝난 뒤 실행됨)
)
def krx_stock_returns(context: dg.AssetExecutionContext, config: ReturnsConfig, postgres: PostgresResource) -> dg.MaterializeResult:
    with postgres.connect() as conn:
        # 1) 입력 읽기
        rows = conn.execute(
            "SELECT isu_cd, bas_dd, tdd_clsprc FROM raw.krx_stock_daily "
            "WHERE bas_dd >= (SELECT max(bas_dd) FROM raw.krx_stock_daily) - %s::int "
            "ORDER BY isu_cd, bas_dd",
            (config.days,),
        ).fetchall()
        # 2) 만든 로직 호출
        result = daily_returns(rows)
        # 3) 결과 쓰기 (None = 테이블 비우고 전부 새로. {"isu_cd": "005930"} 처럼 주면 그 행만 교체)
        n = db.replace_rows(conn, TABLE, None, COLUMNS, result)
        conn.commit()
        context.log.info(f"{TABLE}: {n:,}행")
        # 4) UI 에 행 수·컬럼·앞 5행 미리보기 표시
        return table_result(conn, TABLE, {"rows_loaded": n}, order_by="isu_cd, bas_dd")
```

자주 바꾸는 부분:
- **입력이 파일**(`data/raw/...`)이면: `deps=[dg.AssetKey(["krx", "stock_daily_quotes"])]` 처럼 수집 asset 에 연결하고, 1)에서 파일을 읽으면 됩니다.
- **결과를 파일로만** 남기려면: `db_asset` 대신 `extract_asset("krx", "이름", "설명", output="data/extracted/krx/")` 를 쓰고 결과를 `settings.DATA_DIR / "extracted" / "<소스>"` 아래에 저장 (예: `defs/extract/fundsupermarket.py`).
- **함수가 직접 DB 에 쓰는 경우**: 2)·3) 대신 `my_pipeline(conn)` 만 호출하고 4) 는 그대로.
- 재시도(실패 시 2번 재실행), 실패 기록(`logs/errors.log`)은 `db_asset` 에 이미 들어 있어 따로 할 것 없음.

### 4. 주기 정하기 — `config/pipelines.yaml`

입력 데이터를 만드는 job 의 `assets` 끝에 추가하면 **수집 → 적재 → 추출** 이 한 번에 이어서 돌아갑니다.

```yaml
  krx_open_api:
    ...
    assets: [krx/stock_list, krx/stock_daily_quotes, krx/etf_daily_quotes,
             db/raw/krx_stock_list, db/raw/krx_stock_daily, db/raw/krx_etf_daily,
             db/extracted/krx_stock_returns]          # ← 추가
    config:
      db/extracted/krx_stock_returns: {days: 90}      # ← (선택) 실행 옵션 기본값
```

따로 주기를 주고 싶으면 job 을 새로 만듭니다:
```yaml
  krx_returns:
    description: KRX 일간 수익률 계산
    schedule: "0 7 * * 1"          # 매주 월 07:00
    enabled: true
    assets: [db/extracted/krx_stock_returns]
```

### 5. 확인하고 실행

```bash
uv run dg check defs                                            # 오타·import 오류 확인 → "All definitions loaded successfully"
uv run dg launch --assets db/extracted/krx_stock_returns        # 터미널에서 한 번 실행 (sql/004 도 이때 적용됨)
```
또는 UI(http://127.0.0.1:3000/dagster/) 왼쪽 아래 **Deployment → Reload** → Assets 에서 `db_krx` 그룹 → `krx_stock_returns` → **Materialize**.
끝나면 asset 화면에 행 수·컬럼·미리보기가 보이고, DB 에서도 확인할 수 있습니다:
```bash
docker compose exec postgres psql -U platform -d platform -c "SELECT * FROM extracted.krx_stock_returns LIMIT 5"
```

### 문제가 생기면

| 증상 | 원인 / 해결 |
| --- | --- |
| UI 에 asset 이 안 보임 | Reload 안 함, 또는 `dg check defs` 에 오류 → 메시지의 파일·줄 확인 |
| `relation "extracted.xxx" does not exist` | sql 파일 이름이 `숫자_` 로 시작하는지, `sql/` 폴더에 있는지 확인 |
| sql 을 고쳤는데 반영이 안 됨 | 이미 적용된 파일은 다시 안 돌아감 → 새 번호 파일로 `ALTER TABLE ...` |
| 잘못 만든 테이블을 처음부터 다시 | `DROP TABLE extracted.xxx; DELETE FROM etl.schema_migrations WHERE version = '004_xxx';` 후 다시 실행 |
| `connection refused` | Docker 가 꺼져 있음 → `./start-dagster.sh` 다시 실행 (Postgres 도 같이 켬) |

## 수집 파이프라인 추가하기

1. `src/mac_data_platform/collectors/<소스>.py` — 크롤링 로직 (Dagster 몰라도 됨). 항목 단위 수집은 `common.run_items` 사용
2. `src/mac_data_platform/defs/collect/<소스>.py` — `@collect_asset("<소스>", "<이름>", """Markdown 설명""", site=URL, output=경로)` 으로 asset 정의
   (추출은 `extractors/` + `defs/extract/` 에 `@extract_asset`, 결과 예시는 metadata 에 `preview=csv_preview(...)`)
3. `config/pipelines.yaml` 의 `jobs` 에 job 추가 → Reload

## 코드 변경을 서버에 반영하기

GitHub 에 push 해도 서버에 **자동으로 반영되지 않습니다** (검토 후 수동 반영).

```bash
cd ~/mac-data-platform
git pull                 # 검토한 변경 받기
uv sync                  # pyproject.toml(라이브러리)이 바뀐 경우만
./start-dagster.sh       # 실행 중이던 것은 Ctrl+C 로 끄고 다시 실행
```

- 코드·`pipelines.yaml` 만 바뀌었으면 재시작 대신 UI 의 **Deployment → Reload** 로도 반영됩니다.
- `sql/` 의 새 마이그레이션은 다음 실행 때 자동 적용됩니다.
- `data/`, `.env`, `logs/`, `backups/` 는 git 에 올라가지 않습니다. 새 PC 에서는 `.env.example` 을 복사해 `.env` 를 만들고 값을 채우세요.

## 구조

```
config/pipelines.yaml            # 주기·재시도·옵션 (보통 이것만 고침)
src/mac_data_platform/
  collectors/                    # 수집 로직 (소스별, Dagster 모름)
  extractors/                    # 추출 로직 (PDF→텍스트 등, Dagster 모름)  ← 새 파이프라인 로직
  loaders/                       # 파일 → PostgreSQL 적재 로직
  defs/collect/                  # 수집 asset
  defs/extract/                  # 추출 asset                               ← 새 asset 껍데기
  defs/db/                       # 적재 asset (db/raw/...)
  defs/resources.py              # PostgresResource
  defs/schedules.py              # yaml → job/schedule
  defs/sensors.py                # 실패 run → logs/errors.log
  db.py                          # 마이그레이션·COPY·증분 적재 도구
  pipeline.py, settings.py       # 공통 (asset 데코레이터, 재시도 정책, 경로)
sql/                             # 테이블 정의 (번호 순 마이그레이션)             ← 새 테이블
compose.yaml                     # PostgreSQL 컨테이너
scripts/db_backup.sh             # DB 백업·복원
wrib-crawler/                    # 원본 저장소 clone (참고용)
```
