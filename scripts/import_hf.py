"""Hugging Face(Won1026) 데이터셋을 파이프라인 저장 구조로 옮긴다. 한 번만 실행하면 되는 이관 스크립트.

    HF_TOKEN=... uv run python scripts/import_hf.py --src data/_hf_download

수집 원본 → data/raw/<source>/... (파이프라인이 다음 실행부터 이어서 갱신)
  krx-quotes-raw/<연도>_<stock|etf>.parquet     → data/raw/krx/quotes/<YYYYMMDD>_<kind>.csv
  mics/fund_navs.zip (<ISIN>_<시작>_<끝>.csv)    → data/raw/seibro/funds/<ISIN>/nav.csv (컬럼명은 파이프라인 형식으로)
  mics/fundsupermarket_funds.zip                → data/raw/fundsupermarket/funds/<코드>/ (details.json, PDF)
추출 결과 → data/extracted/<source>/...
  mics/fundsupermarket_funds.zip 의 *.md       → data/extracted/fundsupermarket/funds/<코드>/<문서>.md
건너뜀: krx-quotes (정제본, 정제 파이프라인을 새로 만들 예정이라 2026-11-12 삭제), mics/fundsupermarket_fund_list.csv (DRM 암호화됨), mics/fund_list.csv (파이프라인이 매번 새로 받음)
"""

import argparse
import csv
import io
import shutil
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mac_data_platform.collectors.common import safe_write, write_csv  # noqa: E402
from mac_data_platform.collectors.seibro import NAV_FIELDS  # noqa: E402
from mac_data_platform.settings import DATA_DIR, RAW_DIR  # noqa: E402

EXTRACTED_DIR = DATA_DIR / "extracted"


def import_krx_quotes(src: Path) -> None:
    out_dir = RAW_DIR / "krx" / "quotes"
    n_files = 0
    for f in sorted(src.glob("*_*.parquet")):
        kind = f.stem.split("_")[1]  # stock / etf
        by_day: dict[int, list[dict]] = defaultdict(list)
        for row in pq.read_table(f).to_pylist():
            by_day[row["BAS_DD"]].append(row)
        for day, rows in by_day.items():
            out = out_dir / f"{day}_{kind}.csv"
            if not out.exists():
                write_csv(out, [{k: "" if v is None else v for k, v in r.items()} for r in rows])
                n_files += 1
        print(f"  {f.name}: {len(by_day)}일", flush=True)
    print(f"krx quotes → {out_dir} ({n_files}개 파일)")


def import_seibro_nav(zip_path: Path) -> None:
    out_dir = RAW_DIR / "seibro" / "funds"
    n = 0
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            if not name.endswith(".csv"):
                continue
            isin = name.rsplit("/", 1)[-1].split("_")[0]
            out = out_dir / isin / "nav.csv"
            if out.exists():
                continue
            reader = csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8"))
            rows = sorted(({col: row.get(api, "") for api, col in NAV_FIELDS.items()} for row in reader), key=lambda r: r["date"])
            write_csv(out, rows, list(NAV_FIELDS.values()))
            n += 1
    print(f"seibro nav → {out_dir} ({n}개 펀드)")


def import_fundsupermarket(zip_path: Path) -> None:
    raw_dir, ext_dir = RAW_DIR / "fundsupermarket" / "funds", EXTRACTED_DIR / "fundsupermarket" / "funds"
    counts: dict[str, int] = defaultdict(int)
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            parts = info.filename.split("/")
            if info.is_dir() or len(parts) != 3:
                continue
            _, code, fname = parts
            if fname.endswith(".md"):
                out = ext_dir / code / fname
            else:
                out = raw_dir / code / fname.removesuffix(".raw")  # 이 PC 에서는 .raw 접미사 안 씀
            if out.exists():
                continue
            out.parent.mkdir(parents=True, exist_ok=True)
            with z.open(info) as fin, (out.with_name(out.name + ".part")).open("wb") as fout:
                shutil.copyfileobj(fin, fout, 1 << 20)
            out.with_name(out.name + ".part").replace(out)
            counts[out.suffix] += 1
            if sum(counts.values()) % 5000 == 0:
                print(f"  {dict(counts)}", flush=True)
    print(f"fundsupermarket → {raw_dir}, {ext_dir} {dict(counts)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=Path, default=DATA_DIR / "_hf_download")
    args = parser.parse_args()
    import_seibro_nav(args.src / "mics" / "fund_navs.zip")
    import_krx_quotes(args.src / "krx-quotes-raw")
    import_fundsupermarket(args.src / "mics" / "fundsupermarket_funds.zip")
    safe_write(DATA_DIR / "IMPORTED_FROM_HF.txt", b"Won1026/krx-quotes-raw, Won1026/krx-quotes, Won1026/mics (2026-11-11)\n")


if __name__ == "__main__":
    main()
