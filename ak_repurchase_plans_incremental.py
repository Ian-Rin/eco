# ak_repurchase_plans_incremental.py
import importlib.util
import sqlite3
from pathlib import Path
from typing import List, Optional

import pandas as pd

import ak_repurchase_plans as ak_plans

BASE_DIR = Path(__file__).resolve().parent
RESULT_DIR = BASE_DIR / "result"
DB = RESULT_DIR / "repurchase.db"
ALL_CSV = RESULT_DIR / "plans_all.csv"
INCREMENT_CSV = RESULT_DIR / "plans_increment.csv"
OVERLAP_CSV = RESULT_DIR / "plans_overlap_hint.csv"
LOADER_PATH = BASE_DIR / "load_to_db.py"

PLAN_COLUMNS: List[str] = [
    "code",
    "sec_name",
    "plan_key",
    "version",
    "announce_date",
    "start_date",
    "price_lower",
    "price_upper",
    "amount_upper",
    "volume_upper",
    "latest_price",
    "progress_text",
]


def ensure_result_dir() -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)


def ensure_plan_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ak_plans(
          code TEXT, sec_name TEXT, plan_key TEXT, version INTEGER,
          announce_date TEXT, start_date TEXT,
          price_lower REAL, price_upper REAL, amount_upper REAL, volume_upper REAL,
          latest_price REAL, progress_text TEXT,
          PRIMARY KEY(code, plan_key, version)
        );
        """
    )
    conn.commit()


def align_columns(df: pd.DataFrame) -> pd.DataFrame:
    work = pd.DataFrame(df)
    for col in PLAN_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA
    work = work[PLAN_COLUMNS]
    work["plan_key"] = work["plan_key"].fillna("").astype(str)
    work["code"] = work["code"].apply(ak_plans.normalize_code_str).fillna("").astype(str)
    return work


def load_existing() -> pd.DataFrame:
    ensure_result_dir()
    existing = pd.DataFrame()
    if DB.exists():
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(DB)
            ensure_plan_table(conn)
            existing = pd.read_sql_query("SELECT * FROM ak_plans", conn)
        except Exception:
            existing = pd.DataFrame()
        finally:
            if conn is not None:
                conn.close()
    if existing.empty and ALL_CSV.exists():
        try:
            existing = pd.read_csv(ALL_CSV, encoding="utf-8-sig")
        except Exception:
            existing = pd.DataFrame()
    return align_columns(existing)


def last_announce_date(existing: pd.DataFrame) -> str:
    if existing.empty:
        return "2000-01-01"
    dt = pd.to_datetime(existing["announce_date"], errors="coerce").dropna()
    if dt.empty:
        return "2000-01-01"
    return dt.max().strftime("%Y-%m-%d")


def fetch_normalized() -> pd.DataFrame:
    raw = ak_plans.load_akshare_raw()
    return ak_plans.normalize(raw)


def filter_incremental(df: pd.DataFrame, since: str) -> pd.DataFrame:
    if df.empty:
        return df
    cutoff = pd.to_datetime(since, errors="coerce")
    if pd.isna(cutoff):
        cutoff = pd.Timestamp("2000-01-01")
    dt = pd.to_datetime(df["announce_date"], errors="coerce")
    inc = df.loc[dt >= cutoff].copy()
    return align_columns(inc)


def recompute_versions(df: pd.DataFrame) -> pd.DataFrame:
    work = align_columns(df)
    if work.empty:
        return work
    work["announce_dt"] = pd.to_datetime(work["announce_date"], errors="coerce")
    work = work.sort_values(["code", "plan_key", "announce_dt"], kind="mergesort")
    work = work.drop_duplicates(subset=["code", "plan_key", "announce_date"], keep="last")
    work["version"] = work.groupby(["code", "plan_key"]).cumcount() + 1
    return work.drop(columns=["announce_dt"])


def run_loader() -> None:
    if not LOADER_PATH.exists():
        print("loader script not found:", LOADER_PATH)
        return
    spec = importlib.util.spec_from_file_location("load_to_db_module", LOADER_PATH)
    if spec is None or spec.loader is None:
        print("failed to load loader module spec")
        return
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "load_to_db"):
        print("loader module missing load_to_db() function")
        return
    rows, sources = module.load_to_db()
    print(f"load_to_db executed: {rows} rows merged (sources: {sources})")


def main() -> None:
    ensure_result_dir()
    existing = load_existing()
    since = last_announce_date(existing)
    print("last announce_date:", since)

    normalized = fetch_normalized()
    incremental = filter_incremental(normalized, since)
    if incremental.empty:
        print("no new plan rows")
        return

    combined = recompute_versions(pd.concat([existing, incremental], ignore_index=True))
    ak_plans.to_sqlite(str(DB), combined)

    cutoff = pd.to_datetime(since, errors="coerce")
    if pd.isna(cutoff):
        cutoff = pd.Timestamp("2000-01-01")
    incremental_out = combined.loc[pd.to_datetime(combined["announce_date"], errors="coerce") >= cutoff].copy()

    incremental_out.to_csv(INCREMENT_CSV, index=False, encoding="utf-8-sig")
    combined.to_csv(ALL_CSV, index=False, encoding="utf-8-sig")
    ak_plans.detect_overlap(combined).to_csv(OVERLAP_CSV, index=False, encoding="utf-8-sig")

    print(f"increment saved: {len(incremental_out)} rows -> {INCREMENT_CSV}")
    print(f"plans_all updated: {len(combined)} rows -> {ALL_CSV}")
    run_loader()


if __name__ == "__main__":
    main()
