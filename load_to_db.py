# load_to_db.py
import sqlite3, pandas as pd
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
RESULT_DIR = BASE_DIR / "result"
DB = RESULT_DIR / "repurchase.db"
LATEST_CSV = RESULT_DIR / "repurchase_latest.csv"
INCREMENT_CSV = RESULT_DIR / "repurchase_increment.csv"
PLANS_CSV = RESULT_DIR / "plans_all.csv"
LEGACY_LATEST_CSV = BASE_DIR / "repurchase_latest.csv"
LEGACY_INCREMENT_CSV = BASE_DIR / "repurchase_increment.csv"
LEGACY_PLANS_CSV = BASE_DIR / "plans_all.csv"

LEGACY_PLAN_PREFIX = "__DEFAULT__:"

schema = """
CREATE TABLE IF NOT EXISTS buyback (
  code TEXT NOT NULL,
  plan_key TEXT NOT NULL,
  name TEXT,
  date TEXT NOT NULL,
  amount REAL,
  volume REAL,
  avg_price REAL,
  progress TEXT,
  start_date TEXT,
  end_date TEXT,
  PRIMARY KEY (code, plan_key, date)
);
"""


def ensure_result_dir() -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)


def ensure_buyback_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='buyback'")
    has_table = cur.fetchone() is not None
    if not has_table:
        cur.executescript(schema)
        conn.commit()
        return

    cur.execute("PRAGMA table_info(buyback)")
    info = cur.fetchall()
    columns = {col[1] for col in info}
    if "plan_key" in columns:
        return
    has_plan_code = "plan_code" in columns

    # legacy schema migration: replace plan_code -> plan_key while keeping historical rows
    cur.execute("ALTER TABLE buyback RENAME TO buyback_legacy")
    conn.commit()
    cur.executescript(schema)
    conn.commit()
    if has_plan_code:
        cur.execute(
            """
            INSERT INTO buyback(code, plan_key, name, date, amount, volume, avg_price, progress, start_date, end_date)
            SELECT
                code,
                COALESCE(NULLIF(plan_code,''), ? || COALESCE(NULLIF(code,''), 'UNKNOWN')),
                name,
                date,
                amount,
                volume,
                avg_price,
                progress,
                start_date,
                end_date
            FROM buyback_legacy
            """,
            (LEGACY_PLAN_PREFIX,),
        )
    else:
        cur.execute(
            """
            INSERT INTO buyback(code, plan_key, name, date, amount, volume, avg_price, progress, start_date, end_date)
            SELECT
                code,
                ? || COALESCE(NULLIF(code,''), 'UNKNOWN'),
                name,
                date,
                amount,
                volume,
                avg_price,
                progress,
                start_date,
                end_date
            FROM buyback_legacy
            """,
            (LEGACY_PLAN_PREFIX,),
        )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS buyback_legacy")
    conn.commit()


def normalize_code_value(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int, float)):
        if float(value).is_integer():
            return f"{int(value):06d}"
        return str(value).strip()
    value_str = str(value).strip()
    if not value_str:
        return pd.NA
    try:
        numeric = float(value_str)
        if numeric.is_integer():
            return f"{int(numeric):06d}"
    except ValueError:
        pass
    if value_str.isdigit() and len(value_str) < 6:
        return value_str.zfill(6)
    return value_str.upper()


def load_plan_reference(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        plans = pd.read_sql_query(
            """
            SELECT code, plan_key, version, announce_date, start_date,
                   price_lower, price_upper, amount_upper, volume_upper,
                   latest_price, progress_text
            FROM ak_plans
            """,
            conn,
        )
    except Exception:
        plans = pd.DataFrame()

    csv_candidates = [PLANS_CSV, LEGACY_PLANS_CSV]
    if plans.empty:
        for csv_path in csv_candidates:
            if not csv_path.exists():
                continue
            try:
                plans = pd.read_csv(csv_path, encoding="utf-8-sig")
                break
            except Exception:
                plans = pd.DataFrame()

    if plans.empty:
        return plans

    plans["code"] = plans["code"].apply(normalize_code_value)
    plans["plan_key"] = plans["plan_key"].fillna("").astype(str).str.strip()
    plans = plans[plans["plan_key"] != ""]
    if plans.empty:
        return plans

    plans["announce_date"] = pd.to_datetime(plans["announce_date"], errors="coerce")
    plans["start_date"] = pd.to_datetime(plans["start_date"], errors="coerce")
    plans["price_lower"] = pd.to_numeric(plans.get("price_lower"), errors="coerce")
    plans["price_upper"] = pd.to_numeric(plans.get("price_upper"), errors="coerce")
    plans["amount_upper"] = pd.to_numeric(plans.get("amount_upper"), errors="coerce")
    plans["volume_upper"] = pd.to_numeric(plans.get("volume_upper"), errors="coerce")
    plans["latest_price"] = pd.to_numeric(plans.get("latest_price"), errors="coerce")

    if "version" in plans.columns:
        plans = plans.sort_values(["code", "plan_key", "version"], ascending=[True, True, False])
        plans = plans.drop_duplicates(subset=["code", "plan_key"], keep="first")

    plans = plans.sort_values(["code", "announce_date", "plan_key"], kind="mergesort")
    return plans


def build_plan_lookup(plan_df: pd.DataFrame) -> Dict[str, list[dict[str, Any]]]:
    if plan_df.empty:
        return {}

    plans = plan_df.copy()
    plans["code"] = plans["code"].apply(normalize_code_value)
    plans = plans.dropna(subset=["code"])
    if plans.empty:
        return {}

    plans = plans.rename(columns={"announce_date": "announce_dt"})
    plans["announce_dt"] = pd.to_datetime(plans["announce_dt"], errors="coerce")
    plans["start_dt"] = pd.to_datetime(plans.get("start_date"), errors="coerce")
    plans = plans.dropna(subset=["announce_dt"])
    if plans.empty:
        return {}

    plans = plans.sort_values(["code", "announce_dt", "plan_key"], kind="mergesort").reset_index(drop=True)

    plan_dict: Dict[str, list[dict[str, Any]]] = {}
    for code, group in plans.groupby("code", sort=False):
        plan_dict[str(code)] = group.to_dict("records")
    return plan_dict


def resolve_plan_key(code: Any, date_val: Any, plan_lookup: Dict[str, list[dict[str, Any]]]) -> Optional[str]:
    if not plan_lookup:
        return None
    code_val = normalize_code_value(code)
    if pd.isna(code_val):
        return None
    candidates = plan_lookup.get(str(code_val))
    if not candidates:
        return None
    dt = pd.to_datetime(date_val, errors="coerce")
    if pd.isna(dt):
        return None
    for plan in reversed(candidates):
        ann = plan.get("announce_dt")
        if pd.isna(ann) or ann > dt:
            continue
        start_dt = plan.get("start_dt")
        if pd.notna(start_dt) and start_dt > dt:
            continue
        chosen_key = plan.get("plan_key")
        if chosen_key:
            return chosen_key
    return None


def assign_plan_keys(buy_df: pd.DataFrame, plan_lookup: Dict[str, list[dict[str, Any]]]) -> pd.DataFrame:
    if buy_df.empty or not plan_lookup:
        return buy_df

    work = buy_df.reset_index(drop=True).copy()
    work["plan_key"] = work["plan_key"].fillna("").astype(str).str.strip()
    work["code"] = work["code"].apply(normalize_code_value)
    work["date_dt"] = pd.to_datetime(work["date"], errors="coerce")

    for idx, row in work.iterrows():
        chosen_key = resolve_plan_key(row.get("code"), row.get("date_dt"), plan_lookup)
        if chosen_key:
            work.at[idx, "plan_key"] = chosen_key

    work.drop(columns=["date_dt"], inplace=True)
    return work


def rehydrate_existing_plan_keys(
    conn: sqlite3.Connection,
    plan_lookup: Dict[str, list[dict[str, Any]]]
) -> Tuple[int, int]:
    if not plan_lookup:
        return (0, 0)

    legacy_df = pd.read_sql_query(
        "SELECT rowid, code, plan_key, date FROM buyback WHERE plan_key LIKE ?",
        conn,
        params=(f"{LEGACY_PLAN_PREFIX}%",),
    )
    if legacy_df.empty:
        return (0, 0)

    legacy_df["code"] = legacy_df["code"].apply(normalize_code_value)
    reassigned = assign_plan_keys(legacy_df, plan_lookup)

    cur = conn.cursor()
    updated = 0
    deleted = 0
    for row in reassigned.itertuples(index=False):
        new_key = getattr(row, "plan_key", None)
        if not isinstance(new_key, str) or not new_key or new_key.startswith(LEGACY_PLAN_PREFIX):
            continue
        code_val = getattr(row, "code", None)
        date_val = getattr(row, "date", None)
        rowid = getattr(row, "rowid")
        if code_val is None or date_val is None:
            continue
        if pd.isna(code_val) or pd.isna(date_val):
            continue
        code_str = str(code_val)
        date_str = str(date_val)
        cur.execute(
            "SELECT 1 FROM buyback WHERE code = ? AND plan_key = ? AND date = ?",
            (code_str, new_key, date_str),
        )
        if cur.fetchone():
            cur.execute("DELETE FROM buyback WHERE rowid = ?", (rowid,))
            deleted += 1
            continue
        cur.execute("UPDATE buyback SET plan_key = ? WHERE rowid = ?", (new_key, rowid))
        updated += 1

    if updated or deleted:
        conn.commit()
    return (updated, deleted)


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    def pick(colnames):
        for c in colnames:
            if c in df.columns:
                return c
        return None

    code = pick(["股票代码","SECURITY_CODE","SCODE"])
    name = pick(["股票简称","SECURITY_NAME_ABBR","SNAME"])
    date = pick(["披露日期","记录日期","公告日期","TDATE","JLRQ","NOTICE_DATE","ANNOUNCE_DATE"])
    amount = pick(["已回购金额","BUYBACK_AMT","HGJE"])
    volume = pick(["已回购数量","BUYBACK_VOL","HGSL"])
    avgp = pick(["已回购均价","HGZDJ"])
    prog = pick(["回购进度","REPURCHASE_PROGRESS"])
    sd = pick(["回购开始日期","START_DATE"])
    ed = pick(["回购截止日期","END_DATE"])
    plan = pick(["计划编号","PLAN_CODE","PLAN_ID","REPURCHASE_PLAN_ID","REPURCHASE_ID","BUYBACK_PLAN_CODE"])

    def normalize_code(series):
        if series is None:
            return pd.Series(dtype="string")
        return series.apply(normalize_code_value).astype("string")

    # 日期列安全转换
    def safe_date(col):
        if col is None or col not in df.columns:
            return None
        return pd.to_datetime(df[col], errors="coerce").dt.date.astype("string")

    def normalize_plan(series):
        if series is None:
            return pd.Series(dtype="string")

        def to_plan(value: Any) -> Any:
            if pd.isna(value):
                return pd.NA
            value_str = str(value).strip()
            if not value_str:
                return pd.NA
            # convert float-like strings (e.g. "12345.0") into integers
            try:
                numeric = float(value_str)
                if numeric.is_integer():
                    return f"{int(numeric)}"
            except ValueError:
                pass
            return value_str

        return series.apply(to_plan).astype("string")

    out = pd.DataFrame({
        "code": normalize_code(df.get(code)),
        "plan_key": normalize_plan(df.get(plan)),
        "name": df.get(name),
        "date": safe_date(date),
        "amount": pd.to_numeric(df.get(amount), errors="coerce"),
        "volume": pd.to_numeric(df.get(volume), errors="coerce"),
        "avg_price": pd.to_numeric(df.get(avgp), errors="coerce"),
        "progress": df.get(prog),
        "start_date": safe_date(sd),
        "end_date": safe_date(ed),
    })

    # 去掉 code/date 缺失的行
    out = out.dropna(subset=["code", "date"])

    if "plan_key" not in out.columns:
        out["plan_key"] = pd.Series(dtype="string")

    if not out.empty:
        out["plan_key"] = out["plan_key"].fillna(pd.NA)
        mask = out["plan_key"].isna() | (out["plan_key"].str.strip() == "")
        if mask.any():
            out.loc[mask, "plan_key"] = (
                LEGACY_PLAN_PREFIX + out.loc[mask, "code"].fillna("UNKNOWN")
            )

    return out


def load_to_db(
    latest_csv: Path = LATEST_CSV,
    increment_csv: Path = INCREMENT_CSV,
    db_path: Path = DB
) -> tuple[int, int]:
    ensure_result_dir()
    sources = []
    if latest_csv.exists():
        sources.append(pd.read_csv(latest_csv))
    elif latest_csv == LATEST_CSV and LEGACY_LATEST_CSV.exists():
        sources.append(pd.read_csv(LEGACY_LATEST_CSV))
    if increment_csv.exists():
        sources.append(pd.read_csv(increment_csv))
    elif increment_csv == INCREMENT_CSV and LEGACY_INCREMENT_CSV.exists():
        sources.append(pd.read_csv(LEGACY_INCREMENT_CSV))
    if not sources:
        raise SystemExit("No CSV sources found. Expected repurchase_latest.csv and/or repurchase_increment.csv")

    raw_df = pd.concat(sources, ignore_index=True)
    df = normalize_types(raw_df)

    conn = sqlite3.connect(db_path)
    ensure_buyback_table(conn)
    plan_reference = load_plan_reference(conn)
    plan_lookup = build_plan_lookup(plan_reference)

    updated, deleted = rehydrate_existing_plan_keys(conn, plan_lookup)
    if updated or deleted:
        print(
            f"[INFO] 修复历史计划匹配：更新 {updated} 条，删除 {deleted} 条冗余记录"
        )

    df = assign_plan_keys(df, plan_lookup)

    if not df.empty:
        df["plan_key"] = df["plan_key"].fillna(pd.NA)
        missing = df["plan_key"].isna() | (df["plan_key"].str.strip() == "")
        if missing.any():
            df.loc[missing, "plan_key"] = (
                LEGACY_PLAN_PREFIX + df.loc[missing, "code"].fillna("UNKNOWN")
            )

    df = df.drop_duplicates(subset=["code", "plan_key", "date"], keep="last")

    cur = conn.cursor()

    rows = 0
    for rec in df.itertuples(index=False):
        cur.execute("""
            INSERT INTO buyback(code,plan_key,name,date,amount,volume,avg_price,progress,start_date,end_date)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(code,plan_key,date) DO UPDATE SET
                name=excluded.name,
                amount=COALESCE(excluded.amount,buyback.amount),
                volume=COALESCE(excluded.volume,buyback.volume),
                avg_price=COALESCE(excluded.avg_price,buyback.avg_price),
                progress=COALESCE(excluded.progress,buyback.progress),
                start_date=COALESCE(excluded.start_date,buyback.start_date),
                end_date=COALESCE(excluded.end_date,buyback.end_date)
        """, tuple(rec))
        rows += 1
    cur.execute("""
        DELETE FROM buyback
        WHERE plan_key GLOB ?
          AND EXISTS (
            SELECT 1 FROM buyback AS newer
            WHERE newer.code = buyback.code
              AND newer.date = buyback.date
              AND newer.plan_key <> buyback.plan_key
        )
    """, (f"{LEGACY_PLAN_PREFIX}*",))
    conn.commit(); conn.close()
    return rows, len(sources)


if __name__ == "__main__":
    rows, sources = load_to_db()
    print(f"Upsert done: {rows} rows processed into {DB} (sources: {sources})")
