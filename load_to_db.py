# load_to_db.py
import sqlite3, pandas as pd
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DB = BASE_DIR / "repurchase.db"
LATEST_CSV = BASE_DIR / "repurchase_latest.csv"
INCREMENT_CSV = BASE_DIR / "repurchase_increment.csv"

LEGACY_PLAN_PREFIX = "__DEFAULT__:"

schema = """
CREATE TABLE IF NOT EXISTS buyback (
  code TEXT NOT NULL,
  plan_code TEXT NOT NULL,
  name TEXT,
  date TEXT NOT NULL,
  amount REAL,
  volume REAL,
  avg_price REAL,
  progress TEXT,
  start_date TEXT,
  end_date TEXT,
  PRIMARY KEY (code, plan_code, date)
);
"""


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
    has_plan_code = any(col[1] == "plan_code" for col in info)
    if has_plan_code:
        return

    # legacy schema migration: add plan_code while keeping historical rows
    cur.execute("ALTER TABLE buyback RENAME TO buyback_legacy")
    conn.commit()
    cur.executescript(schema)
    conn.commit()
    cur.execute(
        """
        INSERT INTO buyback(code, plan_code, name, date, amount, volume, avg_price, progress, start_date, end_date)
        SELECT
            code,
            ? || COALESCE(NULLIF(code, ''), 'UNKNOWN'),
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
    plan = pick(["HKCODE","计划编号","PLAN_CODE","PLAN_ID","REPURCHASE_PLAN_ID","REPURCHASE_ID","BUYBACK_PLAN_CODE"])

    def normalize_code(series):
        if series is None:
            return pd.Series(dtype="string")

        def to_code(value: Any) -> Any:
            if pd.isna(value):
                return pd.NA
            # 处理数值类型（含 2352.0 这类浮点表示）
            if isinstance(value, (int, float)):
                if float(value).is_integer():
                    return f"{int(value):06d}"
                return str(value).strip()

            value_str = str(value).strip()
            if not value_str:
                return pd.NA

            # 字符串形式的浮点数（例如 "2352.0"）
            try:
                numeric = float(value_str)
                if numeric.is_integer():
                    return f"{int(numeric):06d}"
            except ValueError:
                pass

            if value_str.isdigit() and len(value_str) < 6:
                return value_str.zfill(6)
            return value_str.upper()

        return series.apply(to_code).astype("string")

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
        "plan_code": normalize_plan(df.get(plan)),
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

    if "plan_code" not in out.columns:
        out["plan_code"] = pd.Series(dtype="string")

    if not out.empty:
        out["plan_code"] = out["plan_code"].fillna(pd.NA)
        mask = out["plan_code"].isna() | (out["plan_code"].str.strip() == "")
        if mask.any():
            out.loc[mask, "plan_code"] = (
                LEGACY_PLAN_PREFIX + out.loc[mask, "code"].fillna("UNKNOWN")
            )

    return out


def load_to_db(
    latest_csv: Path = LATEST_CSV,
    increment_csv: Path = INCREMENT_CSV,
    db_path: Path = DB
) -> tuple[int, int]:
    sources = []
    if latest_csv.exists():
        sources.append(pd.read_csv(latest_csv))
    if increment_csv.exists():
        sources.append(pd.read_csv(increment_csv))
    if not sources:
        raise SystemExit("No CSV sources found. Expected repurchase_latest.csv and/or repurchase_increment.csv")

    raw_df = pd.concat(sources, ignore_index=True)
    df = normalize_types(raw_df)
    df = df.drop_duplicates(subset=["code", "plan_code", "date"], keep="last")

    conn = sqlite3.connect(db_path)
    ensure_buyback_table(conn)
    cur = conn.cursor()

    rows = 0
    for rec in df.itertuples(index=False):
        cur.execute("""
            INSERT INTO buyback(code,plan_code,name,date,amount,volume,avg_price,progress,start_date,end_date)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(code,plan_code,date) DO UPDATE SET
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
        WHERE plan_code GLOB ?
          AND EXISTS (
            SELECT 1 FROM buyback AS newer
            WHERE newer.code = buyback.code
              AND newer.date = buyback.date
              AND newer.plan_code <> buyback.plan_code
        )
    """, (f"{LEGACY_PLAN_PREFIX}*",))
    conn.commit(); conn.close()
    return rows, len(sources)


if __name__ == "__main__":
    rows, sources = load_to_db()
    print(f"Upsert done: {rows} rows processed into {DB} (sources: {sources})")
