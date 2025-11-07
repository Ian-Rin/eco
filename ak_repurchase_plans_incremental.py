#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""增量更新 ak_repurchase_plans 生成的计划表，并自动写入 SQLite 及调用 load_to_db。"""
from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

import ak_repurchase_plans as plans
from fetch_incremental import run_loader as run_buyback_loader

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "repurchase.db"
PLANS_ALL_CSV = BASE_DIR / "plans_all.csv"
PLANS_INCREMENT_CSV = BASE_DIR / "plans_increment.csv"

REVISIT_DAYS = 7  # 为了兜底最近几日的变更，重抓一定缓冲区


def _read_latest_from_db() -> Optional[pd.Timestamp]:
    if not DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(DB_PATH)
    except Exception:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(announce_date) FROM ak_plans")
        row = cur.fetchone()
    except Exception:
        return None
    finally:
        conn.close()
    if not row:
        return None
    value = row[0]
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.normalize()


def _read_latest_from_csv() -> Optional[pd.Timestamp]:
    if not PLANS_ALL_CSV.exists():
        return None
    try:
        df = pd.read_csv(PLANS_ALL_CSV, encoding="utf-8-sig")
    except Exception:
        return None
    if "announce_date" not in df.columns:
        return None
    ts = pd.to_datetime(df["announce_date"], errors="coerce")
    ts = ts.dropna()
    if ts.empty:
        return None
    return ts.max().normalize()


def detect_latest_announce() -> Optional[pd.Timestamp]:
    latest = _read_latest_from_db()
    if latest is not None:
        return latest
    return _read_latest_from_csv()


def filter_since(plans_df: pd.DataFrame, since: Optional[pd.Timestamp]) -> pd.DataFrame:
    if since is None or plans_df.empty:
        return plans_df
    cutoff = since
    if REVISIT_DAYS > 0:
        cutoff = cutoff - timedelta(days=REVISIT_DAYS)
    cutoff = cutoff.normalize()
    dt = pd.to_datetime(plans_df["announce_date"], errors="coerce")
    mask = dt >= cutoff
    return plans_df.loc[mask]


def update_plans_csv(increment: pd.DataFrame) -> None:
    if increment.empty:
        return
    try:
        existing = pd.read_csv(PLANS_ALL_CSV, encoding="utf-8-sig") if PLANS_ALL_CSV.exists() else pd.DataFrame()
    except Exception:
        existing = pd.DataFrame()
    combined = pd.concat([existing, increment], ignore_index=True) if not existing.empty else increment.copy()
    if not combined.empty:
        combined = combined.drop_duplicates(subset=["code", "plan_key", "version"], keep="last")
        combined = combined.sort_values(["code", "announce_date", "version"], kind="mergesort")
    PLANS_ALL_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(PLANS_ALL_CSV, index=False, encoding="utf-8-sig")


def main() -> None:
    latest = detect_latest_announce()
    if latest is not None:
        print(f"[INFO] 最新公告日期: {latest.date()}，将重抓最近 {REVISIT_DAYS} 天的数据")
    else:
        print("[INFO] 未检测到历史计划数据，抓取全量表")

    raw = plans.load_akshare_raw()
    if raw.empty:
        print("[WARN] AkShare 返回空表，可能是网络或源站问题")
        return

    normalized = plans.normalize(raw)
    normalized = filter_since(normalized, latest)

    if normalized.empty:
        print("[INFO] 无新增计划记录")
        return

    normalized.to_csv(PLANS_INCREMENT_CSV, index=False, encoding="utf-8-sig")
    print(f"[OK] 增量计划保存至 {PLANS_INCREMENT_CSV}，行数: {len(normalized)}")

    plans.to_sqlite(str(DB_PATH), normalized)
    print(f"[OK] 已写入 SQLite: {DB_PATH}")

    update_plans_csv(normalized)
    print(f"[OK] plans_all.csv 已更新: {PLANS_ALL_CSV}")

    run_buyback_loader()


if __name__ == "__main__":
    main()
