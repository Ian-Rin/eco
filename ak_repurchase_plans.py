#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ak_plans_min.py
从 AkShare 的 东方财富-股份回购 “计划/方案” 数据构建计划表，生成 plan_key / plan_version。
默认会通过 fetch_runner.py 所能拿到的数据范围，自动确定最早公告日期到当前系统时间的区间；
如需限制为最近 N 天，可通过 --days 指定。
用法：
  python ak_plans_min.py --outdir /root/1/rep --days 183
可选写入 SQLite：
  python ak_plans_min.py --outdir . --sqlite repurchase_plan.db
"""
import json
import os, re, argparse, hashlib, sqlite3
from datetime import timedelta, date
from typing import Optional, Tuple
import pandas as pd
from pathlib import Path

import fetch_runner


DEFAULT_AUTO_WINDOW_DAYS = 183

def load_akshare_raw() -> pd.DataFrame:
    import akshare as ak
    # 东方财富-股票-回购-回购股份-回购进展（含计划区间&起始时间等），AkShare 会聚合全市场
    df = ak.stock_repurchase_em()
    # 典型列（以你日志为准）：
    # ['序号','股票代码','股票简称','最新价','计划回购价格区间','计划回购数量区间-下限','计划回购数量区间-上限',
    #  '占公告前一日总股本比例-下限','占公告前一日总股本比例-上限','计划回购金额区间-下限','计划回购金额区间-上限',
    #  '回购起始时间','实施进度','已回购股份价格区间-下限','已回购股份价格区间-上限','已回购股份数量','已回购金额','最新公告日期']
    return df

def parse_range_to_lo_hi(s: str):
    """把 '10.00-12.00元' / '—' 之类转成 (lo, hi) 浮点对；失败返回 (None, None)"""
    if s is None or s == "" or str(s).strip("—- ").lower() in {"nan", "none"}:
        return (None, None)
    txt = str(s)
    # 提取所有数字（含小数）
    nums = re.findall(r"[0-9]+(?:\.[0-9]+)?", txt)
    if not nums:
        return (None, None)
    if len(nums) == 1:
        x = float(nums[0])
        return (x, x)
    lo, hi = float(nums[0]), float(nums[1])
    if lo > hi:
        lo, hi = hi, lo
    return (lo, hi)

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # 只保留我们关心的列；不同版本可能列名有细微差异，这里尽量兜底
    col = df.columns
    m = {
        "code": "股票代码" if "股票代码" in col else "代码",
        "name": "股票简称" if "股票简称" in col else "名称",
        "latest_price": "最新价",
        "plan_price_range": "计划回购价格区间",
        "plan_vol_lo": "计划回购数量区间-下限",
        "plan_vol_hi": "计划回购数量区间-上限",
        "plan_amt_lo": "计划回购金额区间-下限",
        "plan_amt_hi": "计划回购金额区间-上限",
        "start_date": "回购起始时间",
        "progress": "实施进度",
        "ann_date": "最新公告日期",
    }
    out = pd.DataFrame()
    for k, v in m.items():
        out[k] = df.get(v)

    # 解析价格区间（一般单位：元/股）
    pr_lo, pr_hi = [], []
    for s in out["plan_price_range"]:
        lo, hi = parse_range_to_lo_hi(s)
        pr_lo.append(lo); pr_hi.append(hi)
    out["price_upper"] = pd.Series(pr_hi, dtype="float64")  # 上限价
    out["price_lower"] = pd.Series(pr_lo, dtype="float64")

    # 金额区间（一般单位：亿元；有的页面是“万元”，AkShare通常做过单位统一，这里不强转）
    def to_float(x):
        try:
            return float(str(x).replace(",", ""))
        except:
            return None
    for c in ["plan_vol_lo", "plan_vol_hi", "plan_amt_lo", "plan_amt_hi"]:
        out[c] = out[c].apply(to_float)

    # 规范日期
    out["start_date"] = pd.to_datetime(out["start_date"], errors="coerce").dt.date.astype("string")
    out["ann_date"] = pd.to_datetime(out["ann_date"], errors="coerce").dt.date.astype("string")

    # 生成 plan_key（同一公司可能存在并行计划：用公告日+价格上限+金额上限+数量上限+起始日构指纹）
    def make_plan_key(row):
        s = f"{row.get('code') or ''}|{row.get('ann_date') or ''}|{row.get('price_upper') or ''}|{row.get('plan_amt_hi') or ''}|{row.get('plan_vol_hi') or ''}|{row.get('start_date') or ''}"
        return hashlib.md5(s.encode()).hexdigest()[:16]
    out["plan_key"] = out.apply(make_plan_key, axis=1)

    # 版本号（同一 plan_key 可能有多次“最新公告日期”变更，这里按 ann_date 排序给序号）
    out = out.sort_values(["code", "plan_key", "ann_date"], kind="mergesort")
    out["version"] = out.groupby(["code", "plan_key"]).cumcount() + 1

    # 统一计划表字段
    res = out.rename(columns={
        "plan_amt_hi": "amount_upper",
        "plan_vol_hi": "volume_upper",
        "progress": "progress_text",
        "ann_date": "announce_date",
        "name": "sec_name",
    })[
        ["code","sec_name","plan_key","version",
         "announce_date","start_date",
         "price_lower","price_upper","amount_upper","volume_upper",
         "latest_price","progress_text"]
    ]
    return res

def detect_overlap(plans: pd.DataFrame) -> pd.DataFrame:
    """用 start_date + 缺失 end_date 的情况下，按同 code 的 plan_key 之间的“起始日接近”粗识别并行（Ak 这张表多数无 end_date，更多用于并行提示）"""
    if plans.empty:
        return pd.DataFrame(columns=["code","plan_key_1","plan_key_2","start_1","start_2","announce_1","announce_2"])
    x = plans.copy()
    x["sd"] = pd.to_datetime(x["start_date"], errors="coerce")
    x["ad"] = pd.to_datetime(x["announce_date"], errors="coerce")
    out = []
    for code, g in x.groupby("code"):
        g = g.sort_values(["sd","ad"])
        arr = g.to_dict("records")
        n = len(arr)
        for i in range(n):
            for j in range(i+1, n):
                a, b = arr[i], arr[j]
                # 起始日相差 <= 30 天，且公告日相隔 >= 1 天，认为可能是并行不同计划（经验规则，可按需调整）
                try:
                    d_sd = abs((a["sd"] - b["sd"]).days)
                    d_ad = abs((a["ad"] - b["ad"]).days)
                except Exception:
                    continue
                if pd.notna(a["sd"]) and pd.notna(b["sd"]) and d_sd <= 30 and d_ad >= 1:
                    out.append({
                        "code": code,
                        "plan_key_1": a["plan_key"], "plan_key_2": b["plan_key"],
                        "start_1": str(a["start_date"]), "start_2": str(b["start_date"]),
                        "announce_1": str(a["announce_date"]), "announce_2": str(b["announce_date"]),
                    })
    return pd.DataFrame(out).drop_duplicates()

def to_sqlite(db_path: str, plans: pd.DataFrame):
    if not db_path: return
    directory = os.path.dirname(db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ak_plans(
      code TEXT, sec_name TEXT, plan_key TEXT, version INTEGER,
      announce_date TEXT, start_date TEXT,
      price_lower REAL, price_upper REAL, amount_upper REAL, volume_upper REAL,
      latest_price REAL, progress_text TEXT,
      PRIMARY KEY(code, plan_key, version)
    );""")
    conn.commit()
    if not plans.empty:
        plans.to_sql("tmp_ak_plans", conn, if_exists="replace", index=False)
        cur.executescript("""
        INSERT OR REPLACE INTO ak_plans
        SELECT * FROM tmp_ak_plans;
        DROP TABLE tmp_ak_plans;""")
        conn.commit()
    conn.close()

def filter_recent(df: pd.DataFrame, days: int, min_date: Optional[date] = None) -> pd.DataFrame:
    if days <= 0 and not min_date:
        return df
    col = None
    for cand in ["最新公告日期", "公告日期", "披露日期", "NOTICE_DATE", "ANNOUNCE_DATE"]:
        if cand in df.columns:
            col = cand
            break
    if not col:
        return df
    dt = pd.to_datetime(df[col], errors="coerce")
    if dt.isna().all():
        return df
    today = pd.Timestamp.utcnow().normalize().date()
    if min_date:
        cutoff = min(min_date, today)
    else:
        cutoff = today - timedelta(days=days)
    mask = dt.dt.date >= cutoff
    return df.loc[mask]


def detect_fetch_runner_start() -> Tuple[Optional[date], bool]:
    cfg_path = getattr(fetch_runner, "PARAMS_PATH", Path(__file__).resolve().parent / "repurchase_params.json")
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return None, False

    api_base = cfg.get("api_base")
    params = cfg.get("params") or {}
    referer = cfg.get("referer")
    if not api_base or not params:
        return None, False

    try:
        df = fetch_runner.fetch_all(api_base, params, referer, max_pages=20, sleep_s=0.7)
    except Exception as exc:
        today = pd.Timestamp.utcnow().normalize().date()
        fallback_start = today - timedelta(days=DEFAULT_AUTO_WINDOW_DAYS)
        print(
            f"[WARN] fetch_runner 获取公告日期范围失败: {exc}. "
            f"将使用最近 {DEFAULT_AUTO_WINDOW_DAYS} 天窗口兜底。"
        )
        return fallback_start, True

    if df.empty:
        return None, False

    df = fetch_runner.normalize(df)
    for cand in ["公告日期", "披露日期", "TDATE", "NOTICE_DATE", "ANNOUNCE_DATE"]:
        if cand in df.columns:
            dates = pd.to_datetime(df[cand], errors="coerce")
            dates = dates.dropna()
            if not dates.empty:
                return dates.min().date(), False
    return None, False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=".")
    ap.add_argument("--sqlite", default="", help="写入 SQLite 的文件名（可选）")
    ap.add_argument(
        "--days",
        type=int,
        default=0,
        help="仅保留最近 N 天内公告的计划；默认自动检测 plans_all.csv 的最早公告日期",
    )
    args = ap.parse_args()

    raw = load_akshare_raw()
    min_date = None
    used_fallback_window = False
    if args.days <= 0:
        min_date, used_fallback_window = detect_fetch_runner_start()
        if min_date:
            today = pd.Timestamp.utcnow().normalize().date()
            if used_fallback_window:
                print(
                    f"[INFO] 使用兜底窗口 {min_date} 至 {today} (最近 {DEFAULT_AUTO_WINDOW_DAYS} 天) 作为过滤范围"
                )
            else:
                print(f"[INFO] 使用 fetch_runner 最早可获取的公告日期 {min_date} 至 {today} 作为过滤范围")
    raw = filter_recent(raw, args.days, min_date=min_date)

    if raw.empty:
        print("[INFO] 目标区间无回购相关记录（或源站限流）。已输出空表。")
        # 也导出一个空 CSV 供后续流程保持一致
        empty = pd.DataFrame(columns=[
            "code","sec_name","plan_key","version","announce_date","start_date",
            "price_lower","price_upper","amount_upper","volume_upper",
            "latest_price","progress_text"
        ])
        os.makedirs(args.outdir, exist_ok=True)
        empty.to_csv(os.path.join(args.outdir, "plans_all.csv"), index=False, encoding="utf-8-sig")
        to_sqlite(args.sqlite, empty)
        return

    plans = normalize(raw)
    overlaps = detect_overlap(plans)

    uniq_codes = sorted(set(plans["code"].dropna().astype(str)))
    print(f"[INFO] 检测到 {len(uniq_codes)} 只股票的回购计划，输出目录: {os.path.abspath(args.outdir)}")

    os.makedirs(args.outdir, exist_ok=True)
    plans.to_csv(os.path.join(args.outdir, "plans_all.csv"), index=False, encoding="utf-8-sig")
    overlaps.to_csv(os.path.join(args.outdir, "plans_overlap_hint.csv"), index=False, encoding="utf-8-sig")
    to_sqlite(args.sqlite, plans)
    print(f"[OK] 导出: {os.path.join(args.outdir, 'plans_all.csv')}")
    print(f"[OK] 导出: {os.path.join(args.outdir, 'plans_overlap_hint.csv')}  (经验规则提示并行计划, 可人工复核)")

if __name__ == "__main__":
    main()
