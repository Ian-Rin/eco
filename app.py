# -*- coding: utf-8 -*-
import sqlite3
from pathlib import Path
from functools import lru_cache
from typing import List, Optional, Dict, Any, Union

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
RESULT_DIR = BASE_DIR / "result"
DEFAULT_DB_PATH = RESULT_DIR / "repurchase.db"
LEGACY_DB_PATH = BASE_DIR / "repurchase.db"
DB_PATH = DEFAULT_DB_PATH if DEFAULT_DB_PATH.exists() else LEGACY_DB_PATH
STATIC_DIR = BASE_DIR / "static"
TEMPLATE_DIR = BASE_DIR / "templates"

LEGACY_PLAN_PREFIX = "__DEFAULT__:"

if not DB_PATH.exists():
    raise SystemExit(f"数据库不存在：{DEFAULT_DB_PATH}，请先运行抓取与入库脚本。")

app = FastAPI(title="A股上市公司回购")

# 静态/模板
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


@lru_cache(maxsize=16)
def asset_version(name: str) -> str:
    path = STATIC_DIR / name
    try:
        stat = path.stat()
        return str(int(stat.st_mtime))
    except FileNotFoundError:
        return "0"


def read_sql(query: str, params: Optional[tuple] = None) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def format_code(value: Union[str, int, float, None]) -> str:
    if value is None:
        return ""
    code_str = str(value).strip()
    if not code_str:
        return ""
    # 如果全为数字，补齐为6位
    if code_str.isdigit():
        return code_str.zfill(6)
    return code_str.upper()


def _normalize_label_piece(value: Optional[Any]) -> str:
    """Convert any label component to a clean string."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except TypeError:
        pass
    return str(value).strip()


def build_plan_label(plan_key: str, plan_progress: Optional[Any] = None, plan_announce: Optional[Any] = None) -> str:
    if not plan_key:
        return ""
    if plan_key.startswith(LEGACY_PLAN_PREFIX):
        return "默认计划"
    pieces = []
    plan_announce_norm = _normalize_label_piece(plan_announce)
    if plan_announce_norm:
        pieces.append(plan_announce_norm)
    plan_progress_norm = _normalize_label_piece(plan_progress)
    if plan_progress_norm:
        pieces.append(plan_progress_norm)
    label = " · ".join([p for p in pieces if p])
    return label if label else plan_key.upper()


@lru_cache(maxsize=1)
def load_plan_reference() -> pd.DataFrame:
    try:
        plans = read_sql(
            """
            SELECT code, plan_key, version, announce_date, start_date,
                   price_lower, price_upper, amount_upper, volume_upper,
                   latest_price, progress_text
            FROM ak_plans
            """
        )
    except Exception:
        plans = pd.DataFrame()

    if plans.empty:
        for csv_path in [RESULT_DIR / "plans_all.csv", BASE_DIR / "plans_all.csv"]:
            if csv_path.exists():
                try:
                    plans = pd.read_csv(csv_path, encoding="utf-8-sig")
                    break
                except Exception:
                    plans = pd.DataFrame()

    if plans.empty:
        return plans

    plans["code"] = plans["code"].apply(format_code)
    plans["plan_key"] = plans["plan_key"].fillna("").astype(str).str.strip()
    plans = plans[plans["plan_key"] != ""]
    if plans.empty:
        return plans

    if "version" in plans.columns:
        plans = plans.sort_values(["code", "plan_key", "version"], ascending=[True, True, False])
        plans = plans.drop_duplicates(subset=["code", "plan_key"], keep="first")

    plans["announce_date"] = pd.to_datetime(plans.get("announce_date"), errors="coerce")
    plans["start_date"] = pd.to_datetime(plans.get("start_date"), errors="coerce")
    plans["price_lower"] = pd.to_numeric(plans.get("price_lower"), errors="coerce")
    plans["price_upper"] = pd.to_numeric(plans.get("price_upper"), errors="coerce")
    plans["amount_upper"] = pd.to_numeric(plans.get("amount_upper"), errors="coerce")
    plans["volume_upper"] = pd.to_numeric(plans.get("volume_upper"), errors="coerce")
    plans["latest_price"] = pd.to_numeric(plans.get("latest_price"), errors="coerce")
    if "progress_text" not in plans.columns:
        plans["progress_text"] = pd.NA
    plans = plans.sort_values(["code", "announce_date", "plan_key"], kind="mergesort")
    return plans


def load_buyback(
    date_from: str,
    date_to: Optional[str],
    code: str
) -> pd.DataFrame:
    query = """
    SELECT code,plan_key,name,date,amount,volume,avg_price,progress,start_date,end_date
    FROM buyback
    WHERE date >= ?
    """
    params: List[Any] = [date_from]
    if date_to:
        query += " AND date <= ?"
        params.append(date_to)
    code_filter = code.strip()
    if code_filter:
        trimmed = code_filter.lstrip("0")
        like_exact = f"%{code_filter}%"
        like_trimmed = f"%{trimmed}%" if trimmed and trimmed != code_filter else None
        if like_trimmed:
            query += " AND (code LIKE ? OR code LIKE ?)"
            params.extend([like_exact, like_trimmed])
        else:
            query += " AND code LIKE ?"
            params.append(like_exact)

    df = read_sql(query, tuple(params))
    if df.empty:
        return df

    # 基础类型清洗
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["code"] = df["code"].apply(format_code)
    df["plan_key"] = df["plan_key"].fillna("").astype(str).str.strip()
    df["name"] = df["name"].fillna("").astype(str)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    df["avg_price"] = pd.to_numeric(df["avg_price"], errors="coerce").round(2)
    df["progress"] = df["progress"].fillna("")
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    df = df.dropna(subset=["code", "date"])
    df = df[df["code"] != ""]
    mask = df["plan_key"] == ""
    if mask.any():
        df.loc[mask, "plan_key"] = (
            LEGACY_PLAN_PREFIX + df.loc[mask, "code"].fillna("UNKNOWN")
        )
    plan_ref = load_plan_reference()
    plan_meta = pd.DataFrame()
    if not plan_ref.empty:
        plan_meta = plan_ref.rename(columns={
            "plan_key": "plan_key",
            "announce_date": "plan_announce_dt",
            "start_date": "plan_start_dt",
            "price_lower": "plan_price_lower",
            "price_upper": "plan_price_upper",
            "amount_upper": "plan_amount_upper",
            "volume_upper": "plan_volume_upper",
            "latest_price": "plan_latest_price",
            "progress_text": "plan_progress_text",
        })
        plan_meta["plan_announce_date"] = plan_meta["plan_announce_dt"].dt.strftime("%Y-%m-%d")
        plan_meta["plan_start_date"] = plan_meta["plan_start_dt"].dt.strftime("%Y-%m-%d")
        plan_meta = plan_meta[[
            "code",
            "plan_key",
            "plan_announce_dt",
            "plan_start_dt",
            "plan_announce_date",
            "plan_start_date",
            "plan_price_lower",
            "plan_price_upper",
            "plan_amount_upper",
            "plan_volume_upper",
            "plan_latest_price",
            "plan_progress_text",
        ]]
        df = df.merge(plan_meta, on=["code", "plan_key"], how="left")
    else:
        df["plan_announce_date"] = None
        df["plan_start_date"] = None
        df["plan_price_lower"] = pd.NA
        df["plan_price_upper"] = pd.NA
        df["plan_amount_upper"] = pd.NA
        df["plan_volume_upper"] = pd.NA
        df["plan_latest_price"] = pd.NA
        df["plan_progress_text"] = None

    df["plan_label"] = df.apply(
        lambda row: build_plan_label(
            row.get("plan_key", ""),
            row.get("plan_progress_text"),
            row.get("plan_announce_date")
        ),
        axis=1
    )

    # 分组累计（按计划维度）
    df = df.sort_values(["code", "plan_key", "date"])
    df["cumulative_amount"] = df.groupby(["code", "plan_key"])["amount"].cumsum()
    df["cumulative_volume"] = df.groupby(["code", "plan_key"])["volume"].cumsum()

    # 计算进度百分比（针对单一计划）
    total_per_plan = df.groupby(["code", "plan_key"])["cumulative_amount"].transform("max").replace(0, pd.NA)
    df["progress_pct"] = ((df["cumulative_amount"] / total_per_plan) * 100).round(2)
    df["progress_pct"] = df["progress_pct"].fillna(0.0)

    return df


def build_dashboard_payload(
    date_from: str,
    date_to: Optional[str],
    code: str,
    limit: int
) -> Dict[str, Any]:
    df = load_buyback(date_from, date_to, code)
    if df.empty:
        return {
            "summary": {
                "date_from": date_from,
                "date_to": date_to,
                "total_amount": 0.0,
                "total_volume": 0.0,
                "unique_codes": 0,
                "avg_daily_amount": 0.0,
                "latest_date": None
            },
            "table": [],
            "charts": {
                "trend": {"dates": [], "amounts": []},
                "top": {"date": None, "labels": [], "values": []}
            }
        }

    df = df.reset_index(drop=True)
    df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

    # 汇总
    total_amount = float(df["amount"].sum())
    total_volume = float(df["volume"].sum())
    unique_codes = int(df["code"].nunique())
    unique_plans = int(df[["code", "plan_key"]].drop_duplicates().shape[0])
    distinct_dates = df["date_str"].nunique()
    avg_daily_amount = float(total_amount / distinct_dates) if distinct_dates else 0.0
    latest_date = df["date"].max()
    latest_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else None

    # 趋势图
    trend_df = (
        df.groupby("date_str", as_index=False)["amount"]
        .sum()
        .sort_values("date_str")
    )
    trend_payload = {
        "dates": trend_df["date_str"].tolist(),
        "amounts": [float(x) for x in trend_df["amount"].tolist()]
    }

    # TopN 图
    top_payload = {"date": latest_str, "labels": [], "values": []}
    if latest_str:
        latest_df = df[df["date_str"] == latest_str]
        if not latest_df.empty:
            top_df = (
                latest_df.groupby("code", as_index=False)["amount"]
                .sum()
                .sort_values("amount", ascending=False)
                .head(20)
            )
            top_payload = {
                "date": latest_str,
                "labels": top_df["code"].tolist(),
                "values": [float(x) for x in top_df["amount"].tolist()]
            }

    # 表格数据（按日期/金额排序）
    table_df = df.sort_values(
        ["date", "code", "plan_key", "amount"],
        ascending=[False, True, True, False]
    ).head(limit)
    table_records: List[Dict[str, Any]] = []
    for row in table_df.itertuples(index=False):
        table_records.append({
            "code": row.code,
            "name": row.name,
            "plan_key": row.plan_key,
            "plan_label": row.plan_label,
            "date": row.date.strftime("%Y-%m-%d"),
            "amount": float(row.amount),
            "cumulative_amount": float(row.cumulative_amount),
            "volume": float(row.volume),
            "cumulative_volume": float(row.cumulative_volume),
            "avg_price": float(row.avg_price) if pd.notna(row.avg_price) else None,
            "progress_text": row.progress,
            "plan_progress_text": row.plan_progress_text if isinstance(row.plan_progress_text, str) else None,
            "plan_amount_upper": float(row.plan_amount_upper) if pd.notna(row.plan_amount_upper) else None,
            "plan_volume_upper": float(row.plan_volume_upper) if pd.notna(row.plan_volume_upper) else None,
            "plan_price_lower": float(row.plan_price_lower) if pd.notna(row.plan_price_lower) else None,
            "plan_price_upper": float(row.plan_price_upper) if pd.notna(row.plan_price_upper) else None,
            "plan_latest_price": float(row.plan_latest_price) if pd.notna(row.plan_latest_price) else None,
            "plan_announce_date": row.plan_announce_date if isinstance(row.plan_announce_date, str) else None,
            "plan_start_date": row.plan_start_date if isinstance(row.plan_start_date, str) else None,
            "progress_pct": float(row.progress_pct),
            "start_date": row.start_date.strftime("%Y-%m-%d") if pd.notna(row.start_date) else None,
            "end_date": row.end_date.strftime("%Y-%m-%d") if pd.notna(row.end_date) else None
        })

    return {
        "summary": {
            "date_from": date_from,
            "date_to": date_to,
            "total_amount": total_amount,
            "total_volume": total_volume,
            "unique_codes": unique_codes,
            "unique_plans": unique_plans,
            "avg_daily_amount": avg_daily_amount,
            "latest_date": latest_str
        },
        "table": table_records,
        "charts": {
            "trend": trend_payload,
            "top": top_payload
        }
    }

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    # 页面初始化：给到可选日期范围
    df = read_sql("SELECT MIN(date) AS min_d, MAX(date) AS max_d FROM buyback")
    if df.empty or pd.isna(df.loc[0, "max_d"]):
        raise HTTPException(500, "库里暂无数据，请先入库。")
    min_raw = df.loc[0, "min_d"]
    max_raw = df.loc[0, "max_d"]
    min_dt = pd.to_datetime(min_raw)
    max_dt = pd.to_datetime(max_raw)
    default_from = max_dt - pd.Timedelta(days=30)
    if default_from < min_dt:
        default_from = min_dt
    min_d = min_dt.strftime("%Y-%m-%d")
    max_d = max_dt.strftime("%Y-%m-%d")
    init_from = default_from.strftime("%Y-%m-%d")
    return templates.TemplateResponse("index.html", {
        "request": request,
        "min_date": min_d,
        "max_date": max_d,
        "default_from": init_from,
        "asset_version": asset_version("app.js")
    })

@app.get("/api/dashboard")
def api_dashboard(
    date_from: str = Query(..., description="起始日期 YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD，可空"),
    code: str = Query("", description="按代码模糊过滤，可空"),
    limit: int = Query(500, ge=1, le=5000)
):
    payload = build_dashboard_payload(date_from=date_from, date_to=date_to, code=code, limit=limit)
    return JSONResponse(payload)
