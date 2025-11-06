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
DB_PATH = BASE_DIR / "repurchase.db"
STATIC_DIR = BASE_DIR / "static"
TEMPLATE_DIR = BASE_DIR / "templates"

LEGACY_PLAN_PREFIX = "__DEFAULT__:"

if not DB_PATH.exists():
    raise SystemExit(f"数据库不存在：{DB_PATH}，请先运行抓取与入库脚本。")

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


def build_plan_label(plan_code: str) -> str:
    if not plan_code:
        return ""
    return "默认计划" if plan_code.startswith(LEGACY_PLAN_PREFIX) else plan_code


def load_buyback(
    date_from: str,
    date_to: Optional[str],
    code: str
) -> pd.DataFrame:
    query = """
    SELECT code,plan_code,name,date,amount,volume,avg_price,progress,start_date,end_date
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
    df["plan_code"] = df["plan_code"].fillna("").astype(str).str.strip()
    df["name"] = df["name"].fillna("").astype(str)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    df["avg_price"] = pd.to_numeric(df["avg_price"], errors="coerce").round(2)
    df["progress"] = df["progress"].fillna("")
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    df = df.dropna(subset=["code", "date"])
    df = df[df["code"] != ""]
    mask = df["plan_code"] == ""
    if mask.any():
        df.loc[mask, "plan_code"] = (
            LEGACY_PLAN_PREFIX + df.loc[mask, "code"].fillna("UNKNOWN")
        )
    df["plan_label"] = df["plan_code"].apply(build_plan_label)

    # 分组累计（按计划维度）
    df = df.sort_values(["code", "plan_code", "date"])
    df["cumulative_amount"] = df.groupby(["code", "plan_code"])["amount"].cumsum()
    df["cumulative_volume"] = df.groupby(["code", "plan_code"])["volume"].cumsum()

    # 计算进度百分比（针对单一计划）
    total_per_plan = df.groupby(["code", "plan_code"])["cumulative_amount"].transform("max").replace(0, pd.NA)
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
    unique_plans = int(df[["code", "plan_code"]].drop_duplicates().shape[0])
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
        ["date", "code", "plan_code", "amount"],
        ascending=[False, True, True, False]
    ).head(limit)
    table_records: List[Dict[str, Any]] = []
    for row in table_df.itertuples(index=False):
        table_records.append({
            "code": row.code,
            "name": row.name,
            "plan_code": row.plan_code,
            "plan_label": row.plan_label,
            "date": row.date.strftime("%Y-%m-%d"),
            "amount": float(row.amount),
            "cumulative_amount": float(row.cumulative_amount),
            "volume": float(row.volume),
            "cumulative_volume": float(row.cumulative_volume),
            "avg_price": float(row.avg_price) if pd.notna(row.avg_price) else None,
            "progress_text": row.progress,
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
