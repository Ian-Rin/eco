# fetch_runner.py
import json, time, re
import pandas as pd
import requests
from pathlib import Path

UA = "Mozilla/5.0"
JSONP_RE = re.compile(r'^[\w$]+\((.*)\)\s*;?\s*$')
BASE_DIR = Path(__file__).resolve().parent
PARAMS_PATH = BASE_DIR / "repurchase_params.json"
LATEST_CSV = BASE_DIR / "repurchase_latest.csv"

def parse_json_maybe_jsonp(text: str):
    t = text.strip()
    if t and t[0] in "{[":
        return json.loads(t)
    m = JSONP_RE.match(t)
    if not m:
        raise ValueError("not json/jsonp")
    import json as _j
    return _j.loads(m.group(1))

def fetch_all(api_base, params, referer, max_pages=20, sleep_s=0.8):
    headers = {"User-Agent": UA, "Accept": "application/json, text/javascript, */*; q=0.01", "Referer": referer}
    base = dict(params); base.pop("callback", None)
    frames=[]
    for p in range(1, max_pages+1):
        q = dict(base); q["pageNumber"]=str(p)
        r = requests.get(api_base, params=q, headers=headers, timeout=15)
        try:
            j = parse_json_maybe_jsonp(r.text)
        except Exception:
            # 尝试带 callback
            r2 = requests.get(api_base, params={**params, "pageNumber": str(p)}, headers=headers, timeout=15)
            j = parse_json_maybe_jsonp(r2.text)
        res = (j or {}).get("result") or (j or {}).get("data", {}).get("result") or {}
        data = res.get("data") or []
        df = pd.DataFrame(data)
        if df.empty: break
        frames.append(df)
        if len(df) < int(q.get("pageSize", params.get("pageSize", "200"))): break
        time.sleep(sleep_s)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    m = {
        "SCODE": "股票代码",
        "SNAME": "股票简称",
        "HGSL": "已回购数量",
        "HGJE": "已回购金额",
        "HGZDJ": "已回购均价",
        "TDATE": "披露日期",
        "JLRQ": "记录日期",
        "REPURCHASE_PROGRESS": "回购进度",
        "SECURITY_CODE": "股票代码",
        "SECURITY_NAME_ABBR": "股票简称",
        "BUYBACK_AMT": "已回购金额",
        "BUYBACK_VOL": "已回购数量",
        "ANNOUNCE_DATE": "公告日期",
        "NOTICE_DATE": "公告日期",
        "START_DATE": "回购开始日期",
        "END_DATE": "回购截止日期"
    }
    for k,v in m.items():
        if k in df.columns and v not in df.columns:
            df[v]=df[k]
    return df

if __name__ == "__main__":
    with open(PARAMS_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    df = fetch_all(cfg["api_base"], cfg["params"], cfg["referer"], max_pages=20, sleep_s=0.7)
    df = normalize(df)
    df.to_csv(LATEST_CSV, index=False, encoding="utf-8-sig")
    print(f"CSV updated: {LATEST_CSV}, rows={len(df)}")
