# fetch_incremental.py
import importlib.util
import json, sqlite3, pandas as pd, requests, re, time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RESULT_DIR = BASE_DIR / "result"
DB = RESULT_DIR / "repurchase.db"
CFG = BASE_DIR / "repurchase_params.json"
INCREMENT_CSV = RESULT_DIR / "repurchase_increment.csv"
LOADER_PATH = BASE_DIR / "load_to_db.py"
UA="Mozilla/5.0"
JSONP_RE=re.compile(r'^[\w$]+\((.*)\)\s*;?\s*$')

def ensure_result_dir():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

def parse(text):
    t=text.strip()
    if t and t[0] in "{[": import json as j; return j.loads(t)
    m=JSONP_RE.match(t); import json as j
    return j.loads(m.group(1)) if m else {}

def max_date():
    ensure_result_dir()
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("""
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
            PRIMARY KEY(code,plan_key,date)
        );
    """)
    c.execute("SELECT MAX(date) FROM buyback"); row=c.fetchone(); conn.close()
    return row[0] or "2000-01-01"

def fetch_since(api_base, params, referer, since_date):
    headers={"User-Agent":UA,"Accept":"application/json, text/javascript, */*; q=0.01","Referer":referer}
    base=dict(params); base.pop("callback", None)
    out=[]
    for p in range(1,30):
        q=dict(base); q["pageNumber"]=str(p)
        r=requests.get(api_base, params=q, headers=headers, timeout=15)
        j=parse(r.text)
        res=(j.get("result") or j.get("data",{}).get("result") or {})
        data=res.get("data") or []
        if not data: break
        df=pd.DataFrame(data)
        # 只保留 >= since_date 的
        dt_col=None
        for cand in ["TDATE","JLRQ","NOTICE_DATE","ANNOUNCE_DATE"]:
            if cand in df.columns: dt_col=cand; break
        if dt_col:
            dfx=df[pd.to_datetime(df[dt_col], errors="coerce").dt.strftime("%Y-%m-%d") >= since_date]
        else:
            dfx=df
        out.append(dfx)
        if len(df)<int(q.get("pageSize", base.get("pageSize", "200"))): break
        time.sleep(0.7)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

def run_loader():
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

if __name__=="__main__":
    ensure_result_dir()
    with open(CFG,"r",encoding="utf-8") as f: cfg=json.load(f)
    since=max_date()
    print("since", since)
    df=fetch_since(cfg["api_base"], cfg["params"], cfg["referer"], since)
    if df.empty: 
        print("no new rows"); raise SystemExit(0)
    df.to_csv(INCREMENT_CSV, index=False, encoding="utf-8-sig")
    print("increment saved:", len(df))
    run_loader()
