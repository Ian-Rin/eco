# 项目启动教程
本文档说明如何从零开始准备数据、初始化数据库并启动 FastAPI 服务，以便浏览 A 股上市公司回购数据仪表盘。
## 1. 环境准备
1. 安装 [Python 3.9+](https://www.python.org/downloads/)。
2. 克隆或下载本仓库：
   ```bash
   git clone <your-fork-or-repo-url>
   cd eco
   ```
3. （可选但推荐）创建并启用虚拟环境：
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
   Windows PowerShell 可以使用：
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
4. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
## 2. 数据拉取与入库
以下脚本位于仓库根目录，均需在此目录下运行。
1. **全量拉取东方财富回购数据**（生成 `repurchase_latest.csv`）：
   ```bash
   python fetch_runner.py
   ```
2. **增量拉取并自动合并到数据库**（生成 `repurchase_increment.csv` 并调用 `load_to_db.py`）：
   ```bash
   python fetch_incremental.py
   ```
   > 脚本会根据现有数据库中的最新日期继续抓取，无新增记录时会安全退出。
3. **同步回购计划基准表（AkShare）**，生成/更新 `plans_all.csv`、`plans_overlap_hint.csv` 并写入 SQLite：
   ```bash
   python ak_repurchase_plans.py --sqlite repurchase.db --outdir .
   ```
   - 使用 `--days N` 可以仅保留最近 N 天公告的计划。
4. **（如需手动入库）将 CSV 数据写入 SQLite**：
   ```bash
   python load_to_db.py
   ```
   - 该脚本会自动建表（若不存在）、补全 `plan_key`、去重并回写到 `repurchase.db`。
> 提示：抓取参数存放在 `repurchase_params.json` 中，可按需修改；数据库文件默认位于仓库根目录的 `repurchase.db`。
## 3. 启动 FastAPI 服务
完成数据准备后，即可启动 Web 服务：
```bash
uvicorn app_fastapi:app --host 0.0.0.0 --port 8000
```
Windows CMD 对应命令：
```cmd
uvicorn app_fastapi:app --host 0.0.0.0 --port 8000
```
启动后打开浏览器访问 `http://127.0.0.1:8000/` 查看页面。
## 4. 常用快捷脚本
- **Linux/macOS**：
  ```bash
  bash quick_start.sh
  ```
- **Windows**：
  ```cmd
  quick_start.bat
  ```
> 注意：快捷脚本会串行执行增量抓取、计划同步及服务启动，其中引用的 `ak_repurchase_plans_incremental.py` 需自行补充或替换为合适的计划同步脚本（例如 `ak_repurchase_plans.py`）。
