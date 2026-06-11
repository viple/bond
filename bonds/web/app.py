import os
import sys
import threading

from flask import Flask, render_template, jsonify
import pandas as pd

# ---------- 修复导入路径 ----------
# 将 bond/ 目录加入 Python 搜索路径，使 bond_downloader 可导入
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_BOND_DIR = os.path.join(_APP_DIR, "..", "bond")
if _BOND_DIR not in sys.path:
    sys.path.insert(0, _BOND_DIR)

from bond_downloader import run_download, get_progress_snapshot, reset_progress

app = Flask(__name__)

# ---------- 后台线程引用 ----------
download_thread = None


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "接口不存在"}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "服务器内部错误"}), 500


# ---------- 工具函数 ----------
def _dataframe_to_safe_dict(df, tail=20):
    """将 DataFrame 转为安全的 dict 列表，处理 NaN 和日期类型"""
    df = df.tail(tail).copy()
    # 日期列转字符串
    date_cols = [c for c in df.columns if c.lower() in ("datetime", "date")]
    for col in date_cols:
        df[col] = df[col].astype(str)
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                record[col] = None
            else:
                record[col] = val
        records.append(record)
    return {"data": records, "columns": list(df.columns)}


# ---------- 路由 ----------
@app.route("/")
def index():
    """渲染主页面"""
    return render_template("index.html")


@app.route("/api/progress")
def progress():
    """返回当前下载进度的线程安全快照"""
    return jsonify(get_progress_snapshot())


@app.route("/api/start", methods=["POST"])
def start_download():
    """启动下载任务"""
    global download_thread
    if get_progress_snapshot()["status"] == "running":
        return jsonify({"error": "下载任务已在运行中"}), 409
    reset_progress()
    download_thread = threading.Thread(target=run_download)
    download_thread.daemon = True
    download_thread.start()
    return jsonify({"message": "下载任务已启动", "status": "running"})


@app.route("/api/preview")
def preview():
    """预览最近保存的债券数据"""
    from bond_downloader import get_config

    summary_path = get_config()["download"]["summary_csv_path"]
    if not os.path.exists(summary_path):
        return jsonify({"error": "汇总文件不存在", "data": [], "columns": []}), 404
    try:
        df = pd.read_csv(summary_path, encoding="utf-8")
        return jsonify(_dataframe_to_safe_dict(df, tail=20))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
