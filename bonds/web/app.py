import os
import json
import threading
from flask import Flask, render_template, jsonify, request
import pandas as pd
from bond_downloader import run_download, download_progress, CONFIG

app = Flask(__name__)

# 下载任务的后台线程引用
download_thread = None

@app.route('/')
def index():
    """渲染主页面"""
    return render_template('index.html')

@app.route('/api/progress')
def progress():
    """返回当前下载进度"""
    with download_progress['_lock'] if hasattr(download_progress, '_lock') else None:
        # 简单复制一份
        prog = {
            "status": download_progress["status"],
            "total": download_progress["total"],
            "current": download_progress["current"],
            "current_code": download_progress["current_code"],
            "current_name": download_progress["current_name"],
            "message": download_progress["message"],
            "errors": download_progress["errors"][-10:]  # 只返回最近10条错误
        }
    return jsonify(prog)

@app.route('/api/start', methods=['POST'])
def start_download():
    """启动下载任务（如果未运行）"""
    global download_thread
    if download_progress["status"] == "running":
        return jsonify({"error": "下载任务已在运行中"}), 400
    # 重置进度变量
    download_progress.update({
        "status": "idle",
        "total": 0,
        "current": 0,
        "current_code": "",
        "current_name": "",
        "message": "",
        "errors": []
    })
    download_thread = threading.Thread(target=run_download)
    download_thread.daemon = True
    download_thread.start()
    return jsonify({"message": "下载任务已启动"})

@app.route('/api/preview')
def preview():
    """预览最近保存的债券数据（默认展示 summary.csv 最后几行）"""
    summary_path = CONFIG["download"]["summary_csv_path"]
    if not os.path.exists(summary_path):
        return jsonify({"error": "汇总文件不存在，请先下载数据"}), 404
    try:
        # 读取最后20行，只返回部分列
        df = pd.read_csv(summary_path, encoding='utf-8')
        # 取最后20行
        preview_df = df.tail(20)
        # 转换日期列为字符串
        if 'datetime' in preview_df.columns:
            preview_df['datetime'] = preview_df['datetime'].astype(str)
        data = preview_df.to_dict(orient='records')
        return jsonify({"data": data, "columns": list(preview_df.columns)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)