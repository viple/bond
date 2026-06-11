#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
可转债数据项目入口
支持两种运行模式:
  1. python run.py              # 仅下载数据
  2. python run.py --web         # 启动 Web 服务 (下载 + 可视化)
  3. python run.py --web --port 8080  # 自定义端口
"""

import sys
import os

# 将 bond/ 和 web/ 加入搜索路径
_BASE = os.path.dirname(os.path.abspath(__file__))
_BOND_DIR = os.path.join(_BASE, "bond")
_WEB_DIR = os.path.join(_BASE, "web")
for d in (_BOND_DIR, _WEB_DIR):
    if d not in sys.path:
        sys.path.insert(0, d)


def run_download_only():
    """仅运行数据下载"""
    from bond_downloader import run_download, load_config
    load_config()
    print("=" * 50)
    print("  可转债数据下载器")
    print("=" * 50)
    run_download()


def run_web_server(port=5000):
    """启动 Web 服务"""
    from web.app import app
    print(f"🌐 启动 Web 服务: http://127.0.0.1:{port}")
    app.run(debug=False, port=port)


if __name__ == "__main__":
    if "--web" in sys.argv:
        port = 5000
        for i, arg in enumerate(sys.argv):
            if arg == "--port" and i + 1 < len(sys.argv):
                try:
                    port = int(sys.argv[i + 1])
                except ValueError:
                    pass
        run_web_server(port=port)
    else:
        run_download_only()
