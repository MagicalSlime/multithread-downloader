#!/usr/bin/env python3
"""离屏渲染界面预览图。

**不截屏**。用 Edge 的无头模式在离屏环境里渲染 `web/index.html` 并直接出图，
全程不读取屏幕内容——不需要保证窗口在最前，也不会拍到别人的东西。

做法：把真实的 index.html 读进来，在 app.js 之前插一段假的桥接 API，
写进临时目录再交给无头 Edge 渲染。用的是同一份 HTML/CSS/JS，
所以预览图跟真实界面一致，不会各写一套慢慢跑偏。

用法：  python tests/preview.py [输出路径]
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"

EDGE_CANDIDATES = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
]

MB = 1 << 20

MOCK_TASKS = [
    {"id": 1, "name": "ubuntu-24.04-desktop-amd64.iso", "state": "running",
     "total": 40 * MB, "done": int(23.4 * MB), "percent": 58.5,
     "detail": "23.40 MB / 40.00 MB   ·   2.57 MB/s   ·   剩余 00:06   ·   分片 8/8"},
    {"id": 2, "name": "PyCharm-2026.1.exe", "state": "running",
     "total": 40 * MB, "done": int(21.2 * MB), "percent": 53.0,
     "detail": "21.20 MB / 40.00 MB   ·   2.31 MB/s   ·   剩余 00:08   ·   分片 8/8"},
    {"id": 3, "name": "dataset-2026.tar.gz", "state": "queued",
     "total": 0, "done": 0, "percent": 0,
     "detail": "大小未知   ·   等待其他任务让出名额"},
    {"id": 4, "name": "paused-archive.zip", "state": "paused",
     "total": 100 * MB, "done": int(45.6 * MB), "percent": 45.6,
     "detail": "45.60 MB / 100.00 MB"},
    {"id": 5, "name": "broken-link.bin", "state": "failed",
     "total": 0, "done": 0, "percent": 0,
     "detail": "大小未知   ·   文件不存在（404），请检查下载链接"},
    {"id": 6, "name": "finished-notice.pdf", "state": "done",
     "total": 12 * MB, "done": 12 * MB, "percent": 100.0,
     "detail": "12.00 MB / 12.00 MB"},
]

MOCK_EVENTS = [
    {"time": "21:53:42", "text": "[ubuntu-24.04-desktop-amd64.iso] 服务器支持分片下载，文件大小 40.00 MB。", "tag": ""},
    {"time": "21:53:42", "text": "[ubuntu-24.04-desktop-amd64.iso] 开始下载：共 8 个分片，每片约 5.00 MB。", "tag": ""},
    {"time": "21:53:44", "text": "[PyCharm-2026.1.exe] 分片 3/8 第 1 次重试（连接超时），2.0 秒后从当前位置继续。", "tag": ""},
    {"time": "21:53:47", "text": "[broken-link.bin] 文件不存在（404），请检查下载链接", "tag": "error"},
    {"time": "21:53:51", "text": "[dataset-2026.tar.gz] 排队等待中，等前面任务让出名额", "tag": "muted"},
    {"time": "21:54:02", "text": "[finished-notice.pdf] 下载完成：D:\\Downloads\\finished-notice.pdf", "tag": "ok"},
]

MOCK_SETTINGS = {
    "save_dir": "D:\\Downloads", "threads": 8, "resume": True, "verify_tls": True,
    "max_retries": 5, "user_agent": "", "referer": "", "max_concurrent": 3,
    "rate_limit": 5 * MB, "autoscroll_log": True,
}

NOOP_METHODS = [
    "add_task", "start_task", "pause_task", "cancel_task", "remove_task",
    "start_all", "pause_all", "clear_finished", "set_concurrency",
    "set_rate_limit", "browse", "open_file", "reveal_file", "clipboard",
]


def build_mock() -> str:
    stubs = ",\n".join(
        f"    {name}: async () => ({{ok: false}})" for name in NOOP_METHODS)
    return f"""
window.pywebview = {{ api: {{
    get_settings: async () => ({json.dumps(MOCK_SETTINGS)}),
    snapshot: async () => ({json.dumps(MOCK_TASKS, ensure_ascii=False)}),
    drain_events: async () => ({json.dumps(MOCK_EVENTS, ensure_ascii=False)}),
    prepare_cancel: async () => ({{done: 0}}),
{stubs}
}} }};
"""


def find_edge() -> str:
    for path in EDGE_CANDIDATES:
        if os.path.exists(path):
            return path
    raise SystemExit("找不到 Edge，无法离屏渲染")


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "_preview.png"
    html = (WEB / "index.html").read_text(encoding="utf-8")

    # 在 app.js 之前插入假桥接，其余（HTML/CSS/JS）全部用真货
    marker = '<script src="app.js"></script>'
    if marker not in html:
        raise SystemExit("index.html 里找不到 app.js 的引用，预览脚本需要同步更新")
    mock = f"<script>{build_mock()}</script>\n{marker}"

    work = Path(tempfile.mkdtemp(prefix="dl-preview-"))
    preview = work / "preview.html"
    preview.write_text(html.replace(marker, mock), encoding="utf-8")
    # CSS/JS 走绝对路径引用，省得再拷一份（拷贝就会和真文件脱节）
    for asset in ("style.css", "app.js"):
        target = work / asset
        if not target.exists():
            target.write_text((WEB / asset).read_text(encoding="utf-8"),
                              encoding="utf-8")

    profile = work / "profile"
    cmd = [
        find_edge(), "--headless=new", "--disable-gpu", "--hide-scrollbars",
        f"--user-data-dir={profile}", "--window-size=980,860",
        f"--screenshot={out}", preview.as_uri(),
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=120)
    if not out.exists():
        sys.stderr.write(result.stderr.decode("utf-8", "replace")[-2000:])
        raise SystemExit("无头渲染没有产出图片")
    print(f"预览图已生成：{out}  ({out.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
