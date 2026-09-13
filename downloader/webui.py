"""pywebview 界面：HTML/CSS 渲染，Python 只负责提供数据和执行动作。

界面代码在 `web/` 目录下（index.html / style.css / app.js）。这里只做三件事：
  * 把下载管理器的状态整理成 JSON 交给 JS
  * 接收 JS 发来的操作请求
  * 管好窗口生命周期与偏好设置的存取

刷新沿用和之前一样的两条通道：`snapshot()` 拉高频数值，`drain_events()` 取
离散事件。**Python 侧永远不从工作线程往界面推东西**，省掉一整类线程安全问题。

pywebview 会从别的线程调用这些方法，所以它们必须线程安全——DownloadManager
本身就是线程安全的，这里不需要额外加锁。
"""

from __future__ import annotations

import ctypes
import os
import queue
import sys
import time

import webview

from .config import AppSettings
from .manager import DownloadManager
from .util import derive_filename_from_url, fmt_eta, fmt_size, fmt_speed

WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")

# 事件种类 -> 日志着色
_LOG_TAG = {"error": "error", "done": "ok", "paused": "muted", "cancelled": "muted"}


# --------------------------------------------------------------------------- 剪贴板

def clipboard_text() -> str:
    """读系统剪贴板。pywebview 没有提供，用 ctypes 直接调 Win32。"""
    if sys.platform != "win32":
        return ""
    CF_UNICODETEXT = 13
    u, k = ctypes.windll.user32, ctypes.windll.kernel32
    u.GetClipboardData.restype = ctypes.c_void_p
    k.GlobalLock.restype = ctypes.c_void_p
    k.GlobalLock.argtypes = [ctypes.c_void_p]
    k.GlobalUnlock.argtypes = [ctypes.c_void_p]
    if not u.OpenClipboard(None):
        return ""
    try:
        if not u.IsClipboardFormatAvailable(CF_UNICODETEXT):
            return ""
        handle = u.GetClipboardData(CF_UNICODETEXT)
        if not handle:
            return ""
        pointer = k.GlobalLock(handle)
        if not pointer:
            return ""
        try:
            return ctypes.wstring_at(pointer)
        finally:
            k.GlobalUnlock(handle)
    finally:
        u.CloseClipboard()


# --------------------------------------------------------------------------- 桥

class Bridge:
    def __init__(self, events: queue.Queue, manager: DownloadManager,
                 settings: AppSettings):
        self.events = events
        self.manager = manager
        self.settings = settings
        self.window: webview.Window | None = None

    # ------------------------------------------------------------------ 渲染数据

    def get_settings(self) -> dict:
        s = self.settings
        return {
            "save_dir": s.save_dir, "threads": s.threads, "resume": s.resume,
            "verify_tls": s.verify_tls, "max_retries": s.max_retries,
            "user_agent": s.user_agent, "referer": s.referer,
            "max_concurrent": s.max_concurrent, "rate_limit": s.rate_limit,
            "autoscroll_log": s.autoscroll_log,
        }

    def snapshot(self) -> list[dict]:
        """所有任务的高频状态。JS 每 100ms 拉一次。"""
        out = []
        for entry in self.manager.entries:
            task = entry.task
            snap = task.snapshot()
            out.append({
                "id": entry.id,
                "name": self._name_of(entry),
                "state": snap.state,
                "total": snap.total,
                "done": snap.done,
                "percent": snap.percent or 0.0,
                "detail": self._detail(snap),
            })
        return out

    def drain_events(self) -> list[dict]:
        """取走积压的离散事件，转成日志行。"""
        lines = []
        while True:
            try:
                event = self.events.get_nowait()
            except queue.Empty:
                return lines
            kind = event[0]
            task_id = event[1] if len(event) > 1 else 0
            payload = event[2:] if len(event) > 2 else ()
            name = self._name_of(self.manager.get(task_id))
            text = self._event_text(kind, name, payload)
            if text is None:
                continue
            lines.append({
                "time": time.strftime("%H:%M:%S"),
                "text": text,
                "tag": _LOG_TAG.get(kind, ""),
            })

    # ------------------------------------------------------------------ 任务操作

    def add_task(self, url: str, save_path: str, options: dict) -> dict:
        url = (url or "").strip()
        if not url:
            return {"ok": False, "error": "请先填写下载地址"}
        if not url.lower().startswith(("http://", "https://")):
            return {"ok": False, "error": "下载地址需要以 http:// 或 https:// 开头"}

        s = self.settings
        s.threads = self._int(options.get("threads"), 8, 1, 32)
        s.max_retries = self._int(options.get("retries"), 5, 0, 20)
        s.resume = bool(options.get("resume", True))
        s.verify_tls = bool(options.get("verify_tls", True))
        s.user_agent = (options.get("user_agent") or "").strip()
        s.referer = (options.get("referer") or "").strip()

        cfg = s.to_config(url, (save_path or "").strip() or None,
                          (options.get("cookie") or "").strip())
        self.manager.add(cfg, autostart=True)
        return {"ok": True}

    def start_task(self, task_id: int) -> None:
        entry = self.manager.get(task_id)
        if entry:
            self.manager.start(entry)

    def pause_task(self, task_id: int) -> None:
        entry = self.manager.get(task_id)
        if entry:
            self.manager.pause(entry)

    def prepare_cancel(self, task_id: int) -> dict:
        """取消前先问一下有没有东西要删。"""
        entry = self.manager.get(task_id)
        return {"done": entry.task.snapshot().done if entry else 0}

    def cancel_task(self, task_id: int, delete_partial: bool = False) -> None:
        entry = self.manager.get(task_id)
        if entry:
            self.manager.cancel(entry, delete_partial=bool(delete_partial))

    def remove_task(self, task_id: int, delete_partial: bool = False) -> None:
        entry = self.manager.get(task_id)
        if entry:
            self.manager.remove(entry, delete_partial=bool(delete_partial))

    def start_all(self) -> int:
        return self.manager.start_all()

    def pause_all(self) -> int:
        return self.manager.pause_all()

    def clear_finished(self) -> int:
        return self.manager.clear_finished()

    def set_concurrency(self, value: int) -> None:
        self.manager.set_max_concurrent(self._int(value, 3, 1, 16))

    def set_rate_limit(self, value: int) -> None:
        self.manager.set_rate_limit(max(0, self._int(value, 0, 0, 1 << 40)))

    # ------------------------------------------------------------------ 系统交互

    def clipboard(self) -> str:
        return clipboard_text()

    def browse(self, url: str, save_path: str) -> str | None:
        initial = derive_filename_from_url(url or "") or "download.bin"
        directory = (save_path or "").strip()
        if directory and not os.path.isdir(directory):
            directory = os.path.dirname(directory)
        if not directory or not os.path.isdir(directory):
            directory = os.path.join(os.path.expanduser("~"), "Downloads")
        if self.window is None:
            return None
        result = self.window.create_file_dialog(
            webview.SAVE_DIALOG, directory=directory, save_filename=initial)
        if not result:
            return None
        return result if isinstance(result, str) else result[0]

    def open_file(self, task_id: int) -> None:
        entry = self.manager.get(task_id)
        if entry:
            _open_path(entry.task.final_path)

    def reveal_file(self, task_id: int) -> None:
        entry = self.manager.get(task_id)
        if entry:
            _reveal(entry.task.final_path)

    # ------------------------------------------------------------------ 内部

    def save_settings(self) -> None:
        try:
            s = self.settings
            s.max_concurrent = self.manager.max_concurrent
            s.rate_limit = int(self.manager.rate_limit())
            s.save()
        except Exception:
            pass

    @staticmethod
    def _int(value, fallback: int, low: int, high: int) -> int:
        try:
            return max(low, min(int(float(value)), high))
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _name_of(entry) -> str:
        if entry is None:
            return "任务"
        task = entry.task
        return (os.path.basename(task.final_path)
                or derive_filename_from_url(task.cfg.url) or task.cfg.url)

    @staticmethod
    def _detail(snap) -> str:
        if snap.total > 0:
            parts = [f"{fmt_size(snap.done)} / {fmt_size(snap.total)}"]
        else:
            parts = [f"已下载 {fmt_size(snap.done)}" if snap.done else "大小未知"]
        if snap.state in ("running", "probing"):
            parts.append(fmt_speed(snap.speed))
            if snap.eta is not None:
                parts.append(f"剩余 {fmt_eta(snap.eta)}")
            if snap.mode == "multi" and snap.chunks:
                parts.append(f"分片 {snap.active}/{snap.chunks}")
            if snap.retries:
                parts.append(f"重试 {snap.retries} 次")
            if snap.mode == "single":
                parts.append("单线程")
        elif snap.state == "queued":
            parts.append("等待其他任务让出名额")
        elif snap.state == "failed" and snap.message:
            parts.append(snap.message)
        return "   ·   ".join(parts)

    @staticmethod
    def _event_text(kind: str, name: str, payload: tuple) -> str | None:
        if kind == "log":
            return f"[{name}] {payload[0]}" if payload else ""
        if kind == "error":
            return f"[{name}] {payload[0] if payload else '下载失败'}"
        if kind == "done":
            return f"[{name}] 下载完成：{payload[0] if payload else ''}"
        if kind == "paused":
            return f"[{name}] 已暂停，可随时继续"
        if kind == "cancelled":
            deleted = bool(payload[0]) if payload else False
            return f"[{name}] " + ("已取消，临时文件已删除" if deleted
                                  else "已取消，临时文件保留，可再次开始续传")
        return None          # path 之类的事件界面用不到


# --------------------------------------------------------------------------- 系统调用

def _open_path(path: str) -> None:
    try:
        if sys.platform == "win32":
            os.startfile(path)          # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            import subprocess
            subprocess.Popen(["open", path])
        else:
            import subprocess
            subprocess.Popen(["xdg-open", path])
    except OSError:
        pass


def _reveal(path: str) -> None:
    if sys.platform == "win32" and os.path.exists(path):
        try:
            import subprocess
            subprocess.Popen(["explorer", f"/select,{os.path.normpath(path)}"])
            return
        except OSError:
            pass
    _open_path(os.path.dirname(path) or ".")


def _prepare_high_dpi() -> None:
    if sys.platform != "win32":
        return
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass


# --------------------------------------------------------------------------- 入口

def main() -> int:
    _prepare_high_dpi()

    settings = AppSettings.load()
    events: queue.Queue = queue.Queue()
    manager = DownloadManager(events, settings.max_concurrent)
    manager.set_rate_limit(settings.rate_limit)

    bridge = Bridge(events, manager, settings)
    window = webview.create_window(
        "多线程下载器",
        url=os.path.join(WEB_DIR, "index.html"),
        js_api=bridge,
        width=960, height=820, min_size=(760, 620),
        background_color="#f4f5f8",
    )
    bridge.window = window

    # 收尾只放在 start() 返回之后，不挂 window.events.closed 回调：
    # 实测给 closed 挂回调会明显提高「点关闭没反应」的概率（同一份代码
    # 挂回调时 6 次里能挂掉 5 次，不挂时 6 次全正常）。而 start() 会在
    # 最后一个窗口关闭后返回，这个 finally 一定会跑到，功能上不缺什么。
    try:
        webview.start(debug=os.environ.get("DL_DEBUG") == "1")
    finally:
        bridge.save_settings()
        manager.shutdown()
    return 0
