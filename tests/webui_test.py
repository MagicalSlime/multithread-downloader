#!/usr/bin/env python3
"""界面桥接层测试。

界面换成网页之后，逻辑全在 `Bridge` 里，并不需要真的开窗口就能完整验证——
所以绝大多数用例直接对 Bridge 断言，跑得快也稳定。最后一个用例才真正拉起
WebView2，确认页面能加载、API 通了、任务能渲染出来。

运行：  python tests/webui_test.py
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import random
import shutil
import sys
import tempfile
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from range_server import reset_counters, start_server        # noqa: E402

from downloader import config as config_module               # noqa: E402
from downloader.config import AppSettings                     # noqa: E402
from downloader.manager import DownloadManager                # noqa: E402
from downloader.webui import WEB_DIR, Bridge                  # noqa: E402

SRC_SIZE = 6 << 20
TMP = Path()
SETTINGS_DIR = Path()
BASE = ""
SRC_SHA = ""

PASSED: list[str] = []
FAILED: list[tuple[str, str]] = []


# --------------------------------------------------------------------------- 工具

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


class Client:
    """把 Bridge 包装成一个「假界面」，测试里像 JS 那样调用它。"""

    def __init__(self, concurrency: int = 3):
        self.events: queue.Queue = queue.Queue()
        self.manager = DownloadManager(self.events, concurrency)
        self.settings = AppSettings()
        self.bridge = Bridge(self.events, self.manager, self.settings)

    def tasks(self) -> list[dict]:
        return self.bridge.snapshot()

    def task(self, task_id: int) -> dict:
        for item in self.tasks():
            if item["id"] == task_id:
                return item
        raise AssertionError(f"任务 {task_id} 不在列表里")

    def add(self, url: str, path: Path, **options) -> int:
        opts = {"threads": 4, "retries": 5, "resume": True, "verify_tls": True}
        opts.update(options)
        result = self.bridge.add_task(url, str(path), opts)
        check(result["ok"], f"添加任务失败：{result.get('error')}")
        return self.tasks()[-1]["id"]

    def entry(self, task_id: int):
        entry = self.manager.get(task_id)
        check(entry is not None, f"任务 {task_id} 不存在")
        return entry

    def wait(self, task_id: int, states, timeout: float = 90.0) -> str:
        """等任务进入指定状态。

        注意调度是异步的：`start_task` 只是把「想跑」标记挂上，真正启动要等
        调度线程下一轮；而 `pause_task` 会同步把状态改成「已暂停」，但管理器
        线程收尾、状态落盘、事件投递都要晚一步。所以任何依赖副作用发生的断言
        都必须走这里等，不能假定调用返回时就已经生效。
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.task(task_id)["state"]
            if state in states:
                return state
            time.sleep(0.02)
        return self.task(task_id)["state"]

    def wait_stopped(self, task_id: int, timeout: float = 30.0) -> bool:
        """等管理器线程真正退出——暂停/取消时状态是同步改的，线程收尾不是。

        注意要在调用 remove_task 之前先拿住任务对象：移除之后它就不在管理器里了，
        再按 id 去查会查不到。
        """
        return self.wait_task_stopped(self.entry(task_id).task, timeout)

    @staticmethod
    def wait_task_stopped(task, timeout: float = 30.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not task.is_active():
                return True
            time.sleep(0.02)
        return False

    def start_and_wait(self, task_id: int, timeout: float = 20.0) -> str:
        """点「继续」之后等它真的跑起来，别和调度器抢跑。"""
        self.bridge.start_task(task_id)
        return self.wait(task_id, ("probing", "running", "done", "failed"), timeout)

    def wait_progress(self, task_id: int, ratio: float, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            item = self.task(task_id)
            if item["total"] and item["done"] >= item["total"] * ratio:
                return True
            if item["state"] in ("done", "failed", "cancelled"):
                return False
            time.sleep(0.02)
        return False

    def logs(self) -> list[dict]:
        return self.bridge.drain_events()

    def shutdown(self) -> None:
        self.manager.shutdown(timeout=2)


# --------------------------------------------------------------------------- 用例

def case_add_and_complete() -> str:
    reset_counters()
    client = Client()
    out = TMP / "web_basic.bin"
    task_id = client.add(f"{BASE}/source.bin", out)

    # 调度是异步的，给调度线程一点时间把任务拉起来
    state = client.wait(task_id, ("probing", "running", "done"), timeout=15)
    check(state in ("probing", "running", "done"), f"添加后状态不对：{state}")
    check(client.task(task_id)["name"], "任务应当有名字")

    state = client.wait(task_id, ("done", "failed", "cancelled"))
    check(state == "done", f"终态为 {state}：{client.task(task_id)['detail']}")
    check(sha256_file(out) == SRC_SHA, "下载内容不一致")

    item = client.task(task_id)
    check(abs(item["percent"] - 100.0) < 0.01, f"百分比不对：{item['percent']}")
    check("MB" in item["detail"], f"详情里应当有大小：{item['detail']}")
    check(any("下载完成" in line["text"] for line in client.logs()), "没有完成日志")
    client.shutdown()
    return "添加 → 完成，内容与百分比都正确"


def case_pause_resume() -> str:
    reset_counters()
    client = Client()
    out = TMP / "web_pause.bin"
    task_id = client.add(f"{BASE}/source.bin?slow=30", out)
    check(client.wait_progress(task_id, 0.25), "没有下到 25%")

    client.bridge.pause_task(task_id)
    check(client.wait(task_id, ("paused",)) == "paused", "暂停没生效")
    check(client.wait_stopped(task_id), "暂停后管理器线程没停下来")
    item = client.task(task_id)
    # 停下来了就不该再报速度——滑动窗口会拖着长尾慢慢衰减，是在骗人
    check("/s" not in item["detail"], f"暂停后不该显示速度：{item['detail']}")
    check("MB" in item["detail"], f"暂停后应当仍显示已下载量：{item['detail']}")

    client.start_and_wait(task_id)
    check(client.wait(task_id, ("done",)) == "done", "继续后没完成")
    check(sha256_file(out) == SRC_SHA, "续传后内容不一致")
    client.shutdown()
    return "暂停后速度归零，继续后内容正确"


def case_cancel_branches() -> str:
    reset_counters()
    client = Client()
    out = TMP / "web_cancel.bin"
    task_id = client.add(f"{BASE}/source.bin?slow=30", out)
    check(client.wait_progress(task_id, 0.2), "没有下到 20%")

    info = client.bridge.prepare_cancel(task_id)
    check(info["done"] > 0, "准备取消时应当报出已下载字节数")

    client.bridge.cancel_task(task_id, False)          # 保留
    check(client.wait(task_id, ("cancelled",)) == "cancelled", "取消没生效")
    check(client.wait_stopped(task_id), "取消后管理器线程没停下来")
    check(Path(str(out) + ".part").exists(), "选保留时临时文件应当还在")
    check(client.task(task_id)["percent"] == 0, "取消后进度应当归零")

    client.start_and_wait(task_id)                      # 再下一点
    check(client.wait_progress(task_id, 0.2), "重启后没有下到 20%")
    client.bridge.cancel_task(task_id, True)            # 删除
    check(client.wait(task_id, ("cancelled",)) == "cancelled", "取消没生效")
    check(client.wait_stopped(task_id), "取消后管理器线程没停下来")
    check(not Path(str(out) + ".part").exists(), "选删除时临时文件应当被删掉")
    check(any("已删除" in line["text"] for line in client.logs()), "日志没说文件已删除")
    client.shutdown()
    return "保留 / 删除 两个分支都对"


def case_invalid_input() -> str:
    client = Client()
    for url, hint in (("", "空地址"), ("ftp://x/y", "非法协议"), ("   ", "纯空格")):
        result = client.bridge.add_task(url, "", {})
        check(not result["ok"], f"{hint} 应当被拒绝")
        check(result.get("error"), f"{hint} 应当给出原因")
    check(len(client.tasks()) == 0, "被拒绝时不该创建任务")
    client.shutdown()
    return "空地址 / 非法协议 / 纯空格 都被拦下"


def case_queue_limit() -> str:
    reset_counters()
    client = Client(concurrency=1)
    first = client.add(f"{BASE}/source.bin?slow=40", TMP / "web_q1.bin")
    second = client.add(f"{BASE}/source.bin?slow=40", TMP / "web_q2.bin")

    check(client.wait(second, ("queued",), timeout=15) == "queued",
          f"第二个任务应当排队，实际 {client.task(second)['state']}")
    check(client.wait(first, ("done",), timeout=90) == "done", "第一个没完成")
    check(client.wait(second, ("done",), timeout=90) == "done", "排队任务没有被调度起来")
    check(sha256_file(TMP / "web_q1.bin") == SRC_SHA, "第一个文件不一致")
    check(sha256_file(TMP / "web_q2.bin") == SRC_SHA, "第二个文件不一致")
    client.shutdown()
    return "并发上限为 1 时正确排队并依次完成"


def case_bulk_actions() -> str:
    reset_counters()
    client = Client(concurrency=4)
    a = client.add(f"{BASE}/source.bin?slow=60", TMP / "web_b1.bin")
    b = client.add(f"{BASE}/source.bin?slow=60", TMP / "web_b2.bin")
    check(client.wait_progress(a, 0.1) and client.wait_progress(b, 0.1), "没有同时开跑")

    client.bridge.pause_all()
    check(client.wait(a, ("paused",), timeout=30) == "paused", "全部暂停没生效")
    check(client.task(b)["state"] == "paused", "全部暂停没生效")

    check(client.bridge.clear_finished() == 0, "暂停中的任务不该被「清除已完成」清掉")
    check(len(client.tasks()) == 2, "任务不该被清掉")

    client.bridge.start_all()
    check(client.wait(a, ("done",), timeout=90) == "done", "全部开始没完成")
    check(client.wait(b, ("done",), timeout=90) == "done", "全部开始没完成")
    check(client.bridge.clear_finished() == 2, "清除已完成的返回值不对")
    check(len(client.tasks()) == 0, "任务应当被清空")
    client.shutdown()
    return "全部暂停 / 全部开始 / 清除已完成 都正确"


def case_rate_and_concurrency() -> str:
    client = Client()
    client.bridge.set_rate_limit(2 << 20)
    check(int(client.manager.rate_limit()) == 2 << 20, "限速没有传到调度器")
    client.bridge.set_rate_limit(0)
    check(int(client.manager.rate_limit()) == 0, "取消限速没生效")

    client.bridge.set_concurrency(5)
    check(client.manager.max_concurrent == 5, "并发数没生效")
    client.bridge.set_concurrency(999)                 # 应当被夹到上限
    check(client.manager.max_concurrent == 16, "并发数没有上限保护")
    client.bridge.set_concurrency(0)
    check(client.manager.max_concurrent == 1, "并发数没有下限保护")
    client.shutdown()
    return "限速与并发设置正确，并且有上下限保护"


def case_events_become_logs() -> str:
    reset_counters()
    client = Client()
    out = TMP / "web_log.bin"
    task_id = client.add(f"{BASE}/source.bin?slow=30", out)
    check(client.wait_progress(task_id, 0.2), "没有下到 20%")
    client.bridge.pause_task(task_id)
    check(client.wait(task_id, ("paused",)) == "paused", "暂停没生效")
    check(client.wait_stopped(task_id), "暂停后管理器线程没停下来")

    lines = client.logs()
    check(lines, "应当收到日志行")
    for line in lines:
        check(set(line) == {"time", "text", "tag"}, f"日志行结构不对：{line}")
        check(len(line["time"]) == 8, f"时间格式不对：{line['time']}")
    check(any("已暂停" in line["text"] for line in lines), "没有暂停日志")
    check(any(line["tag"] == "muted" for line in lines), "暂停日志应当带 muted 标记")
    check(any(line["tag"] == "" for line in lines), "普通日志不该带标记")
    client.shutdown()
    return f"{len(lines)} 行日志，结构与着色标记都正确"


def case_settings_round_trip() -> str:
    client = Client()
    client.bridge.set_concurrency(6)
    client.bridge.set_rate_limit(3 << 20)
    client.settings.save_dir = "D:/Somewhere"
    client.settings.threads = 12
    client.bridge.save_settings()

    path = SETTINGS_DIR / "settings.json"
    check(path.exists(), "设置没有写到磁盘")
    raw = json.loads(path.read_text(encoding="utf-8"))
    check(raw["max_concurrent"] == 6, f"并发数没保存：{raw.get('max_concurrent')}")
    check(raw["rate_limit"] == 3 << 20, f"限速没保存：{raw.get('rate_limit')}")
    check(raw["threads"] == 12, "线程数没保存")
    check("cookie" not in raw, "Cookie 是登录凭据，绝不该落盘")

    reloaded = AppSettings.load()
    check(reloaded.max_concurrent == 6 and reloaded.threads == 12, "重新载入不匹配")
    client.shutdown()
    return "设置持久化，Cookie 不落盘"


def case_live_window() -> str:
    """真正拉起 WebView2，确认页面能加载、API 通了、任务能渲染出来。"""
    try:
        import webview
    except ImportError:
        return "跳过（未安装 pywebview）"

    reset_counters()
    client = Client()
    out = TMP / "web_window.bin"
    client.bridge.settings.save_dir = str(TMP)

    window = webview.create_window(
        "测试", url=os.path.join(WEB_DIR, "index.html"),
        js_api=client.bridge, width=980, height=840)
    client.bridge.window = window

    report: dict = {}

    def worker(win):
        try:
            time.sleep(2.5)
            task_id = client.add(f"{BASE}/source.bin?slow=25", out)
            deadline = time.monotonic() + 20
            rendered = None
            while time.monotonic() < deadline:
                info = win.evaluate_js(
                    "(() => { const t = document.querySelector('.task');"
                    " return t ? {name: t.querySelector('.task-name').textContent,"
                    " chip: t.querySelector('.chip').textContent,"
                    " width: t.querySelector('.bar-fill').style.width,"
                    " actions: [...t.querySelectorAll('.task-actions button')]"
                    "   .filter(b => b.style.display !== 'none')"
                    "   .map(b => b.textContent) } : null })()")
                if info and info.get("name"):
                    rendered = info
                    break
                time.sleep(0.3)
            report["dom"] = rendered
            report["count"] = win.evaluate_js("document.querySelectorAll('.task').length")
            report["errors"] = win.evaluate_js(
                "window.__err || (document.querySelectorAll('.task').length ? '' : '没有渲染出任务卡片')")
            check(client.wait(task_id, ("done", "failed"), timeout=90) == "done",
                  "通过网页界面下载没有完成")
            report["sha_ok"] = sha256_file(out) == SRC_SHA
        except Exception as exc:                          # noqa: BLE001
            report["exception"] = f"{type(exc).__name__}: {exc}"
        finally:
            try:
                win.destroy()
            except Exception:
                pass

    webview.start(worker, window)

    if "exception" in report:
        raise AssertionError(report["exception"])
    dom = report.get("dom")
    check(dom, f"任务卡片没有渲染出来（{report.get('errors')}）")
    check(report.get("count", 0) >= 1, "任务卡片数量不对")
    check(dom["chip"] in ("下载中", "连接中", "已完成"),
          f"状态胶囊文字不对：{dom['chip']}")
    check(dom["actions"], "任务卡片上应当有操作按钮")
    check(report.get("sha_ok"), "通过网页界面下载的内容不一致")
    client.shutdown()
    return f"页面渲染出「{dom['name']}」「{dom['chip']}」，按钮 {dom['actions']}"


def case_remove_task() -> str:
    reset_counters()
    client = Client()
    out = TMP / "web_remove.bin"
    task_id = client.add(f"{BASE}/source.bin?slow=30", out)
    check(client.wait_progress(task_id, 0.2), "没有下到 20%")

    first = client.entry(task_id).task          # 移除后就查不到了，先拿住
    client.bridge.remove_task(task_id)
    check(Client.wait_task_stopped(first), "移除后管理器线程没停下来")
    check(all(item["id"] != task_id for item in client.tasks()), "移除后任务还在列表里")
    # 移除默认保留临时文件，方便之后重新添加时续传
    check(Path(str(out) + ".part").exists(), "移除默认应当保留临时文件")

    other = client.add(f"{BASE}/source.bin", TMP / "web_remove2.bin")
    second = client.entry(other).task
    client.bridge.remove_task(other, True)
    check(Client.wait_task_stopped(second), "移除后管理器线程没停下来")
    check(not Path(str(TMP / "web_remove2.bin") + ".part").exists(),
          "指定删除时临时文件应当被清掉")
    client.shutdown()
    return "移除会先中止任务，并遵守是否删除临时文件"


CASES = [
    ("添加并完成下载", case_add_and_complete),
    ("暂停与继续", case_pause_resume),
    ("取消的两个分支", case_cancel_branches),
    ("非法输入被拒绝", case_invalid_input),
    ("多任务排队", case_queue_limit),
    ("批量操作", case_bulk_actions),
    ("限速与并发设置", case_rate_and_concurrency),
    ("事件转成日志", case_events_become_logs),
    ("设置持久化", case_settings_round_trip),
    ("移除任务", case_remove_task),
    ("真实窗口渲染", case_live_window),
]


def main() -> int:
    global TMP, BASE, SRC_SHA, SETTINGS_DIR

    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass

    TMP = Path(tempfile.mkdtemp(prefix="dl-web-"))
    SETTINGS_DIR = TMP / "cfg"
    SETTINGS_DIR.mkdir()
    # 别碰用户真实的配置文件
    config_module.settings_path = lambda: str(SETTINGS_DIR / "settings.json")

    source = TMP / "source.bin"
    source.write_bytes(random.Random(2024).randbytes(SRC_SIZE))
    SRC_SHA = sha256_file(source)

    httpd, _thread = start_server(str(TMP))
    BASE = f"http://127.0.0.1:{httpd.server_port}"

    print(f"临时目录: {TMP}")
    print(f"测试服务器: {BASE}\n")

    try:
        for name, fn in CASES:
            print(f"  • {name} ... ", end="", flush=True)
            try:
                detail = fn()
            except Exception as exc:
                FAILED.append((name, traceback.format_exc()))
                print("失败")
                print(f"      {type(exc).__name__}: {exc}")
            else:
                PASSED.append(name)
                print(f"通过  [{detail}]" if detail else "通过")
    finally:
        httpd.shutdown()
        httpd.server_close()

    print(f"\n{'=' * 60}")
    print(f"通过 {len(PASSED)} / {len(CASES)}")
    if FAILED:
        for name, tb in FAILED:
            print(f"\n--- {name} ---\n{tb}")
        return 1
    shutil.rmtree(TMP, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
