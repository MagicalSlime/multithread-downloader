#!/usr/bin/env python3
"""无 GUI 的端到端测试。

运行：  python tests/smoke_test.py
失败时以非零码退出，可直接用于 CI。

测试用真实 HTTP 服务器（见 range_server.py）+ 确定性生成的 20 MiB 文件，
不需要真的去外网下载大文件即可覆盖分片、续传、重试、回退、失效校验等全部路径。
"""

from __future__ import annotations

import hashlib
import os
import queue
import random
import shutil
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))          # range_server
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # downloader 包

from range_server import bytes_served, reset_counters, start_server                 # noqa: E402

from downloader.config import DEFAULT_UA, DownloadConfig                             # noqa: E402
from downloader.core import (                                                       # noqa: E402
    DownloadError,
    DownloadTask,
    RateLimiter,
    Stats,
)
from downloader.state import (                                                      # noqa: E402
    covers_exactly,
    rebase_progress,
    split_ranges,
)
from downloader.util import (                                                       # noqa: E402
    fmt_eta,
    fmt_size,
    parse_content_disposition_filename,
    parse_content_range,
    resolve_filename,
    sanitize_filename,
)

SRC_SIZE = 20 << 20
TMP = Path()
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


def make_task(url_suffix: str, save_path: str, threads: int = 4, **kwargs):
    """返回 (task, 事件队列)。"""
    cfg = DownloadConfig(url=f"{BASE}/source.bin{url_suffix}", save_path=save_path,
                         threads=threads, min_chunk_size=1 << 20, **kwargs)
    events: queue.Queue = queue.Queue()
    return DownloadTask(cfg, events), events


def wait_idle(task: DownloadTask, timeout: float = 120.0) -> str:
    """等管理器线程真正退出，返回终态。"""
    task.wait(timeout)
    return task.snapshot().state


def pause_at(task: DownloadTask, ratio: float, timeout: float = 60.0) -> bool:
    """进度一越过 ratio 就立刻暂停；返回是否成功停在 paused。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = task.snapshot()
        if snapshot.total and snapshot.done >= snapshot.total * ratio:
            task.pause()
            return wait_idle(task) == "paused"
        if snapshot.state in ("done", "failed", "cancelled"):
            return False
        time.sleep(0.005)
    return False


def drain(events: queue.Queue) -> list[tuple]:
    out = []
    while True:
        try:
            out.append(events.get_nowait())
        except queue.Empty:
            return out


def logs_contain(events: queue.Queue, needle: str) -> bool:
    # 事件格式是 (kind, tag, *payload)：tag 让界面知道是哪个任务发的
    return any(item[0] == "log" and needle in item[2] for item in drain(events))


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


# --------------------------------------------------------------------------- 用例

def case_basic_multithread() -> str:
    reset_counters()
    out = TMP / "basic.bin"
    task, _events = make_task("", str(out), threads=4)
    task.start()
    state = wait_idle(task)
    check(state == "done", f"终态为 {state}，期望 done")
    check(sha256_file(out) == SRC_SHA, "下载内容与源文件不一致")

    served = bytes_served()
    check(served >= SRC_SIZE, f"只收到 {served} 字节，少于源文件")
    check(served <= SRC_SIZE + 4096, f"多下了 {served - SRC_SIZE} 字节，分片可能重叠")
    check(task.snapshot().mode == "multi", "没有走多线程路径")
    return f"4 分片，服务器共发出 {fmt_size(served)}"


def case_resume_after_pause() -> str:
    reset_counters()
    out = TMP / "resume.bin"
    state_file = Path(str(out) + ".part.json")
    task, _events = make_task("?slow=30", str(out), threads=4)

    task.start()
    check(pause_at(task, 0.30), "没能停在 30% 的暂停点")
    check(state_file.exists(), "暂停后没有留下状态文件")

    served_at_pause = bytes_served()
    check(served_at_pause < SRC_SIZE, "暂停时其实已经下完了，测试没有意义")

    task.start()                       # 「继续」= 走与崩溃重启相同的续传路径
    state = wait_idle(task)
    check(state == "done", f"续传后终态为 {state}")
    check(sha256_file(out) == SRC_SHA, "续传后的文件内容不一致")
    check(not state_file.exists(), "完成后没有清理状态文件")

    served = bytes_served()
    check(served < SRC_SIZE * 1.2,
          f"共发出 {served} 字节，超过 {SRC_SIZE * 1.2:.0f}，说明重复下载了")
    return f"暂停于 {fmt_size(served_at_pause)}，总计只发出 {fmt_size(served)}"


def case_resume_with_different_thread_count() -> str:
    reset_counters()
    out = TMP / "rethread.bin"

    first, _ = make_task("?slow=60", str(out), threads=8)
    first.start()
    check(pause_at(first, 0.40), "没能停在 40% 的暂停点")

    second, _ = make_task("?slow=60", str(out), threads=3)
    second.start()
    check(wait_idle(second) == "done", "改成 3 线程后未能续传完成")
    check(sha256_file(out) == SRC_SHA, "改线程数后续传的文件内容不一致")
    return "8 线程下到 40% → 改 3 线程续传成功"


def case_server_without_range() -> str:
    reset_counters()
    out = TMP / "norange.bin"
    task, _ = make_task("?norange=1", str(out), threads=8)
    task.start()
    state = wait_idle(task)
    check(state == "done", f"终态为 {state}")
    check(task.snapshot().mode == "single", "没有回退为单线程")
    check(sha256_file(out) == SRC_SHA, "单线程下载的内容不一致")
    return "自动回退单线程且内容正确"


def case_unknown_length() -> str:
    reset_counters()
    out = TMP / "nolen.bin"
    task, _ = make_task("?nolen=1", str(out), threads=4)
    task.start()
    state = wait_idle(task)
    check(state == "done", f"终态为 {state}")
    check(task.snapshot().mode == "single", "长度未知时应使用单线程")
    check(task.snapshot().total == 0, "长度未知时 total 应为 0")
    check(sha256_file(out) == SRC_SHA, "长度未知时下载的内容不一致")
    return "长度未知 → 单线程流式，内容正确"


def case_retry_on_flaky_server() -> str:
    reset_counters(flaky=3)
    out = TMP / "flaky.bin"
    task, events = make_task("?flaky=1", str(out), threads=4)
    task.start()
    state = wait_idle(task)
    check(state == "done", f"终态为 {state}")
    check(sha256_file(out) == SRC_SHA, "重试后文件内容不一致")
    check(logs_contain(events, "重试"), "日志里没有出现重试记录")
    return "连接被中途切断 3 次，重试后内容仍正确"


def case_invalidate_when_source_changed() -> str:
    # 用单独的源文件，避免污染其他用例
    source = TMP / "changing.bin"
    source.write_bytes(random.Random(7).randbytes(6 << 20))
    out = TMP / "changed.bin"

    def make(suffix: str):
        cfg = DownloadConfig(url=f"{BASE}/changing.bin{suffix}", save_path=str(out),
                             threads=4, min_chunk_size=1 << 20)
        events: queue.Queue = queue.Queue()
        return DownloadTask(cfg, events), events

    first, _ = make("?slow=30")
    first.start()
    check(pause_at(first, 0.25), "没能停在 25% 的暂停点")

    # 服务器上的文件被换成了另一份、大小不同的内容
    source.write_bytes(random.Random(99).randbytes(3 << 20))
    new_sha = hashlib.sha256(source.read_bytes()).hexdigest()

    second, events = make("")
    second.start()
    check(wait_idle(second) == "done", "换源后没有重新下载完成")
    check(sha256_file(out) == new_sha, "没有按新文件重新下载")
    check(logs_contain(events, "无法续传"), "没有提示「无法续传」")
    return "源文件变化 → 丢弃旧进度重新下载"


def case_cancel_keeps_partial() -> str:
    reset_counters()
    out = TMP / "cancel.bin"
    part = Path(str(out) + ".part")
    task, _ = make_task("?slow=30", str(out), threads=4)

    task.start()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        snapshot = task.snapshot()
        if snapshot.total and snapshot.done >= snapshot.total * 0.25:
            break
        time.sleep(0.005)
    else:
        raise AssertionError("60 秒内没有下到 25%")

    started = time.monotonic()
    task.cancel()
    state = wait_idle(task, timeout=30)
    elapsed = time.monotonic() - started

    check(state == "cancelled", f"取消后终态为 {state}")
    check(not task.is_active(), "取消后线程仍在运行")
    check(part.exists(), "取消后没有保留临时文件")
    check(elapsed < 5.0, f"取消耗时 {elapsed:.1f} 秒，响应过慢")
    return f"{elapsed * 1000:.0f} ms 内停止，临时文件保留"


def case_empty_file() -> str:
    (TMP / "empty.bin").write_bytes(b"")
    out = TMP / "empty_out.bin"
    task = DownloadTask(DownloadConfig(
        url=f"{BASE}/empty.bin", save_path=str(out), threads=8), queue.Queue())
    task.start()
    check(wait_idle(task) == "done", "空文件没有成功完成")
    check(out.exists() and out.stat().st_size == 0, "空文件结果不对")
    return "0 字节文件正确处理"


def case_error_404() -> str:
    task = DownloadTask(DownloadConfig(
        url=f"{BASE}/does-not-exist.bin", save_path=str(TMP / "nope.bin")), queue.Queue())
    task.start()
    check(wait_idle(task) == "failed", "404 应该失败")
    message = task.snapshot().message
    check("404" in message, f"错误信息不明确：{message}")
    return message


def case_content_disposition_end_to_end() -> str:
    """不指定文件名，落到目录里，验证服务端给的中文文件名被正确采用。"""
    reset_counters()
    directory = TMP / "cd_dir"
    directory.mkdir(exist_ok=True)
    task = DownloadTask(DownloadConfig(
        url=f"{BASE}/source.bin?cd=1", save_path=str(directory) + os.sep,
        threads=2), queue.Queue())
    task.start()
    check(wait_idle(task) == "done", "下载失败")

    expected = directory / "测试文件.bin"
    check(expected.exists(),
          f"没有使用 filename* 里的文件名，目录内容：{os.listdir(directory)}")
    check(sha256_file(expected) == SRC_SHA, "内容不一致")

    snapshot_path = task.snapshot().path
    check(snapshot_path.endswith("测试文件.bin"), f"最终路径不对：{snapshot_path}")
    return "文件名取自 filename*（filename 优先级更低）"


def case_cancel_while_paused() -> str:
    """回归：暂停后管理器线程已退出，取消曾经完全不起作用——状态永远停在「暂停」，
    只把提示文字改成「正在取消…」，因为没有任何线程会去处理那个标志位。"""
    reset_counters()
    out = TMP / "paused_cancel.bin"
    task, events = make_task("?slow=30", str(out), threads=4)

    task.start()
    check(pause_at(task, 0.25), "没能停在 25% 的暂停点")
    check(not task.is_active(), "暂停后管理器线程应当已经退出")
    drain(events)

    task.cancel()
    check(task.snapshot().state == "cancelled",
          f"暂停后取消没有生效，状态仍是 {task.snapshot().state}")
    check(task.snapshot().message.startswith("已取消"),
          f"提示文字不对：{task.snapshot().message}")
    check(any(item[0] == "cancelled" for item in drain(events)),
          "取消后没有发出 cancelled 事件，界面会永远停在「正在取消…」")
    check(Path(str(out) + ".part").exists(), "默认应当保留临时文件")
    return "暂停后取消生效，临时文件保留"


def case_cancel_while_paused_deletes_partial() -> str:
    reset_counters()
    out = TMP / "paused_wipe.bin"
    task, _ = make_task("?slow=30", str(out), threads=4)

    task.start()
    check(pause_at(task, 0.25), "没能停在 25% 的暂停点")
    task.cancel(delete_partial=True)

    check(task.snapshot().state == "cancelled", "取消没有生效")
    check(not Path(str(out) + ".part").exists(), "临时文件应当被删除")
    check(not Path(str(out) + ".part.json").exists(), "状态文件应当被删除")
    return "暂停后取消并删除临时文件"


def case_cancel_while_running_deletes_partial() -> str:
    """删除必须发生在工作线程停下之后，否则 Windows 会拒绝删除正在写入的文件。"""
    reset_counters()
    out = TMP / "running_wipe.bin"
    task, _ = make_task("?slow=30", str(out), threads=4)

    task.start()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        snapshot = task.snapshot()
        if snapshot.total and snapshot.done >= snapshot.total * 0.25:
            break
        time.sleep(0.005)
    else:
        raise AssertionError("60 秒内没有下到 25%")

    task.cancel(delete_partial=True)
    check(wait_idle(task, timeout=30) == "cancelled", "取消没有生效")
    check(not Path(str(out) + ".part").exists(), "下载中取消后临时文件应当被删除")
    check(not Path(str(out) + ".part.json").exists(), "状态文件应当被删除")
    return "下载中取消并删除临时文件"


def case_cancel_then_resume_still_works() -> str:
    """取消默认保留临时文件，所以再点一次开始应当能续传，而不是从头下载。"""
    reset_counters()
    out = TMP / "cancel_resume.bin"
    task, _ = make_task("?slow=30", str(out), threads=4)

    task.start()
    check(pause_at(task, 0.30), "没能停在 30% 的暂停点")
    task.cancel()
    check(task.snapshot().state == "cancelled", "取消没有生效")
    served = bytes_served()

    task.start()                       # 再次开始 = 续传
    check(wait_idle(task) == "done", "取消后再次开始没有完成")
    check(sha256_file(out) == SRC_SHA, "取消后续传的文件内容不一致")
    check(bytes_served() < SRC_SIZE * 1.2,
          f"共发出 {bytes_served()} 字节，取消后似乎从头重下了")
    return f"取消于 {fmt_size(served)}，续传后总计 {fmt_size(bytes_served())}"


def case_cancel_resets_progress() -> str:
    """取消后已下载字节应该归零，方便直接开始下一个任务；
    但 total 必须保留，否则界面会误判成「长度未知」而把进度条切成滚动条。"""
    reset_counters()
    out = TMP / "cancel_reset.bin"
    task, _ = make_task("?slow=30", str(out), threads=4)

    task.start()
    check(pause_at(task, 0.30), "没能停在 30% 的暂停点")
    before = task.snapshot()
    check(before.done > 0 and before.total > 0, "暂停后应当有进度")

    task.cancel()
    after = task.snapshot()
    check(after.state == "cancelled", f"取消没有生效：{after.state}")
    check(after.done == 0, f"取消后已下载字节应归零，实际 {after.done}")
    check(after.total == before.total, f"取消后总大小不该变：{before.total} → {after.total}")
    check(after.speed == 0.0, f"取消后速度应为 0，实际 {after.speed}")
    check(Path(str(out) + ".part").exists(), "默认应当保留临时文件")

    # 归零只是为了显示；磁盘上的续传信息不能丢，再次开始必须还能续传
    task.start()
    check(wait_idle(task) == "done", "取消后再次开始没有完成")
    check(sha256_file(out) == SRC_SHA, "取消后重新开始的内容不一致")
    return "取消后进度归零、总量保留、续传仍可用"


def case_speed_stops_immediately() -> str:
    """回归：数据早就停了，但滑动窗口里的旧样本会一个个老化出去，
    速度读数于是拖着长尾慢慢衰减，看起来像「下载还在慢慢停」。
    现在最近一次采样超过 3 秒没更新就直接报 0。"""
    stats = Stats()
    for _ in range(30):
        stats.add(65536)
    _done, speed, _eta = stats.snapshot()
    check(speed > 0, "刚喂完数据时速度应当大于 0")

    time.sleep(3.3)
    done, speed, eta = stats.snapshot()
    check(speed == 0.0, f"数据停了 3.3 秒后速度应当归零，实际 {speed / 1048576:.2f} MB/s")
    check(eta is None, "速度为 0 时不该给出 ETA")
    check(done == 30 * 65536, "归零的只有速度，已下载字节不能动")

    # 真实任务：暂停后速度也应当立刻归零，而不是慢慢衰减
    reset_counters()
    out = TMP / "speed_stop.bin"
    task, _ = make_task("?slow=30", str(out), threads=4)
    task.start()
    check(pause_at(task, 0.25), "没能停在 25% 的暂停点")
    check(task.snapshot().speed == 0.0,
          f"暂停后速度应立刻归零，实际 {task.snapshot().speed / 1048576:.2f} MB/s")
    return "旧样本不再拖出衰减长尾"


def case_rate_limiter() -> str:
    """令牌桶：取额度要按速率等够时间，关掉限速要立刻放行。"""
    limiter = RateLimiter(1 << 20)          # 1 MB/s
    started = time.monotonic()
    limiter.acquire(1 << 20)                # 桶是空的，得等满 1 秒
    first = time.monotonic() - started
    check(0.8 <= first <= 2.0, f"按 1MB/s 取 1MB 用了 {first:.2f}s，限速不对")

    started = time.monotonic()
    limiter.acquire(256 << 10)              # 再要 256KB，约需 0.25s
    second = time.monotonic() - started
    check(0.15 <= second <= 1.0, f"补取 256KB 用了 {second:.2f}s，限速不对")

    limiter.set_rate(0)                     # 关掉限速应当立刻放行
    started = time.monotonic()
    limiter.acquire(1 << 30)
    check(time.monotonic() - started < 0.1, "关掉限速后不该还在等")

    # 桶容量必须至少能装下单次请求，否则额度永远攒不够，会死循环
    tiny = RateLimiter(64)                  # 64 B/s，但一次要 1MB
    stop = threading.Event()
    timer = threading.Timer(0.5, stop.set)
    timer.start()
    try:
        check(not tiny.acquire(1 << 20, stop), "限速远低于单次请求量时应当能被停止信号打断")
    finally:
        timer.cancel()
    return "限速计时正确，且不会因额度攒不够而死循环"


def case_rate_limit_throttles_download() -> str:
    """端到端：4 MB 的文件限到 2 MB/s，应当下满约 2 秒。"""
    small = TMP / "small.bin"
    small.write_bytes(random.Random(5).randbytes(4 << 20))
    expected = hashlib.sha256(small.read_bytes()).hexdigest()
    out = TMP / "limited.bin"

    task = DownloadTask(
        DownloadConfig(url=f"{BASE}/small.bin", save_path=str(out),
                       threads=4, min_chunk_size=1 << 20),
        queue.Queue(), limiter=RateLimiter(2 << 20))
    started = time.monotonic()
    task.start()
    check(wait_idle(task, timeout=60) == "done", "限速下载没有完成")
    elapsed = time.monotonic() - started
    check(sha256_file(out) == expected, "限速下载的内容不一致")
    check(elapsed >= 1.6, f"4MB 限到 2MB/s 只用了 {elapsed:.2f}s，限速没生效")
    check(elapsed <= 15, f"限速下得过慢：{elapsed:.2f}s")
    return f"4 MB @ 2 MB/s 实际用时 {elapsed:.2f}s"


def case_probe_retries_transient_errors() -> str:
    """回归：探测阶段一次网络抖动不该让整个任务判死。

    真实网络里瞬时超时很常见，早先只有 HTTP 状态码类错误会重试，
    一次读超时就报「无法连接到服务器」直接失败，用户一头雾水。
    """
    # 前两次抛瞬时错误，第三次走真实探测
    attempts: list[int] = []
    out = TMP / "probe_retry.bin"
    task = DownloadTask(DownloadConfig(url=f"{BASE}/source.bin", save_path=str(out),
                                       threads=2), queue.Queue())
    real_probe = task._probe_once

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise DownloadError("服务器响应超时，对方可能限速或网络不稳。",
                                "simulated", transient=True)
        return real_probe()

    task._probe_once = flaky
    task.start()
    state = wait_idle(task, timeout=60)
    check(state == "done", f"瞬时错误后应当重试成功，实际 {state}：{task.snapshot().message}")
    check(len(attempts) == 3, f"应当探测 3 次，实际 {len(attempts)} 次")
    check(sha256_file(out) == SRC_SHA, "重试成功后内容不一致")

    # 非瞬时错误（比如 404）不该重试，免得白白多等
    fatal_attempts: list[int] = []
    doomed = DownloadTask(DownloadConfig(url=f"{BASE}/nope.bin",
                                         save_path=str(TMP / "probe_fatal.bin")),
                          queue.Queue())

    def fatal():
        fatal_attempts.append(1)
        raise DownloadError("文件不存在（404）", "simulated")

    doomed._probe_once = fatal
    doomed.start()
    check(wait_idle(doomed, timeout=30) == "failed", "404 应当失败")
    check(len(fatal_attempts) == 1, f"非瞬时错误不该重试，实际试了 {len(fatal_attempts)} 次")
    return "瞬时错误重试 3 次成功，404 只试 1 次"


BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


def case_user_agent_policy() -> str:
    """回归：清华 TUNA 等镜像站会拦截浏览器 UA（以及 python-requests 默认 UA）。

    默认 UA 必须能通过这种拦截，同时被拦时给出的提示要指向「高级设置」。
    """
    denial = "?denyua=AppleWebKit"
    check("AppleWebKit" not in DEFAULT_UA, "默认 UA 不应带浏览器指纹")

    blocked = DownloadTask(DownloadConfig(
        url=f"{BASE}/source.bin{denial}", save_path=str(TMP / "ua_blocked.bin"),
        threads=2, user_agent=BROWSER_UA), queue.Queue())
    blocked.start()
    check(wait_idle(blocked) == "failed", "浏览器 UA 应该被 403 拦下")
    message = blocked.snapshot().message
    check("403" in message, f"错误信息不明确：{message}")
    check("User-Agent" in message, f"403 提示应指向 User-Agent 设置：{message}")

    ok = DownloadTask(DownloadConfig(
        url=f"{BASE}/source.bin{denial}", save_path=str(TMP / "ua_ok.bin"),
        threads=2), queue.Queue())
    ok.start()
    check(wait_idle(ok) == "done", "默认 UA 应该能通过镜像站的拦截策略")
    check(sha256_file(TMP / "ua_ok.bin") == SRC_SHA, "内容不一致")
    return "默认 UA 通过拦截；被拦时的提示指向高级设置"


def case_creates_missing_directories() -> str:
    reset_counters()
    out = TMP / "nested" / "deeper" / "made.bin"
    task, _ = make_task("", str(out), threads=4)
    task.start()
    check(wait_idle(task) == "done", "保存到不存在的目录时失败")
    check(sha256_file(out) == SRC_SHA, "内容不一致")
    return "自动创建了多级目录"


def case_unit_helpers() -> str:
    # --- Content-Disposition：filename* 必须优先于 filename
    cd = ("attachment; filename=\"fallback.bin\"; "
          "filename*=UTF-8''%E6%B5%8B%E8%AF%95%E6%96%87%E4%BB%B6.bin")
    parsed = parse_content_disposition_filename(cd)
    check(parsed == "测试文件.bin", f"filename* 优先级错误：{parsed!r}")
    check(parse_content_disposition_filename('attachment; filename="plain.txt"') == "plain.txt",
          "普通 filename= 解析错误")
    check(parse_content_disposition_filename(None) is None, "空头应返回 None")
    check(resolve_filename(cd, "http://x/y.bin") == "测试文件.bin", "文件名优先级错误")
    check(resolve_filename(None, "http://x/%E4%B8%AD%E6%96%87.zip") == "中文.zip",
          "URL 兜底解析错误")
    check(resolve_filename(None, "http://x/") == "download.bin", "兜底文件名错误")

    # --- 文件名清洗
    check(sanitize_filename("../../evil.bin") == "evil.bin", "路径穿越没有被消除")
    check(sanitize_filename("..\\..\\evil.bin") == "evil.bin", "反斜杠穿越没有被消除")
    check(sanitize_filename("CON") == "_CON", "保留设备名没有被处理")
    check(sanitize_filename("a<b>c:d.bin") == "a_b_c_d.bin", "非法字符没有被替换")
    check(sanitize_filename("trailing. ") == "trailing", "结尾的点/空格没有被去掉")
    check(sanitize_filename("..") is None, "「..」应该被拒绝")
    long_name = "x" * 400 + ".bin"
    check(len(sanitize_filename(long_name)) <= 180, "超长文件名没有被截断")
    check(sanitize_filename(long_name).endswith(".bin"), "截断后扩展名丢失")

    # --- Content-Range
    cr = parse_content_range("bytes 0-99/1000")
    check(cr is not None and (cr.start, cr.end, cr.total) == (0, 99, 1000),
          "Content-Range 解析错误")
    check(parse_content_range("bytes 5-9/*").total == 0, "未知总长度应解析为 0")
    check(parse_content_range("garbage") is None, "非法 Content-Range 应返回 None")

    # --- 分片代数
    ranges = split_ranges(1000, 4, min_chunk=1)
    check(covers_exactly(ranges, 1000), "分片没有恰好覆盖整个文件")
    check(all(len(entry) == 3 and entry[2] == 0 for entry in ranges), "分片结构错误")
    check(len(split_ranges(1000, 8, min_chunk=1 << 20)) == 1, "小文件应自动减为 1 个分片")
    check(split_ranges(0, 8) == [], "0 字节应返回空分片列表")

    # 只有从新分片起点开始连续覆盖的部分才算已完成。
    # 旧分片已完成 [0,49] 与 [100,149]，新分片 [0,149] 里 50-99 是洞，
    # 所以只能记 50，绝不能把两段求和的 100 记进去（那会把残缺文件当成完整）。
    holes = [[0, 99, 50], [100, 199, 50]]
    target = [[0, 149, 0]]
    rebase_progress(holes, target)
    check(target[0][2] == 50, f"有洞时不该跨越，实际记了 {target[0][2]}")

    # 相邻分片首尾相接时，连续覆盖应当贯通到下一片
    contiguous = [[0, 99, 100], [100, 199, 50]]
    target = [[0, 199, 0]]
    rebase_progress(contiguous, target)
    check(target[0][2] == 150, f"连续覆盖应当贯通，实际记了 {target[0][2]}")

    old = split_ranges(1000, 4, min_chunk=1)
    for entry in old:                      # 假装每片各下了一半
        entry[2] = (entry[1] - entry[0] + 1) // 2
    new = split_ranges(1000, 3, min_chunk=1)
    rebase_progress(old, new)
    check(covers_exactly(new, 1000), "重新分片后覆盖关系被破坏")
    check(all(0 <= r[2] <= r[1] - r[0] + 1 for r in new), "重新分片后进度越界")
    check(sum(r[2] for r in new) <= sum(r[2] for r in old),
          "重新分片后进度不该凭空变多")

    # --- 格式化
    check(fmt_size(1536).endswith("KB"), "fmt_size 单位错误")
    check(fmt_eta(None) == "未知" and fmt_eta(65) == "01:05", "fmt_eta 输出错误")

    return "文件名/范围/分片/格式化 全部通过"


CASES = [
    ("基本多线程下载", case_basic_multithread),
    ("暂停后断点续传", case_resume_after_pause),
    ("改线程数后续传", case_resume_with_different_thread_count),
    ("服务器不支持 Range", case_server_without_range),
    ("服务器不提供长度", case_unknown_length),
    ("连接中断自动重试", case_retry_on_flaky_server),
    ("源文件变化则丢弃进度", case_invalidate_when_source_changed),
    ("取消保留临时文件", case_cancel_keeps_partial),
    ("暂停后取消（保留）", case_cancel_while_paused),
    ("暂停后取消（删除）", case_cancel_while_paused_deletes_partial),
    ("下载中取消并删除", case_cancel_while_running_deletes_partial),
    ("取消后仍可续传", case_cancel_then_resume_still_works),
    ("取消后进度归零", case_cancel_resets_progress),
    ("停止后速度立刻归零", case_speed_stops_immediately),
    ("探测瞬时错误重试", case_probe_retries_transient_errors),
    ("限速令牌桶", case_rate_limiter),
    ("限速实际生效", case_rate_limit_throttles_download),
    ("空文件", case_empty_file),
    ("404 错误提示", case_error_404),
    ("镜像站 UA 拦截", case_user_agent_policy),
    ("Content-Disposition 文件名", case_content_disposition_end_to_end),
    ("保存到不存在的目录", case_creates_missing_directories),
    ("纯函数单测", case_unit_helpers),
]


def main() -> int:
    global TMP, BASE, SRC_SHA

    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass

    TMP = Path(tempfile.mkdtemp(prefix="dl-smoke-"))
    source = TMP / "source.bin"
    source.write_bytes(random.Random(1234).randbytes(SRC_SIZE))
    SRC_SHA = sha256_file(source)

    httpd, _thread = start_server(str(TMP))
    BASE = f"http://127.0.0.1:{httpd.server_port}"

    print(f"临时目录: {TMP}")
    print(f"测试服务器: {BASE}   源文件: {fmt_size(SRC_SIZE)}  sha256={SRC_SHA[:16]}…\n")

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
        print(f"失败 {len(FAILED)} 项：")
        for name, tb in FAILED:
            print(f"\n--- {name} ---\n{tb}")
        return 1

    shutil.rmtree(TMP, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
