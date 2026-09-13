"""下载核心：探测、分片、并发、重试、暂停/取消、断点续传。

本模块**不得导入 tkinter** —— 它要能脱离 GUI 单独跑端到端测试。
与界面的交互只有两条通道：
  * 出：`events` 队列（离散事件）+ `snapshot()`（高频数值，加锁读取）
  * 入：`start()` / `pause()` / `cancel()`，内部用 threading.Event 实现，天然线程安全
"""

from __future__ import annotations

import os
import queue
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum

import requests

from .config import DownloadConfig
from .state import (
    ResumeState,
    delete_state,
    load_state,
    rebase_progress,
    save_state,
    split_ranges,
    state_path_for,
    validate_resume,
)
from .util import fmt_size, parse_content_range, resolve_filename

__all__ = ["DownloadError", "DownloadTask", "Snapshot", "TaskState", "build_session"]


# --------------------------------------------------------------------------- 异常

class DownloadError(Exception):
    """带中文展示文案的下载错误。"""

    def __init__(self, message: str, detail: str = "", transient: bool = False):
        super().__init__(message)
        self.message = message      # 中文，可直接显示给用户
        self.detail = detail        # 原始异常文本，只进日志
        self.transient = transient  # 网络抖动类错误，值得重试


class RangeUnsupported(Exception):
    """服务器没有按请求的偏移返回 206。"""


class RangeNotSatisfiable(Exception):
    """416：记录的偏移已失效，必须丢弃续传状态从零重下。"""


class _RetryableStatus(Exception):
    def __init__(self, status: int, retry_after: str | None = None):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


class _FatalStatus(Exception):
    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.status = status


class _ChunkFailed(Exception):
    def __init__(self, index: int, error: DownloadError):
        super().__init__(error.message)
        self.index = index
        self.error = error


class _RetryableProbe(Exception):
    def __init__(self, error: DownloadError):
        super().__init__(error.message)
        self.error = error


# --------------------------------------------------------------------------- 状态

class TaskState(Enum):
    IDLE = "idle"
    QUEUED = "queued"
    PROBING = "probing"
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProbeResult:
    supports_range: bool
    total: int                 # 0 表示未知
    etag: str | None = None
    last_modified: str | None = None
    content_disposition: str | None = None
    final_url: str = ""
    is_empty: bool = False     # 服务器确认这是个 0 字节文件


@dataclass
class Snapshot:
    state: str
    mode: str
    total: int
    done: int
    speed: float
    eta: float | None
    active: int
    chunks: int
    retries: int
    message: str
    path: str

    @property
    def percent(self) -> float | None:
        if self.total <= 0:
            return None
        return min(100.0, self.done * 100.0 / self.total)


# --------------------------------------------------------------------------- 统计

class Stats:
    """跨线程累计已下载字节，并用滑动窗口估算速度与剩余时间。

    滑动窗口有个必须处理的副作用：数据停了之后，旧样本会一个个老化出窗口，
    但分母仍是整个窗口长度，于是速度读数会拖一条长尾慢慢衰减到 0——看起来
    像「下载还在慢慢停」，其实字节早就一个不涨了。所以这里加了新鲜度判断：
    最近一次采样超过 `_stale_after` 秒没有更新，就直接报 0。
    """

    def __init__(self, window: float = 4.0, stale_after: float = 3.0):
        self._stale_after = stale_after
        self._lock = threading.Lock()
        self._window = window
        self._samples: deque[tuple[float, int]] = deque()
        self.total = 0
        self.done = 0
        self.retries = 0

    def reset(self, total: int, done: int = 0) -> None:
        with self._lock:
            self.total = total
            self.done = done
            self.retries = 0
            self._samples.clear()

    def add(self, count: int) -> None:
        if count <= 0:
            return
        with self._lock:
            self.done += count
            self._samples.append((time.monotonic(), count))

    def inc_retry(self) -> None:
        with self._lock:
            self.retries += 1

    def snapshot(self) -> tuple[int, float, float | None]:
        """返回 (已下载字节, 速度 B/s, ETA 秒或 None)。"""
        with self._lock:
            now = time.monotonic()
            while self._samples and now - self._samples[0][0] > self._window:
                self._samples.popleft()
            done, total = self.done, self.total
            if not self._samples or now - self._samples[-1][0] > self._stale_after:
                return done, 0.0, None      # 已经不再来数据了，速度就是 0
            if len(self._samples) >= 2:
                span = max(now - self._samples[0][0], 1e-3)
                speed = sum(count for _, count in self._samples) / span
            else:
                speed = 0.0
            eta = (total - done) / speed if (total and speed > 1) else None
            return done, speed, eta


# --------------------------------------------------------------------------- 辅助

_RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

_STATUS_MESSAGES = {
    400: "服务器拒绝了请求（400）",
    401: "需要登录认证（401），请填写 Cookie 后重试",
    403: "服务器拒绝访问（403）。可尝试在「高级设置」里更换 User-Agent，"
         "或补充 Referer / Cookie 后重试",
    404: "文件不存在（404），请检查下载链接",
    405: "服务器不允许该请求方式（405）",
    407: "需要代理认证（407）",
    410: "文件已被移除（410）",
    416: "服务器拒绝了断点续传请求（416）",
    429: "请求过于频繁（429），请稍后重试",
    451: "因法律原因不可访问（451）",
    500: "服务器内部错误（500）",
    502: "网关错误（502）",
    503: "服务暂不可用（503）",
    504: "网关超时（504）",
}


def describe_status(status: int) -> str:
    return _STATUS_MESSAGES.get(status, f"服务器返回错误状态码 {status}")


class RateLimiter:
    """跨任务共享的令牌桶限速。rate <= 0 表示不限速。

    分片线程每写完一块就来取一次额度，不够就睡到够为止，因此限速是全局的
    （所有任务、所有分片共享同一个桶），符合用户对「总下载速度不超过 X」的预期。
    """

    def __init__(self, rate: float = 0.0, burst_seconds: float = 0.5):
        self._lock = threading.Lock()
        self._rate = max(0.0, float(rate))
        self._burst_seconds = burst_seconds
        self._allowance = 0.0
        self._last = time.monotonic()

    @property
    def rate(self) -> float:
        with self._lock:
            return self._rate

    def set_rate(self, rate: float) -> None:
        with self._lock:
            self._rate = max(0.0, float(rate))
            self._allowance = 0.0
            self._last = time.monotonic()

    def acquire(self, amount: int, stop: threading.Event | None = None) -> bool:
        """取用 amount 字节的额度，不够就等到够。返回 False 表示中途被要求停止。"""
        if amount <= 0:
            return True
        while True:
            if stop is not None and stop.is_set():
                return False
            with self._lock:
                rate = self._rate
                if rate <= 0:
                    return True
                now = time.monotonic()
                # 桶容量至少要能装下单次请求，否则额度永远攒不够，会死循环
                capacity = max(rate * self._burst_seconds, float(amount))
                self._allowance = min(
                    self._allowance + (now - self._last) * rate, capacity)
                self._last = now
                if self._allowance >= amount:
                    self._allowance -= amount
                    return True
                wait = (amount - self._allowance) / rate
            time.sleep(min(wait, 0.2))     # 分片睡，保证取消/暂停响应及时


def build_session(cfg: DownloadConfig) -> requests.Session:
    """每个线程独立持有一个 Session（连接池互不争抢）。"""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=2, pool_maxsize=4,
        max_retries=0,  # 重试由分片层自己控制，避免双层重试互相干扰
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def preallocate(path: str, size: int) -> None:
    """预分配磁盘空间。NTFS 上 truncate 立即设置 EOF 并保留空间，零填充是惰性的。"""
    with open(path, "wb") as fh:
        fh.truncate(size)


def _write_all(fh, data: bytes) -> None:
    """FileIO.write 原则上可能短写，循环补齐。"""
    view = memoryview(data)
    while view:
        written = fh.write(view)
        if not written:
            raise OSError("文件写入未取得进展")
        view = view[written:]


def _brief(exc: BaseException) -> str:
    text = " ".join(str(exc).split()) or exc.__class__.__name__
    return text[:160]


def _backoff(attempt: int, retry_after: str | None = None) -> float:
    if retry_after:
        try:
            return max(0.5, min(float(retry_after), 60.0))
        except (TypeError, ValueError):
            pass
    return min(2 ** attempt, 30) * (0.5 + random.random() / 2)


def _int_or_none(value: str | None) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# 由 _run_once 返回的结局
_DONE, _FAILED, _PAUSED, _CANCELLED, _RESTART = "done", "failed", "paused", "cancelled", "restart"

# 真正意义上的终态。PAUSED 不在其中：暂停之后仍然可以被取消。
_FINAL_STATES = (TaskState.DONE, TaskState.FAILED, TaskState.CANCELLED)


# --------------------------------------------------------------------------- 主类

class DownloadTask:
    def __init__(self, cfg: DownloadConfig, events: queue.Queue,
                 tag: int = 0, limiter: RateLimiter | None = None):
        self.cfg = cfg
        self.events = events
        self.tag = tag                 # 事件里带上它，界面才知道是哪个任务发的
        self.limiter = limiter

        self._lock = threading.RLock()
        self._save_lock = threading.Lock()
        self._state = TaskState.IDLE
        self._message = "就绪"

        self._pause = threading.Event()
        self._pause.set()          # set = 运行中
        self._halt = threading.Event()   # 要求所有线程尽快停止（取消或致命错误）
        self._user_cancel = False
        self._delete_on_cancel = False
        self._fatal: DownloadError | None = None
        self._restart_requested = False
        # 管理器线程是否还在跑。不能用 _state 判断：pause() 会立刻把状态置成
        # PAUSED，而此时线程还在收尾，靠状态会误判成「已经没人处理标志位了」。
        self._manager_running = False

        self._thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._responses: list[requests.Response] = []
        self._response_lock = threading.Lock()
        self._stats = Stats()
        self._dirty = threading.Event()
        self._last_save = 0.0

        self.mode = "-"            # "multi" | "single"
        self.ranges: list[list[int]] = []
        self.final_path = ""
        self.part_path = ""
        self.state_path = ""
        self._resume_state: ResumeState | None = None

    # ------------------------------------------------------------- 界面调用

    def start(self) -> None:
        """开始下载。若处于暂停/取消/失败状态，则等同于继续（走续传路径）。"""
        with self._lock:
            if self._state in (TaskState.PROBING, TaskState.RUNNING):
                return
            if not self.cfg.url:
                self._emit("error", "请先填写下载链接。", "")
                return
            self._halt.clear()
            self._pause.set()
            self._user_cancel = False
            self._delete_on_cancel = False
            self._fatal = None
            self._restart_requested = False
            self._manager_running = True
            self._stats.reset(0, 0)
            self._state = TaskState.PROBING
            self._message = "正在探测服务器…"
            self._thread = threading.Thread(
                target=self._run, name="download-manager", daemon=True)
            self._thread.start()

    def _mark_running(self) -> None:
        """探测完成、真正开始拉数据了。

        以前这里忘了改状态，任务会一直停在 PROBING，于是「下载中」和「连接中」
        分不出来——界面按状态显示文字时就会整场都写着「连接中」。
        """
        with self._lock:
            if self._state not in _FINAL_STATES:
                self._state = TaskState.RUNNING
        self._set_message("下载中…")

    def mark_queued(self) -> None:
        """由调度器调用：任务在排队，还没轮到它跑。"""
        with self._lock:
            if self._state in (TaskState.IDLE, TaskState.QUEUED):
                self._state = TaskState.QUEUED
                self._message = "排队等待中…"

    def pause(self) -> None:
        """请求暂停：分片线程写盘后退出。是幂等的。"""
        with self._lock:
            if self._state is TaskState.QUEUED:
                # 还没轮到它跑，没有线程需要通知，改个状态就够了。
                # 早先这里直接返回，于是暂停排队中的任务时它会一直显示「排队中」。
                self._state = TaskState.PAUSED
                self._message = "已暂停（点击继续以恢复）"
                return
            if self._state not in (TaskState.PROBING, TaskState.RUNNING):
                return
            self._state = TaskState.PAUSED
            self._message = "正在暂停…"
        self._pause.clear()

    def cancel(self, delete_partial: bool = False) -> None:
        """请求取消。delete_partial=True 时连同临时文件一起删除。是幂等的。

        暂停之后管理器线程已经退出了，标志位没人处理，所以这里必须自己收尾；
        而下载进行中时又不能在调用方线程直接收尾——工作线程还在写同一个文件。
        判断「线程是否还在跑」必须和设置标志位放在同一个锁里，否则会和正在
        退出的线程互相错过，导致两边都以为对方会处理。
        """
        with self._lock:
            if self._state is TaskState.IDLE or self._state in _FINAL_STATES:
                return
            self._delete_on_cancel = delete_partial
            self._user_cancel = True
            self._message = "正在取消…"
            manager_running = self._manager_running

        self._halt.set()
        self._pause.set()          # 让处于暂停等待的线程也醒过来检查取消
        self._close_responses()

        if not manager_running:
            self._finish_cancelled()

    def _finish_cancelled(self) -> None:
        if self._delete_on_cancel:
            self._remove_partial_files()
        # 已下载字节归零（保留 total，否则界面会误以为长度未知而切成滚动条）。
        # 这批数据已经作废了：任务到此结束，下次开始会重新从 .part.json 里
        # 读出真实的续传位置，内存里这个计数器没有任何后续用途，
        # 留着只会让进度条停在半路，下次下载前还要用户自己看着别扭。
        self._stats.reset(self._stats.total, 0)
        self._finish(TaskState.CANCELLED)

    def _remove_partial_files(self) -> None:
        delete_state(self.state_path)
        if not self.part_path:
            return
        try:
            os.remove(self.part_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # 句柄没关干净时 Windows 会拒绝删除，不影响正确性，留个记录即可
            self._log(f"临时文件删除失败：{exc}")

    def snapshot(self) -> Snapshot:
        done, speed, eta = self._stats.snapshot()
        with self._lock:
            state, message = self._state, self._message
            active = sum(1 for t in self._workers if t.is_alive())
            if state not in (TaskState.RUNNING, TaskState.PROBING):
                # 已经停下来的任务没有速度可言。交给滑动窗口去算的话，旧样本会
                # 一个个老化出窗口而分母不变，读数就拖着长尾慢慢降，看起来像
                # 「下载还在慢慢停」——数据其实早就一个字节都不进了。
                speed, eta = 0.0, None
            return Snapshot(
                state=state.value, mode=self.mode, total=self._stats.total, done=done,
                speed=speed, eta=eta, active=active, chunks=len(self.ranges),
                retries=self._stats.retries, message=message, path=self.final_path)

    def is_active(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def wait(self, timeout: float | None = None) -> bool:
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout)
        return not thread.is_alive()

    # ------------------------------------------------------------- 与管理器线程交互

    def _emit(self, kind: str, *payload) -> None:
        self.events.put((kind, self.tag, *payload))

    def _log(self, message: str) -> None:
        self._emit("log", message)

    def _set_message(self, message: str) -> None:
        with self._lock:
            self._message = message

    def _register_response(self, response: requests.Response) -> None:
        with self._response_lock:
            self._responses.append(response)

    def _unregister_response(self, response: requests.Response) -> None:
        with self._response_lock:
            try:
                self._responses.remove(response)
            except ValueError:
                pass

    def _close_responses(self) -> None:
        """关闭在途响应，让阻塞在 socket 读上的线程立刻抛出异常返回。"""
        with self._response_lock:
            responses, self._responses = self._responses, []
        for response in responses:
            try:
                response.close()
            except Exception:
                pass

    def _fail(self, error: DownloadError) -> None:
        """记录首个致命错误并让所有线程停下来。"""
        with self._lock:
            if self._fatal is not None:
                return
            self._fatal = error
        self._halt.set()
        self._pause.set()
        self._close_responses()

    def _sleep_interruptible(self, seconds: float) -> bool:
        """可被打断的睡眠。返回 False 表示收到了停止/暂停信号。"""
        deadline = time.monotonic() + seconds
        while True:
            if self._halt.is_set() or not self._pause.is_set():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            time.sleep(min(0.2, remaining))

    # ------------------------------------------------------------- 状态文件

    def _save_state_now(self) -> None:
        state = self._resume_state
        if state is None or not self.state_path:
            return
        with self._save_lock:
            with self._lock:
                state.ranges = [list(r) for r in self.ranges]
            try:
                save_state(state, self.state_path)
            except OSError as exc:
                self._log(f"状态文件保存失败：{exc}")
                return
            self._dirty.clear()

    def _watchdog(self) -> None:
        if not self._dirty.is_set():
            return
        if time.monotonic() - self._last_save < self.cfg.state_save_interval:
            return
        self._last_save = time.monotonic()
        self._save_state_now()

    # ------------------------------------------------------------- 管理器线程

    def _run(self) -> None:
        restarted = False
        try:
            while True:
                try:
                    outcome = self._run_once()
                except DownloadError as exc:
                    self._finish(TaskState.FAILED, exc)
                    return
                except Exception as exc:  # 兜底，避免线程静默死亡导致界面卡住
                    self._finish(TaskState.FAILED,
                                 DownloadError(f"内部错误：{_brief(exc)}", repr(exc)))
                    return
                if outcome == _RESTART and not restarted:
                    restarted = True
                    self._log("服务器拒绝了续传请求，将丢弃已有进度重新下载。")
                    self._discard_partial()
                    continue
                return
        finally:
            with self._lock:
                self._manager_running = False
                # 收尾：可能有取消请求是在 _run_once 已经决定好结局之后才到的，
                # 那时它看到线程还活着，就把收尾托付给了这里。
                late_cancel = (self._user_cancel
                               and self._state not in _FINAL_STATES)
            if late_cancel:
                self._finish_cancelled()

    def _discard_partial(self) -> None:
        self._remove_partial_files()
        with self._lock:
            self._resume_state = None
            self.ranges = []
            self._fatal = None
            self._restart_requested = False
            self._stats.reset(0, 0)
        self._halt.clear()
        self._pause.set()

    def _run_once(self) -> str:
        self._set_message("正在探测服务器…")
        info = self._probe()
        if self._halt.is_set():
            return self._abort_outcome()

        self._resolve_paths(info)
        self._emit("path", self.final_path)
        self.mode = "multi" if info.supports_range else "single"
        self._log_probe(info)

        if info.is_empty:
            self._create_empty_file()
            delete_state(self.state_path)
            self._finish(TaskState.DONE)
            return _DONE

        done = self._prepare_file(info)
        self._stats.reset(info.total, done)
        self._mark_running()

        if self._halt.is_set():
            if not (self._user_cancel and self._delete_on_cancel):
                self._save_state_now()
            return self._abort_outcome()

        self._start_workers()
        self._join_workers()
        if self._user_cancel and self._delete_on_cancel:
            # 工作线程已停、句柄已关，这时删文件才安全
            self._remove_partial_files()
        else:
            self._save_state_now()

        if self._fatal is not None:
            self._finish(TaskState.FAILED, self._fatal)
            return _FAILED
        if self._user_cancel:
            self._finish_cancelled()
            return _CANCELLED
        if not self._pause.is_set():
            self._finish(TaskState.PAUSED)
            return _PAUSED
        if self._restart_requested:
            return _RESTART
        return self._complete()

    def _abort_outcome(self) -> str:
        if self._fatal is not None:
            self._finish(TaskState.FAILED, self._fatal)
            return _FAILED
        if self._user_cancel:
            self._finish_cancelled()
            return _CANCELLED
        self._finish(TaskState.FAILED, DownloadError("下载已中止"))
        return _FAILED

    def _finish(self, state: TaskState, error: DownloadError | None = None) -> None:
        with self._lock:
            if self._state in _FINAL_STATES:
                # 终态只认第一次。注意 PAUSED 不在其中——暂停是可以被取消覆盖的，
                # 若把暂停也当成终态，暂停后点取消就会被这里挡掉。
                return
            self._state = state
            if error is not None:
                self._message = error.message
        if state is TaskState.DONE:
            self._set_message(f"已完成：{self.final_path}")
            self._emit("done", self.final_path)
        elif state is TaskState.FAILED:
            message = error.message if error else "下载失败"
            self._set_message(message)
            self._emit("error", message, error.detail if error else "")
        elif state is TaskState.CANCELLED:
            if self._delete_on_cancel:
                self._set_message("已取消（临时文件已删除）")
            else:
                self._set_message("已取消（可再次点击开始以续传）")
            self._emit("cancelled", self._delete_on_cancel)
        elif state is TaskState.PAUSED:
            self._set_message("已暂停（点击继续以恢复）")
            self._emit("paused")

    # ------------------------------------------------------------- 探测

    def _probe(self) -> ProbeResult:
        last: DownloadError | None = None
        for attempt in range(1, 4):
            try:
                return self._probe_once()
            except _RetryableProbe as exc:
                last = exc.error
            except DownloadError as exc:
                # 网络抖动（超时/连接被拒）值得再试一次。早先只有 HTTP 状态码类
                # 错误才重试，一次瞬时超时就会让整个任务直接失败。
                if not exc.transient:
                    raise
                last = exc
            if attempt >= 3 or self._halt.is_set():
                break
            delay = _backoff(attempt)
            self._log(f"连接失败（{last.message}），{delay:.1f} 秒后重试…")
            if not self._sleep_interruptible(delay):
                break
        raise last or DownloadError("探测服务器失败")

    def _probe_once(self, use_range: bool = True) -> ProbeResult:
        """用 `Range: bytes=0-0` 探测，而不是 HEAD —— 很多服务器对 HEAD 回 405。"""
        session = build_session(self.cfg)
        try:
            headers = self.cfg.build_headers()
            if use_range:
                headers["Range"] = "bytes=0-0"
            response = session.get(
                self.cfg.url, headers=headers, stream=True, allow_redirects=True,
                verify=self.cfg.verify_tls,
                timeout=(self.cfg.connect_timeout, self.cfg.read_timeout))
        except requests.exceptions.SSLError as exc:
            session.close()
            raise DownloadError(
                "SSL 证书验证失败。如果确认该站点可信，可勾选「忽略证书错误」后重试。",
                str(exc)) from exc
        except requests.exceptions.ConnectTimeout as exc:
            session.close()
            raise DownloadError("连接服务器超时，请检查下载链接和网络。",
                                str(exc), transient=True) from exc
        except requests.exceptions.ReadTimeout as exc:
            # 连上了但服务器迟迟不给响应——和「连不上」是两回事，别混为一谈
            session.close()
            raise DownloadError("服务器响应超时，对方可能限速或网络不稳。",
                                str(exc), transient=True) from exc
        except requests.exceptions.RequestException as exc:
            session.close()
            raise DownloadError(f"无法连接到服务器：{_brief(exc)}",
                                repr(exc), transient=True) from exc

        try:
            with response:
                status = response.status_code
                resp_headers = response.headers
                final_url = response.url
                disposition = resp_headers.get("Content-Disposition")
                etag = resp_headers.get("ETag")
                last_modified = resp_headers.get("Last-Modified")
                content_length = _int_or_none(resp_headers.get("Content-Length"))
        finally:
            session.close()

        def build(supports_range: bool, total: int, is_empty: bool = False) -> ProbeResult:
            return ProbeResult(
                supports_range=supports_range, total=total, etag=etag,
                last_modified=last_modified, content_disposition=disposition,
                final_url=final_url, is_empty=is_empty)

        if status == 416:
            if use_range:
                # 416 既可能是「0 字节文件」，也可能是「服务器拒绝 Range」。
                # 不带 Range 再问一次，用 Content-Length 区分。
                return self._probe_once(use_range=False)
            raise DownloadError(describe_status(416), f"HTTP 416 {self.cfg.url}")
        if status == 206:
            content_range = parse_content_range(resp_headers.get("Content-Range"))
            if content_range and content_range.start == 0 and content_range.total > 0:
                return build(True, content_range.total)
            return build(False, content_length or 0)
        if status == 200:
            return build(False, content_length or 0, is_empty=(content_length == 0))
        if status in _RETRYABLE_STATUS:
            raise _RetryableProbe(DownloadError(describe_status(status)))
        raise DownloadError(describe_status(status), f"HTTP {status} {self.cfg.url}")

    def _log_probe(self, info: ProbeResult) -> None:
        if info.is_empty:
            self._log("服务器返回的是 0 字节文件。")
        elif info.supports_range:
            self._log(f"服务器支持分片下载，文件大小 {fmt_size(info.total)}。")
        elif info.total:
            self._log(f"服务器不支持分片下载（忽略 Range 请求），改用单线程。"
                      f"文件大小 {fmt_size(info.total)}。")
        else:
            self._log("服务器不支持分片下载，且未提供文件大小，改用单线程流式下载。")

    def _create_empty_file(self) -> None:
        with open(self.final_path, "wb"):
            pass

    # ------------------------------------------------------------- 路径与文件准备

    def _resolve_paths(self, info: ProbeResult) -> None:
        filename = resolve_filename(info.content_disposition, self.cfg.url)
        save_path = (self.cfg.save_path or "").strip()
        if not save_path:
            target = os.path.join(os.path.expanduser("~"), "Downloads", filename)
        elif save_path.endswith(("/", "\\")) or os.path.isdir(save_path):
            target = os.path.join(save_path, filename)
        else:
            target = save_path

        self.final_path = os.path.abspath(target)
        self.part_path = self.final_path + ".part"
        self.state_path = state_path_for(self.part_path)
        directory = os.path.dirname(self.final_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

    def _prepare_file(self, info: ProbeResult) -> int:
        """决定从零开始还是续传，并返回已完成字节数。"""
        if self.mode == "single":
            # 单线程流式：无法记录可校验的偏移，因此不做续传
            delete_state(self.state_path)
            self._resume_state = None
            return 0

        state = load_state(self.state_path) if self.cfg.resume else None
        if state is not None:
            reason, warning = validate_resume(
                state, url=self.cfg.url, total_size=info.total, etag=info.etag,
                last_modified=info.last_modified, part_path=self.part_path)
            if reason:
                self._log(f"无法续传（{reason}），将重新下载。")
                state = None
            elif warning:
                self._log(f"注意：{warning}，仍按续传处理。")

        if state is not None:
            new_ranges = split_ranges(info.total, self.cfg.threads, self.cfg.min_chunk_size)
            rebase_progress(state.ranges, new_ranges)
            self.ranges = new_ranges
            self._resume_state = ResumeState(
                url=self.cfg.url, final_path=self.final_path, total_size=info.total,
                etag=info.etag, last_modified=info.last_modified, supports_range=True,
                chunk_read=self.cfg.chunk_read, ranges=new_ranges)
            completed = sum(entry[2] for entry in new_ranges)
            if completed < info.total:
                self._log(f"续传：已下载 {fmt_size(completed)} / {fmt_size(info.total)}，"
                          f"使用 {len(new_ranges)} 个分片。")
            return completed

        delete_state(self.state_path)
        self.ranges = split_ranges(info.total, self.cfg.threads, self.cfg.min_chunk_size)
        preallocate(self.part_path, info.total)
        self._resume_state = ResumeState(
            url=self.cfg.url, final_path=self.final_path, total_size=info.total,
            etag=info.etag, last_modified=info.last_modified, supports_range=True,
            chunk_read=self.cfg.chunk_read, ranges=self.ranges)
        self._log(f"开始下载：共 {len(self.ranges)} 个分片，"
                  f"每片约 {fmt_size(info.total // max(1, len(self.ranges)))}。")
        return 0

    # ------------------------------------------------------------- 工作线程

    def _start_workers(self) -> None:
        self._workers = []
        if self.mode == "multi":
            for index in range(len(self.ranges)):
                self._workers.append(threading.Thread(
                    target=self._chunk_worker, args=(index,),
                    name=f"chunk-{index}", daemon=True))
        else:
            self._workers.append(threading.Thread(
                target=self._single_worker, name="single", daemon=True))
        for worker in self._workers:
            worker.start()

    def _join_workers(self) -> None:
        deadline: float | None = None
        while True:
            self._watchdog()
            if all(not worker.is_alive() for worker in self._workers):
                return
            if self._halt.is_set():
                # 正常情况秒级退出；留一个上限，避免个别卡死的连接拖住界面
                if deadline is None:
                    deadline = time.monotonic() + self.cfg.read_timeout + 5
                elif time.monotonic() > deadline:
                    self._log("部分线程未能及时停止，将保留当前进度直接退出。")
                    return
            time.sleep(0.2)

    def _persist(self, index: int, offset: int) -> None:
        """把分片的完成进度推进到 offset，并把增量计入统计。"""
        with self._lock:
            entry = self.ranges[index]
            start, end = entry[0], entry[1]
            new_done = min(offset - start, end - start + 1)
            delta = new_done - entry[2]
            entry[2] = new_done
        if delta:
            self._stats.add(delta)
            self._dirty.set()

    def _chunk_worker(self, index: int) -> None:
        start, end, already = self.ranges[index]
        offset = start + already
        total_chunks = len(self.ranges)
        session = build_session(self.cfg)
        handle = None
        try:
            handle = open(self.part_path, "r+b", buffering=0)
            handle.seek(offset)  # 顺序写，之后文件位置天然等于 offset
            attempt = 0
            while offset <= end:
                if self._halt.is_set():
                    return
                if not self._pause.is_set():
                    return
                try:
                    headers = self.cfg.build_headers()
                    headers["Range"] = f"bytes={offset}-{end}"
                    response = session.get(
                        self.cfg.url, headers=headers, stream=True, allow_redirects=True,
                        verify=self.cfg.verify_tls,
                        timeout=(self.cfg.connect_timeout, self.cfg.read_timeout))
                    with response:
                        if response.status_code == 416:
                            raise RangeNotSatisfiable("服务器返回 416")
                        if response.status_code != 206:
                            if response.status_code in _RETRYABLE_STATUS:
                                raise _RetryableStatus(
                                    response.status_code, response.headers.get("Retry-After"))
                            raise _FatalStatus(response.status_code)
                        content_range = parse_content_range(response.headers.get("Content-Range"))
                        if content_range is None or content_range.start != offset:
                            raise RangeUnsupported("服务器返回的 Content-Range 与请求不符")
                        attempt = 0  # 只有成功握手才清零
                        before = offset
                        self._register_response(response)
                        try:
                            for block in response.iter_content(self.cfg.chunk_read):
                                if self._halt.is_set() or not self._pause.is_set():
                                    return
                                _write_all(handle, block)
                                offset += len(block)
                                self._persist(index, offset)
                                if self.limiter is not None and not self.limiter.acquire(
                                        len(block), self._halt):
                                    return
                        finally:
                            self._unregister_response(response)
                    if offset <= end and offset == before:
                        # 响应体为空且没有抛异常：再循环一次会变成死循环，必须当作失败
                        raise RangeUnsupported("服务器没有返回任何数据")
                except RangeNotSatisfiable:
                    raise
                except _FatalStatus as exc:
                    raise _ChunkFailed(index, DownloadError(describe_status(exc.status))) from exc
                except _ChunkFailed:
                    raise
                except (requests.RequestException, RangeUnsupported, _RetryableStatus, OSError) as exc:
                    if self._halt.is_set():
                        return
                    attempt += 1
                    if attempt > self.cfg.max_retries:
                        raise _ChunkFailed(index, DownloadError(
                            f"分片 {index + 1}/{total_chunks} 重试 {self.cfg.max_retries} 次后仍失败："
                            f"{_brief(exc)}", repr(exc))) from exc
                    self._stats.inc_retry()
                    delay = _backoff(attempt, getattr(exc, "retry_after", None))
                    self._log(f"分片 {index + 1}/{total_chunks} 第 {attempt} 次重试"
                              f"（{_brief(exc)}），{delay:.1f} 秒后从当前位置继续。")
                    if not self._sleep_interruptible(delay):
                        return
            self._persist(index, end + 1)
        except RangeNotSatisfiable:
            with self._lock:
                self._restart_requested = True
            self._halt.set()
            self._pause.set()
            self._close_responses()
        except _ChunkFailed as exc:
            self._fail(exc.error)
        except Exception as exc:
            self._fail(DownloadError(
                f"分片 {index + 1}/{total_chunks} 发生未预期的错误：{_brief(exc)}", repr(exc)))
        finally:
            if handle is not None:
                try:
                    handle.close()
                except OSError:
                    pass
            session.close()

    def _single_worker(self) -> None:
        """服务器不支持 Range 时的回退路径：单连接流式下载（不支持续传）。"""
        session = build_session(self.cfg)
        handle = None
        try:
            handle = open(self.part_path, "wb", buffering=0)
            attempt = 0
            offset = 0
            while True:
                if self._halt.is_set():
                    return
                if not self._pause.is_set():
                    return
                try:
                    response = session.get(
                        self.cfg.url, headers=self.cfg.build_headers(), stream=True,
                        allow_redirects=True, verify=self.cfg.verify_tls,
                        timeout=(self.cfg.connect_timeout, self.cfg.read_timeout))
                    with response:
                        if response.status_code >= 400:
                            if response.status_code in _RETRYABLE_STATUS:
                                raise _RetryableStatus(
                                    response.status_code, response.headers.get("Retry-After"))
                            raise _FatalStatus(response.status_code)
                        self._register_response(response)
                        try:
                            for block in response.iter_content(self.cfg.chunk_read):
                                if self._halt.is_set() or not self._pause.is_set():
                                    return
                                _write_all(handle, block)
                                offset += len(block)
                                self._stats.add(len(block))
                                self._dirty.set()
                                if self.limiter is not None and not self.limiter.acquire(
                                        len(block), self._halt):
                                    return
                        finally:
                            self._unregister_response(response)
                    expected = self._stats.total
                    if expected and offset < expected:
                        raise requests.exceptions.ChunkedEncodingError(
                            f"连接提前关闭（{offset}/{expected}）")
                    return
                except _FatalStatus as exc:
                    self._fail(DownloadError(describe_status(exc.status)))
                    return
                except (requests.RequestException, _RetryableStatus, OSError) as exc:
                    if self._halt.is_set():
                        return
                    attempt += 1
                    if attempt > self.cfg.max_retries:
                        self._fail(DownloadError(
                            f"单线程下载重试 {self.cfg.max_retries} 次后仍失败：{_brief(exc)}",
                            repr(exc)))
                        return
                    self._stats.inc_retry()
                    delay = _backoff(attempt, getattr(exc, "retry_after", None))
                    self._log(f"单线程下载第 {attempt} 次重试（{_brief(exc)}），"
                              f"{delay:.1f} 秒后从头开始。")
                    if not self._sleep_interruptible(delay):
                        return
                    # 服务器不支持 Range，只能整体重来
                    handle.seek(0)
                    handle.truncate(0)
                    offset = 0
                    self._stats.reset(self._stats.total, 0)
        except Exception as exc:
            self._fail(DownloadError(f"下载发生未预期的错误：{_brief(exc)}", repr(exc)))
        finally:
            if handle is not None:
                try:
                    handle.close()
                except OSError:
                    pass
            session.close()

    # ------------------------------------------------------------- 收尾

    def _complete(self) -> str:
        if self.mode == "multi":
            if sum(entry[2] for entry in self.ranges) != self._stats.total:
                self._finish(TaskState.FAILED, DownloadError("下载不完整：仍有分片未完成"))
                return _FAILED
        else:
            total = self._stats.total
            if total and self._stats.done < total:
                self._finish(TaskState.FAILED, DownloadError(
                    f"下载不完整：只收到 {fmt_size(self._stats.done)} / {fmt_size(total)}"))
                return _FAILED

        try:
            os.replace(self.part_path, self.final_path)
        except OSError as exc:
            # 保留 .part 与状态文件，用户重试时会直接再尝试一次改名
            self._finish(TaskState.FAILED, DownloadError(
                f"无法写入目标文件（可能正被其他程序占用）：{self.final_path}", str(exc)))
            return _FAILED

        delete_state(self.state_path)
        with self._lock:
            self._resume_state = None
        self._finish(TaskState.DONE)
        return _DONE
