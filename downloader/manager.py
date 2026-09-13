"""多任务调度：并发上限、排队、全局限速。

`DownloadTask` 只管一个文件的下载，这里负责把多个任务排好队：
用户点启动的任务进入「想跑」列表，调度线程按并发上限逐个放行，超出的显示为排队中。
"""

from __future__ import annotations

import itertools
import queue
import threading
import time
from dataclasses import dataclass, field

from .config import DownloadConfig
from .core import DownloadTask, RateLimiter, TaskState

__all__ = ["DownloadManager", "TaskEntry"]

# 已经是终局的状态，调度器见到就把「想跑」标记摘掉，避免无限重启
_FINISHED = ("done", "failed", "cancelled")


@dataclass
class TaskEntry:
    """界面持有的一条任务记录。"""

    id: int
    task: DownloadTask
    wanted: bool = False          # 用户希望它跑（暂停/取消会把这个摘掉）
    # 用户刚刚显式点了「开始/继续/重试」。必须和 wanted 分开：调度器见到终态会
    # 自动摘掉 wanted（防止任务一结束就被反复拉起来），但那样会把用户在下一条
    # 指令里刚设的 wanted 也擦掉，导致取消过的任务再也无法重启。
    start_requested: bool = False
    added_at: float = field(default_factory=time.time)


class DownloadManager:
    def __init__(self, events: queue.Queue, max_concurrent: int = 3):
        self.events = events
        self.limiter = RateLimiter()
        self._lock = threading.RLock()
        self._entries: list[TaskEntry] = []
        self._counter = itertools.count(1)
        self._max_concurrent = max(1, int(max_concurrent))
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="scheduler", daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------ 查询

    @property
    def entries(self) -> list[TaskEntry]:
        with self._lock:
            return list(self._entries)

    def get(self, task_id: int) -> TaskEntry | None:
        with self._lock:
            for entry in self._entries:
                if entry.id == task_id:
                    return entry
        return None

    @property
    def max_concurrent(self) -> int:
        with self._lock:
            return self._max_concurrent

    def active_count(self) -> int:
        return sum(1 for entry in self.entries if entry.task.is_active())

    # ------------------------------------------------------------------ 控制

    def add(self, cfg: DownloadConfig, autostart: bool = True) -> TaskEntry:
        with self._lock:
            task_id = next(self._counter)
            task = DownloadTask(cfg, self.events, tag=task_id, limiter=self.limiter)
            entry = TaskEntry(id=task_id, task=task, wanted=autostart)
            self._entries.append(entry)
        self._wake.set()
        return entry

    def start(self, entry: TaskEntry) -> None:
        """启动或继续。已经跑着的忽略。"""
        with self._lock:
            entry.wanted = True
            entry.start_requested = True
        self._wake.set()

    def pause(self, entry: TaskEntry) -> None:
        """暂停：同时摘掉「想跑」标记，否则调度器下一轮又把它拉起来。"""
        with self._lock:
            entry.wanted = False
        entry.task.pause()

    def cancel(self, entry: TaskEntry, delete_partial: bool = False) -> None:
        with self._lock:
            entry.wanted = False
        entry.task.cancel(delete_partial=delete_partial)

    def remove(self, entry: TaskEntry, delete_partial: bool = False) -> None:
        """从列表里移除。正在跑的先停下来，免得后台继续写文件。"""
        with self._lock:
            entry.wanted = False
            if entry in self._entries:
                self._entries.remove(entry)
        if entry.task.is_active():
            entry.task.cancel(delete_partial=delete_partial)
            entry.task.wait(timeout=5)
        elif delete_partial:
            entry.task.cancel(delete_partial=delete_partial)

    def start_all(self) -> int:
        """把所有还没下完的任务排上（失败和取消的也重试，「已完成」的不动）。"""
        started = 0
        for entry in self.entries:
            if entry.task.snapshot().state == "done":
                continue
            with self._lock:
                entry.wanted = True
                entry.start_requested = True
            started += 1
        self._wake.set()
        return started

    def pause_all(self) -> int:
        entries = self.entries
        for entry in entries:
            with self._lock:
                entry.wanted = False
        for entry in entries:
            entry.task.pause()
        return len(entries)

    def clear_finished(self) -> int:
        with self._lock:
            keep, gone = [], []
            for entry in self._entries:
                (gone if entry.task.snapshot().state in _FINISHED else keep).append(entry)
            self._entries = keep
        self._wake.set()
        return len(gone)

    # ------------------------------------------------------------------ 配置

    def set_max_concurrent(self, value: int) -> None:
        with self._lock:
            self._max_concurrent = max(1, min(int(value), 16))
        self._wake.set()

    def set_rate_limit(self, bytes_per_sec: float) -> None:
        self.limiter.set_rate(bytes_per_sec)

    def rate_limit(self) -> float:
        return self.limiter.rate

    def shutdown(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        for entry in self.entries:
            if entry.task.is_active():
                entry.task.cancel()
        for entry in self.entries:
            if entry.task.is_active():
                entry.task.wait(timeout=timeout / max(1, len(self.entries)))

    # ------------------------------------------------------------------ 调度

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._schedule()
            except Exception:
                # 调度线程绝不能死，否则整个列表就卡住了
                pass
            self._wake.wait(timeout=0.3)
            self._wake.clear()

    def _schedule(self) -> None:
        entries = self.entries
        capacity = self.max_concurrent
        running = sum(1 for entry in entries if entry.task.is_active())

        for entry in entries:
            state = entry.task.snapshot().state
            with self._lock:
                explicit = entry.start_requested
                entry.start_requested = False
            if state in _FINISHED and not explicit:
                # 自动摘掉「想跑」，免得任务一结束就被反复拉起来；
                # 但用户刚点的那一下不算，否则取消过的任务永远重启不了
                if entry.wanted:
                    with self._lock:
                        entry.wanted = False
                continue
            if not entry.wanted or entry.task.is_active():
                continue
            if running >= capacity:
                # 队伍排满了，标记成排队中让用户看到
                if state != TaskState.QUEUED.value:
                    entry.task.mark_queued()
                continue
            entry.task.start()
            running += 1
