#!/usr/bin/env python3
"""真实网络加速比验证：对同一个远程文件分别用 N 个线程下载固定时长，比较吞吐。

每个配置只跑 DURATION 秒就取消，不会把整个文件拉下来。
用法：  python tests/bench_live.py [URL] [每个配置的秒数]
"""

from __future__ import annotations

import os
import queue
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from downloader.config import DownloadConfig     # noqa: E402
from downloader.core import DownloadTask         # noqa: E402
from downloader.util import fmt_size             # noqa: E402

DEFAULT_URL = ("https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/22.04/"
               "ubuntu-22.04.5-live-server-amd64.iso")


def bench(url: str, threads: int, duration: float, workdir: Path) -> float:
    out = workdir / f"bench_{threads}.iso"
    for path in (out, Path(str(out) + ".part"), Path(str(out) + ".part.json")):
        try:
            os.remove(path)
        except OSError:
            pass

    cfg = DownloadConfig(url=url, save_path=str(out), threads=threads)
    events: queue.Queue = queue.Queue()
    task = DownloadTask(cfg, events)
    task.start()

    # 等真正开始出数据（跳过探测和建连）
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if task.snapshot().done > 0:
            break
        if task.snapshot().state == "failed":
            raise SystemExit(f"下载失败：{task.snapshot().message}")
        time.sleep(0.05)
    else:
        raise SystemExit("30 秒内没有收到任何数据")

    samples: list[float] = []
    start = time.monotonic()
    while time.monotonic() - start < duration:
        samples.append(task.snapshot().speed)
        time.sleep(0.5)

    snapshot = task.snapshot()
    task.cancel()
    task.wait(timeout=20)

    # 丢掉前 2 秒（滑动窗口还没填满）
    steady = samples[4:] or samples
    average = sum(steady) / len(steady)
    print(f"    {threads:>2} 线程：稳态均速 {fmt_size(average):>10}/s   "
          f"共收到 {fmt_size(snapshot.done)}")
    if snapshot.mode != "multi" and threads > 1:
        print(f"      （注意：本次走的是 {snapshot.mode} 模式）")
    return average


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0

    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass

    print(f"目标：{url}")
    print(f"每个配置跑 {duration:.0f} 秒\n")

    workdir = Path(tempfile.mkdtemp(prefix="dl-bench-"))
    results: dict[int, float] = {}
    try:
        for threads in (1, 4, 8, 16):
            results[threads] = bench(url, threads, duration, workdir)
    finally:
        for path in workdir.glob("*"):
            try:
                os.remove(path)
            except OSError:
                pass
        try:
            os.rmdir(workdir)
        except OSError:
            pass

    print()
    baseline = results.get(1) or 0
    if baseline <= 0:
        print("单线程没有拿到有效速度，无法计算加速比")
        return 1
    for threads, speed in results.items():
        print(f"  {threads:>2} 线程：{speed / baseline:>5.2f}×   {fmt_size(speed)}/s")
    best = max(results, key=results.get)
    print(f"\n最佳：{best} 线程，相对单线程 {results[best] / baseline:.2f}×")
    return 0


if __name__ == "__main__":
    sys.exit(main())
