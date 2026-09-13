"""断点续传状态：分片代数 + `<输出文件>.part.json` 的原子读写与失效校验。

分片用闭区间三元组表示：`[start, end, done]`，其中 `done` 是**从 start 起连续已下载
的字节数**（分片线程严格顺序写入，所以已完成的部分必定是一个前缀）。
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field

__all__ = [
    "STATE_VERSION",
    "ResumeState",
    "state_path_for",
    "save_state",
    "load_state",
    "delete_state",
    "validate_resume",
    "split_ranges",
    "rebase_progress",
    "covers_exactly",
]

STATE_VERSION = 1


@dataclass
class ResumeState:
    url: str
    final_path: str
    total_size: int
    etag: str | None = None
    last_modified: str | None = None
    supports_range: bool = True
    chunk_read: int = 65536
    ranges: list[list[int]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    version: int = STATE_VERSION


# --------------------------------------------------------------------------- 读写

def state_path_for(part_path: str) -> str:
    return part_path + ".json"


def save_state(state: ResumeState, path: str) -> None:
    """原子写入：先写 .tmp 并 fsync，再 os.replace，避免崩溃留下半截 JSON。"""
    state.updated_at = time.time()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(asdict(state), fh, ensure_ascii=False, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def load_state(path: str) -> ResumeState | None:
    """读取状态；文件缺失、损坏或版本不符一律返回 None（静默从零开始）。"""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or raw.get("version") != STATE_VERSION:
        return None
    try:
        ranges = [[int(a), int(b), int(c)] for a, b, c in raw["ranges"]]
        return ResumeState(
            url=str(raw["url"]),
            final_path=str(raw["final_path"]),
            total_size=int(raw["total_size"]),
            etag=raw.get("etag"),
            last_modified=raw.get("last_modified"),
            supports_range=bool(raw.get("supports_range", True)),
            chunk_read=int(raw.get("chunk_read", 65536)),
            ranges=ranges,
            created_at=float(raw.get("created_at", time.time())),
            updated_at=float(raw.get("updated_at", time.time())),
        )
    except (KeyError, TypeError, ValueError):
        return None


def delete_state(path: str) -> None:
    for candidate in (path, path + ".tmp"):
        try:
            os.remove(candidate)
        except OSError:
            pass


# --------------------------------------------------------------------------- 校验

def validate_resume(
    state: ResumeState,
    *,
    url: str,
    total_size: int,
    etag: str | None,
    last_modified: str | None,
    part_path: str,
) -> tuple[str | None, str | None]:
    """能续传则返回 (None, 警告或 None)；不能则返回 (中文原因, None)。"""
    if state.url != url:
        return "URL 已变化", None

    try:
        actual_size = os.path.getsize(part_path)
    except OSError:
        return "临时文件不存在", None
    if actual_size != state.total_size:
        return "临时文件大小与记录不符", None

    if total_size and state.total_size != total_size:
        return "服务器上的文件大小已变化", None

    warning = None
    if state.etag and etag:
        if state.etag != etag:
            return "服务器上的文件已更新（ETag 不同）", None
    elif state.last_modified and last_modified:
        if state.last_modified != last_modified:
            return "服务器上的文件已更新（最后修改时间不同）", None
    else:
        warning = "服务器未提供 ETag/Last-Modified，无法校验文件是否被改动"

    if not covers_exactly(state.ranges, state.total_size):
        return "分片记录不完整", None

    return None, warning


def covers_exactly(ranges: list[list[int]], total_size: int) -> bool:
    """分片是否无缝隙、无重叠地恰好覆盖 [0, total_size)。"""
    if total_size <= 0 or not ranges:
        return False
    position = 0
    for start, end, done in ranges:
        if start != position or end < start:
            return False
        if not (0 <= done <= end - start + 1):
            return False
        position = end + 1
    return position == total_size


# --------------------------------------------------------------------------- 分片代数

def split_ranges(total: int, threads: int, min_chunk: int = 1 << 20) -> list[list[int]]:
    """把 [0, total) 均分成尽量多的、每片不小于 min_chunk 的闭区间。

    返回 [[start, end, done], ...]；不变式：首片从 0 开始、末片到 total-1 结束、相邻无缝。
    """
    if total <= 0:
        return []
    n = max(1, min(int(threads), max(1, total // min_chunk), total))
    base, remainder = divmod(total, n)
    ranges: list[list[int]] = []
    start = 0
    for i in range(n):
        size = base + (1 if i < remainder else 0)
        if size <= 0:
            continue
        ranges.append([start, start + size - 1, 0])
        start += size
    return ranges


def rebase_progress(saved: list[list[int]], new: list[list[int]]) -> None:
    """用户改了线程数时，把旧分片的已完成字节映射到新分片上（就地修改 new[i][2]）。

    注意：**不能**简单地对新分片和旧已完成区间求交再求和。各分片进度不一致时，
    旧区间在新分片内可能是「两段、中间有洞」（例如旧分片已完成 [0,124] 与
    [250,374]，而新分片是 [0,333]）——求和会把洞也算成已下载，导致残缺文件被当成完整。
    因此这里只取从新分片起点开始**连续**覆盖的那一段长度。
    """
    merged: list[list[int]] = []
    for start, _end, done in sorted((s, e, d) for s, e, d in saved if d > 0):
        hi = start + done - 1
        if merged and start <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([start, hi])
    if not merged:
        return

    for entry in new:
        low, high = entry[0], entry[1]
        carried = 0
        for lo, hi in merged:
            if hi < low:
                continue
            if lo <= low:            # 从 low 起被连续覆盖
                carried = min(hi, high) - low + 1
            break                    # lo > low：起点处有洞，后面再多也不算
        entry[2] = min(carried, high - low + 1)
