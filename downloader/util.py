"""纯函数工具：文件名解析/清洗、单位格式化、Content-Range 解析。

本模块不依赖 requests / tkinter，可独立测试。
"""

from __future__ import annotations

import email.message
import os
import re
from dataclasses import dataclass
from email.header import decode_header
from urllib.parse import unquote, urlsplit

__all__ = [
    "ContentRange",
    "parse_content_range",
    "parse_content_disposition_filename",
    "sanitize_filename",
    "derive_filename_from_url",
    "resolve_filename",
    "fmt_size",
    "fmt_speed",
    "fmt_eta",
]


# --------------------------------------------------------------------------- 范围

@dataclass(frozen=True)
class ContentRange:
    """`Content-Range: bytes 0-99/1000` 的解析结果，区间为闭区间。"""

    start: int
    end: int
    total: int  # 0 表示未知，即 `bytes 0-99/*`

    @property
    def length(self) -> int:
        return self.end - self.start + 1


_CR_RE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", re.I)


def parse_content_range(value: str | None) -> ContentRange | None:
    if not value:
        return None
    m = _CR_RE.match(value.strip())
    if not m:
        return None
    start, end, total = m.groups()
    return ContentRange(int(start), int(end), 0 if total == "*" else int(total))


# --------------------------------------------------------------------------- 文件名

# RFC 5987 / RFC 6266 的 filename*=charset'lang'value 形式。
# 必须手工解析：当响应头同时含 filename= 与 filename*= 时，
# email.message.Message.get_filename() 会返回前者，违反 RFC 6266 的优先级规定。
_RFC5987_RE = re.compile(r"filename\*\s*=\s*([\w-]+)'([\w-]*)'([^;]+)", re.I)

# Windows 文件名非法字符（含控制字符）
_INVALID_CHARS_RE = re.compile(r'[<>:"|?*\x00-\x1f]')

_RESERVED_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}

MAX_FILENAME_LEN = 180


def parse_content_disposition_filename(content_disposition: str | None) -> str | None:
    """从 Content-Disposition 中取文件名，`filename*` 优先。返回 None 表示取不到。"""
    if not content_disposition:
        return None

    m = _RFC5987_RE.search(content_disposition)
    if m:
        charset, _lang, raw = m.groups()
        raw = raw.strip().strip('"')
        try:
            return unquote(raw, encoding=charset or "utf-8", errors="replace")
        except (LookupError, UnicodeDecodeError):
            return unquote(raw)

    msg = email.message.Message()
    msg["Content-Disposition"] = content_disposition
    name = msg.get_filename()
    if name and "=?" in name:
        # 非标准的 encoded-word 形式，部分服务器会用
        try:
            parts = decode_header(name)
        except Exception:
            return name
        chunks = []
        for payload, enc in parts:
            if isinstance(payload, bytes):
                try:
                    chunks.append(payload.decode(enc or "utf-8", "replace"))
                except LookupError:
                    chunks.append(payload.decode("utf-8", "replace"))
            else:
                chunks.append(payload)
        name = "".join(chunks)
    return name or None


def sanitize_filename(name: str | None) -> str | None:
    """把服务端给出的名字清洗成可安全落盘的 Windows 文件名；None 表示不可用。"""
    if not name:
        return None

    # 统一分隔符后只取最后一段，顺带消除 ../ 路径穿越
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = _INVALID_CHARS_RE.sub("_", name)
    name = name.strip(" .")
    if not name or name in (".", ".."):
        return None

    stem, ext = os.path.splitext(name)
    if stem.upper() in _RESERVED_NAMES:
        stem = "_" + stem
    if len(stem) + len(ext) > MAX_FILENAME_LEN:
        stem = stem[: max(1, MAX_FILENAME_LEN - len(ext))]
    return stem + ext


def derive_filename_from_url(url: str) -> str | None:
    try:
        path = urlsplit(url).path
    except ValueError:
        return None
    return sanitize_filename(unquote(path.rsplit("/", 1)[-1]))


def resolve_filename(content_disposition: str | None, url: str) -> str:
    """决定落盘文件名：Content-Disposition 优先，URL 兜底，最后 download.bin。"""
    for candidate in (
        parse_content_disposition_filename(content_disposition),
        derive_filename_from_url(url),
    ):
        clean = sanitize_filename(candidate)
        if clean:
            return clean
    return "download.bin"


# --------------------------------------------------------------------------- 格式化

_UNITS = ("B", "KB", "MB", "GB", "TB", "PB")


def fmt_size(num: float | int | None) -> str:
    if num is None:
        return "未知"
    value = float(num)
    if value < 1024:
        return f"{value:.0f} B"
    for unit in _UNITS[1:]:
        value /= 1024.0
        if value < 1024 or unit == _UNITS[-1]:
            return f"{value:.2f} {unit}"
    return f"{value:.2f} PB"


def fmt_speed(bytes_per_sec: float | None) -> str:
    if not bytes_per_sec or bytes_per_sec <= 0:
        return "-- B/s"
    return fmt_size(bytes_per_sec) + "/s"


def fmt_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0 or seconds != seconds or seconds == float("inf"):
        return "未知"
    total = int(seconds)
    if total < 3600:
        return f"{total // 60:02d}:{total % 60:02d}"
    return f"{total // 3600:d}:{(total % 3600) // 60:02d}:{total % 60:02d}"
