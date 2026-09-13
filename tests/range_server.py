"""测试用的本地 HTTP 服务器：支持 Range，并可模拟各种「坏服务器」行为。

为什么不用标准库的 SimpleHTTPRequestHandler：它**不支持 Range**，
所有请求都回 200 全量，无法用来测分片下载。

用法：
    httpd, thread = start_server(directory)
    url = f"http://127.0.0.1:{httpd.server_port}/file.bin"

查询参数开关（可叠加）：
    ?norange=1   忽略 Range 请求，一律回 200 全量（模拟不支持分片的服务器）
    ?nolen=1     不发送 Content-Length（模拟长度未知的流式响应）
    ?slow=<ms>   每个 64 KiB 数据块之间停顿，默认 20ms（便于测试暂停/取消）
    ?flaky=1     按 flaky_remaining 计数，前 N 次请求只发 1/3 就断开（触发重试）
    ?cd=1        Content-Disposition 同时含 filename= 与 filename*=（测优先级）
    ?cd2=1       Content-Disposition 只含 filename=
    ?cd3=1       Content-Disposition 使用非标准的 encoded-word 形式
    ?hang=<秒>   先挂起指定秒数再处理，用于制造稳定的「正在探测」观察窗口
    ?denyua=<子串> UA 含该子串就回 403（复现镜像站的 UA 拦截）
"""

from __future__ import annotations

import functools
import http.server
import os
import re
import threading
import time
import urllib.parse

__all__ = ["RangeHandler", "start_server"]

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")

BLOCK = 64 * 1024


class RangeHandler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    bytes_served = 0          # 累计真正写出的响应体字节数（测试用它证明没有重复下载）
    flaky_remaining = 0       # 还需制造多少次「发一半就断」的响应
    _lock = threading.Lock()

    def log_message(self, *args):  # 静音
        pass

    # ---------------------------------------------------------------- 入口

    def do_GET(self):
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)

        # ?hang=<秒>：先挂起再处理，用来制造「客户端正在探测」的稳定观察窗口
        hang = params.get("hang", [None])[0]
        if hang:
            try:
                time.sleep(float(hang))
            except (TypeError, ValueError):
                pass

        fs_path = self.translate_path(self.path)
        if not os.path.isfile(fs_path):
            self.send_error(404, "Not Found")
            return

        # ?denyua=<子串>：UA 命中就回 403，用来复现镜像站的 UA 拦截策略
        deny = params.get("denyua", [None])[0]
        if deny and deny in (self.headers.get("User-Agent") or ""):
            self.send_error(403, "Forbidden")
            return

        size = os.path.getsize(fs_path)
        range_header = self.headers.get("Range")
        # nolen 同时关掉 Range：真实的「流式、长度未知」服务器也不会支持分片
        if range_header and "norange" not in params and "nolen" not in params and size > 0:
            start, end = self._parse_range(range_header, size)
            if start is None:
                self.send_error(416, "Requested Range Not Satisfiable")
                return
            with open(fs_path, "rb") as fh:
                fh.seek(start)
                body = fh.read(end - start + 1)
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self._common_headers(params, len(body))
            self.end_headers()
            self._write_body(body, params)
            return

        with open(fs_path, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self._common_headers(params, None if "nolen" in params else len(body), size=size)
        self.end_headers()
        self._write_body(body, params)

    def _parse_range(self, header: str, size: int) -> tuple[int | None, int]:
        m = _RANGE_RE.match(header.strip())
        if not m:
            return None, 0
        raw_start, raw_end = m.group(1), m.group(2)
        if not raw_start and raw_end:        # bytes=-N 后缀形式
            start = max(0, size - int(raw_end))
            end = size - 1
        else:
            start = int(raw_start or 0)
            end = int(raw_end) if raw_end else size - 1
        if start >= size:
            return None, 0
        return start, min(end, size - 1)

    def _common_headers(self, params, length: int | None, size: int = 0) -> None:
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        try:
            stamp = int(os.path.getmtime(self.translate_path(self.path)))
        except OSError:
            stamp = 0
        self.send_header("ETag", f'"{size}-{stamp}"')
        if "cd" in params:
            self.send_header(
                "Content-Disposition",
                "attachment; filename=\"fallback.bin\"; "
                "filename*=UTF-8''%E6%B5%8B%E8%AF%95%E6%96%87%E4%BB%B6.bin")
        elif "cd2" in params:
            self.send_header("Content-Disposition", 'attachment; filename="plain.txt"')
        elif "cd3" in params:
            self.send_header("Content-Disposition",
                             'attachment; filename="=?utf-8?B?5rWL6K+V?=.bin"')
        if length is None:
            self.close_connection = True     # 无 Content-Length，只能靠关闭连接表示结束
        else:
            self.send_header("Content-Length", str(length))

    # ---------------------------------------------------------------- 写出

    def _write_body(self, body: bytes, params) -> None:
        try:
            if "flaky" in params and self._consume_flaky():
                cut = max(1, len(body) // 3)
                self._send(body[:cut])
                self.close_connection = True     # 少于声明的长度即断开，客户端应报错
                return

            if "slow" in params:
                try:
                    delay = float(params["slow"][0] or 20) / 1000.0
                except (TypeError, ValueError):
                    delay = 0.02
                for index in range(0, len(body), BLOCK):
                    self._send(body[index:index + BLOCK])
                    time.sleep(delay)
                return

            self._send(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            self.close_connection = True

    def _send(self, data: bytes) -> None:
        self.wfile.write(data)
        with RangeHandler._lock:
            RangeHandler.bytes_served += len(data)

    @classmethod
    def _consume_flaky(cls) -> bool:
        with cls._lock:
            if cls.flaky_remaining <= 0:
                return False
            cls.flaky_remaining -= 1
            return True


class QuietThreadingHTTPServer(http.server.ThreadingHTTPServer):
    """客户端被中途切断时会向 stderr 打一堆 traceback，这里静音。"""

    # socketserver 的默认 backlog 只有 5。多任务并发时（比如两个任务各 4 个分片
    # 线程）会瞬间发起 8 条以上连接，把队列挤爆，客户端直接连不上——表现为任务
    # 莫名其妙失败。这不是下载器的问题，是测试服务器自己的容量不够。
    request_queue_size = 128

    def handle_error(self, request, client_address):
        pass


def start_server(directory: str, port: int = 0):
    """启动后台 HTTP 服务器，返回 (httpd, thread)。"""
    handler = functools.partial(RangeHandler, directory=directory)
    httpd = QuietThreadingHTTPServer(("127.0.0.1", port), handler)
    httpd.daemon_threads = True
    thread = threading.Thread(target=httpd.serve_forever, name="range-server", daemon=True)
    thread.start()
    return httpd, thread


def reset_counters(flaky: int = 0) -> None:
    with RangeHandler._lock:
        RangeHandler.bytes_served = 0
        RangeHandler.flaky_remaining = flaky


def bytes_served() -> int:
    with RangeHandler._lock:
        return RangeHandler.bytes_served
