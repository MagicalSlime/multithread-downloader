"""下载配置与应用设置。"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field

# 刻意不用浏览器标识。国内镜像站（如清华 TUNA）会拦截浏览器 UA 和 python-requests
# 的默认 UA，只放行能自我说明的客户端；实测 "Mozilla/5.0 (compatible; …)" 这类
# 传统非浏览器标识可以通过，同时又不至于被需要 Mozilla 前缀的站点拒绝。
# 遇到要求真实浏览器 UA 的站点，在界面「高级设置」里自行填写即可。
DEFAULT_UA = "Mozilla/5.0 (compatible; MultiThreadDownloader/1.0)"

MAX_THREADS = 32


@dataclass
class DownloadConfig:
    url: str
    save_path: str | None = None       # None / 空 → 探测后由 Content-Disposition 或 URL 推导
    threads: int = 8
    chunk_read: int = 64 * 1024        # 每次读取的块大小，决定暂停的响应速度
    min_chunk_size: int = 1 << 20      # 分片不小于 1 MiB，否则自动减少线程数
    max_retries: int = 5
    connect_timeout: float = 10.0
    read_timeout: float = 20.0         # 同时是「暂停/取消」的延迟上界
    resume: bool = True
    verify_tls: bool = True
    user_agent: str = DEFAULT_UA
    referer: str = ""
    cookie: str = ""
    extra_headers: dict[str, str] = field(default_factory=dict)
    state_save_interval: float = 2.0

    def __post_init__(self) -> None:
        self.url = (self.url or "").strip()
        self.threads = max(1, min(int(self.threads), MAX_THREADS))
        self.max_retries = max(0, int(self.max_retries))
        self.chunk_read = max(4096, min(int(self.chunk_read), 1 << 22))

    def build_headers(self) -> dict[str, str]:
        """组装请求头。空字段不发送。"""
        headers = {
            # 必须显式要求不压缩：否则 gzip 会让进度基于解压后的字节数而超过 100%
            "Accept-Encoding": "identity",
            "Accept": "*/*",
        }
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        if self.referer:
            headers["Referer"] = self.referer
        if self.cookie:
            headers["Cookie"] = self.cookie
        headers.update(self.extra_headers)
        return headers


# --------------------------------------------------------------------------- 应用设置

def settings_path() -> str:
    """设置文件放在用户目录下，跟着用户走，不污染程序目录。"""
    base = os.environ.get("APPDATA") or os.path.expanduser("~")
    return os.path.join(base, "MultiThreadDownloader", "settings.json")


@dataclass
class AppSettings:
    """界面上的偏好设置，退出时保存、启动时恢复。

    注意：**Cookie 刻意不持久化**。它是登录凭据，明文落盘风险太大，
    每次启动都需要重新填写。
    """

    save_dir: str = ""
    threads: int = 8
    resume: bool = True
    verify_tls: bool = True
    max_retries: int = 5
    user_agent: str = DEFAULT_UA
    referer: str = ""
    max_concurrent: int = 3
    rate_limit: int = 0            # 全局限速，字节/秒；0 表示不限
    autoscroll_log: bool = True
    geometry: str = ""

    @classmethod
    def load(cls, path: str | None = None) -> "AppSettings":
        target = path or settings_path()
        try:
            with open(target, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, ValueError):
            return cls()
        if not isinstance(raw, dict):
            return cls()
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in raw.items() if k in known})

    def save(self, path: str | None = None) -> None:
        target = path or settings_path()
        try:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            tmp = target + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(asdict(self), fh, ensure_ascii=False, indent=1)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)     # 原子替换，避免退出时写坏
        except OSError:
            pass                        # 设置存不下来不值得打扰用户

    def to_config(self, url: str, save_path: str | None, cookie: str = "") -> DownloadConfig:
        return DownloadConfig(
            url=url, save_path=save_path, threads=self.threads,
            max_retries=self.max_retries, resume=self.resume,
            verify_tls=self.verify_tls, user_agent=self.user_agent,
            referer=self.referer, cookie=cookie)
