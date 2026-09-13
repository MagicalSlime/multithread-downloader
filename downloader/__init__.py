"""多线程下载器：分片并发下载 + 断点续传 + 自动重试。

界面是网页（`web/` 目录，用系统自带的 WebView2 渲染），下载逻辑与界面完全解耦——
`core` / `manager` / `state` / `util` 都不依赖任何 UI 库。
"""

from .config import DEFAULT_UA, AppSettings, DownloadConfig
from .core import DownloadError, DownloadTask, RateLimiter, Snapshot, TaskState
from .manager import DownloadManager, TaskEntry

__version__ = "2.0.0"

__all__ = [
    "DownloadConfig",
    "AppSettings",
    "DownloadTask",
    "DownloadManager",
    "TaskEntry",
    "RateLimiter",
    "DownloadError",
    "Snapshot",
    "TaskState",
    "DEFAULT_UA",
    "__version__",
]
