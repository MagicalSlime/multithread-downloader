#!/usr/bin/env python3
"""多线程下载器入口。

用法：  python main.py
"""

import sys

from downloader.webui import main

if __name__ == "__main__":
    sys.exit(main())
