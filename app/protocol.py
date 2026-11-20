"""终端 -> 打印机标签流中的控制标记与 JSON 行协议。

模拟打印机只检查数据流开头的标记,真实标签数据对网关完全透明,
网关不解析载荷,只做字节转发。
"""
from __future__ import annotations

import json
from typing import Any

# 流首行带该标记 => 打印机立即拒收,后面跟人类可读原因
MARKER_REJECT = b"@REJECT@"
# 流首行带该标记 => 打印机停止读取,模拟上传写入卡死
MARKER_BLOCK = b"@BLOCK@"


def make_event(status: str, **fields: Any) -> bytes:
    """构造打印机回执事件:单行 JSON + LF。"""
    payload = {"status": status, **fields}
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")


def parse_event(line: str) -> dict[str, Any] | None:
    try:
        value = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def build_reject_stream(reason: str, tail_bytes: int) -> bytes:
    """构造会被打印机提前拒收的标签流。

    标记放在最前面,后面追加足量数据,使终端在打印机停止读取后
    必然遭遇写阻塞(填满网关/内核缓冲)。
    """
    head = MARKER_REJECT + b" " + reason.encode("utf-8") + b"\n"
    return head + b"X" * max(0, tail_bytes - len(head))


def build_block_stream(total_bytes: int) -> bytes:
    """构造会让打印机停止读取的标签流(触发网关写超时回收)。"""
    head = MARKER_BLOCK + b" printer-buffer-full\n"
    return head + b"Y" * max(0, total_bytes - len(head))


def build_label_stream(filename: str, content_size: int) -> bytes:
    """构造普通标签流,末尾由终端半关闭写端告知打印机数据结束。"""
    header = f"# {filename}\n".encode("utf-8")
    body = (b"^XA^FO50,50^FDHELLO-LABEL^FS^XZ\n" * 16)
    if len(body) < content_size:
        body += b"L" * (content_size - len(body))
    return header + body[:content_size]
