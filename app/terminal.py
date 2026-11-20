"""模拟条码终端的上传客户端。

要点:
- 上传协程逐块写入,写完后 write_eof() 半关闭写端;
- 接收协程并发读取回执,因此即便上传协程卡在 drain() 上,
  打印机提前拒收的原因仍能被收到并记录;
- 每次 drain 耗时超过阈值即记录一次 write_blocked,供演示验证
  "上传卡在写入时仍收到拒收"。
"""
from __future__ import annotations

import asyncio
import contextlib
import socket
import time
from dataclasses import dataclass, field

from .protocol import parse_event


@dataclass
class UploadResult:
    connected: bool = False
    half_closed: bool = False
    bytes_sent: int = 0
    write_blocked: bool = False
    max_drain_ms: float = 0.0
    events: list[dict] = field(default_factory=list)
    raw_reply: str = ""
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "connected": self.connected,
            "half_closed": self.half_closed,
            "bytes_sent": self.bytes_sent,
            "write_blocked": self.write_blocked,
            "max_drain_ms": round(self.max_drain_ms, 1),
            "events": self.events,
            "raw_reply": self.raw_reply,
            "error": self.error,
        }


async def upload_label(
    host: str,
    port: int,
    payload: bytes,
    *,
    chunk_size: int = 8192,
    overall_timeout: float = 20.0,
    block_threshold_ms: float = 200.0,
    connect_timeout: float = 5.0,
    sndbuf: int | None = 64 * 1024,
) -> UploadResult:
    """通过网关向打印机上传一个标签流并等待回执。

    sndbuf: 固定终端自身的内核发送缓冲(显式 setsockopt 会关闭
    Linux 发送缓冲自动调节),模拟资源受限的真实条码终端,使写阻塞
    可被观测;传 None 则保持系统默认。
    """
    result = UploadResult()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=connect_timeout
        )
    except (OSError, asyncio.TimeoutError) as exc:
        result.error = f"connect failed: {exc!r}"
        return result

    if sndbuf is not None:
        sock: socket.socket | None = writer.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)

    result.connected = True
    reply_lines: list[str] = []

    async def read_replies() -> None:
        # 并发接收:写协程阻塞不影响这里读取拒收原因
        while True:
            line = await reader.readline()
            if not line:
                return
            text = line.decode("utf-8", "replace").strip()
            reply_lines.append(text)
            event = parse_event(text)
            if event is not None:
                result.events.append(event)

    reader_task = asyncio.create_task(read_replies())
    writer_task: asyncio.Task | None = None

    async def send_all() -> None:
        try:
            for offset in range(0, len(payload), chunk_size):
                chunk = payload[offset : offset + chunk_size]
                writer.write(chunk)
                t0 = time.monotonic()
                await writer.drain()
                dt_ms = (time.monotonic() - t0) * 1000
                result.max_drain_ms = max(result.max_drain_ms, dt_ms)
                if dt_ms >= block_threshold_ms:
                    result.write_blocked = True
                result.bytes_sent += len(chunk)
            # 终端发完:半关闭写端,保留读端等待回执
            writer.write_eof()
            result.half_closed = True
        except (ConnectionError, OSError, RuntimeError) as exc:
            # 还有数据没送出去连接就断了:对端(经网关)停止排空,
            # 写路径被背压顶住后回收——同样证明上传曾卡在写入
            if result.bytes_sent < len(payload):
                result.write_blocked = True
            result.error = f"upload interrupted: {exc!r}"

    try:
        writer_task = asyncio.create_task(send_all())
        # 写协程可能被连接回收打断;读协程在对端 EOF 后结束
        await asyncio.wait_for(
            asyncio.gather(writer_task, reader_task, return_exceptions=True),
            timeout=overall_timeout,
        )
    except asyncio.TimeoutError:
        result.error = f"overall timeout after {overall_timeout:.0f}s"
    finally:
        for task in (writer_task, reader_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        with contextlib.suppress(Exception):
            writer.close()
            await writer.wait_closed()

    result.raw_reply = "\n".join(reply_lines)
    if result.error is None and not result.events:
        result.error = "connection closed before any reply event"
    return result
