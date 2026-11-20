"""模拟标签打印机。

行为规则:
- 正常接单:读到 EOF(终端半关闭写端)后才回 accepted 回执,
  回执写完也半关闭写端。
- 提前拒收:流首行带 @REJECT@ 标记时,立即回 rejected 回执并停止
  读取上传数据(模拟打印机拒收后不再排空接收缓冲),再半关闭写端。
- 停止读取:流首行带 @BLOCK@ 标记时彻底不 read,用来制造上传写
  阻塞,触发网关的写超时回收。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from .config import PrinterSettings
from .protocol import MARKER_BLOCK, MARKER_REJECT, make_event

logger = logging.getLogger("printer")


async def _drain_quietly(writer: asyncio.StreamWriter) -> None:
    with contextlib.suppress(ConnectionError, OSError, RuntimeError):
        await writer.drain()


async def _half_close(
    writer: asyncio.StreamWriter, grace: float
) -> None:
    """半关闭写端并宽限片刻,让网关把回执转发回终端后再完全关闭。"""
    if not writer.is_closing():
        with contextlib.suppress(NotImplementedError, OSError, RuntimeError):
            writer.write_eof()
        await _drain_quietly(writer)
    # 等反向 pipe 有时间把这一方向的回执送出去
    await asyncio.sleep(grace)
    if not writer.is_closing():
        with contextlib.suppress(OSError, RuntimeError):
            writer.close()


async def _handle(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    settings: PrinterSettings,
) -> None:
    addr = writer.get_extra_info("peername")
    started = time.monotonic()
    logger.info("printer: connection from %s", addr)
    total = 0
    try:
        first = await reader.readline()
        if first.startswith(MARKER_BLOCK):
            reason = first[len(MARKER_BLOCK):].strip().decode(
                "utf-8", "replace"
            ) or "printer buffer full"
            logger.warning("printer: BLOCK marker, stop reading (%s)", reason)
            # 完全不读取,直到对端/网关把连接回收
            with contextlib.suppress(asyncio.CancelledError):
                while not writer.is_closing():
                    await asyncio.sleep(0.5)
            return

        if first.startswith(MARKER_REJECT):
            reason = first[len(MARKER_REJECT):].strip().decode(
                "utf-8", "replace"
            ) or "unsupported label format"
            logger.warning("printer: REJECT marker, reason=%r", reason)
            # 关键:即使上传还卡在写入,拒收原因也要先送回终端
            writer.write(make_event(
                "rejected", reason=reason, after_bytes=total,
            ))
            await _drain_quietly(writer)
            await _half_close(writer, settings.grace_seconds)
            return

        total += len(first)
        # 正常接单:必须读到 EOF 才确认
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            total += len(chunk)

        elapsed = time.monotonic() - started
        logger.info(
            "printer: EOF received, accepting job bytes=%d %.3fs", total, elapsed
        )
        writer.write(make_event(
            "accepted",
            bytes_received=total,
            elapsed_ms=round(elapsed * 1000, 1),
            message="label spooled, print order confirmed",
        ))
        await _drain_quietly(writer)
        await _half_close(writer, settings.grace_seconds)
    except (ConnectionError, OSError) as exc:
        logger.info("printer: connection lost: %r", exc)
    finally:
        if not writer.is_closing():
            with contextlib.suppress(Exception):
                writer.close()
        logger.info("printer: connection closed bytes=%d", total)


async def serve(settings: PrinterSettings | None = None) -> asyncio.Server:
    settings = settings or PrinterSettings.from_env()
    server = await asyncio.start_server(
        lambda r, w: _handle(r, w, settings),
        host=settings.listen_host,
        port=settings.listen_port,
    )
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets or [])
    logger.info("mock printer listening on %s", addrs)
    return server


async def _amain() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    server = await serve()
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        server.close()
        await server.wait_closed()


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_amain())


if __name__ == "__main__":
    main()
