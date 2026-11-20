"""端到端测试:在回环口上启动真实的打印机与网关,用模拟终端走 TCP。

运行(项目根目录):
    .venv/bin/python -m unittest tests.e2e_test -v
"""
from __future__ import annotations

import asyncio
import contextlib
import socket
import time
import unittest

from app.config import GatewaySettings, PrinterSettings
from app.gateway import serve as serve_gateway
from app.printer import serve as serve_printer
from app.protocol import (
    build_block_stream,
    build_label_stream,
    build_reject_stream,
)
from app.terminal import upload_label


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class E2ETestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.printer_port = _free_port()
        self.gateway_port = _free_port()
        self.write_timeout = 0.8
        self.printer = await serve_printer(
            PrinterSettings(
                listen_host="127.0.0.1",
                listen_port=self.printer_port,
                grace_seconds=0.2,
            )
        )
        self.gateway = await serve_gateway(
            GatewaySettings(
                listen_host="127.0.0.1",
                listen_port=self.gateway_port,
                printer_host="127.0.0.1",
                printer_port=self.printer_port,
                buffer_bytes=16 * 1024,
                write_timeout=self.write_timeout,
                connect_timeout=2.0,
            )
        )

    async def asyncTearDown(self) -> None:
        self.gateway.close()
        self.printer.close()
        await self.gateway.wait_closed()
        await self.printer.wait_closed()

    async def test_accept_after_half_close(self) -> None:
        """正常流程:终端半关闭 -> 打印机读到 EOF -> 接单回执。"""
        payload = build_label_stream("lbl-1.lbl", 300_000)
        result = await upload_label(
            "127.0.0.1", self.gateway_port, payload, overall_timeout=10
        )
        self.assertTrue(result.connected)
        self.assertTrue(result.half_closed, "终端必须半关闭写端")
        self.assertEqual(result.bytes_sent, len(payload))
        statuses = [e.get("status") for e in result.events]
        self.assertIn("accepted", statuses)
        accepted = next(e for e in result.events if e["status"] == "accepted")
        self.assertEqual(accepted["bytes_received"], len(payload))
        self.assertIsNone(result.error)

    async def test_reject_delivered_while_write_blocked(self) -> None:
        """拒收优先:上传卡在写入,拒收原因仍由反向通道送回终端。"""
        reason = "paper jam / job refused"
        # 2MB 远超 16KB 缓冲 + 内核缓冲,打印机拒收后停止读取必然阻塞
        payload = build_reject_stream(reason, 2_000_000)
        result = await upload_label(
            "127.0.0.1",
            self.gateway_port,
            payload,
            overall_timeout=10,
            block_threshold_ms=50,
        )
        self.assertTrue(
            result.write_blocked and result.bytes_sent < len(payload),
            "终端应观测到写阻塞(数据未发完连接即被中断)",
        )
        statuses = [e.get("status") for e in result.events]
        self.assertIn("rejected", statuses)
        rejected = next(e for e in result.events if e["status"] == "rejected")
        self.assertEqual(rejected["reason"], reason)
        # 打印机停止读取,上传不可能完整结束;但拒收必须先到达
        self.assertLess(result.bytes_sent, len(payload))

    async def test_timeout_recycles_both_connections(self) -> None:
        """写阻塞超时:网关回收本作业两条连接,终端被中断且无回执。"""
        payload = build_block_stream(2_000_000)
        t0 = time.monotonic()
        result = await upload_label(
            "127.0.0.1", self.gateway_port, payload, overall_timeout=10
        )
        elapsed = time.monotonic() - t0
        self.assertEqual(result.events, [], "打印机不读数据,不可能产生回执")
        self.assertIsNotNone(result.error, "连接应被网关回收而中断")
        # 回收时限应贴近网关写超时,而不是等终端整体超时
        self.assertLess(elapsed, 4.0, f"回收不及时,实际耗时 {elapsed:.1f}s")
        self.assertGreaterEqual(elapsed, self.write_timeout * 0.8)

    async def test_other_jobs_unaffected_after_recycle(self) -> None:
        """作业隔离:一个作业被回收,后续作业与并发作业照常。"""
        bad = await upload_label(
            "127.0.0.1",
            self.gateway_port,
            build_block_stream(1_000_000),
            overall_timeout=8,
        )
        self.assertIsNotNone(bad.error)
        self.assertEqual(bad.events, [])

        good = await upload_label(
            "127.0.0.1",
            self.gateway_port,
            build_label_stream("lbl-ok.lbl", 200_000),
            overall_timeout=8,
        )
        statuses = [e.get("status") for e in good.events]
        self.assertIn("accepted", statuses)

        # 再做一个并发批次:坏作业与好作业同时进行
        results = await asyncio.gather(
            upload_label(
                "127.0.0.1", self.gateway_port,
                build_block_stream(1_000_000), overall_timeout=8,
            ),
            upload_label(
                "127.0.0.1", self.gateway_port,
                build_label_stream("lbl-concurrent.lbl", 150_000),
                overall_timeout=8,
            ),
        )
        self.assertEqual(results[0].events, [])
        self.assertIsNotNone(results[0].error)
        self.assertIn(
            "accepted", [e.get("status") for e in results[1].events]
        )

    async def test_half_close_is_propagated_not_full_close(self) -> None:
        """网关必须传递半关闭(SHUT_WR),反向仍能收到回执。"""
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", self.gateway_port
        )
        try:
            payload = build_label_stream("raw.lbl", 50_000)
            writer.write(payload)
            await writer.drain()
            writer.write_eof()  # 仅半关闭写端

            # 半关闭后本端读通道必须仍可用,并能读到打印机回执
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            self.assertIn(b'"accepted"', line)
            # 打印机随后也半关闭,读到 EOF
            tail = await asyncio.wait_for(reader.read(), timeout=5)
            self.assertEqual(tail, b"")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


if __name__ == "__main__":
    unittest.main(verbosity=2)
