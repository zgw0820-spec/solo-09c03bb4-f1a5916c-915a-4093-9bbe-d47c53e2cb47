"""取证试传端到端测试:真实 TCP 经网关取证端口,真实落盘到临时目录。

运行(项目根目录):
    .venv/bin/python -m unittest tests.forensics_test -v
"""
from __future__ import annotations

import asyncio
import os
import shutil
import socket
import tempfile
import unittest
from pathlib import Path

from app.config import ForensicSettings, GatewaySettings, PrinterSettings
from app.forensics import (
    STATUS_ABORTED,
    STATUS_COMPLETE,
    ForensicManager,
    check_downloadable,
    list_trials,
    load_meta,
    new_trial_id,
    register_pending,
    cancel_pending,
    trial_dir,
)
from app.gateway import serve as serve_gateway
from app.printer import serve as serve_printer
from app.protocol import build_block_stream, build_label_stream, build_reject_stream
from app.terminal import UploadResult, upload_label


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ForensicsTestCase(unittest.IsolatedAsyncioTestCase):
    """默认限额:缓存 256KB,记录总量 4MB。"""

    capture_buffer = 256 * 1024
    store_limit = 4 * 1024 * 1024

    async def asyncSetUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="forensics-test-"))
        self.printer_port = _free_port()
        self.gateway_port = _free_port()
        self.forensic_port = _free_port()
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
                write_timeout=0.8,
                connect_timeout=2.0,
            ),
            forensics=ForensicSettings(
                listen_host="127.0.0.1",
                listen_port=self.forensic_port,
                data_dir=str(self.tmp),
                buffer_bytes=self.capture_buffer,
                store_bytes=self.store_limit,
                pending_ttl=5.0,
            ),
        )

    async def asyncTearDown(self) -> None:
        self.gateway.close()
        self.printer.close()
        await self.gateway.wait_closed()
        await self.printer.wait_closed()
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def _wait_meta(self, trial_id: str, timeout: float = 5.0) -> dict | None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            meta = load_meta(self.tmp, trial_id)
            if meta is not None:
                return meta
            await asyncio.sleep(0.05)
        return None

    async def _run_trial(
        self, payload: bytes
    ) -> tuple[str, UploadResult, dict | None]:
        """登记 -> 真实 TCP 试传 -> 等落盘终态,与 API 的处理顺序一致。"""
        trial_id = new_trial_id()
        register_pending(self.tmp, trial_id)
        try:
            result = await upload_label(
                "127.0.0.1", self.forensic_port, payload, overall_timeout=10
            )
        finally:
            cancel_pending(self.tmp, trial_id)
        meta = await self._wait_meta(trial_id)
        return trial_id, result, meta

    # -- 用例 ---------------------------------------------------------------

    async def test_records_both_directions_raw_bytes(self) -> None:
        """两方向交给 TCP 发送的原始字节都被记录,结束方式标明为正常半关闭。"""
        payload = build_label_stream("fx-1.lbl", 100_000)
        trial_id, result, meta = await self._run_trial(payload)

        self.assertIn("accepted", [e.get("status") for e in result.events])
        self.assertIsNotNone(meta, "取证应落盘出终态")
        self.assertEqual(meta["status"], STATUS_COMPLETE)
        self.assertEqual(meta["trial_id"], trial_id)

        tdir = trial_dir(self.tmp, trial_id)
        uplink = (tdir / "uplink.bin").read_bytes()
        downlink = (tdir / "downlink.bin").read_bytes()
        # 上行:网关发给打印机的字节与终端提交的标签原始字节完全一致
        self.assertEqual(uplink, payload)
        # 下行:网关发给终端的字节即打印机回执
        self.assertIn(b'"accepted"', downlink)
        for direction in ("uplink", "downlink"):
            d = meta["directions"][direction]
            self.assertEqual(d["end_reason"], "eof", f"{direction} 应正常半关闭")
            self.assertTrue(d["clean_half_close"])
            self.assertEqual(d["bytes"], len(uplink if direction == "uplink"
                                             else downlink))
        ok, _reason = check_downloadable(self.tmp, trial_id)
        self.assertTrue(ok)

    async def test_reject_receipt_recorded_in_downlink(self) -> None:
        """提前拒收:下行记录里能拿到拒收原因,两方向仍正常半关闭。"""
        payload = build_reject_stream("ribbon empty", 50_000)
        _trial_id, result, meta = await self._run_trial(payload)

        self.assertIn("rejected", [e.get("status") for e in result.events])
        self.assertEqual(meta["status"], STATUS_COMPLETE)
        downlink = (trial_dir(self.tmp, meta["trial_id"])
                    / "downlink.bin").read_bytes()
        self.assertIn(b"rejected", downlink)
        self.assertIn("ribbon empty".encode(), downlink)

    async def test_normal_port_jobs_are_not_recorded(self) -> None:
        """普通作业(主端口)照常转发且不留存任何取证记录。"""
        payload = build_label_stream("normal.lbl", 60_000)
        result = await upload_label(
            "127.0.0.1", self.gateway_port, payload, overall_timeout=10
        )
        self.assertIn("accepted", [e.get("status") for e in result.events])
        self.assertEqual(list_trials(self.tmp), [], "普通作业不得留存记录")

    async def test_capture_buffer_limit_aborts_but_forwarding_continues(self) -> None:
        """缓存满:放弃取证并清理残件,但转发与回执不受影响,后续作业照常。"""
        payload = build_label_stream("too-big.lbl", 400_000)  # > 256KB 缓存上限
        trial_id, result, meta = await self._run_trial(payload)

        # 转发不等取证:标签仍然完整送达并接单
        self.assertIn("accepted", [e.get("status") for e in result.events])
        self.assertEqual(result.bytes_sent, len(payload))

        self.assertEqual(meta["status"], STATUS_ABORTED)
        self.assertIn("缓存超限", meta["abort_reason"])
        # 清理残件:目录里只剩 meta.json,没有部分写出的数据文件
        tdir = trial_dir(self.tmp, trial_id)
        self.assertEqual([p.name for p in tdir.iterdir()], ["meta.json"])
        ok, reason = check_downloadable(self.tmp, trial_id)
        self.assertFalse(ok)
        self.assertIn("缓存超限", reason)

        # 放弃取证后,普通作业仍能接单
        good = await upload_label(
            "127.0.0.1", self.gateway_port,
            build_label_stream("after.lbl", 40_000), overall_timeout=10,
        )
        self.assertIn("accepted", [e.get("status") for e in good.events])

    async def test_store_limit_aborts_and_normal_jobs_unaffected(self) -> None:
        """记录总量超限:第二笔取证被放弃,普通作业仍照常接单。"""
        # 把记录总量上限调到只能容纳一笔(通过重建网关实现)
        self.gateway.close()
        await self.gateway.wait_closed()
        self.gateway = await serve_gateway(
            GatewaySettings(
                listen_host="127.0.0.1",
                listen_port=self.gateway_port,
                printer_host="127.0.0.1",
                printer_port=self.printer_port,
                buffer_bytes=16 * 1024,
                write_timeout=0.8,
                connect_timeout=2.0,
            ),
            forensics=ForensicSettings(
                listen_host="127.0.0.1",
                listen_port=self.forensic_port,
                data_dir=str(self.tmp),
                buffer_bytes=self.capture_buffer,
                store_bytes=150 * 1024,  # 只够一笔 ~100KB 的记录
                pending_ttl=5.0,
            ),
        )

        first_id, _r1, meta1 = await self._run_trial(
            build_label_stream("first.lbl", 100_000)
        )
        self.assertEqual(meta1["status"], STATUS_COMPLETE)

        second_id, result2, meta2 = await self._run_trial(
            build_label_stream("second.lbl", 100_000)
        )
        # 第二笔转发本身成功(回执照达),但记录总量超限 -> 放弃取证
        self.assertIn("accepted", [e.get("status") for e in result2.events])
        self.assertEqual(meta2["status"], STATUS_ABORTED)
        self.assertIn("总量超限", meta2["abort_reason"])
        ok, reason = check_downloadable(self.tmp, second_id)
        self.assertFalse(ok)
        self.assertIn("总量超限", reason)
        # 第一笔不受影响,仍可下载
        ok1, _ = check_downloadable(self.tmp, first_id)
        self.assertTrue(ok1)

        # 超限之后普通作业仍能接单
        good = await upload_label(
            "127.0.0.1", self.gateway_port,
            build_label_stream("still-ok.lbl", 40_000), overall_timeout=10,
        )
        self.assertIn("accepted", [e.get("status") for e in good.events])

    async def test_persist_failure_aborts_and_gateway_survives(self) -> None:
        """写盘失败:放弃取证、不留残件,网关继续转发。"""
        store = self.tmp / "store"
        store.mkdir(parents=True, exist_ok=True)
        os.chmod(store, 0o500)  # 只读,落盘必失败
        try:
            trial_id, result, meta = await self._run_trial(
                build_label_stream("disk-fail.lbl", 50_000)
            )
            # 转发与回执不受写盘失败影响
            self.assertIn("accepted", [e.get("status") for e in result.events])
            # meta 也写不进去 -> 无记录残留
            self.assertIsNone(meta)
            self.assertEqual(list(trial_dir(self.tmp, trial_id).glob("*")), []
                             if trial_dir(self.tmp, trial_id).exists() else [])
        finally:
            os.chmod(store, 0o700)

        good = await upload_label(
            "127.0.0.1", self.gateway_port,
            build_label_stream("after-fail.lbl", 40_000), overall_timeout=10,
        )
        self.assertIn("accepted", [e.get("status") for e in good.events])

    async def test_forensic_port_rejects_without_pending(self) -> None:
        """没有 API 登记时,取证端口直接拒绝连接并说明原因。"""
        result = await upload_label(
            "127.0.0.1", self.forensic_port,
            build_label_stream("no-pending.lbl", 10_000), overall_timeout=5,
        )
        self.assertIn("error", [e.get("status") for e in result.events])
        self.assertIn("no pending forensic trial", result.raw_reply)

    async def test_job_watchdog_releases_stuck_trial(self) -> None:
        """打印机装死且数据全进内核缓冲时,看门狗在有限时间内收尾落盘。

        100KB 的 @BLOCK@ 载荷全部沉入对端内核缓冲(不触发写超时),
        没有看门狗的话作业会无限挂起、取证端口被卡死。
        """
        self.gateway.close()
        await self.gateway.wait_closed()
        self.gateway = await serve_gateway(
            GatewaySettings(
                listen_host="127.0.0.1",
                listen_port=self.gateway_port,
                printer_host="127.0.0.1",
                printer_port=self.printer_port,
                buffer_bytes=16 * 1024,
                write_timeout=0.8,
                connect_timeout=2.0,
            ),
            forensics=ForensicSettings(
                listen_host="127.0.0.1",
                listen_port=self.forensic_port,
                data_dir=str(self.tmp),
                buffer_bytes=self.capture_buffer,
                store_bytes=self.store_limit,
                pending_ttl=5.0,
                job_timeout=1.5,
            ),
        )
        trial_id, _result, meta = await self._run_trial(
            build_block_stream(100_000)
        )
        self.assertIsNotNone(meta, "看门狗应让卡死的取证作业收尾落盘")
        self.assertEqual(meta["status"], STATUS_COMPLETE)
        # 上行:终端数据全部交给 TCP 后正常半关闭;下行:打印机装死,
        # 被看门狗掐断 -> 异常结束
        self.assertEqual(meta["directions"]["uplink"]["end_reason"], "eof")
        self.assertTrue(meta["directions"]["uplink"]["clean_half_close"])
        self.assertEqual(meta["directions"]["downlink"]["end_reason"], "closed")
        self.assertFalse(meta["directions"]["downlink"]["clean_half_close"])
        ok, _ = check_downloadable(self.tmp, trial_id)
        self.assertTrue(ok, "记录本身完整(到异常结束为止),应可下载")

        # 取证端口未被卡死:下一笔取证照常
        _id2, result2, meta2 = await self._run_trial(
            build_label_stream("next.lbl", 40_000)
        )
        self.assertIn("accepted", [e.get("status") for e in result2.events])
        self.assertEqual(meta2["status"], STATUS_COMPLETE)

    async def test_write_timeout_recycle_finishes_job(self) -> None:
        """写超时回收后作业必须收尾落盘(回归:close 等缓冲排空曾挂死作业)。

        8MB @BLOCK@ 载荷远超全部缓冲,必然触发写超时回收;回收后
        取证记录要落盘、取证端口要释放,方向结束方式为 timeout/closed。
        """
        trial_id, result, meta = await self._run_trial(
            build_block_stream(8_000_000)
        )
        self.assertIsNotNone(result.error, "终端应观测到连接被回收")
        self.assertIsNotNone(meta, "写超时回收后取证必须落盘出终态")
        # 8MB 超过 256KB 取证缓存 -> 放弃取证,但残件清理与原因登记要完整
        self.assertEqual(meta["status"], STATUS_ABORTED)
        self.assertIn("缓存超限", meta["abort_reason"])
        self.assertEqual(meta["directions"]["uplink"]["end_reason"], "timeout")
        self.assertFalse(meta["directions"]["uplink"]["clean_half_close"])

        # 取证端口已释放:下一笔取证照常
        _id2, result2, meta2 = await self._run_trial(
            build_label_stream("after-recycle.lbl", 40_000)
        )
        self.assertIn("accepted", [e.get("status") for e in result2.events])
        self.assertEqual(meta2["status"], STATUS_COMPLETE)

    async def test_downloadable_gap_reporting(self) -> None:
        """缺口检测:记录被放弃或文件缺失时给出具体原因。"""
        # 不存在的试传号
        ok, reason = check_downloadable(self.tmp, "Tnonexistent")
        self.assertFalse(ok)
        self.assertIn("没有已落盘的取证记录", reason)

        # 完整记录被人为删掉一路 -> 报缺口
        trial_id, _r, meta = await self._run_trial(
            build_label_stream("gap.lbl", 50_000)
        )
        self.assertEqual(meta["status"], STATUS_COMPLETE)
        os.unlink(trial_dir(self.tmp, trial_id) / "uplink.bin")
        ok, reason = check_downloadable(self.tmp, trial_id)
        self.assertFalse(ok)
        self.assertIn("uplink.bin", reason)


class ForensicManagerUnitTest(unittest.TestCase):
    """管理器单并发与 pending 认领逻辑(不起服务,同步可测)。"""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="forensics-mgr-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.manager = ForensicManager(
            ForensicSettings(data_dir=str(self.tmp), pending_ttl=5.0)
        )

    def test_single_active_trial(self) -> None:
        """同一时刻只承载一笔:有进行中取证时新连接拿不到会话。"""
        register_pending(self.tmp, "Tone")
        register_pending(self.tmp, "Ttwo")
        first = self.manager.try_begin()
        self.assertIsNotNone(first)
        # 已有进行中取证,第二笔被拒绝(即使还有 pending 登记)
        self.assertIsNone(self.manager.try_begin())

    def test_no_pending_no_session(self) -> None:
        self.assertIsNone(self.manager.try_begin())

    def test_expired_pending_is_discarded(self) -> None:
        path = register_pending(self.tmp, "Told")
        old = path.stat().st_mtime - 60  # 人为做旧,超过 pending_ttl
        os.utime(path, (old, old))
        self.assertIsNone(self.manager.try_begin())
        self.assertFalse(path.exists(), "过期登记应被清理")


if __name__ == "__main__":
    unittest.main(verbosity=2)
