"""asyncio TCP 转发网关。

关键语义:
1. 两个方向各自运行独立的 pipe 协程:读端读到 EOF 后,先把该方向
   剩余数据全部 drain 给对端,再向对端半关闭写端(write_eof),反向
   转发不受影响,直到对端也 EOF 或关闭。
2. StreamReader 带容量限制(limit):缓冲达到高水位时传输层自动
   暂停读取(背压),对端写入随之阻塞,网关内存不随慢对端膨胀。
3. 每次 drain() 套超时:写阻塞超过配置时限即判定本作业卡死,
   回收(recycle)该作业的两条连接;其他作业由独立连接和任务承载,
   互不影响。
4. 可选的取证端口(forensics):该端口上的连接在转发时被旁路记录
   两个方向"交给 TCP 发送的原始字节",同一时刻只承载一笔;主端口
   的普通作业照常转发且不留存。记录是纯内存追加、落盘在后台,
   转发路径不被取证阻塞。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from typing import Callable

from .config import ForensicSettings, GatewaySettings
from .forensics import DOWNLINK, UPLINK, CaptureSession, ForensicManager

logger = logging.getLogger("gateway")


def _cap_sndbuf(writer: asyncio.StreamWriter, size: int) -> None:
    """固定一条连接的内核发送缓冲并关闭发送缓冲自动调节。

    Linux 上一旦显式 setsockopt(SO_SNDBUF),该 socket 的发送缓冲
    自动调节(autotuning)即关闭,慢对端导致窗口收紧时 drain() 会
    在约 size 字节处可预期地阻塞,而不是让内核缓冲扩张到数 MB。
    """
    sock: socket.socket | None = writer.get_extra_info("socket")
    if sock is None:
        return
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, size)
        snd = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        logger.debug("SNDBUF fixed to %s (requested %s)", snd, size)


async def _start_capped_server(
    client_connected_cb, host: str, port: int, limit: int
) -> asyncio.Server:
    """start_server 的等价物,但在监听 socket 上预设 SO_RCVBUF。

    accept 出来的新连接继承受限的接收缓冲,配合 StreamReader 的
    limit 共同限制"终端->网关"方向在网关侧的驻留数据。
    """
    loop = asyncio.get_running_loop()

    def factory() -> asyncio.StreamReaderProtocol:
        reader = asyncio.StreamReader(limit=limit)
        return asyncio.StreamReaderProtocol(reader, client_connected_cb, loop=loop)

    server = await loop.create_server(factory, host=host, port=port)
    for sock in server.sockets or []:
        with contextlib.suppress(OSError):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, limit)
            logger.debug(
                "listening socket %s RCVBUF=%s",
                sock.getsockname(),
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
            )
    return server


class Job:
    """一次终端<->打印机转发作业,封装两条连接的回收。"""

    def __init__(
        self,
        job_id: str,
        client_w: asyncio.StreamWriter,
        printer_w: asyncio.StreamWriter,
    ) -> None:
        self.job_id = job_id
        self._client_w = client_w
        self._printer_w = printer_w
        self._recycled = False

    def recycle(self, reason: str) -> None:
        """写阻塞超时等硬故障:立刻关闭两条连接的两个方向。

        用 abort() 而不是 close():发送缓冲里还有未冲完的数据时,
        close() 会等缓冲排空才触发 connection_lost,对端不读就会
        一直挂住;abort() 丢弃缓冲立即断连(对端看到 RST)。
        """
        if self._recycled:
            return
        self._recycled = True
        logger.warning("job=%s recycling both connections: %s", self.job_id, reason)
        for name, writer in (("client", self._client_w), ("printer", self._printer_w)):
            if not writer.is_closing():
                writer.close()
            with contextlib.suppress(OSError, RuntimeError):
                writer.transport.abort()
        # close/abort 是异步生效的;失败清理交由连接关闭回调统一处理

    @property
    def was_recycled(self) -> bool:
        return self._recycled

    def finish(self) -> None:
        """正常结束:关闭仍处于半开状态的连接。"""
        for name, writer in (("client", self._client_w), ("printer", self._printer_w)):
            if not writer.is_closing():
                with contextlib.suppress(OSError, RuntimeError):
                    writer.close()


async def _pump(
    name: str,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    job: Job,
    write_timeout: float,
    on_write: Callable[[bytes], None] | None = None,
) -> str:
    """单向转发。返回退出原因:eof / error / timeout / closed。

    - eof: 读端半关闭,缓冲已全部转发并向对端传递了半关闭
    - timeout: 对端写阻塞超时,已请求回收
    - error/closed: 连接异常或被回收

    on_write: 非空时,每次把数据交给 TCP 发送(writer.write)后同步
    回调一次——取证记录的就是网关实际交给 TCP 发送的原始字节。
    回调必须是纯内存操作,转发路径不被取证阻塞。
    """
    peer = writer.get_extra_info("peername")
    try:
        while True:
            data = await reader.read(65536)
            if not data:  # 读端半关闭或完全关闭
                break
            writer.write(data)
            if on_write is not None:
                on_write(data)
            try:
                # drain 在传输层缓冲低于低水位时立即返回;
                # 对端长时间不读时这里才会真正等待
                await asyncio.wait_for(writer.drain(), timeout=write_timeout)
            except asyncio.TimeoutError:
                job.recycle(
                    f"{name} write blocked over {write_timeout:.1f}s (peer={peer})"
                )
                return "timeout"
            except (ConnectionError, RuntimeError, OSError) as exc:
                logger.warning("job=%s %s write failed: %r", job.job_id, name, exc)
                return "error"
        if job.was_recycled:
            # EOF 是回收 abort 出来的,不是对端真实的半关闭
            return "closed"
        # 关键:先把本方向数据转完(read 循环里每次都 drain 过),
        # 再向对端传递半关闭,反向 pipe 继续工作
        if not writer.is_closing():
            with contextlib.suppress(NotImplementedError, OSError, RuntimeError):
                writer.write_eof()
            try:
                await asyncio.wait_for(writer.drain(), timeout=write_timeout)
            except asyncio.TimeoutError:
                job.recycle(
                    f"{name} half-close drain blocked over {write_timeout:.1f}s"
                )
                return "timeout"
            except (ConnectionError, RuntimeError, OSError) as exc:
                logger.warning("job=%s %s half-close drain failed: %r",
                               job.job_id, name, exc)
                return "error"
        logger.info("job=%s %s half-close propagated", job.job_id, name)
        return "eof"
    except (ConnectionError, OSError) as exc:
        logger.warning("job=%s %s read failed: %r", job.job_id, name, exc)
        return "error"


def _pump_reason(task: asyncio.Task) -> str:
    """把 pump 任务的终态折算成取证记录用的结束原因。"""
    if task.cancelled() or not task.done():
        return "closed"  # 被看门狗/回收掐断:异常结束
    if task.exception() is not None:
        return "error"
    return task.result()  # _pump 自报的 eof / timeout / error / closed


async def _relay_job(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    settings: GatewaySettings,
    job_id: str,
    capture: CaptureSession | None = None,
) -> None:
    """建立到打印机的连接并双向转发。

    capture 非空时(取证试传),两个方向交给 TCP 发送的原始字节都会
    同步追加到会话缓存,各方向的结束原因(eof=正常半关闭,其余=异常
    结束)在收尾时登记;普通作业传 None,转发路径完全不留存。
    """
    client_addr = writer.get_extra_info("peername")
    logger.info("job=%s terminal connected addr=%s", job_id, client_addr)

    try:
        printer_r, printer_w = await asyncio.wait_for(
            asyncio.open_connection(
                settings.printer_host,
                settings.printer_port,
                limit=settings.buffer_bytes,
            ),
            timeout=settings.connect_timeout,
        )
    except (OSError, asyncio.TimeoutError) as exc:
        logger.error("job=%s cannot reach printer: %r", job_id, exc)
        error_line = b'{"status":"error","reason":"gateway: printer unreachable"}\n'
        with contextlib.suppress(Exception):
            writer.write(error_line)
            if capture is not None:
                capture.record_downlink(error_line)
            await writer.drain()
        writer.close()
        with contextlib.suppress(OSError, ConnectionError):
            await writer.wait_closed()
        if capture is not None:
            capture.set_end(UPLINK, "error")
            capture.set_end(DOWNLINK, "error")
        return

    job = Job(job_id, client_w=writer, printer_w=printer_w)
    # 网关两个发送方向都固定 SNDBUF:打印机慢/拒收停止读取时,
    # terminal->printer 方向在约 buffer_bytes 处可预期阻塞;
    # 反向回执通道同理。接收侧缓冲由监听 socket 的 RCVBUF 与
    # StreamReader(limit) 双重限定。
    _cap_sndbuf(printer_w, settings.buffer_bytes)
    _cap_sndbuf(writer, settings.buffer_bytes)

    # 取证:记录网关 -> 打印机(uplink)与网关 -> 终端(downlink)
    # 两个方向交给 TCP 发送的原始字节
    on_uplink = capture.record_uplink if capture is not None else None
    on_downlink = capture.record_downlink if capture is not None else None

    forward = asyncio.create_task(
        _pump("terminal->printer", reader, printer_w, job, settings.write_timeout,
              on_write=on_uplink),
        name=f"{job_id}-fwd",
    )
    reverse = asyncio.create_task(
        _pump("printer->terminal", printer_r, writer, job, settings.write_timeout,
              on_write=on_downlink),
        name=f"{job_id}-rev",
    )
    tasks = ((forward, UPLINK), (reverse, DOWNLINK))

    try:
        # 两个方向各自跑到 EOF/出错/超时。
        # - 正常半关闭:正向 EOF 后只传 SHUT_WR,反向 pipe 继续,直到
        #   打印机回执发完也 EOF,两边自然结束;
        # - 提前拒收:反向先结束(回执+EOF),正向仍在写,等其报错或被关;
        # - 写超时:_pump 内部调用 job.recycle() 同时掐断两条连接,
        #   另一方向随之结束。
        await asyncio.gather(*(t for t, _ in tasks), return_exceptions=True)
    finally:
        for task, _direction in tasks:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        # 登记各方向结束方式(正常半关闭 or 异常结束);被看门狗取消时
        # 也会走到这里,未跑完的方向记为 closed
        if capture is not None:
            for task, direction in tasks:
                capture.set_end(direction, _pump_reason(task))
        # 兜底回收(正常路径下连接多半已因对端 EOF 关闭)
        job.finish()
        for w in (writer, printer_w):
            # close() 在发送缓冲未排空时会等缓冲冲完才触发
            # connection_lost;对端不读时不能无限等,超时后 abort 强杀
            try:
                await asyncio.wait_for(w.wait_closed(), timeout=1.0)
            except asyncio.TimeoutError:
                logger.warning("job=%s close flush stuck, aborting transport",
                               job_id)
                with contextlib.suppress(OSError, RuntimeError):
                    w.transport.abort()
            except (OSError, RuntimeError, ConnectionError):
                pass
        logger.info("job=%s closed", job_id)


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    settings: GatewaySettings,
    job_seq: Callable[[], str],
) -> None:
    """主端口普通作业:照常转发,不做任何留存。"""
    await _relay_job(reader, writer, settings, job_seq(), capture=None)


async def _handle_forensic(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    settings: GatewaySettings,
    job_seq: Callable[[], str],
    manager: ForensicManager,
    job_timeout: float,
) -> None:
    """取证端口作业:同一时刻只承载一笔,转发时旁路记录双方向字节。"""
    job_id = job_seq()
    session = manager.try_begin()
    if session is None:
        # 没有 API 登记,或已有一笔取证进行中:直接拒绝
        logger.warning("job=%s forensic connection rejected (no pending "
                       "registration or trial already active)", job_id)
        with contextlib.suppress(Exception):
            writer.write(
                b'{"status":"error","reason":"gateway: no pending forensic '
                b'trial or another trial is active"}\n'
            )
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        return
    logger.info("job=%s forensic trial=%s started", job_id, session.trial_id)
    try:
        # 整体看门狗:打印机不应答且数据量不足以触发写超时回收时,
        # 作业不能无限挂着——取证端口同一时刻只有一笔,必须在有限
        # 时间内收尾并落盘(未跑完的方向记为 closed)
        await asyncio.wait_for(
            _relay_job(reader, writer, settings, job_id, capture=session),
            timeout=job_timeout,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "job=%s forensic trial=%s recycled by job watchdog (%.0fs)",
            job_id, session.trial_id, job_timeout,
        )
    finally:
        # 落盘是后台任务,不阻塞连接收尾与下一笔取证的开始
        manager.finish(session, job_id)


class GatewayServer:
    """主转发端口 + 可选取证端口的组合体,对齐 asyncio.Server 用法。"""

    def __init__(
        self,
        main: asyncio.Server,
        forensic: asyncio.Server | None = None,
        manager: ForensicManager | None = None,
    ) -> None:
        self._servers = [s for s in (main, forensic) if s is not None]
        self.manager = manager

    def close(self) -> None:
        for server in self._servers:
            server.close()

    async def wait_closed(self) -> None:
        for server in self._servers:
            await server.wait_closed()
        if self.manager is not None:
            # 停机前等在途取证落盘,避免留下残件
            await self.manager.wait_persisted()

    async def serve_forever(self) -> None:
        await asyncio.gather(*(s.serve_forever() for s in self._servers))


async def serve(
    settings: GatewaySettings | None = None,
    forensics: ForensicSettings | None = None,
) -> GatewayServer:
    """启动网关服务。forensics 非空时额外监听取证端口。"""
    settings = settings or GatewaySettings.from_env()
    seq = 0
    # 强引用所有在途作业任务。StreamReaderProtocol 在 connection_lost
    # 时会把自己持有的 _task 置空,若没有别的引用,正在收尾的
    # _handle_client 可能被 GC 提前销毁("Task was destroyed but pending")。
    in_flight: set[asyncio.Task] = set()

    def next_id() -> str:
        nonlocal seq
        seq += 1
        return f"J{seq:04d}"

    def spawn(coro, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)

    def on_connect(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        spawn(_handle_client(r, w, settings, next_id), "gateway-job")

    main_server = await _start_capped_server(
        on_connect,
        host=settings.listen_host,
        port=settings.listen_port,
        limit=settings.buffer_bytes,
    )
    addrs = ", ".join(str(sock.getsockname()) for sock in main_server.sockets or [])
    logger.info(
        "gateway listening on %s -> printer %s:%s (buffer=%s bytes, "
        "write-timeout=%.1fs)",
        addrs,
        settings.printer_host,
        settings.printer_port,
        settings.buffer_bytes,
        settings.write_timeout,
    )

    forensic_server: asyncio.Server | None = None
    manager: ForensicManager | None = None
    if forensics is not None:
        manager = ForensicManager(forensics)

        def on_forensic(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            spawn(
                _handle_forensic(
                    r, w, settings, next_id, manager, forensics.job_timeout
                ),
                "gateway-forensic",
            )

        forensic_server = await _start_capped_server(
            on_forensic,
            host=forensics.listen_host,
            port=forensics.listen_port,
            limit=settings.buffer_bytes,
        )
        faddrs = ", ".join(
            str(sock.getsockname()) for sock in forensic_server.sockets or []
        )
        logger.info(
            "gateway forensic port on %s (single trial at a time, "
            "capture<=%d bytes, store<=%d bytes, job-timeout=%.0fs, dir=%s)",
            faddrs,
            forensics.buffer_bytes,
            forensics.store_bytes,
            forensics.job_timeout,
            forensics.data_dir,
        )

    return GatewayServer(main_server, forensic_server, manager)


async def _amain() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    server = await serve(forensics=ForensicSettings.from_env())
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
