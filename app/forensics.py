"""取证试传记录:单并发、双方向原始字节、限额与落盘。

值班员排查"标签打成乱码"时,需要拿到网关实际转发的字节交给设备
维护方。本模块在网关转发路径上旁路记录两个方向交给 TCP 发送的
原始字节,并在作业结束后落盘,供 API 按试传号提供下载。

进程分工(网关与 API 通过共享目录 data_dir 协作,Docker 里挂同一卷):

- API 进程: 登记 incoming/<trial_id>.json -> 向网关取证端口发起真实
  TCP 试传 -> 轮询 store/<trial_id>/meta.json 的终态 -> 提供下载。
- 网关进程: 取证端口接受连接时认领最新的 pending 登记 -> 转发时
  record() 两个方向的字节(纯内存追加,转发不等取证) -> 作业结束
  finish() 异步落盘。

目录布局(data_dir):

  incoming/<trial_id>.json     API 登记的待取证试传(网关认领即删)
  store/<trial_id>/uplink.bin    网关发给打印机的原始字节(终端->打印机)
  store/<trial_id>/downlink.bin  网关发给终端的原始字节(打印机->终端)
  store/<trial_id>/meta.json     试传状态与各方向结束方式

放弃取证(缓存满 / 记录总量超限 / 写盘失败)时清理残件,只留下带
abort_reason 的 meta.json;下载接口据此返回缺口原因。
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import shutil
import time
import uuid
from pathlib import Path

from .config import ForensicSettings

logger = logging.getLogger("forensics")

# 两个记录方向:网关 -> 打印机(uplink)与网关 -> 终端(downlink)
UPLINK = "uplink"
DOWNLINK = "downlink"
DIRECTIONS = (UPLINK, DOWNLINK)

STATUS_COMPLETE = "complete"  # 两路记录完整落盘,可下载
STATUS_ABORTED = "aborted"    # 取证被放弃(缓存满/超限/写盘失败),有缺口

# _pump 的退出原因:eof 为正常半关闭,其余一律视为异常结束
END_HALF_CLEAN = "eof"


def new_trial_id() -> str:
    """生成试传号,如 T3f9a1c2e7b48。"""
    return "T" + uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# 共享目录小工具(网关与 API 两个进程都会用到)
# ---------------------------------------------------------------------------

def incoming_dir(data_dir: Path) -> Path:
    return data_dir / "incoming"


def store_dir(data_dir: Path) -> Path:
    return data_dir / "store"


def trial_dir(data_dir: Path, trial_id: str) -> Path:
    return store_dir(data_dir) / trial_id


def register_pending(data_dir: Path, trial_id: str) -> Path:
    """API 侧:登记一笔待取证试传,返回登记文件路径。"""
    inc = incoming_dir(data_dir)
    inc.mkdir(parents=True, exist_ok=True)
    path = inc / f"{trial_id}.json"
    path.write_text(
        json.dumps({"trial_id": trial_id, "created_at": time.time()}),
        encoding="utf-8",
    )
    return path


def cancel_pending(data_dir: Path, trial_id: str) -> None:
    """API 侧:试传结束(或失败)后清理自己登记的文件;已被网关认领则无事。"""
    with contextlib.suppress(OSError):
        (incoming_dir(data_dir) / f"{trial_id}.json").unlink()


def load_meta(data_dir: Path, trial_id: str) -> dict | None:
    """读取某试传的 meta.json;不存在或损坏返回 None。"""
    try:
        return json.loads(
            (trial_dir(data_dir, trial_id) / "meta.json").read_text(
                encoding="utf-8"
            )
        )
    except (OSError, json.JSONDecodeError):
        return None


def list_trials(data_dir: Path) -> list[dict]:
    """列出全部已落盘试传的 meta,按开始时间倒序。"""
    metas: list[dict] = []
    root = store_dir(data_dir)
    with contextlib.suppress(OSError):
        for child in root.iterdir():
            if child.is_dir():
                meta = load_meta(data_dir, child.name)
                if meta is not None:
                    metas.append(meta)
    metas.sort(key=lambda m: m.get("started_at", 0), reverse=True)
    return metas


def store_usage_bytes(data_dir: Path) -> int:
    """store/ 下两路原始字节记录(*.bin)的总量,用于记录总量上限判断。"""
    total = 0
    root = store_dir(data_dir)
    with contextlib.suppress(OSError):
        for child in root.iterdir():
            if not child.is_dir():
                continue
            for direction in DIRECTIONS:
                with contextlib.suppress(OSError):
                    total += (child / f"{direction}.bin").stat().st_size
    return total


def check_downloadable(data_dir: Path, trial_id: str) -> tuple[bool, str]:
    """检查某试传是否可下载:两路记录都完整落盘才放行。

    返回 (可否下载, 原因)。任何缺口(未落盘、被放弃、文件缺失或字节数
    与 meta 不符)都给出具体原因。
    """
    meta = load_meta(data_dir, trial_id)
    if meta is None:
        return False, f"试传号 {trial_id} 没有已落盘的取证记录(不存在或已清理)"
    if meta.get("status") != STATUS_COMPLETE:
        reason = meta.get("abort_reason") or "取证未完成落盘"
        return False, f"取证已放弃,记录有缺口: {reason}"
    tdir = trial_dir(data_dir, trial_id)
    directions = meta.get("directions") or {}
    for direction in DIRECTIONS:
        path = tdir / f"{direction}.bin"
        expected = (directions.get(direction) or {}).get("bytes")
        if expected is None:
            return False, f"取证记录有缺口: meta.json 缺少 {direction} 登记"
        try:
            actual = path.stat().st_size
        except OSError:
            return False, f"取证记录有缺口: 缺少 {direction}.bin 数据文件"
        if actual != expected:
            return False, (
                f"取证记录有缺口: {direction}.bin 大小 {actual} 与登记 "
                f"{expected} 不符"
            )
    return True, "ok"


# ---------------------------------------------------------------------------
# 网关侧:进行中的取证会话
# ---------------------------------------------------------------------------

class CaptureSession:
    """一笔进行中的取证:两个方向的内存缓存与结束方式。

    record() 只做长度检查与内存追加,绝不执行 IO——转发路径不被取证
    拖慢;超过缓存上限即放弃本笔取证并清空已缓存内容(清理残件),
    此后 record() 变为空操作,转发照常。
    """

    def __init__(self, trial_id: str, buffer_limit: int) -> None:
        self.trial_id = trial_id
        self.buffer_limit = buffer_limit
        self.buffers: dict[str, bytearray] = {
            UPLINK: bytearray(),
            DOWNLINK: bytearray(),
        }
        # 各方向结束方式:eof=正常半关闭;timeout/error/closed=异常结束
        self.end_reasons: dict[str, str] = {UPLINK: "closed", DOWNLINK: "closed"}
        self.abort_reason: str | None = None
        self.started_at = time.time()

    @property
    def buffered_bytes(self) -> int:
        return len(self.buffers[UPLINK]) + len(self.buffers[DOWNLINK])

    def record(self, direction: str, data: bytes) -> None:
        if self.abort_reason is not None:
            return
        if self.buffered_bytes + len(data) > self.buffer_limit:
            self.abort_reason = (
                f"取证缓存超限(上限 {self.buffer_limit} 字节),已放弃本笔记证"
            )
            # 清理残件:释放已缓存字节,避免继续占用内存
            self.buffers[UPLINK].clear()
            self.buffers[DOWNLINK].clear()
            logger.warning("trial=%s %s", self.trial_id, self.abort_reason)
            return
        self.buffers[direction] += data

    def record_uplink(self, data: bytes) -> None:
        """记录网关 -> 打印机方向交给 TCP 发送的原始字节。"""
        self.record(UPLINK, data)

    def record_downlink(self, data: bytes) -> None:
        """记录网关 -> 终端方向交给 TCP 发送的原始字节。"""
        self.record(DOWNLINK, data)

    def set_end(self, direction: str, reason: str) -> None:
        self.end_reasons[direction] = reason

    def meta(self, status: str, job_id: str) -> dict:
        return {
            "trial_id": self.trial_id,
            "job_id": job_id,
            "status": status,
            "abort_reason": self.abort_reason,
            "started_at": self.started_at,
            "finished_at": time.time(),
            "directions": {
                direction: {
                    "bytes": len(self.buffers[direction]),
                    "end_reason": self.end_reasons[direction],
                    "clean_half_close": self.end_reasons[direction] == END_HALF_CLEAN,
                }
                for direction in DIRECTIONS
            },
        }


class ForensicManager:
    """网关侧取证管理:同一时刻只承载一笔取证试传。

    - try_begin(): 取证端口来连接时认领 API 登记的试传号;已有进行中
      的取证或没有登记时返回 None(调用方应直接拒绝该连接)。
    - finish(): 作业结束时调用,落盘放入后台任务,不阻塞连接收尾;
      写盘失败只影响本笔取证,转发与其他作业不受影响。
    """

    def __init__(self, settings: ForensicSettings) -> None:
        self._settings = settings
        self._data_dir = Path(settings.data_dir)
        self._active: CaptureSession | None = None
        self._persist_tasks: set[asyncio.Task] = set()

    @property
    def active(self) -> CaptureSession | None:
        return self._active

    def try_begin(self) -> CaptureSession | None:
        if self._active is not None:
            logger.warning("forensic trial already active, rejecting new one")
            return None
        trial_id = self._claim_latest_pending()
        if trial_id is None:
            return None
        session = CaptureSession(trial_id, self._settings.buffer_bytes)
        self._active = session
        logger.info("trial=%s forensic capture started", trial_id)
        return session

    def finish(self, session: CaptureSession, job_id: str) -> None:
        if self._active is session:
            self._active = None
        task = asyncio.create_task(self._persist(session, job_id))
        self._persist_tasks.add(task)
        task.add_done_callback(self._persist_tasks.discard)

    async def wait_persisted(self) -> None:
        """等待在途落盘任务结束(测试与停机时用)。"""
        while self._persist_tasks:
            await asyncio.wait(
                self._persist_tasks, return_when=asyncio.ALL_COMPLETED
            )

    # -- 内部 ---------------------------------------------------------------

    def _claim_latest_pending(self) -> str | None:
        inc = incoming_dir(self._data_dir)
        try:
            candidates = [
                p for p in inc.glob("*.json") if p.is_file()
            ]
        except OSError:
            return None
        now = time.time()
        fresh: list[tuple[float, Path]] = []
        for path in candidates:
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if now - mtime > self._settings.pending_ttl:
                # 过期登记:清理残件,避免被错误认领
                with contextlib.suppress(OSError):
                    path.unlink()
                continue
            fresh.append((mtime, path))
        if not fresh:
            return None
        fresh.sort()
        latest = fresh[-1][1]
        trial_id = latest.stem
        # 认领即删除,防止下一笔取证误关联到同一登记
        with contextlib.suppress(OSError):
            latest.unlink()
        return trial_id

    async def _persist(self, session: CaptureSession, job_id: str) -> None:
        # 文件 IO 放进线程,事件循环(转发)不等取证落盘
        try:
            await asyncio.to_thread(self._persist_sync, session, job_id)
        except Exception:
            # 落盘失败只影响本笔记证,绝不上抛影响转发与关停
            logger.exception("trial=%s persist task failed", session.trial_id)

    def _persist_sync(self, session: CaptureSession, job_id: str) -> None:
        tdir = trial_dir(self._data_dir, session.trial_id)
        try:
            if session.abort_reason is None:
                usage = store_usage_bytes(self._data_dir)
                new_bytes = session.buffered_bytes
                if usage + new_bytes > self._settings.store_bytes:
                    session.abort_reason = (
                        f"取证记录总量超限(已存 {usage} + 本笔 {new_bytes} > "
                        f"上限 {self._settings.store_bytes} 字节),已放弃本笔记证"
                    )
                    logger.warning("trial=%s %s", session.trial_id,
                                   session.abort_reason)
            if session.abort_reason is not None:
                self._write_aborted(tdir, session, job_id)
                return
            # 正常落盘:先写两路原始字节,最后写 meta(complete 出现即终态)
            tdir.mkdir(parents=True, exist_ok=False)
            for direction in DIRECTIONS:
                (tdir / f"{direction}.bin").write_bytes(
                    bytes(session.buffers[direction])
                )
            self._write_meta(tdir, session, STATUS_COMPLETE, job_id)
            logger.info(
                "trial=%s persisted uplink=%d downlink=%d bytes",
                session.trial_id,
                len(session.buffers[UPLINK]),
                len(session.buffers[DOWNLINK]),
            )
        except OSError as exc:
            # 写盘失败:放弃取证,清理残件,只留下带原因的 meta
            logger.error("trial=%s persist failed: %r", session.trial_id, exc)
            session.abort_reason = f"取证写盘失败({exc!r}),已放弃本笔记证"
            with contextlib.suppress(OSError):
                self._write_aborted(tdir, session, job_id)

    def _write_aborted(
        self, tdir: Path, session: CaptureSession, job_id: str
    ) -> None:
        # 清理残件:部分写出的数据文件全部移除,只留带原因的 meta
        shutil.rmtree(tdir, ignore_errors=True)
        tdir.mkdir(parents=True, exist_ok=True)
        self._write_meta(tdir, session, STATUS_ABORTED, job_id)

    def _write_meta(
        self, tdir: Path, session: CaptureSession, status: str, job_id: str
    ) -> None:
        (tdir / "meta.json").write_text(
            json.dumps(
                session.meta(status, job_id), ensure_ascii=False, indent=2
            ),
            encoding="utf-8",
        )
