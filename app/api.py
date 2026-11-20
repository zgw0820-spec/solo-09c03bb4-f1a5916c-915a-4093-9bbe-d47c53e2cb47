"""FastAPI 演示入口:从 Swagger UI 发起真实 TCP 试传。

普通场景(走网关主端口,不留存):
- /demo/accept   正常上传,半关闭后收到打印机接单回执
- /demo/reject   打印机提前拒收,即使上传写阻塞也能收到拒收原因
- /demo/timeout  打印机停止读取,网关写超时后回收本作业两条连接
- /demo/isolation 超时作业被回收后,新作业仍可正常上传

取证场景(走网关取证端口,双方向原始字节落盘):
- POST /forensics/trial                 提交标签原始字节发起取证试传
- GET  /forensics                       列出全部取证记录
- GET  /forensics/{trial_id}            查看某试传的方向状态与缺口原因
- GET  /forensics/{trial_id}/download   两路记录完整落盘后打包下载
"""
from __future__ import annotations

import asyncio
import io
import logging
import tarfile
import time
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, Field

from .config import ApiSettings, ForensicSettings, GatewaySettings
from .forensics import (
    STATUS_COMPLETE,
    cancel_pending,
    check_downloadable,
    list_trials,
    load_meta,
    new_trial_id,
    register_pending,
    trial_dir,
)
from .protocol import build_block_stream, build_label_stream, build_reject_stream
from .terminal import UploadResult, upload_label

logger = logging.getLogger("api")

settings = ApiSettings.from_env()
_gw = GatewaySettings.from_env()
_fx = ForensicSettings.from_env()
_fx_dir = Path(_fx.data_dir)

app = FastAPI(
    title="条码标签 TCP 转发网关演示",
    version="1.1.0",
    description=(
        "所有接口都会发起**真实 TCP 连接**到转发网关,再由网关转发给"
        "模拟打印机。终端在发完数据后半关闭写端(SHUT_WR)。\n\n"
        "**取证试传**:提交标签原始字节,网关记录两个方向实际交给 TCP "
        "发送的字节并落盘,可凭试传号下载交给设备维护方。同一时刻只"
        "允许一笔取证试传;普通作业照常转发且不留存。"
    ),
)


# ---------------------------------------------------------------------------
# 普通演示场景
# ---------------------------------------------------------------------------


class AcceptRequest(BaseModel):
    filename: str = Field("label-001.lbl", description="标签文件名")
    content_size: int = Field(128 * 1024, ge=1, le=4 * 1024 * 1024,
                              description="标签载荷字节数")
    timeout_seconds: float = Field(15.0, ge=1.0, le=60.0)


class RejectRequest(BaseModel):
    reason: str = Field("unsupported label format / printer busy",
                        description="打印机给出的拒收原因")
    payload_bytes: int = Field(
        2 * 1024 * 1024, ge=64 * 1024, le=8 * 1024 * 1024,
        description="上传总量;大于网关+内核缓冲即可在拒收后写阻塞",
    )
    timeout_seconds: float = Field(15.0, ge=1.0, le=60.0)


class TimeoutRequest(BaseModel):
    payload_bytes: int = Field(
        4 * 1024 * 1024, ge=64 * 1024, le=8 * 1024 * 1024,
        description="上传总量,打印机完全不读取,必然写阻塞",
    )
    timeout_seconds: float = Field(12.0, ge=1.0, le=60.0,
                                   description="终端侧整体等待上限(应大于网关写超时)")


def _summary(result: UploadResult) -> dict:
    data = result.as_dict()
    statuses = [e.get("status") for e in data["events"]]
    data["statuses"] = statuses
    return data


@app.get("/health", tags=["demo"])
async def health() -> dict:
    return {
        "service": "label-gateway-demo",
        "gateway": f"{settings.gateway_host}:{settings.gateway_port}",
        "gateway_forensic_port": settings.gateway_forensic_port,
        "gateway_write_timeout_seconds": _gw.write_timeout,
        "gateway_buffer_bytes": _gw.buffer_bytes,
        "forensic_capture_buffer_bytes": _fx.buffer_bytes,
        "forensic_store_limit_bytes": _fx.store_bytes,
    }


@app.post("/demo/accept", tags=["demo"], summary="正常接单:半关闭后收到接单回执")
async def demo_accept(req: AcceptRequest) -> dict:
    payload = build_label_stream(req.filename, req.content_size)
    result = await upload_label(
        settings.gateway_host,
        settings.gateway_port,
        payload,
        overall_timeout=req.timeout_seconds,
    )
    return {
        "scenario": "accept",
        "expectation": "终端半关闭写端 -> 打印机读到 EOF -> 回 accepted",
        **_summary(result),
    }


@app.post("/demo/reject", tags=["demo"], summary="提前拒收:上传写阻塞时仍收到拒收原因")
async def demo_reject(req: RejectRequest) -> dict:
    payload = build_reject_stream(req.reason, req.payload_bytes)
    result = await upload_label(
        settings.gateway_host,
        settings.gateway_port,
        payload,
        overall_timeout=req.timeout_seconds,
    )
    return {
        "scenario": "reject",
        "expectation": "打印机立即回 rejected 并停止读取;终端写协程阻塞,读协程仍收到拒收原因",
        **_summary(result),
    }


@app.post("/demo/timeout", tags=["demo"], summary="写阻塞超时:网关回收本作业两条连接")
async def demo_timeout(req: TimeoutRequest) -> dict:
    payload = build_block_stream(req.payload_bytes)
    result = await upload_label(
        settings.gateway_host,
        settings.gateway_port,
        payload,
        overall_timeout=req.timeout_seconds,
    )
    return {
        "scenario": "timeout",
        "expectation": (
            f"写阻塞超过网关 {_gw.write_timeout:.0f}s 上限,网关回收连接;"
            "终端无回执,连接被中断"
        ),
        **_summary(result),
    }


@app.post("/demo/isolation", tags=["demo"],
          summary="作业隔离:超时作业被回收后,新作业照常上传")
async def demo_isolation(req: TimeoutRequest) -> dict:
    block_payload = build_block_stream(req.payload_bytes)
    good_payload = build_label_stream("after-timeout.lbl", 64 * 1024)

    blocked = await upload_label(
        settings.gateway_host,
        settings.gateway_port,
        block_payload,
        overall_timeout=req.timeout_seconds,
    )
    # 被回收后立刻发起一个全新作业,验证其他作业不受影响
    await asyncio.sleep(0.2)
    good = await upload_label(
        settings.gateway_host,
        settings.gateway_port,
        good_payload,
        overall_timeout=req.timeout_seconds,
    )
    return {
        "scenario": "isolation",
        "expectation": "坏作业被回收,随后的新作业仍能收到 accepted",
        "blocked_job": _summary(blocked),
        "healthy_job": _summary(good),
    }


# ---------------------------------------------------------------------------
# 取证试传
# ---------------------------------------------------------------------------

# 同一时刻只允许一笔取证试传(API 侧闸门;网关取证端口同样只承载一笔)
_trial_lock = asyncio.Lock()


async def _wait_meta(trial_id: str, timeout: float) -> dict | None:
    """轮询等待网关把本笔记证落盘出终态(complete/aborted)。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        meta = await asyncio.to_thread(load_meta, _fx_dir, trial_id)
        if meta is not None:
            return meta
        await asyncio.sleep(0.1)
    return None


@app.post(
    "/forensics/trial",
    tags=["forensics"],
    summary="取证试传:提交标签原始字节走真实 TCP,返回试传号与回执",
)
async def forensic_trial(
    payload: bytes = Body(
        ...,
        media_type="application/octet-stream",
        description="标签原始字节(将原样经网关转发给打印机)",
    ),
    timeout_seconds: float = Query(15.0, ge=1.0, le=60.0),
    persist_wait_seconds: float = Query(
        10.0, ge=1.0, le=90.0,
        description="等待取证落盘出终态的上限;打印机卡死的试传需等网关看门狗收尾",
    ),
) -> dict:
    if not payload:
        raise HTTPException(400, "请求体为空:请在 Body 中提交标签原始字节")
    if _trial_lock.locked():
        raise HTTPException(
            409, "已有取证试传进行中,同一时刻只允许一笔;请稍后重试"
        )
    async with _trial_lock:
        trial_id = new_trial_id()
        # 1) 登记待取证试传,网关取证端口接受连接时认领该试传号
        try:
            await asyncio.to_thread(register_pending, _fx_dir, trial_id)
        except OSError as exc:
            raise HTTPException(
                503, f"取证目录不可写,无法登记试传: {exc!r}"
            ) from exc
        try:
            # 2) 标签原始字节走真实 TCP(经取证端口 -> 网关 -> 打印机)
            result = await upload_label(
                settings.gateway_host,
                settings.gateway_forensic_port,
                payload,
                overall_timeout=timeout_seconds,
            )
        finally:
            # 网关未认领(如取证端口拒绝)时清掉自己的登记,避免残件
            await asyncio.to_thread(cancel_pending, _fx_dir, trial_id)
        # 3) 等网关把两个方向的记录落盘出终态
        meta = None
        if result.connected:
            meta = await _wait_meta(trial_id, persist_wait_seconds)
        forensic = meta if meta is not None else {
            "status": "unknown",
            "abort_reason": "等待落盘超时或取证连接未建立,记录可能有缺口",
        }
        return {
            "scenario": "forensic-trial",
            "trial_id": trial_id,
            "payload_bytes": len(payload),
            "capture_buffer_limit_bytes": _fx.buffer_bytes,
            "forensic": forensic,
            "download_url": f"/forensics/{trial_id}/download",
            **_summary(result),
        }


@app.get("/forensics", tags=["forensics"], summary="列出全部取证记录")
async def forensic_list() -> dict:
    trials = await asyncio.to_thread(list_trials, _fx_dir)
    return {"trials": trials, "count": len(trials)}


@app.get("/forensics/{trial_id}", tags=["forensics"],
         summary="查看某试传的方向状态与可下载性")
async def forensic_detail(trial_id: str) -> dict:
    meta = await asyncio.to_thread(load_meta, _fx_dir, trial_id)
    if meta is None:
        raise HTTPException(404, f"试传号 {trial_id} 没有已落盘的取证记录")
    downloadable, reason = await asyncio.to_thread(
        check_downloadable, _fx_dir, trial_id
    )
    return {
        "meta": meta,
        "downloadable": downloadable,
        "reason": reason,
        "download_url": f"/forensics/{trial_id}/download",
    }


@app.get("/forensics/{trial_id}/download", tags=["forensics"],
         summary="下载取证记录(两路完整落盘才放行,有缺口返回原因)")
async def forensic_download(trial_id: str) -> Response:
    downloadable, reason = await asyncio.to_thread(
        check_downloadable, _fx_dir, trial_id
    )
    if not downloadable:
        meta = await asyncio.to_thread(load_meta, _fx_dir, trial_id)
        if meta is None:
            raise HTTPException(404, reason)
        # 有 meta 但不完整:说明缺口原因
        raise HTTPException(409, reason)

    def build_tar() -> bytes:
        tdir = trial_dir(_fx_dir, trial_id)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for name in ("meta.json", "uplink.bin", "downlink.bin"):
                tar.add(tdir / name, arcname=f"{trial_id}/{name}")
        return buf.getvalue()

    archive = await asyncio.to_thread(build_tar)
    return Response(
        content=archive,
        media_type="application/x-tar",
        headers={
            "Content-Disposition":
                f'attachment; filename="forensics-{trial_id}.tar"'
        },
    )
