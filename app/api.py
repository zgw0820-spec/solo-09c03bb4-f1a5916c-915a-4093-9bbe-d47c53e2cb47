"""FastAPI 演示入口:从 Swagger UI 发起真实 TCP 试传。

四个场景:
- /demo/accept   正常上传,半关闭后收到打印机接单回执
- /demo/reject   打印机提前拒收,即使上传写阻塞也能收到拒收原因
- /demo/timeout  打印机停止读取,网关写超时后回收本作业两条连接
- /demo/isolation 超时作业被回收后,新作业仍可正常上传
"""
from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .config import ApiSettings, GatewaySettings
from .protocol import build_block_stream, build_label_stream, build_reject_stream
from .terminal import UploadResult, upload_label

logger = logging.getLogger("api")

settings = ApiSettings.from_env()
_gw = GatewaySettings.from_env()

app = FastAPI(
    title="条码标签 TCP 转发网关演示",
    version="1.0.0",
    description=(
        "所有接口都会发起**真实 TCP 连接**到转发网关,再由网关转发给"
        "模拟打印机。终端在发完数据后半关闭写端(SHUT_WR)。"
    ),
)


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
        "gateway_write_timeout_seconds": _gw.write_timeout,
        "gateway_buffer_bytes": _gw.buffer_bytes,
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
