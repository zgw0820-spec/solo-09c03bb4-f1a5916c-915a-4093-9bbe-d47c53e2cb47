"""集中式环境变量配置。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


@dataclass(frozen=True)
class GatewaySettings:
    listen_host: str = "0.0.0.0"
    listen_port: int = 9000
    printer_host: str = "127.0.0.1"
    printer_port: int = 9100
    # 单条 StreamReader/传输层的缓冲高水位,双向各自受限
    buffer_bytes: int = 64 * 1024
    # 单次 drain() 写阻塞上限,超时回收本作业两条连接
    write_timeout: float = 5.0
    # 连接上游模拟打印机的超时
    connect_timeout: float = 5.0

    @staticmethod
    def from_env() -> "GatewaySettings":
        return GatewaySettings(
            listen_host=_env("GATEWAY_LISTEN_HOST", "0.0.0.0"),
            listen_port=_env_int("GATEWAY_LISTEN_PORT", 9000),
            printer_host=_env("PRINTER_HOST", "127.0.0.1"),
            printer_port=_env_int("PRINTER_PORT", 9100),
            buffer_bytes=_env_int("FORWARD_BUFFER_BYTES", 64 * 1024),
            write_timeout=_env_float("WRITE_TIMEOUT_SECONDS", 5.0),
            connect_timeout=_env_float("CONNECT_TIMEOUT_SECONDS", 5.0),
        )


@dataclass(frozen=True)
class PrinterSettings:
    listen_host: str = "0.0.0.0"
    listen_port: int = 9100
    # 拒收后保持套接字的宽限时间,便于网关把拒收回执送回终端
    grace_seconds: float = 2.0

    @staticmethod
    def from_env() -> "PrinterSettings":
        return PrinterSettings(
            listen_host=_env("PRINTER_LISTEN_HOST", "0.0.0.0"),
            listen_port=_env_int("PRINTER_LISTEN_PORT", 9100),
            grace_seconds=_env_float("PRINTER_GRACE_SECONDS", 2.0),
        )


@dataclass(frozen=True)
class ForensicSettings:
    """取证试传:网关取证端口 + 双方向原始字节记录的限额。

    data_dir 由网关与 API 两个进程共享(Docker 里挂同一个卷):
      incoming/<trial_id>.json   API 登记的待取证试传
      store/<trial_id>/          网关落盘的双方向记录与 meta.json
    """

    listen_host: str = "0.0.0.0"
    listen_port: int = 9001
    data_dir: str = ".forensics"
    # 单笔取证在内存中缓存的原始字节上限(两个方向合计),
    # 超过即放弃取证并清空已缓存内容,转发不受影响
    buffer_bytes: int = 1024 * 1024
    # data_dir/store 下全部取证记录(两路原始字节)的总量上限,
    # 超过即放弃当前这笔取证(不淘汰历史记录)
    store_bytes: int = 64 * 1024 * 1024
    # API 登记后网关等待取证连接到来的时限,过期 pending 记录被清理
    pending_ttl: float = 60.0
    # 单笔取证作业的整体看门狗时限:打印机不应答且数据量又不足以
    # 触发写超时回收时,靠它在有限时间内结束作业并落盘,避免取证
    # 端口被一笔卡死的作业长期占用
    job_timeout: float = 30.0

    @staticmethod
    def from_env() -> "ForensicSettings":
        return ForensicSettings(
            listen_host=_env("FORENSIC_LISTEN_HOST", "0.0.0.0"),
            listen_port=_env_int("FORENSIC_LISTEN_PORT", 9001),
            data_dir=_env("FORENSIC_DATA_DIR", ".forensics"),
            buffer_bytes=_env_int("FORENSIC_BUFFER_BYTES", 1024 * 1024),
            store_bytes=_env_int("FORENSIC_STORE_BYTES", 64 * 1024 * 1024),
            pending_ttl=_env_float("FORENSIC_PENDING_TTL_SECONDS", 60.0),
            job_timeout=_env_float("FORENSIC_JOB_TIMEOUT_SECONDS", 30.0),
        )


@dataclass(frozen=True)
class ApiSettings:
    gateway_host: str = "127.0.0.1"
    gateway_port: int = 9000
    # 网关取证端口(POST /forensics/trial 走这里)
    gateway_forensic_port: int = 9001
    # 单个演示请求的整体等待上限
    request_timeout: float = 20.0

    @staticmethod
    def from_env() -> "ApiSettings":
        return ApiSettings(
            gateway_host=_env("API_GATEWAY_HOST", "127.0.0.1"),
            gateway_port=_env_int("API_GATEWAY_PORT", 9000),
            gateway_forensic_port=_env_int("API_FORENSIC_PORT", 9001),
            request_timeout=_env_float("API_REQUEST_TIMEOUT", 20.0),
        )
