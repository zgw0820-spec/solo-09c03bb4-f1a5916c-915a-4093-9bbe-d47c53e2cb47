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
class ApiSettings:
    gateway_host: str = "127.0.0.1"
    gateway_port: int = 9000
    # 单个演示请求的整体等待上限
    request_timeout: float = 20.0

    @staticmethod
    def from_env() -> "ApiSettings":
        return ApiSettings(
            gateway_host=_env("API_GATEWAY_HOST", "127.0.0.1"),
            gateway_port=_env_int("API_GATEWAY_PORT", 9000),
            request_timeout=_env_float("API_REQUEST_TIMEOUT", 20.0),
        )
