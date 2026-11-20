# 条码标签 TCP 转发网关（asyncio）

条码终端通过 TCP 上传标签文件，发完后**半关闭写端（SHUT_WR）**；模拟打印机**读到 EOF 才确认接单**。本项目是位于两者之间的 asyncio 转发网关，并附带 FastAPI/Swagger 演示入口与模拟打印机。

## 语义与设计

| 需求 | 实现位置 | 说明 |
| --- | --- | --- |
| 半关闭透传，反向照常转发 | `app/gateway.py::_pump` | 读到 EOF 后先把该方向数据全部 `drain`，再 `write_eof()` 传递 SHUT_WR；两个方向是独立协程，互不影响 |
| 打印机提前拒收时拒收原因必达 | `app/terminal.py` 并发读写 + `app/printer.py` | 终端读协程与写协程并发，写协程即使卡在 `drain()`，反向通道的 `rejected` 回执仍被读取 |
| 限制转发缓冲 | `app/gateway.py` | `StreamReader(limit)` + 监听 socket `SO_RCVBUF` + 两个发送方向固定 `SO_SNDBUF`（显式设置即关闭 Linux 发送缓冲自动调节），背压在可预期的小阈值触发，网关内存不随慢对端膨胀 |
| 写阻塞超时回收本作业 | `app/gateway.py::_pump` + `Job.recycle` | 每次 `drain()` 套 `asyncio.wait_for`，超时立即关闭该作业的两条连接；其他作业的连接与任务独立，互不影响 |

### 为什么同时设置应用层 limit 和内核 socket 缓冲

Linux 回环/高速链路上 TCP 发送缓冲会自动扩张到数 MB，仅靠 `StreamReader` 的高水位会让背压迟迟不触发（2MB 数据可以全部堆进内核，应用层完全无感）。显式 `setsockopt(SO_SNDBUF)` 会关闭该 socket 的发送缓冲自动调节，使写阻塞在约一个缓冲上限处稳定出现，`WRITE_TIMEOUT_SECONDS` 才有确定的语义。

## 组件

- `app/gateway.py` — 转发网关（监听 `9000`，转发到打印机 `9100`）
- `app/printer.py` — 模拟打印机：
  - 普通流：读到 EOF 回 `{"status":"accepted",...}`
  - 首行 `@REJECT@ <原因>`：立即回 `rejected` 并停止读取上传数据
  - 首行 `@BLOCK@`：完全不读取，制造写阻塞以触发网关超时回收
- `app/terminal.py` — 模拟终端：分块写、写完 `write_eof()`、并发读回执、记录写阻塞
- `app/api.py` — FastAPI，Swagger UI 在 `/docs`，每个接口发起**真实 TCP** 试传
- `tests/e2e_test.py` — 回环口上的真实 TCP 端到端测试

## 用 Docker Compose 启动

```bash
docker compose up --build
# Swagger UI:  http://localhost:8080/docs
# 网关 TCP:    localhost:9000
# 打印机 TCP:  localhost:9100
```

在 Swagger 中依次调用：

1. `POST /demo/accept` — 半关闭后收到 `accepted` 接单回执
2. `POST /demo/reject` — 上传写阻塞时仍收到 `rejected` 拒收原因
3. `POST /demo/timeout` — 打印机不读，网关在 `WRITE_TIMEOUT_SECONDS` 后回收两条连接
4. `POST /demo/isolation` — 坏作业被回收后，新作业仍正常 `accepted`

## 无 Docker 时本地运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
scripts/run_local.sh          # 起三进程栈，日志在 .run_logs/
```

另开终端：

```bash
.venv/bin/python -m unittest tests.e2e_test -v
curl -X POST http://127.0.0.1:8080/demo/accept \
  -H 'Content-Type: application/json' \
  -d '{"filename":"label-001.lbl","content_size":131072,"timeout_seconds":15}'
```

## 关键环境变量

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `GATEWAY_LISTEN_PORT` | 9000 | 网关监听端口 |
| `PRINTER_HOST` / `PRINTER_PORT` | 127.0.0.1 / 9100 | 上游打印机地址 |
| `FORWARD_BUFFER_BYTES` | 65536 | 单方向转发缓冲上限（应用层 + socket） |
| `WRITE_TIMEOUT_SECONDS` | 5.0 | 单次写阻塞上限，超时回收本作业 |
| `CONNECT_TIMEOUT_SECONDS` | 5.0 | 网关连接打印机超时 |
| `PRINTER_GRACE_SECONDS` | 2.0 | 打印机发完回执后保留连接的宽限时间 |

## 备注

- 打印机是纯 TCP 服务，**不要用裸 TCP 建连做健康检查**——那会被当成一个 0 字节标签作业；Compose 因此使用启动顺序而非 healthcheck。
- 网关对载荷透明，`@REJECT@`/`@BLOCK@` 只是模拟打印机识别的演示标记。
