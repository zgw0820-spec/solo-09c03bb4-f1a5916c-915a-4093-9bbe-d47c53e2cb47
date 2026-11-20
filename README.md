# 条码标签 TCP 转发网关(asyncio)

条码终端通过 TCP 上传标签文件,发完后**半关闭写端(SHUT_WR)**;模拟打印机**读到 EOF 才确认接单**。本项目是位于两者之间的 asyncio 转发网关,附带 FastAPI/Swagger 演示入口、模拟打印机,以及**取证试传**:标签偶尔打成乱码时,值班员可以从 Swagger 发起一笔取证试传,拿到网关在两个方向实际交给 TCP 发送的原始字节,交给设备维护方分析。

## 语义与设计

| 需求 | 实现位置 | 说明 |
| --- | --- | --- |
| 半关闭透传,反向照常转发 | `app/gateway.py::_pump` | 读到 EOF 后先把该方向数据全部 `drain`,再 `write_eof()` 传递 SHUT_WR;两个方向是独立协程,互不影响 |
| 打印机提前拒收时拒收原因必达 | `app/terminal.py` 并发读写 + `app/printer.py` | 终端读协程与写协程并发,写协程即使卡在 `drain()`,反向通道的 `rejected` 回执仍被读取 |
| 限制转发缓冲 | `app/gateway.py` | `StreamReader(limit)` + 监听 socket `SO_RCVBUF` + 两个发送方向固定 `SO_SNDBUF`(显式设置即关闭 Linux 发送缓冲自动调节),背压在可预期的小阈值触发,网关内存不随慢对端膨胀 |
| 写阻塞超时回收本作业 | `app/gateway.py::_pump` + `Job.recycle` | 每次 `drain()` 套 `asyncio.wait_for`,超时 `abort()` 两条连接(丢弃未冲完的缓冲,连接立即死亡);其他作业的连接与任务独立,互不影响 |
| 取证试传 | `app/forensics.py` + 网关取证端口 + `app/api.py` | 见下节 |

### 为什么同时设置应用层 limit 和内核 socket 缓冲

Linux 回环/高速链路上 TCP 发送缓冲会自动扩张到数 MB,仅靠 `StreamReader` 的高水位会让背压迟迟不触发(2MB 数据可以全部堆进内核,应用层完全无感)。显式 `setsockopt(SO_SNDBUF)` 会关闭该 socket 的发送缓冲自动调节,使写阻塞在约一个缓冲上限处稳定出现,`WRITE_TIMEOUT_SECONDS` 才有确定的语义。

## 取证试传

值班员在 Swagger 提交标签原始字节,走**真实 TCP**(API → 网关取证端口 → 打印机),网关旁路记录两个方向**交给 TCP 发送的原始字节**并落盘,凭试传号下载。

### 工作方式

- 网关在主端口之外另开**取证端口**(默认 `9001`)。主端口的普通作业照常转发,**不留存任何数据**。
- **同一时刻只做一笔取证试传**:API 侧有锁(并发请求得 409),网关侧同时只承载一笔取证连接(多余连接直接拒绝并说明原因)。
- 记录是转发路径上的**纯内存追加**,落盘在作业结束后由后台任务完成——**转发不等取证**;取证失败(缓存满/超限/写盘失败)不影响转发与回执。
- `meta.json` 标明每个方向的结束方式:`eof` = 正常半关闭,`timeout` / `error` / `closed` = 异常结束。
- 取证作业有**整体看门狗**(`FORENSIC_JOB_TIMEOUT_SECONDS`):打印机装死且数据量又不足以触发写超时回收时,作业也会在有限时间内收尾落盘,取证端口不会被一笔卡死的作业长期占用。

### 限额与放弃语义

| 情况 | 行为 |
| --- | --- |
| 取证缓存满(两方向合计超 `FORENSIC_BUFFER_BYTES`) | 放弃本笔记证,清空已缓存字节;转发与回执照常 |
| 记录总量超限(已存记录 + 本笔超 `FORENSIC_STORE_BYTES`) | 放弃本笔记证;不淘汰历史记录 |
| 写盘失败 | 放弃本笔记证 |

放弃时**清理残件**(部分写出的数据文件全部移除),只留下带 `abort_reason` 的 `meta.json`;下载接口据此返回缺口原因(HTTP 409)。**两路记录都完整落盘**(`status=complete` 且两个 `.bin` 与登记字节数一致)才可下载。

### 目录布局(网关与 API 共享的 `FORENSIC_DATA_DIR`)

```
incoming/<trial_id>.json     API 登记的待取证试传(网关认领即删)
store/<trial_id>/uplink.bin    网关发给打印机的原始字节(终端->打印机)
store/<trial_id>/downlink.bin  网关发给终端的原始字节(打印机->终端)
store/<trial_id>/meta.json     试传状态与各方向结束方式
```

### Swagger 操作流程

1. `POST /forensics/trial` — Body 选 `application/octet-stream` 上传标签原始字节;返回**试传号**、终端收到的**回执**和取证落盘状态
2. `GET /forensics` — 列出全部取证记录(找试传号)
3. `GET /forensics/{trial_id}` — 查看方向结束方式与可下载性
4. `GET /forensics/{trial_id}/download` — 下载 tar 包(`uplink.bin` + `downlink.bin` + `meta.json`);有缺口返回 409 与原因

## 组件

- `app/gateway.py` — 转发网关(主端口 `9000` 转发到打印机 `9100`,取证端口 `9001`)
- `app/forensics.py` — 取证会话、单并发管理、限额与落盘、可下载性检查
- `app/printer.py` — 模拟打印机:
  - 普通流:读到 EOF 回 `{"status":"accepted",...}`
  - 首行 `@REJECT@ <原因>`:立即回 `rejected` 并停止读取上传数据
  - 首行 `@BLOCK@`:完全不读取,制造写阻塞以触发网关超时回收
- `app/terminal.py` — 模拟终端:分块写、写完 `write_eof()`、并发读回执、记录写阻塞
- `app/api.py` — FastAPI,Swagger UI 在 `/docs`,每个接口发起**真实 TCP** 试传
- `tests/e2e_test.py` — 回环口上的真实 TCP 端到端测试
- `tests/forensics_test.py` — 取证端到端测试(真实 TCP + 真实落盘)

## 用 Docker Compose 启动

```bash
docker compose up --build
# Swagger UI:  http://localhost:8080/docs
# 网关 TCP:    localhost:9000 (普通作业) / 9001 (取证试传)
# 打印机 TCP:  localhost:9100
```

取证记录保存在命名卷 `forensics-data` 中,网关写入、API 读取并提供下载。

在 Swagger 中依次调用:

1. `POST /demo/accept` — 半关闭后收到 `accepted` 接单回执
2. `POST /demo/reject` — 上传写阻塞时仍收到 `rejected` 拒收原因
3. `POST /demo/timeout` — 打印机不读,网关在 `WRITE_TIMEOUT_SECONDS` 后回收两条连接
4. `POST /demo/isolation` — 坏作业被回收后,新作业仍正常 `accepted`
5. `POST /forensics/trial` — 取证试传,然后 `GET /forensics/{trial_id}/download` 下载记录

## 无 Docker 时本地运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
scripts/run_local.sh          # 起三进程栈,日志在 .run_logs/,取证在 .forensics/
```

另开终端:

```bash
.venv/bin/python -m unittest tests.e2e_test tests.forensics_test -v
curl -X POST http://127.0.0.1:8080/demo/accept \
  -H 'Content-Type: application/json' \
  -d '{"filename":"label-001.lbl","content_size":131072,"timeout_seconds":15}'
# 取证试传:提交标签原始字节
curl -X POST http://127.0.0.1:8080/forensics/trial \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @label.bin
curl -O -J http://127.0.0.1:8080/forensics/<trial_id>/download
```

## 关键环境变量

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `GATEWAY_LISTEN_PORT` | 9000 | 网关主端口(普通作业) |
| `PRINTER_HOST` / `PRINTER_PORT` | 127.0.0.1 / 9100 | 上游打印机地址 |
| `FORWARD_BUFFER_BYTES` | 65536 | 单方向转发缓冲上限(应用层 + socket) |
| `WRITE_TIMEOUT_SECONDS` | 5.0 | 单次写阻塞上限,超时回收本作业 |
| `CONNECT_TIMEOUT_SECONDS` | 5.0 | 网关连接打印机超时 |
| `PRINTER_GRACE_SECONDS` | 2.0 | 打印机发完回执后保留连接的宽限时间 |
| `FORENSIC_LISTEN_PORT` | 9001 | 网关取证端口 |
| `FORENSIC_DATA_DIR` | .forensics | 取证目录(网关与 API 共享) |
| `FORENSIC_BUFFER_BYTES` | 1048576 | 单笔取证内存缓存上限(两方向合计),超过即放弃 |
| `FORENSIC_STORE_BYTES` | 67108864 | 取证记录总量(两路 `.bin`)上限,超过即放弃当前笔 |
| `FORENSIC_PENDING_TTL_SECONDS` | 60 | API 登记的有效期,过期登记被清理 |
| `FORENSIC_JOB_TIMEOUT_SECONDS` | 30 | 单笔取证作业整体看门狗时限 |
| `API_FORENSIC_PORT` | 9001 | API 连接网关取证端口 |

## 备注

- 打印机是纯 TCP 服务,**不要用裸 TCP 建连做健康检查**——那会被当成一个 0 字节标签作业;Compose 因此使用启动顺序而非 healthcheck。
- 网关对载荷透明,`@REJECT@`/`@BLOCK@` 只是模拟打印机识别的演示标记。
- 已知边界:主端口作业若打印机不应答、且载荷小到能全部沉入内核缓冲(不触发写超时),该作业会滞留到打印机关闭连接为止——取证端口有整体看门狗兜底,主端口保持"只按写超时回收"的原有语义。
