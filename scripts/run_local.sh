#!/usr/bin/env bash
# 本地(无 Docker)启动完整三进程栈:打印机 + 网关 + FastAPI。
# 用法: scripts/run_local.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  echo "请先创建虚拟环境并安装依赖:" >&2
  echo "  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

export PRINTER_LISTEN_HOST=127.0.0.1 PRINTER_LISTEN_PORT=9100 PRINTER_GRACE_SECONDS=2
export GATEWAY_LISTEN_HOST=127.0.0.1 GATEWAY_LISTEN_PORT=9000
export PRINTER_HOST=127.0.0.1 PRINTER_PORT=9100
export FORWARD_BUFFER_BYTES=65536 WRITE_TIMEOUT_SECONDS=3
export API_GATEWAY_HOST=127.0.0.1 API_GATEWAY_PORT=9000

LOGDIR="$ROOT/.run_logs"
mkdir -p "$LOGDIR"

cleanup() {
  echo
  echo "[run_local] stopping ..."
  kill 0 2>/dev/null || true
}
trap cleanup EXIT INT TERM

"$PY" -m app.printer >"$LOGDIR/printer.log" 2>&1 &
PRINTER_PID=$!
echo "[run_local] printer  pid=$PRINTER_PID log=$LOGDIR/printer.log"

# 等打印机日志确认监听(不做裸 TCP 探活,那会被当成 0 字节标签作业)
for _ in $(seq 1 50); do
  grep -q "mock printer listening" "$LOGDIR/printer.log" 2>/dev/null && break
  sleep 0.1
done

"$PY" -m app.gateway >"$LOGDIR/gateway.log" 2>&1 &
GATEWAY_PID=$!
echo "[run_local] gateway  pid=$GATEWAY_PID log=$LOGDIR/gateway.log"

for _ in $(seq 1 50); do
  grep -q "gateway listening" "$LOGDIR/gateway.log" 2>/dev/null && break
  sleep 0.1
done

echo "[run_local] Swagger UI: http://127.0.0.1:8080/docs"
"$PY" -m uvicorn app.api:app --host 127.0.0.1 --port 8080
