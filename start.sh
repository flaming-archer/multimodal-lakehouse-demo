#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-${ROOT_DIR}/.venv}"
PYTHON="${VENV_DIR}/bin/python"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8888}"
LOG_LEVEL="${LOG_LEVEL:-info}"
SETUP=false
CHECK_ONLY=false

# 本地启动与 Docker Compose 共用项目根目录的 .env。文件已被 .gitignore
# 排除；set -a 让其中变量自动导出给 Uvicorn 子进程。
if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

usage() {
  sed -n '/^# 启动多模态/,/^#   https_proxy/p' "$0" | sed 's/^# \{0,1\}//'
}

# 启动多模态湖仓 Demo Server。
#
# 用法：
#   ./start.sh                 使用已有 .venv 启动
#   ./start.sh --setup         创建/补齐 .venv 后启动
#   ./start.sh --check         仅检查环境，不启动
#   ./start.sh --help          显示帮助
#
# 千问 VLM（不要把密钥写入脚本）：
#   export IMAGE_VLM_API_KEY='你的 API Key'
#   export IMAGE_VLM_MODEL='qwen-vl-max'
#   ./start.sh
#
# 可选环境变量：
#   VENV_DIR       虚拟环境目录，默认 .venv
#   HOST           监听地址，默认 0.0.0.0
#   PORT           服务端口，默认 8888
#   LOG_LEVEL      Uvicorn 日志级别，默认 info
#   IMAGE_VLM_*    VLM 配置
#   http_proxy     HTTP 代理
#   https_proxy    HTTPS 代理

while (($#)); do
  case "$1" in
    --setup)
      SETUP=true
      ;;
    --check)
      CHECK_ONLY=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数：$1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

setup_venv() {
  if command -v uv >/dev/null 2>&1; then
    if [[ ! -x "$PYTHON" ]]; then
      echo "[setup] 创建虚拟环境：${VENV_DIR}"
      uv venv "$VENV_DIR"
    fi
    echo "[setup] 安装项目依赖"
    uv pip install --python "$PYTHON" -r "${ROOT_DIR}/requirements.txt"
    # 图片流水线和本地 PyIceberg SQL catalog 实际使用，但 requirements.txt
    # 当前未完整声明的运行依赖。
    uv pip install --python "$PYTHON" lancedb sqlalchemy
  else
    if [[ ! -x "$PYTHON" ]]; then
      python3 -m venv "$VENV_DIR"
    fi
    "$PYTHON" -m pip install -r "${ROOT_DIR}/requirements.txt"
    "$PYTHON" -m pip install lancedb sqlalchemy
  fi
}

if [[ "$SETUP" == true ]]; then
  setup_venv
fi

if [[ ! -x "$PYTHON" ]]; then
  echo "未找到项目虚拟环境：${VENV_DIR}" >&2
  echo "请先运行：./start.sh --setup" >&2
  exit 1
fi

if ! "$PYTHON" - <<'PY'
import importlib.util
import sys

required = ("fastapi", "uvicorn", "lancedb", "sqlalchemy")
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    print("缺少运行依赖：" + ", ".join(missing), file=sys.stderr)
    raise SystemExit(1)
PY
then
  echo "请运行：./start.sh --setup" >&2
  exit 1
fi

if [[ -n "${IMAGE_VLM_API_KEY:-}" ]]; then
  export IMAGE_VLM_BASE_URL="${IMAGE_VLM_BASE_URL:-https://dashscope.aliyuncs.com/compatible-mode/v1}"
  export IMAGE_VLM_MODEL="${IMAGE_VLM_MODEL:-qwen-vl-max}"
  echo "[config] VLM 已启用：${IMAGE_VLM_MODEL}"
else
  echo "[config] 未配置 IMAGE_VLM_API_KEY，将使用本地头像合规规则"
fi

if [[ -n "${https_proxy:-${HTTPS_PROXY:-}}" ]]; then
  echo "[config] 已检测到 HTTPS 代理"
fi

if [[ "$CHECK_ONLY" == true ]]; then
  echo "[check] 环境检查通过"
  exit 0
fi

if ! "$PYTHON" - "$HOST" "$PORT" <<'PY'
import socket
import sys

host, port = sys.argv[1], int(sys.argv[2])
probe_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
with socket.socket() as sock:
    sock.settimeout(0.3)
    if sock.connect_ex((probe_host, port)) == 0:
        print(f"端口已被占用：{host}:{port}", file=sys.stderr)
        raise SystemExit(1)
PY
then
  exit 1
fi

echo "[server] 前端：http://127.0.0.1:${PORT}/"
echo "[server] API：http://127.0.0.1:${PORT}/docs"
echo "[server] 按 Ctrl+C 停止"

cd "${ROOT_DIR}/backend"
exec "$PYTHON" -m uvicorn main:app \
  --host "$HOST" \
  --port "$PORT" \
  --log-level "$LOG_LEVEL"
