#!/usr/bin/env bash
set -euo pipefail

# ====== OneBot Bridge - 进程管理 ======
# 管理 bridge.py（ACP 协议版）
# bridge.py 自己管理 ACP agent 子进程（stdio）
# bridge.sh 只负责启动/停止 bridge.py 本身
#
# 用法: ./bridge.sh {start|stop|status|restart}

BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="${BRIDGE_DIR}/pids"
LOG_DIR="${BRIDGE_DIR}/logs"
PID_FILE="${PID_DIR}/bridge.pid"

mkdir -p "$PID_DIR" "$LOG_DIR"

pidfile() { echo "${PID_DIR}/bridge.pid"; }
logfile() { echo "${LOG_DIR}/bridge.log"; }

cmd_start() {
    local pidf=$(pidfile)

    if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
        echo "  ⏩ Bridge 已在运行 (PID: $(cat "$pidf"))"
        return 0
    fi

    local logf=$(logfile)
    echo "🚀 启动 Bridge..."
    OPENCODE_ENABLE_EXA=1 PYTHONUNBUFFERED=1 nohup "${BRIDGE_DIR}/.venv/bin/python" -m agent_channel_bridge > "$logf" 2>&1 &
    local pid=$!
    echo "$pid" > "$pidf"

    # 等待启动确认
    sleep 5
    if kill -0 "$pid" 2>/dev/null; then
        echo "  ✅ Bridge 已启动 (PID: $pid)"
        echo "  📝 日志: $logf"
    else
        echo "  ❌ Bridge 启动失败，检查日志: $logf"
        tail -20 "$logf"
    fi
}

cmd_stop() {
    local pidf=$(pidfile)

    echo "🛑 停止 Bridge..."

    # 1. 先杀 PID 文件记录的进程（如果还有效）
    if [ -f "$pidf" ]; then
        local pid=$(cat "$pidf")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            for i in $(seq 1 10); do
                if ! kill -0 "$pid" 2>/dev/null; then break; fi
                sleep 1
            done
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pidf"
    fi

    # 2. 兜底：pkill 清理所有 agent_channel_bridge 残留进程（防止旧 PID 文件丢失导致孤儿进程）
    pkill -f "agent_channel_bridge" 2>/dev/null || true
    sleep 1

    echo "  ✅ Bridge 已停止"
}

cmd_status() {
    local pidf=$(pidfile)

    if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
        local pid=$(cat "$pidf")
        local uptime=$(ps -o etime= -p "$pid" 2>/dev/null | tr -d ' ')
        echo "  ✅ Bridge 运行中 (PID: $pid, 已运行: ${uptime:-?})"
    else
        echo "  ❌ Bridge 未运行"
    fi
}

cmd_restart() {
    echo "🔄 重启 Bridge..."
    cmd_stop
    sleep 1
    cmd_start
}

# ====== 入口 ======

case "${1:-help}" in
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    status)
        cmd_status
        ;;
    restart)
        cmd_restart
        ;;
    help|--help|-h)
        echo "用法: ./bridge.sh {start|stop|status|restart}"
        echo ""
        echo "命令:"
        echo "  start             启动 bridge（自动管理 ACP agent worker）"
        echo "  stop              停止 bridge"
        echo "  status            查看 bridge 状态"
        echo "  restart           重启 bridge"
        ;;
    *)
        echo "未知命令: $1 (使用 ./bridge.sh help)"
        exit 1
        ;;
esac
