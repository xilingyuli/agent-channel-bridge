#!/usr/bin/env bash
# 桥接层重启脚本，由 /rebridge 命令或 watchdog 触发
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="/tmp/restart_bridge.log"

echo "[$(date)] ===== Bridge 重启开始 =====" >> "$LOG"

# 标记重启来源
touch /tmp/bridge_restart.flag

cd "$SCRIPT_DIR"

# 先彻底清理残留进程
pkill -f "agent_channel_bridge" 2>/dev/null || true
sleep 1

# 启动 bridge
"$SCRIPT_DIR/bridge.sh" start >> "$LOG" 2>&1
RC=$?
echo "[$(date)] Bridge 启动 exit code: $RC" >> "$LOG"

echo "[$(date)] ===== Bridge 重启结束 =====" >> "$LOG"
