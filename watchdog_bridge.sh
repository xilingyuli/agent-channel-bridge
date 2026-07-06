#!/usr/bin/env bash
# Watchdog: 300秒后检查桥接层是否存活，若未运行则自动拉起
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
sleep 300
if ! pgrep -f "agent_channel_bridge" > /dev/null 2>&1; then
  echo "[$(date)] Watchdog: Bridge 未运行，自动拉起..." >> /tmp/watchdog_bridge.log
  "$SCRIPT_DIR/restart_bridge.sh" >> /tmp/watchdog_bridge.log 2>&1
fi
