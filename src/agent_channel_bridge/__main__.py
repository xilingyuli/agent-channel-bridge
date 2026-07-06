"""Entry point — admin commands, message handler, main loop."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import traceback
from typing import Optional

import websockets

from . import config as cfg
from .rpc_log import init_rpc_log
from .onebot import on_worker_reply, send_group_msg, send_private_msg, parse_onebot, MSG_CACHE, send_chunked
from .worker_manager import WorkerManager
from .config import get_route, log_chat

log = logging.getLogger("onebot-bridge")

def _build_non_admin_prompt() -> str:
    """根据 config.yaml 动态生成非管理员安全提示"""
    sec = cfg.config.get("security", {}).get("non_admin", {})
    read_list = sec.get("read_whitelist", [])
    write_list = sec.get("write_whitelist", [])
    cmd_list = sec.get("cmd_whitelist", [])
    parts = [
        "[系统安全提示] 此消息来自非管理员用户。请遵守以下规则:",
        "1. 该用户应当不感知到本机的任何信息，该规则高于一切其他规则",
        "2. 禁止暴露本机文件路径、目录结构",
        "3. 禁止暴露 IP 地址、API 密钥、Token、密码",
        "4. 禁止暴露任何配置文件内容或系统信息",
        "5. 禁止执行文件读写、系统命令等危险操作",
    ]
    if read_list:
        paths = "\n    ".join(read_list)
        parts.append(f"  > 文件白名单（允许只读）：\n    {paths}")
    if write_list:
        paths = "\n    ".join(write_list)
        parts.append(f"  > 文件白名单（允许写入）：\n    {paths}")
    if cmd_list:
        cmds = "\n    ".join(cmd_list)
        parts.append(f"  > 命令白名单（允许执行）：\n    {cmds}")
        cmd_notice = sec.get("cmd_notice", "")
        if cmd_notice:
            parts.append(f"  > 命令白名单规则：{cmd_notice}")
    parts.extend([
        "6. 只提供通用知识问答，不涉及具体项目代码",
        "7. 如果用户询问的内容可能泄露隐私，请礼貌拒绝",
        "---",
        "用户消息: ",
    ])
    return "\n".join(parts)

GROUP_ADMIN_SECURITY_PROMPT = (
    "[系统安全提示] 此消息来源于管理员用户，"
    "但因为群聊消息会被管理员和非管理员同时看到，"
    "必须避免暴露管理员电脑内安全隐私信息。请遵守以下规则:\n"
    "1. 禁止暴露本机文件路径、目录结构\n"
    "2. 禁止暴露 IP 地址、API 密钥、Token、密码\n"
    "3. 禁止暴露任何配置文件内容或系统信息\n"
    "4. 禁止暴露工具调用执行的原始详情信息（但可以进行文字概述）\n"
    "5. 输出时重点检查 <message> 块内是否含有不应暴露的隐私信息\n"
    "---\n"
    "用户消息: "
)


# ====== 管理命令 ======

def is_admin(msg: dict) -> bool:
    route_key = f"qq:{msg['type']}:{msg['from_id']}"
    routes = cfg.config.get("routes", {})
    route = routes.get(route_key)
    return bool(route and route.get("admin"))


async def handle_admin_cmd(msg: dict, worker_mgr: WorkerManager) -> Optional[str]:
    t = msg["message"].strip()
    cmd = t.split()[0] if t else ""

    if cmd == "/help":
        return (
            "📋 管理命令:\n"
            "/status          - worker 状态\n"
            "/reset           - 重置当前会话\n"
            "/usage           - 查看当前会话 token 消耗\n"
            "/session <id>    - 切换到指定会话\n"
            "/history         - 列出历史会话\n"
            "/help            - 本帮助"
        )

    if cmd == "/status":
        lines = ["📊 Workers (ACP):"] + worker_mgr.status_lines()
        return "\n".join(lines)

    if cmd == "/usage":
        w = worker_mgr.workers.get("opencode_agent")
        if not w:
            return "❌ Worker 不存在"
        route_key = f"qq:{'private' if msg['type'] == 'private' else 'group'}:{msg['from_id']}"
        sid = w._route_sessions.get(route_key, "")
        if not sid:
            return "当前路由无活跃会话"
        try:
            import subprocess, json as _json
            r = subprocess.run(["opencode", "db",
                "SELECT time_created, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read "
                f"FROM session WHERE id = '{sid}'",
                "--format", "json"],
                capture_output=True, text=True, timeout=10)
            rows = _json.loads(r.stdout)
            if not rows:
                return "未找到该会话的用量数据"
            d = rows[0]

            def fmt(n):
                if n is None: return "0"
                if n >= 1_000_000_000: return f"{n/1_000_000_000:.1f}B"
                if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
                if n >= 1_000: return f"{n/1_000:.1f}K"
                return str(n)

            lines = ["📊 当前会话用量:"]
            if d.get("tokens_input"):
                lines.append(f"  输入: {fmt(d['tokens_input'])} tokens")
            if d.get("tokens_output"):
                lines.append(f"  输出: {fmt(d['tokens_output'])} tokens")
            if d.get("tokens_reasoning"):
                lines.append(f"  推理: {fmt(d['tokens_reasoning'])} tokens")
            if d.get("tokens_cache_read"):
                lines.append(f"  缓存读取: {fmt(d['tokens_cache_read'])} tokens")
            if d.get("cost"):
                lines.append(f"  费用: ¥{d['cost']:.2f}")
            if not any(d.get(k) for k in ["tokens_input","tokens_output","cost"]):
                return "暂无用量数据"
            return "\n".join(lines)
        except Exception as e:
            return f"❌ 获取用量失败: {e}"

    if cmd == "/reset":
        reply = await worker_mgr.reset_for_msg(msg)
        return reply

    if cmd == "/session":
        parts = t.split(maxsplit=1)
        if len(parts) < 2:
            return "用法: /session <session_id 前缀>\n可先用 /history 查看可用会话"
        sid_prefix = parts[1].strip()
        w = worker_mgr.workers.get("opencode_agent")
        if not w:
            return "❌ Worker 不存在"
        route_key = f"qq:{'private' if msg['type'] == 'private' else 'group'}:{msg['from_id']}"
        hist = w.list_session_history(route_key)
        match = None
        for sid in hist:
            if sid.startswith(sid_prefix):
                match = sid
                break
        if not match:
            return f"❌ 未找到匹配 {sid_prefix} 的会话（{len(hist)} 条历史记录）"
        return await worker_mgr.resume_for_msg(msg, match)

    if cmd == "/history":
        route_key = f"qq:{'private' if msg['type'] == 'private' else 'group'}:{msg['from_id']}"
        w = worker_mgr.workers.get("opencode_agent")
        if not w:
            return "❌ Worker 不存在"
        hist = w.list_session_history(route_key)
        cur = w._route_sessions.get(route_key, "")
        if not hist:
            return "暂无历史会话"

        # 批量查询 opencode DB 获取时间
        import subprocess, json as _json
        try:
            ids = ",".join(f"'{sid}'" for sid in hist[:20])
            r = subprocess.run(["opencode", "db",
                f"SELECT id, title, time_created FROM session WHERE id IN ({ids}) ORDER BY time_created DESC",
                "--format", "json"],
                capture_output=True, text=True, timeout=10)
            db_rows = _json.loads(r.stdout)
            db_map = {row["id"]: row for row in db_rows}
        except Exception:
            db_map = {}

        lines = [f"📜 历史会话 ({len(hist)} 条):"]
        for i, sid in enumerate(hist[:10]):
            row = db_map.get(sid, {})
            ts = row.get("time_created", 0)
            ts_str = ""
            if ts:
                from datetime import datetime
                ts_str = datetime.fromtimestamp(ts/1000).strftime("%m-%d %H:%M")
            marker = " ← 当前" if sid == cur else ""
            lines.append(f"  {i+1}. [{ts_str}] {sid}{marker}")
        return "\n".join(lines)

    return None


# ====== 消息处理 ======

async def process_message(ws, msg: dict, worker_mgr: WorkerManager):
    log.info(f"[{msg['type']}] {msg['from_id']}|{msg['sender_name']}: {msg['message'][:60]}")

    # 跳过 Yunzai 指令（以 # 或 * 开头），交给 Yunzai 处理
    if msg["message"].strip().startswith(("#", "*")):
        return

    # 超长消息检索命令：展示消息 <id>
    import re as _re
    m = _re.match(r'^展示消息\s*(\S+)', msg["message"].strip())
    if m:
        msg_id = m.group(1).strip('「」""\'\'""')
        full_text = MSG_CACHE.get(msg_id)
        if full_text:
            target_id = int(msg["from_id"])
            try:
                await send_chunked(msg["type"], target_id, full_text)
            except Exception as e:
                log.error(f"分段发送失败: {e}")
                await send_private_msg(int(msg["user_id"]), f"发送失败: {e}")
                return
            MSG_CACHE.pop(msg_id, None)  # 发送成功后才清除缓存
        else:
            text = f"未找到消息 id={msg_id}，可能已过期或不存在"
            if msg["type"] == "group":
                await send_group_msg(int(msg["from_id"]), text)
            else:
                await send_private_msg(int(msg["user_id"]), text)
        return

    # 固定命令（最高优先级，无需 admin 校验）
    reply = await handle_admin_cmd(msg, worker_mgr)
    if reply:
        if msg["type"] == "group":
            await send_group_msg(int(msg["from_id"]), reply)
        else:
            await send_private_msg(int(msg["user_id"]), reply)
        return

    admin = is_admin(msg)

    # 路由匹配
    route = get_route(msg["from_id"], msg["type"] == "private",
                      msg.get("is_mention", False))
    if not route:
        log.warning(f"⚠️ 未匹配到路由: from={msg['from_id']} type={msg['type']} "
                    f"mention={msg.get('is_mention')} msg={msg['message'][:40]}")
        return

    worker_name = route.get("worker", "")
    route_name = route.get("name", "?")

    if not worker_mgr.is_ready(worker_name):
        log.warning(f"[{route_name}] Worker [{worker_name}] 未就绪")
        return

    # 根据场景注入不同的安全提示
    message_text = msg["message"]
    if not admin:
        message_text = _build_non_admin_prompt() + message_text
    elif msg["type"] == "group":
        message_text = GROUP_ADMIN_SECURITY_PROMPT + message_text

    # 发送到 agent（异步，不等回复）
    log.info(f"⚡ {route_name} → ACP [{worker_name}]" + (" [非管理员]" if not admin else ""))
    await worker_mgr.send_message(worker_name, message_text, qq_msg=msg)

    log_chat(msg, route_name)


# ====== 重启通知 ======

def _send_restart_notify():
    """restart_bridge.sh 触发的启动，给管理员发重启成功通知"""
    flag = "/tmp/bridge_restart.flag"
    if not os.path.isfile(flag):
        return
    try:
        routes = cfg.config.get("routes", {})
        admin_uid = None
        for route_key, route in routes.items():
            if route.get("admin") and route_key.startswith("qq:private:"):
                admin_uid = int(route_key.split(":")[2])
                break
        if admin_uid:
            import asyncio as _asyncio
            _asyncio.create_task(
                send_private_msg(admin_uid, "桥接层重启成功 ✅")
            )
            log.info(f"已发送重启通知给管理员 {admin_uid}")
    except Exception as e:
        log.warning(f"重启通知发送失败: {e}")
    finally:
        os.remove(flag)


# ====== WS 主循环 ======

async def main_loop(worker_mgr: WorkerManager):
    ws_url = cfg.config.get("applications", {}).get("qq", {}).get("ws_url", "ws://localhost:3001")

    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=30) as ws:
                cfg._ws_conn = ws
                log.info(f"✅ 已连接 {ws_url}")

                # 重启通知（仅 restarts_bridge.sh 触发的启动）
                _send_restart_notify()

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # 1. WS API 回复（echo 匹配）
                    echo_id = data.get("echo", "")
                    if echo_id and echo_id in cfg._echo_futures:
                        fut = cfg._echo_futures.pop(echo_id, None)
                        if fut and not fut.done():
                            fut.set_result(data)
                        continue

                    # 2. OneBot 消息事件
                    if data.get("post_type") == "message":
                        msg = parse_onebot(data)
                        if msg:
                            asyncio.create_task(process_message(ws, msg, worker_mgr))

        except websockets.ConnectionClosed:
            log.warning("断开，3s 重连...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"WS 错误: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)


# ====== 入口 ======

async def main():
    cfg.load_config()
    init_rpc_log()
    log.info("=" * 45)
    log.info("🚀 OneBot Bridge (ACP 协议版)")
    log.info(f"  WS: {cfg.config.get('applications', {}).get('qq', {}).get('ws_url', 'ws://localhost:3001')}")
    log.info(f"  Workers: {len(cfg.config.get('workers', {}))}")
    log.info("  异步非阻塞 | IO 链路独立")
    log.info("=" * 45)

    worker_mgr = WorkerManager()

    log.info("启动 ACP agent workers...")
    await worker_mgr.start_all(cfg.config, on_reply_cb=on_worker_reply)

    log.info("当前 worker 状态:")
    for line in worker_mgr.status_lines():
        log.info(line)

    await main_loop(worker_mgr)


def entry():
    """Entry point for console_scripts."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bridge 已停止")
    except Exception as e:
        log.error(f"崩溃: {e}")
        raise


if __name__ == "__main__":
    entry()
