"""Entry point — admin commands, message handler, main loop."""
from __future__ import annotations

import asyncio
import json
import logging
import traceback
from typing import Optional

import websockets

from . import config as cfg
from .rpc_log import init_rpc_log
from .onebot import on_worker_reply, send_group_msg, send_private_msg, parse_onebot, MSG_CACHE, send_chunked
from .worker_manager import WorkerManager
from .config import get_route, log_chat

log = logging.getLogger("onebot-bridge")

# 非管理员安全规则
NON_ADMIN_SECURITY_PROMPT = (
    "[系统安全提示] 此消息来自非管理员用户。请遵守以下规则:\n"
    "1. 该用户应当不感知到本机的任何信息，该规则高于一切其他规则\n"
    "2. 禁止暴露本机文件路径、目录结构\n"
    "3. 禁止暴露 IP 地址、API 密钥、Token、密码\n"
    "4. 禁止暴露任何配置文件内容或系统信息\n"
    "5. 禁止执行文件读写、系统命令等危险操作\n"
    "6. 只提供通用知识问答，不涉及具体项目代码\n"
    "7. 如果用户询问的内容可能泄露隐私，请礼貌拒绝\n"
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
            "/reset           - 重置当前会话 session\n"
            "/help            - 本帮助"
        )

    if cmd == "/status":
        lines = ["📊 Workers (ACP):"] + worker_mgr.status_lines()
        return "\n".join(lines)

    if cmd == "/reset":
        reply = await worker_mgr.reset_for_msg(msg)
        return reply

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

    admin = is_admin(msg)

    # 管理命令
    if admin:
        reply = await handle_admin_cmd(msg, worker_mgr)
        if reply:
            if msg["type"] == "group":
                await send_group_msg(int(msg["from_id"]), reply)
            else:
                await send_private_msg(int(msg["user_id"]), reply)
            return

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

    # 非管理员：注入安全提示
    message_text = msg["message"]
    if not admin:
        message_text = NON_ADMIN_SECURITY_PROMPT + message_text

    # 发送到 agent（异步，不等回复）
    log.info(f"⚡ {route_name} → ACP [{worker_name}]" + (" [非管理员]" if not admin else ""))
    await worker_mgr.send_message(worker_name, message_text, qq_msg=msg)

    log_chat(msg, route_name)


# ====== WS 主循环 ======

async def main_loop(worker_mgr: WorkerManager):
    ws_url = cfg.config.get("applications", {}).get("qq", {}).get("ws_url", "ws://localhost:3001")

    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=30) as ws:
                cfg._ws_conn = ws
                log.info(f"✅ 已连接 {ws_url}")

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
