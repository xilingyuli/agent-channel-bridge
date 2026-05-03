"""OneBot v11 protocol — message building, parsing, and API calls."""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Optional

import websockets

from .config import config, _ws_conn, _echo_futures

log = logging.getLogger("onebot-bridge")


# ====== WS 回复 ======

async def send_api_action(action: str, params: dict = None,
                          ws: "websockets.WebSocketClientProtocol" = None) -> dict:
    global _ws_conn
    ws = ws or _ws_conn
    if ws is None:
        log.error("WS 未连接，无法发送 API 请求")
        return {}
    import asyncio
    import uuid
    echo_id = str(uuid.uuid4())[:12]
    fut = asyncio.get_event_loop().create_future()
    _echo_futures[echo_id] = fut
    payload = {"action": action, "params": params or {}, "echo": echo_id}
    await ws.send(json.dumps(payload, ensure_ascii=False))
    try:
        return await asyncio.wait_for(fut, timeout=10)
    except asyncio.TimeoutError:
        log.warning(f"API 超时: {action}")
        return {}


# ====== OneBot v11 消息段构建 ======

def _build_message_segments(text: str) -> list:
    segments = []
    # Image: MEDIA:/path/to/file
    medias = re.findall(r'MEDIA:([^\s]+)', text)
    remaining = re.sub(r'MEDIA:[^\s]+\s*', '', text).strip()

    for media_path in medias:
        if os.path.isfile(media_path):
            import base64
            _, ext = os.path.splitext(media_path)
            ext = ext.lstrip(".").lower()
            if ext in ("jpg", "jpeg"):
                mime = "image/jpeg"
            elif ext == "png":
                mime = "image/png"
            elif ext == "gif":
                mime = "image/gif"
            elif ext in ("webp",):
                mime = "image/webp"
            else:
                mime = "image/jpeg"
            try:
                with open(media_path, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                segments.append({
                    "type": "image",
                    "data": {"file": f"base64://{b64}"},
                })
                log.info(f"图片已转 base64: {media_path}")
            except Exception as e:
                log.warning(f"图片读取失败: {media_path} {e}")
                segments.append({"type": "text", "data": {"text": f"[图片加载失败] "}})
        else:
            segments.append({"type": "text", "data": {"text": f"[图片文件不存在: {media_path}] "}})

    # File: FILE:/path/to/file
    files = re.findall(r'FILE:([^\s]+)', remaining)
    remaining = re.sub(r'FILE:[^\s]+\s*', '', remaining).strip()

    for file_path in files:
        if os.path.isfile(file_path):
            import base64
            with open(file_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            file_name = os.path.basename(file_path)
            segments.append({
                "type": "file",
                "data": {"file": f"base64://{b64}", "name": file_name},
            })
            log.info(f"文件已转 base64: {file_path}")
        else:
            segments.append({"type": "text", "data": {"text": f"[文件不存在: {file_path}] "}})

    if remaining:
        segments.append({"type": "text", "data": {"text": remaining}})

    return segments or [{"type": "text", "data": {"text": ""}}]


async def send_group_msg(group_id: int, text: str) -> bool:
    segments = _build_message_segments(text)
    result = await send_api_action("send_group_msg", {
        "group_id": group_id,
        "message": segments,
    })
    return bool(result.get("status") == "ok")


async def send_private_msg(user_id: int, text: str) -> bool:
    segments = _build_message_segments(text)
    result = await send_api_action("send_private_msg", {
        "user_id": user_id,
        "message": segments,
    })
    return bool(result.get("status") == "ok")


# ====== 回复回调（IO 链路独立）=====

async def on_worker_reply(worker_key: str, agent_name: str,
                          reply_text: str, qq_msg: dict):
    if not reply_text or not qq_msg:
        return

    log.info(f"[{agent_name}] reply → {qq_msg.get('type','?')} "
             f"{qq_msg.get('from_id','?')}: {reply_text[:60]}")

    user_id = int(qq_msg.get("user_id", 0))
    from_id = qq_msg.get("from_id", "")

    try:
        if qq_msg["type"] == "group":
            await send_group_msg(int(from_id), reply_text)
        elif qq_msg["type"] == "private":
            await send_private_msg(user_id, reply_text)
    except Exception as e:
        log.error(f"发送回复失败: {e}")


# ====== OneBot v11 协议解析 ======

def parse_onebot(data: dict) -> Optional[dict]:
    from .config import config
    qq_cfg = config.get("applications", {}).get("qq", {})
    bot_qq = str(qq_cfg.get("bot_qq", ""))
    bot_name = str(qq_cfg.get("bot_name", ""))

    msg_type = data.get("message_type", "")
    user_id = str(data.get("user_id", ""))
    raw_msg = data.get("raw_message", "")

    if not raw_msg:
        return None

    sender = data.get("sender", {})
    sender_name = sender.get("nickname", "") or sender.get("card", "") or f"QQ{user_id}"

    is_mention = False
    message = raw_msg.strip()

    if msg_type == "group":
        group_id = str(data.get("group_id", ""))
        # Check for @mention
        for mention in data.get("message", []):
            if mention.get("type") == "at":
                target = str(mention.get("data", {}).get("qq", ""))
                if target == bot_qq or target == "all":
                    is_mention = True
                    break
        # Fallback: text-based @detection
        if not is_mention:
            if f"@{bot_name}" in message or f"@{bot_qq}" in message:
                is_mention = True
        # Strip @mention prefix
        if is_mention:
            message = re.sub(r'\u0040' + re.escape(bot_name), '', message).strip()
            message = re.sub(rf'\[CQ:at,qq={re.escape(bot_qq)}\]', '', message).strip()
        from_id = group_id
    elif msg_type == "private":
        from_id = user_id
    else:
        return None

    return {
        "type": msg_type,
        "user_id": user_id,
        "from_id": from_id,
        "sender_name": sender_name,
        "message": message,
        "is_mention": is_mention,
        "raw": data,
    }
