"""OneBot v11 protocol — message building, parsing, and API calls."""
from __future__ import annotations

import asyncio
import base64 as _b64
import json
import logging
import os
import re
import uuid
from typing import Optional

from . import config as cfg

log = logging.getLogger("onebot-bridge")


# ====== WS 回复 ======

async def send_api_action(action: str, params: dict = None,
                          ws: object = None) -> dict:
    ws = ws or cfg._ws_conn
    if ws is None:
        log.error("WS 未连接，无法发送 API 请求")
        return {}
    echo_id = str(uuid.uuid4())[:12]
    fut = asyncio.get_event_loop().create_future()
    cfg._echo_futures[echo_id] = fut
    payload = {"action": action, "params": params or {}, "echo": echo_id}
    await ws.send(json.dumps(payload, ensure_ascii=False))
    try:
        return await asyncio.wait_for(fut, timeout=10)
    except asyncio.TimeoutError:
        log.warning(f"API 超时: {action}")
        return {}


# ====== OneBot v11 消息段构建 ======

# 标签格式（支持在 message 内任意位置，URL 可跨行）
SEND_IMAGE_TAG_RE = re.compile(r"<img>(.*?)</img>", re.IGNORECASE | re.DOTALL)
SEND_AUDIO_TAG_RE = re.compile(r"<audio>(.*?)</audio>", re.IGNORECASE | re.DOTALL)
SEND_FILE_TAG_RE = re.compile(r"<file>(.*?)</file>", re.IGNORECASE | re.DOTALL)


def _build_message_segments(text: str) -> list:
    """将回复文本解析为 OneBot v11 消息段列表，支持标签格式和 @ 提及"""
    # 提取所有标签中的 URL（去除空白和换行）
    image_urls = [u.strip().replace("\n", "").replace("\r", "") for u in SEND_IMAGE_TAG_RE.findall(text)]
    audio_urls = [u.strip().replace("\n", "").replace("\r", "") for u in SEND_AUDIO_TAG_RE.findall(text)]
    file_urls = [u.strip().replace("\n", "").replace("\r", "") for u in SEND_FILE_TAG_RE.findall(text)]

    # 移除所有标签
    clean = text
    for pat in [SEND_IMAGE_TAG_RE, SEND_AUDIO_TAG_RE, SEND_FILE_TAG_RE]:
        clean = pat.sub("", clean)
    clean = clean.strip()

    segments = []
    if clean:
        # 拆分 @QQ号 为 OneBot at 段
        at_re = re.compile(r"@(\d{5,11})")
        last_end = 0
        for m in at_re.finditer(clean):
            if m.start() > last_end:
                segments.append({"type": "text", "data": {"text": clean[last_end:m.start()]}})
            segments.append({"type": "at", "data": {"qq": m.group(1)}})
            last_end = m.end()
        if last_end < len(clean):
            remaining = clean[last_end:].strip()
            if remaining:
                segments.append({"type": "text", "data": {"text": remaining[:2000]}})
        if last_end == 0:
            segments.append({"type": "text", "data": {"text": clean[:2000]}})
    for url in image_urls:
        if os.path.isfile(url):
            try:
                with open(url, "rb") as f:
                    raw = f.read()
                b64 = _b64.b64encode(raw).decode()
                segments.append({"type": "image", "data": {"file": f"base64://{b64}"}})
                log.info(f"📎 本地图片已转为 base64 ({len(raw)} bytes)")
            except Exception as e:
                log.warning(f"📎 本地图片读取失败: {e}")
                segments.append({"type": "text", "data": {"text": f"[图片读取失败: {url}]"}})
        else:
            segments.append({"type": "image", "data": {"url": url}})
    for url in audio_urls:
        segments.append({"type": "record", "data": {"url": url}})
    for url in file_urls:
        if os.path.isfile(url):
            try:
                with open(url, "rb") as f:
                    raw = f.read()
                fname = url.split("/")[-1][:64] or "file"
                b64 = _b64.b64encode(raw).decode()
                segments.append({"type": "file", "data": {"file": f"base64://{b64}", "name": fname}})
                log.info(f"📎 本地文件已转为 base64 ({len(raw)} bytes, name={fname})")
            except Exception as e:
                log.warning(f"📎 本地文件读取失败: {e}")
                segments.append({"type": "text", "data": {"text": f"[文件发送失败: {url}]"}})
        else:
            segments.append({"type": "file", "data": {"url": url, "name": url.split("/")[-1][:64] or "file"}})
    if not segments:
        segments.append({"type": "text", "data": {"text": "(无内容)"}})
    return segments


async def send_group_msg(group_id: int, text: str, at_user: str = "") -> bool:
    segments = _build_message_segments(text)
    if at_user and not text.lstrip().startswith("@"):
        segments.insert(0, {"type": "at", "data": {"qq": at_user}})
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


# ====== 超长消息缓存 ======

MSG_CACHE: dict[str, str] = {}  # msg_id → 完整文本
MAX_MSG_LEN = 1500
CHUNK_SIZE = 1800


async def send_chunked(msg_type: str, target_id: int, full_text: str):
    """将长文本分段发送（私聊或群聊）"""
    chunks = [full_text[i:i+CHUNK_SIZE] for i in range(0, len(full_text), CHUNK_SIZE)]
    send = send_private_msg if msg_type == "private" else send_group_msg
    for i, chunk in enumerate(chunks):
        prefix = f"[{i+1}/{len(chunks)}] " if len(chunks) > 1 else ""
        await send(target_id, prefix + chunk)
    log.info(f"📤 分段发送完成: {msg_type} {target_id}, {len(chunks)} 段, 共 {len(full_text)} 字")


# ====== 回复回调（IO 链路独立）=====

def _fix_group_at_spacing(text: str, target_user_id: str) -> str:
    """群聊回复第一行以 @QQ号 开头但缺少空格时自动补上。"""
    first_line = text.split("\n")[0]
    prefix = f"@{target_user_id}"
    if first_line.startswith(prefix):
        rest = first_line[len(prefix):]
        if rest and not rest.startswith(" "):
            text = prefix + " " + text[len(prefix):]
    return text


async def on_worker_reply(worker_key: str, agent_name: str,
                          reply_text: str, qq_msg: dict):
    if not reply_text or not qq_msg:
        return

    log.info(f"[{agent_name}] reply → {qq_msg.get('type','?')} "
             f"{qq_msg.get('from_id','?')}: {reply_text[:60]}")

    user_id = int(qq_msg.get("user_id", 0))
    from_id = qq_msg.get("from_id", "")
    msg_type = qq_msg["type"]

    # 群聊回复：自动补 @QQ号 后的空格
    if msg_type == "group":
        reply_text = _fix_group_at_spacing(reply_text, str(user_id))

    # 超长消息：缓存到本地，回复摘要
    if len(reply_text) > MAX_MSG_LEN:
        msg_id = str(uuid.uuid4())[:6]
        MSG_CACHE[msg_id] = reply_text
        preview = reply_text[:50].replace("\n", " ").replace("\r", "")
        short = (f"[消息过长({len(reply_text)}字)已缓存，id={msg_id}]\n"
                 f"前50字: {preview}...\n"
                 f"回复「展示消息 {msg_id}」查看完整内容")
        try:
            if msg_type == "group":
                if from_id and from_id != "TEST_USER_ID":
                    await send_group_msg(int(from_id), short, str(user_id))
            elif msg_type == "private":
                if user_id:
                    await send_private_msg(user_id, short)
        except Exception as e:
            log.error(f"发送超长预览失败: {e}")
        return

    try:
        if msg_type == "group":
            if not from_id or from_id == "TEST_USER_ID":
                log.warning(f"跳过测试群消息: from_id={from_id}")
                return
            ok = await send_group_msg(int(from_id), reply_text, str(user_id))
            if not ok:
                log.error(f"发送群聊消息失败: group_id={from_id}")
            if not ok:
                log.error(f"发送群聊消息失败: group_id={from_id}")
        elif msg_type == "private":
            if not user_id or user_id == 0:
                log.warning(f"跳过测试私聊: user_id={qq_msg.get('user_id')}")
                return
            ok = await send_private_msg(user_id, reply_text)
            if not ok:
                log.error(f"发送私聊消息失败: user_id={user_id}")
    except (ValueError, TypeError) as e:
        log.warning(f"跳过无效消息 (user_id={qq_msg.get('user_id')} from_id={from_id}): {e}")
    except Exception as e:
        log.error(f"发送回复失败: {e}")


# ====== OneBot v11 协议解析 ======

def parse_onebot(data: dict) -> Optional[dict]:
    qq_cfg = cfg.config.get("applications", {}).get("qq", {})
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

    # 提取 CQ reply 信息
    reply_id = None
    reply_match = re.match(r'^\[CQ:reply,id=(\d+)\](.*)', message)
    if reply_match:
        reply_id = reply_match.group(1)
        message = reply_match.group(2).strip()

    if msg_type == "group":
        group_id = str(data.get("group_id", ""))
        # Check for @mention via CQ at
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
        "reply_id": reply_id,
        "raw": data,
    }
