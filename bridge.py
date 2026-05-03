#!/usr/bin/env python3
"""
OneBot Bridge — ACP 协议版
==========================
通过 OneBot v11 协议连接 Napcat，将 QQ 消息路由到 ACP agent worker。

架构:
  QQ → Napcat WS → bridge
       ↓ 路由
  ACP agent stdio (JSON-RPC) → session/prompt
       ↓ (异步, 不等待)
  session/update ← agent 回复
       ↓ 收集完整回复 (等待 step_finish)
  bridge → WS → QQ 回复

IO 链路完全独立，不互相阻塞。
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime
from typing import Optional

# JSON RPC 文件日志
_rpc_log_file: Optional[str] = None
_rpc_log_fh: Optional[object] = None

def init_rpc_log():
    global _rpc_log_file, _rpc_log_fh
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    _rpc_log_file = os.path.join(log_dir, "rpc.jsonl")
    _rpc_log_fh = open(_rpc_log_file, "a", buffering=1)
    log.info(f"📋 RPC 日志: {_rpc_log_file}")

def log_rpc(worker: str, direction: str, data: dict):
    """direction: '>>' (发送) 或 '<<' (接收)"""
    if _rpc_log_fh is None:
        return
    try:
        record = {
            "ts": datetime.now().isoformat(),
            "worker": worker,
            "dir": direction,
            "data": data,
        }
        _rpc_log_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass

import yaml

try:
    import websockets
except ImportError:
    print("需要安装 websockets: pip install websockets")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")
CHAT_LOG_DIR = os.path.join(BASE_DIR, "chat_logs")

os.makedirs(CHAT_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("onebot-bridge")

config: dict = {}
_ws_conn: Optional['websockets.WebSocketClientProtocol'] = None
_echo_futures: dict[str, asyncio.Future] = {}  # echo_id → Future（WS回复匹配）


# ====== ACP Worker 通信 ======

_pending_requests: dict[str, asyncio.Future] = {}
_session_buf: dict[str, str] = {}  # session_id → 累积文本


class AcpWorker:
    def __init__(self, key: str, work_dir: str, name: str = "", start_command: str = ""):
        self.key = key
        self.name = name
        self.work_dir = work_dir
        self.agent_name = name
        self.start_command = start_command
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.session_id: Optional[str] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._connected = False
        self._msg_id = 0
        # 会话路由: route_key → session_id
        self._route_sessions: dict[str, str] = {}
        # sessionId → 最后一条 qq_msg（用于回复路由）
        self._last_qq_msg: dict[str, dict] = {}
        # sessionId → 积压消息列表 [(text, qq_msg), ...]
        self._pending_msgs: dict[str, list[tuple[str, dict]]] = {}
        # msg_id → sessionId (用来在 prompt result 时反查 session)
        self._prompt_msg_map: dict[str, str] = {}
        # 回调: on_reply(worker_name, reply_text, qq_msg)
        self.on_reply = None
        # session 持久化路径
        self._session_file = os.path.join(work_dir, ".bridge_sessions.json")

    def _save_sessions(self):
        """保存 session 映射到文件"""
        import json as _json
        try:
            data = {rk: sid for rk, sid in self._route_sessions.items()}
            with open(self._session_file, "w") as f:
                _json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            log.warning(f"[{self.agent_name}] session 持久化写入失败: {e}")

    async def _restore_sessions(self):
        """从文件恢复 session，逐个尝试 resume"""
        import json as _json
        try:
            if not os.path.isfile(self._session_file):
                return
            with open(self._session_file) as f:
                data = _json.load(f)
            if not isinstance(data, dict):
                return
            restored = {}
            for route_key, sid in data.items():
                try:
                    await self._send_request("session/resume", {
                        "sessionId": sid,
                        "cwd": self.work_dir,
                    })
                    restored[route_key] = sid
                    log.info(f"[{self.agent_name}] 🔄 恢复 Session [{route_key}]: {sid}")
                except Exception as e:
                    log.info(f"[{self.agent_name}] ⏭ Session [{route_key}] 恢复失败，后续会重建: {e}")
            if restored:
                self._route_sessions.update(restored)
                log.info(f"[{self.agent_name}] ✅ 恢复 {len(restored)}/{len(data)} 个 session")
        except Exception as e:
            log.warning(f"[{self.agent_name}] session 恢复过程异常: {e}")

    async def start(self):
        if not self.start_command:
            log.error(f"[{self.agent_name}] ❌ start_command 未配置")
            return
        cmd = self.start_command.split()
        log.info(f"[{self.agent_name}] 启动: {' '.join(cmd)}")

        self.proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=self.work_dir,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "HOME": os.environ.get("HOME", "/home/ubuntu")},
        )

        self._stderr_task = asyncio.create_task(self._read_stderr())
        self._reader_task = asyncio.create_task(self._read_stdout())

        await self._send_request("initialize", {
            "protocolVersion": 6,
            "clientCapabilities": {
                "fs": {"readTextFile": True, "writeTextFile": True},
                "terminals": {"create": True, "output": True, "release": True,
                              "waitForExit": True, "kill": True},
                "prompts": {"image": True},
            },
            "clientInfo": {
                "name": "onebot-bridge",
                "title": "OneBot Bridge",
                "version": "1.0.0",
            },
        })
        log.info(f"[{self.agent_name}] ✅ ACP 初始化完成")
        self._connected = True

        # 尝试恢复持久化的 session
        await self._restore_sessions()

    async def _read_stderr(self):
        try:
            while self.proc and self.proc.stderr and not self.proc.stderr.at_eof():
                line = await self.proc.stderr.readline()
                if line:
                    text = line.decode(errors='replace').strip()
                    if text:
                        log.info(f"[{self.agent_name} stderr] {text}")
        except Exception:
            pass

    async def _read_stdout(self):
        try:
            buf = b""
            while self.proc and self.proc.stdout and not self.proc.stdout.at_eof():
                chunk = await self.proc.stdout.read(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        await self._handle_message(msg)
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            log.error(f"[{self.agent_name}] stdout 读取错误: {e}")

    async def _handle_message(self, msg: dict):
        log_rpc(self.agent_name, "<<", msg)
        # DEBUG: 打印所有消息
        if "id" in msg:
            log.info(f"[{self.agent_name}] 📩 RPC id={msg['id']} keys={list(msg.keys())} {'result' in msg} has_stopReason={'stopReason' in msg.get('result',{})}")
        elif msg.get("method") == "session/update":
            ut = msg.get("params", {}).get("update", {}).get("sessionUpdate", "")
            if ut in ("agent_message_chunk", "step_finish", "tool_call_start"):
                log.info(f"[{self.agent_name}] 📩 UPDATE type={ut}")

        # JSON-RPC 回复
        if "id" in msg:
            msg_id = str(msg["id"])
            is_prompt_result = (
                msg.get("id") is not None
                and "result" in msg
                and isinstance(msg["result"], dict)
                and "stopReason" in msg["result"]
            )
            
            if is_prompt_result:
                # session/prompt 的 result 中包含 stopReason → 回复完成
                reason = msg["result"].get("stopReason", "")
                usage = msg["result"].get("usage", {})
                cost = msg["result"].get("cost", 0)
                log.info(f"[{self.agent_name}] 🏁 prompt 完成 (reason={reason}, cost={cost})")
                # 清除 prompt_msg_map 记录
                self._prompt_msg_map.pop(msg_id, None)
            
            # 普通 RPC 回复
            fut = _pending_requests.pop(msg_id, None)
            if fut and not fut.done():
                if "result" in msg:
                    fut.set_result(msg["result"])
                elif "error" in msg:
                    fut.set_exception(
                        Exception(msg["error"].get("message", str(msg["error"])))
                    )
            return

        # session/update 通知（收集文本块）
        if msg.get("method") == "session/update":
            await self._handle_session_update(msg.get("params", {}))

    async def _handle_session_update(self, params: dict):
        update = params.get("update", {})
        update_type = update.get("sessionUpdate", "")
        content = update.get("content", {})

        # 从通知中获取 sessionId
        sid = params.get("sessionId", "")
        if not sid:
            return

        # 只关注最终输出的文本块
        if update_type == "agent_message_chunk":
            if content.get("type") == "text":
                text = content.get("text", "")
                clean = text
                for ch in ["┃", "╹", "▣", "■", "▌", "▐", "▀", "▄", "░", "▒", "▓",
                           "│", "║", "═", "╔", "╗", "╚", "╝", "╠", "╣", "╦", "╩", "╬"]:
                    clean = clean.replace(ch, "")
                if clean.strip():
                    _session_buf.setdefault(sid, "")
                    _session_buf[sid] += clean

                    # 检测到 </message> → 立即发送
                    if "</message>" in _session_buf[sid]:
                        full = _session_buf.pop(sid, "")
                        import re as _re
                        msgs = _re.findall(r'<message>(.*?)</message>', full, _re.DOTALL)
                        qq_msg = self._last_qq_msg.get(sid)
                        for mtext in msgs:
                            mtext = mtext.strip()
                            if not mtext:
                                continue
                            log.info(f"[{self.agent_name}] 📬 条: {mtext[:60]}")
                            if qq_msg and self.on_reply:
                                asyncio.create_task(self.on_reply(
                                    self.name, self.agent_name, mtext, qq_msg
                                ))

    async def send_message(self, text: str, qq_msg: dict = None):
        """发送消息到 agent，立即返回，不等待回复"""
        if not self._connected:
            log.warning(f"[{self.agent_name}] ACP 未就绪")
            return

        # 按来源确定 route_key
        if qq_msg:
            is_private = qq_msg.get("type") == "private"
            route_key = f"qq:private:{qq_msg['user_id']}" if is_private else f"qq:group:{qq_msg['from_id']}"
        else:
            route_key = "_default"

        # 获取或创建 session
        sid = self._route_sessions.get(route_key)
        if not sid:
            try:
                result = await self._send_request("session/new", {
                    "cwd": self.work_dir,
                    "mcpServers": [],
                })
                sid = result["sessionId"]
            except Exception as e:
                log.error(f"[{self.agent_name}] ❌ session/new 失败: {e}", exc_info=True)
                return
            self._route_sessions[route_key] = sid
            self._save_sessions()
            log.info(f"[{self.agent_name}] 📝 新 Session [{route_key}]: {sid}")

        self._last_qq_msg[sid] = qq_msg or {}
        _session_buf.pop(sid, None)  # 清除上一轮残留

        # 检查 session 是否忙碌（有正在处理的 prompt）
        if sid in self._prompt_msg_map.values():
            # session 正在处理中 → close 旧 session，重建新 session
            log.info(f"[{self.agent_name}] 🔌 Session [{route_key}] 忙碌，准备打断重建")
            try:
                await self._send_request("session/close", {"sessionId": sid})
            except Exception as e:
                log.warning(f"[{self.agent_name}] session/close [{route_key}] 失败: {e}")
            del self._route_sessions[route_key]
            self._pending_msgs.pop(sid, None)
            _session_buf.pop(sid, None)
            for mid in list(self._prompt_msg_map.keys()):
                if self._prompt_msg_map[mid] == sid:
                    del self._prompt_msg_map[mid]

            # 重建新 session
            try:
                result = await self._send_request("session/new", {
                    "cwd": self.work_dir,
                    "mcpServers": [],
                })
                sid = result["sessionId"]
            except Exception as e:
                log.error(f"[{self.agent_name}] ❌ session/new 失败: {e}", exc_info=True)
                return
            self._route_sessions[route_key] = sid
            self._save_sessions()
            log.info(f"[{self.agent_name}] 📝 重建 Session [{route_key}]: {sid}")

        self._last_qq_msg[sid] = qq_msg or {}
        _session_buf.pop(sid, None)  # 清除上一轮残留

        await self._do_send_prompt(sid, route_key, text, qq_msg)

    async def _do_send_prompt(self, sid: str, route_key: str, text: str, qq_msg: dict = None):
        """构造系统上下文并发起 session/prompt，同时记录 prompt msg_id → sessionId 映射"""
        # ===== 构造系统上下文 + 用户消息 =====
        ctx_lines = ["<message>"]
        ctx_lines.append("<tips>")

        # 平台信息
        src = "群聊" if qq_msg and qq_msg.get("type") == "group" else "私聊"
        sender = qq_msg.get("sender_name", "用户") if qq_msg else "用户"
        user_id = qq_msg.get("user_id", "") if qq_msg else ""
        ctx_lines.append(f"你正在通过QQ和用户对话。来源: {src}，发送者: {sender}。")
        if qq_msg and qq_msg.get("type") == "group" and user_id:
            ctx_lines.append(f"⚠️ 重要：在群聊中回复时，第一行必须用 @用户QQ号 开头！否则用户收不到提示。")
            ctx_lines.append(f"   例如回复 \"@{user_id} 你好呀～\"")

        # 图片
        if qq_msg and qq_msg.get("has_image"):
            ctx_lines.append(f"用户同时发送了 {len(qq_msg.get('images', []))} 张图片:")
            for i, img in enumerate(qq_msg.get("images", [])):
                url = img.get("url", "")
                if url:
                    ctx_lines.append(f"  图片{i+1}: {url}")

        # 特殊格式指令
        ctx_lines.append("")
        ctx_lines.append("【回复规则】")
        ctx_lines.append("  1. 每条独立回复用 <message> 包裹，支持一次输出多条：")
        ctx_lines.append("     <message>")
        ctx_lines.append("     第一条回复")
        ctx_lines.append("     </message>")
        ctx_lines.append("     <message>")
        ctx_lines.append("     第二条回复")
        ctx_lines.append("     </message>")
        ctx_lines.append("  2. 每条 <message> 输出后立即发送给用户，无需等待")
        ctx_lines.append("  3. 群聊时每条 <message> 第一行必须 @用户QQ号")
        ctx_lines.append("  4. 思考过程、内部推理不要用 <message> 包裹，不会发给用户")
        ctx_lines.append("")
        ctx_lines.append("【发送图片/文件/语音 - 标签格式】")
        ctx_lines.append("  1. 在 <message> 内的任意位置插入标签即可发送媒体：")
        ctx_lines.append("     <message>这是查询结果<img>https://example.com/result.png</img></message>")
        ctx_lines.append("  2. <img>URL</img> — 发送图片")
        ctx_lines.append("  3. <audio>URL</audio> — 发送语音")
        ctx_lines.append("  4. <file>URL</file> — 发送文件")
        ctx_lines.append("  5. ⚠️ 标签可以放在 message 内的任何位置，文字前后中间都行")
        ctx_lines.append("  6. ⚠️ 如果你用 webfetch/grab/curl 等工具下载了图片或生成了本地图片文件：")
        ctx_lines.append("     • <img>https://example.com/pic.png</img> — 远程 URL")
        ctx_lines.append("     • <img>/absolute/path/to/file.png</img> — 本地绝对路径（自动转 base64 发送）")
        ctx_lines.append("     • <img>base64://iVBORw0KGgo...</img> — base64 编码（太长不建议）")
        ctx_lines.append("")
        ctx_lines.append("【同步节奏建议】")
        ctx_lines.append("  1. 可以在一次回复中连续输出多条 <message>，每条都会逐一发给用户")
        ctx_lines.append("  2. AI 可以同时发送多条 <message>，用户可以更快地看到进展")
        ctx_lines.append("  3. 长时间工作时：每隔几秒输出一条 <message> 同步进度")
        ctx_lines.append("  4. 遇到问题时：及时输出多条 <message> 告知用户并询问意见")
        ctx_lines.append("  5. 代码执行慢或工具卡住时：先输出一条告知用户 '正在执行，请稍候'")

        ctx_lines.append("</tips>")
        ctx_lines.append("")
        ctx_lines.append(text)
        ctx_lines.append("</message>")

        prompt_text = "\n".join(ctx_lines)

        # 用 request 发 prompt，但后台等 result，不阻塞这里
        self._send_request_bg("session/prompt", {
            "sessionId": sid,
            "prompt": [{"type": "text", "text": prompt_text}],
        }, sid=sid)
        log.info(f"[{self.agent_name}] 📤 消息已发送: {text[:60]}{' 📷' if qq_msg and qq_msg.get('has_image') else ''}")

    def _send_request_bg(self, method: str, params: dict, sid: str = ""):
        """后台发送 JSON-RPC 请求，不等待回复"""
        self._msg_id += 1
        msg_id = str(self._msg_id)
        if sid and method == "session/prompt":
            self._prompt_msg_map[msg_id] = sid
        msg = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "method": method,
            "params": params,
        }
        log_rpc(self.agent_name, ">>", msg)
        if self.proc and self.proc.stdin:
            self.proc.stdin.write((json.dumps(msg) + "\n").encode())
            asyncio.create_task(self.proc.stdin.drain())

    async def _send_request(self, method: str, params: dict) -> dict:
        self._msg_id += 1
        msg_id = str(self._msg_id)
        msg = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "method": method,
            "params": params,
        }
        log_rpc(self.agent_name, ">>", msg)
        fut = asyncio.get_event_loop().create_future()
        _pending_requests[msg_id] = fut

        if self.proc and self.proc.stdin:
            self.proc.stdin.write((json.dumps(msg) + "\n").encode())
            await self.proc.stdin.drain()

        try:
            return await asyncio.wait_for(fut, timeout=30)
        except asyncio.TimeoutError:
            _pending_requests.pop(msg_id, None)
            raise TimeoutError(f"[{self.agent_name}] RPC 超时: {method}")

    async def _send_notification(self, method: str, params: dict):
        msg = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }
        log_rpc(self.agent_name, ">>", msg)
        if self.proc and self.proc.stdin:
            self.proc.stdin.write((json.dumps(msg) + "\n").encode())
            await self.proc.stdin.drain()

    async def reset_sessions(self) -> dict:
        """关闭所有 session 并清空路由映射"""
        self._save_sessions()  # 先保存当前状态，万一关机恢复
        closed = 0
        errors = 0
        for route_key, sid in list(self._route_sessions.items()):
            try:
                await self._send_request("session/close", {
                    "sessionId": sid,
                })
                closed += 1
            except Exception as e:
                log.warning(f"[{self.agent_name}] session/close [{route_key}] 失败: {e}")
                errors += 1
            del self._route_sessions[route_key]
        self._save_sessions()
        log.info(f"[{self.agent_name}] 🧹 已重置 {closed} 个 session{'，' + str(errors) + ' 个失败' if errors else ''}")
        return {"closed": closed, "errors": errors}

    async def reset_route_session(self, route_key: str) -> bool:
        """关闭指定 route_key 的 session"""
        sid = self._route_sessions.pop(route_key, None)
        if not sid:
            return False
        try:
            await self._send_request("session/close", {
                "sessionId": sid,
            })
            self._save_sessions()
            log.info(f"[{self.agent_name}] 🧹 已重置 session [{route_key}]: {sid}")
            return True
        except Exception as e:
            log.warning(f"[{self.agent_name}] session/close [{route_key}] 失败: {e}")
            return False

    @property
    def connected(self) -> bool:
        return self._connected

    async def stop(self):
        self._connected = False
        for task in [self._reader_task, self._stderr_task]:
            if task:
                task.cancel()
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.proc.kill()


# ====== Worker 管理器 ======

class WorkerManager:
    def __init__(self):
        self._workers: dict[str, AcpWorker] = {}

    async def start_all(self, cfg: dict, on_reply_cb=None):
        for key, wc in cfg.get("workers", {}).items():
            await self._start_one(key, wc, on_reply_cb)

    async def _start_one(self, key: str, wc: dict, on_reply_cb=None):
        work_dir = os.path.expanduser(wc.get("work_dir", ""))
        agent_name = wc.get("name", key)
        worker = AcpWorker(key, work_dir, agent_name, wc.get("start_command", ""))
        if on_reply_cb:
            worker.on_reply = on_reply_cb
        try:
            await worker.start()
            self._workers[key] = worker
            log.info(f"[{agent_name}] ✅ Worker 启动完成")
        except Exception as e:
            log.error(f"[{agent_name}] ❌ 启动失败: {e}")

    async def send_message(self, worker_key: str, text: str, qq_msg: dict = None):
        worker = self._workers.get(worker_key)
        if not worker or not worker.connected:
            log.warning(f"[{worker_key}] Worker 未就绪")
            return
        await worker.send_message(text, qq_msg)

    async def stop_all(self):
        for key, worker in self._workers.items():
            log.info(f"[{key}] 🛑 停止...")
            await worker.stop()
        self._workers.clear()

    def status_lines(self) -> list[str]:
        lines = []
        for key, worker in self._workers.items():
            status = "✅" if worker.connected else "❌"
            lines.append(f"  {status} {worker.agent_name}")
        return lines

    def is_ready(self, worker_key: str) -> bool:
        w = self._workers.get(worker_key)
        return w is not None and w.connected

    async def reset_for_msg(self, msg: dict) -> Optional[str]:
        """根据消息来源重置对应的 session，返回描述文字"""
        route = get_route(msg["from_id"], msg["type"] == "private",
                          msg.get("is_mention", False))
        if not route:
            return "❌ 未匹配到路由"
        worker_name = route.get("worker", "")
        worker = self._workers.get(worker_name)
        if not worker:
            return f"❌ Worker [{worker_name}] 未找到"

        is_private = msg["type"] == "private"
        route_key = f"qq:private:{msg['user_id']}" if is_private else f"qq:group:{msg['from_id']}"
        ok = await worker.reset_route_session(route_key)
        if ok:
            return f"✅ [{route.get('name', '?')}] session 已重置"
        return "✅ session 已清空（无活跃 session）"


# ====== 配置加载 ======

def load_config():
    global config
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    log.info(f"配置已加载: {len(config.get('workers', {}))} 个 worker")


# ====== 路由匹配 ======

def get_route(from_id: str, is_private: bool = False,
              is_mention: bool = False) -> Optional[dict]:
    defaults = config.get("default", {})
    route_key = f"qq:private:{from_id}" if is_private else f"qq:group:{from_id}"
    routes = config.get("routes", {})

    if route_key in routes:
        if is_private or is_mention:
            r = routes[route_key]
            worker_name = r.get("worker", defaults.get("worker", ""))
            return {"name": r.get("name", route_key), "worker": worker_name}

    if is_private:
        worker_name = defaults.get("worker", "")
        if worker_name:
            return {"name": "默认私聊", "worker": worker_name}

    if not is_private and is_mention:
        worker_name = defaults.get("worker", "")
        if worker_name:
            return {"name": "默认群聊@", "worker": worker_name}

    return None


# ====== WS 回复 ======

async def send_api_action(action: str, params: dict = None,
                          timeout: float = 10.0) -> Optional[dict]:
    global _ws_conn, _echo_futures
    try:
        closed = _ws_conn is None or _ws_conn.close_code is not None
    except Exception:
        closed = True
    if closed:
        return None
    echo_id = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    req = {"action": action, "params": params or {}, "echo": echo_id}
    try:
        fut = asyncio.get_event_loop().create_future()
        _echo_futures[echo_id] = fut
        await _ws_conn.send(json.dumps(req))
        data = await asyncio.wait_for(fut, timeout=timeout)
        return data
    except asyncio.TimeoutError:
        _echo_futures.pop(echo_id, None)
        return None
    except Exception:
        _echo_futures.pop(echo_id, None)
        return None


# ====== OneBot v11 消息段构建 ======

# 标签格式（支持在 message 内任意位置，URL 可跨行）
SEND_IMAGE_TAG_RE = re.compile(r"<img>(.*?)</img>", re.IGNORECASE | re.DOTALL)
SEND_AUDIO_TAG_RE = re.compile(r"<audio>(.*?)</audio>", re.IGNORECASE | re.DOTALL)
SEND_FILE_TAG_RE = re.compile(r"<file>(.*?)</file>", re.IGNORECASE | re.DOTALL)


def _build_message_segments(text: str) -> list:
    """将回复文本解析为 OneBot v11 消息段列表，支持标签格式"""
    import os as _os
    import base64 as _b64

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
        segments.append({"type": "text", "data": {"text": clean[:2000]}})
    for url in image_urls:
        if _os.path.isfile(url):
            # 本地文件 → base64
            try:
                with open(url, "rb") as f:
                    raw = f.read()
                # 根据文件头判断 MIME
                if raw[:4] == b"\x89PNG":
                    mime = "image/png"
                elif raw[:2] in (b"\xff\xd8",):
                    mime = "image/jpeg"
                elif raw[:4] == b"RIFF":
                    mime = "image/webp"
                elif raw[:2] == b"BM":
                    mime = "image/bmp"
                else:
                    mime = "image/png"
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
        if _os.path.isfile(url):
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


async def send_group_msg(group_id: int, text: str) -> bool:
    segments = _build_message_segments(text)
    result = await send_api_action("send_group_msg", {
        "group_id": group_id,
        "message": segments,
    })
    return result is not None and result.get("status") == "ok"


async def send_private_msg(user_id: int, text: str) -> bool:
    segments = _build_message_segments(text)
    result = await send_api_action("send_private_msg", {
        "user_id": user_id,
        "message": segments,
    })
    return result is not None and result.get("status") == "ok"


# ====== 回复回调（IO 链路独立）=====

async def on_worker_reply(worker_key: str, agent_name: str,
                          reply_text: str, qq_msg: dict):
    """ACP agent 回复后回调 — 发回 QQ，完全独立于消息接收 IO"""
    log.info(f"[{agent_name}] 📬 发回 QQ: {reply_text[:80]}")
    try:
        if qq_msg["type"] == "group":
            ok = await send_group_msg(int(qq_msg["from_id"]), reply_text)
        else:
            ok = await send_private_msg(int(qq_msg["user_id"]), reply_text)
        if ok:
            log.info(f"✅ [{agent_name}] QQ 回复成功")
        else:
            log.warning(f"❌ [{agent_name}] QQ 回复失败 (send_api_action 返回空或非ok)")
    except Exception as e:
        log.error(f"[{agent_name}] QQ 回复异常: {e}")


# ====== OneBot v11 协议解析 ======

def parse_onebot(data: dict) -> Optional[dict]:
    if data.get("post_type") != "message":
        return None
    uid = str(data.get("user_id", ""))
    gid = str(data.get("group_id", ""))
    sender = data.get("sender", {})
    mtype = data.get("message_type", "")
    mid = str(data.get("message_id", ""))
    raw = data.get("message", "")

    parts = []
    images = []
    has_image = False
    if isinstance(raw, list):
        for seg in raw:
            seg_type = seg.get("type", "")
            seg_data = seg.get("data", {})
            if seg_type == "text":
                parts.append(seg_data.get("text", ""))
            elif seg_type == "image":
                has_image = True
                images.append({
                    "file": seg_data.get("file", ""),
                    "url": seg_data.get("url", ""),
                    "file_size": seg_data.get("file_size", 0),
                })
                parts.append(f"[图片: {seg_data.get('url', seg_data.get('file', '未知'))}]")
    else:
        parts.append(str(raw))
    text = " ".join(parts).strip()
    if not text:
        return None

    app = config.get("applications", {}).get("qq", {})
    bot_qq = app.get("bot_qq", "")
    bot_name = app.get("bot_name", "")
    is_mention = False
    if mtype == "group":
        if isinstance(raw, list):
            for seg in raw:
                if seg.get("type") == "at" and str(seg.get("data", {}).get("qq", "")) == bot_qq:
                    is_mention = True
        else:
            # 文本格式消息：检测 @机器人名或 @机器人QQ
            if bot_name and f"@{bot_name}" in text:
                is_mention = True
            elif f"@{bot_qq}" in text or f"[CQ:at,qq={bot_qq}]" in text:
                is_mention = True

    return {
        "type": "group" if mtype == "group" else "private",
        "from_id": gid if mtype == "group" else uid,
        "user_id": uid,
        "sender_name": sender.get("nickname", "") or sender.get("card", ""),
        "message": text,
        "message_id": mid,
        "is_mention": is_mention,
        "has_image": has_image,
        "images": images,
    }


def is_admin(msg: dict) -> bool:
    """按当前消息的路由在 routes 里是否有 admin: true"""
    route_key = f"qq:private:{msg['user_id']}" if msg.get("type") == "private" else f"qq:group:{msg['from_id']}"
    route = config.get("routes", {}).get(route_key, {})
    return route.get("admin", False)


# ====== 管理命令 ======

async def handle_admin_cmd(msg: dict, worker_mgr: WorkerManager) -> Optional[str]:
    t = msg["message"].strip()
    cmd = t.split()[0] if t else ""

    if cmd == "/help":
        return (
            "📋 管理命令:\n"
            "/status          - worker 状态\n"
            # "/reload" removed — restart bridge to reload config
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


# ====== 聊天日志 ======

def log_chat(msg: dict, route_name: str = ""):
    date = datetime.now().strftime("%Y%m%d")
    path = os.path.join(CHAT_LOG_DIR, f"{date}.log")
    entry = {
        "time": datetime.now().isoformat(),
        "type": msg["type"],
        "from": msg["from_id"],
        "sender": msg["sender_name"],
        "user": msg["user_id"],
        "route": route_name,
        "msg": msg["message"],
    }
    with open(path, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ====== 消息处理 ======

async def process_message(ws, msg: dict, worker_mgr: WorkerManager):
    log.info(f"[{msg['type']}] {msg['from_id']}|{msg['sender_name']}: {msg['message'][:60]}")

    # 管理命令
    if is_admin(msg):
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
        log.warning(f"⚠️ 未匹配到路由: from={msg['from_id']} type={msg['type']} mention={msg.get('is_mention')} msg={msg['message'][:40]}")
        return

    worker_name = route.get("worker", "")
    route_name = route.get("name", "?")

    if not worker_mgr.is_ready(worker_name):
        log.warning(f"[{route_name}] Worker [{worker_name}] 未就绪")
        return

    # 发送到 agent（异步，不等回复）
    log.info(f"⚡ {route_name} → ACP [{worker_name}]")
    await worker_mgr.send_message(worker_name, msg["message"], qq_msg=msg)

    log_chat(msg, route_name)


# ====== WS 主循环 ======

async def main_loop(worker_mgr: WorkerManager):
    global _ws_conn, _echo_futures
    ws_url = config.get("applications", {}).get("qq", {}).get("ws_url", "ws://localhost:3001")

    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=30) as ws:
                _ws_conn = ws
                log.info(f"✅ 已连接 {ws_url}")

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # 1. WS API 回复（echo 匹配）
                    echo_id = data.get("echo", "")
                    if echo_id and echo_id in _echo_futures:
                        fut = _echo_futures.pop(echo_id, None)
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
            import traceback
            traceback.print_exc()
            await asyncio.sleep(5)


# ====== 入口 ======

async def main():
    load_config()
    init_rpc_log()
    log.info("=" * 45)
    log.info("🚀 OneBot Bridge (ACP 协议版)")
    log.info(f"  WS: {config.get('napcat', {}).get('ws_url', 'ws://localhost:3001')}")
    log.info(f"  Workers: {len(config.get('workers', {}))}")
    log.info("  异步非阻塞 | IO 链路独立")
    log.info("=" * 45)

    worker_mgr = WorkerManager()

    log.info("启动 ACP agent workers...")
    await worker_mgr.start_all(config, on_reply_cb=on_worker_reply)

    log.info("当前 worker 状态:")
    for line in worker_mgr.status_lines():
        log.info(line)

    # 自动发送测试消息（10s 后）
    async def auto_test():
        await asyncio.sleep(8)
        log.info("=" * 40)
        log.info("🧪 自动发送测试消息到默认 worker...")
        log.info("=" * 40)
        text = "说一句话"
        first_worker = next(iter(config.get("workers", {})), None)
        if first_worker and worker_mgr.is_ready(first_worker):
            await worker_mgr.send_message(first_worker, text, qq_msg={
                "type": "private",
                "user_id": "TEST_USER_ID",
                "from_id": "TEST_USER_ID",
                "message": text,
            })
            log.info("🧪 测试消息已发送，等待回复...")
        else:
            log.error(f"🧪 {first_worker or "default worker"} 未就绪！")
    
    asyncio.create_task(auto_test())

    await main_loop(worker_mgr)


def entry():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bridge 已停止")
    except Exception as e:
        log.error(f"崩溃: {e}")
        raise


if __name__ == "__main__":
    entry()
