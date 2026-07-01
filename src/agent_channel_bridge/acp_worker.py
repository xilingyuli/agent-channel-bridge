"""ACP Worker — manages a single ACP agent subprocess."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Optional

from .config import config
from .rpc_log import log_rpc

log = logging.getLogger("onebot-bridge")



# ====== ACP Worker 通信 ======


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
        # 实例级变量，每个 Worker 独立，避免并发冲突
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._session_buf: dict[str, str] = {}
        # 会话路由: route_key → session_id
        self._route_sessions: dict[str, str] = {}
        # sessionId → session 元信息
        self._session_last_activity: dict[str, float] = {}
        self._session_tool_running: dict[str, bool] = {}
        # sessionId → 本轮 prompt 是否已发送过进度消息
        self._session_progress_sent: dict[str, bool] = {}
        # sessionId → 是否为管理员私聊（该模式下所有 agent_message_chunk 直接转发，不等待 </message>）
        self._session_admin_private: dict[str, bool] = {}
        # sessionId → 管理员私聊的原始文本缓冲区
        self._session_raw_buf: dict[str, str] = {}
        # 按 key 分类的频控缓冲区与上次发送时间（key: tool_call, tool_call_update_bash 等）
        self._rate_limit_buf: dict[str, dict[str, str]] = {}
        self._rate_limit_last: dict[str, dict[str, float]] = {}
        # 运行时动态配置路径
        self._runtime_config_path = os.path.join(work_dir, ".bridge_runtime.json")
        # sessionId → 当前 prompt 的运行时配置缓存（prompt 开始时读取一次）
        self._session_runtime_config: dict[str, dict] = {}
        # sessionId → 管理员私聊时子代理/task 工具的输出积攒（raw buffer 空时兜底发送）
        self._session_task_output: dict[str, str] = {}
        # sessionId → 上一条 agent_message_chunk 的 messageId（用于检测新消息边界）
        self._session_last_message_id: dict[str, str] = {}
        # sessionId → [(timestamp, qq_msg), ...]（FIFO 队列，每次 prompt 推入，回复时 peek，prompt 完成时 pop）
        self._pending_qq_msgs: dict[str, list[tuple[float, dict]]] = {}
        # 忙碌时排队的 prompt：(text, route_key, qq_msg)
        self._pending_prompts: dict[str, list[tuple[str, str, dict]]] = {}
        # msg_id → sessionId (用来在 prompt result 时反查 session)
        self._prompt_msg_map: dict[str, str] = {}
        # 回调: on_reply(worker_name, agent_name, reply_text, qq_msg)
        self.on_reply = None
        # 统一回复收口
        self._reply_queue: asyncio.Queue = asyncio.Queue()
        self._reply_worker_task: Optional[asyncio.Task] = None
        self._reply_seq = 0  # 回复消息序号，用于追踪入队/出队配对
        # session 持久化路径
        self._session_file = os.path.join(work_dir, ".bridge_sessions.json")
        self._history_file = os.path.join(work_dir, ".bridge_session_history.json")
        # route_key → [session_id, ...] 历史会话（越新越靠前）
        self._session_history: dict[str, list[str]] = {}
        try:
            if os.path.isfile(self._history_file):
                with open(self._history_file) as f:
                    self._session_history = json.load(f)
        except Exception:
            pass

    def _save_sessions(self):
        """保存 session 映射到文件"""
        import json as _json
        try:
            data = {rk: sid for rk, sid in self._route_sessions.items()}
            with open(self._session_file, "w") as f:
                _json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            log.warning(f"[{self.agent_name}] session 持久化写入失败: {e}")

    def _save_session_history(self):
        """保存 session 历史到文件"""
        import json as _json
        try:
            with open(self._history_file, "w") as f:
                _json.dump(self._session_history, f, ensure_ascii=False)
        except Exception as e:
            log.warning(f"[{self.agent_name}] session history 持久化写入失败: {e}")

    def list_session_history(self, route_key: str) -> list[str]:
        """列出指定 route_key 的历史 session ID"""
        return self._session_history.get(route_key, [])

    # -------- 统一回复收口 --------

    def _clean_stale_qq_msgs(self, sid: str):
        """清理超过 1 小时的僵尸 qq_msg 条目"""
        q_list = self._pending_qq_msgs.get(sid, [])
        cutoff = time.time() - 3600
        self._pending_qq_msgs[sid] = [(ts, qm) for ts, qm in q_list if ts > cutoff]

    def _push_qq_msg(self, sid: str, qq_msg: dict):
        """将 qq_msg 推入 session 的待回复 FIFO 队列"""
        self._pending_qq_msgs.setdefault(sid, [])
        self._clean_stale_qq_msgs(sid)
        self._pending_qq_msgs[sid].append((time.time(), qq_msg or {}))

    def _peek_qq_msg(self, sid: str) -> dict | None:
        """查看 session 队列中下一个待回复的 qq_msg（不弹出）"""
        q_list = self._pending_qq_msgs.get(sid, [])
        return q_list[0][1] if q_list else None

    def _pop_qq_msg(self, sid: str) -> dict | None:
        """弹出 session 队列中已完成的 prompt 对应的 qq_msg"""
        q_list = self._pending_qq_msgs.get(sid, [])
        result = q_list.pop(0)[1] if q_list else None
        self._clean_stale_qq_msgs(sid)
        return result

    def _split_at_goal_heading(self, text: str) -> list[str]:
        """如果文本包含 ## Goal 开头行（OpenCode 重放的旧会话总结），在第一个 ## Goal 前截断为两条消息"""
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if i > 0 and line.startswith("## Goal"):
                before = "\n".join(lines[:i]).strip()
                after = "\n".join(lines[i:]).strip()
                if before and after:
                    log.info(f"📎 检测到 ## Goal 标题行(第{i+1}行)，拆分为两条消息发送")
                    return [before, after]
                break
        return [text]

    def _enqueue_reply(self, sid: str, text: str):
        """从 session 队列 peek qq_msg，将回复推入统一发送队列"""
        parts = self._split_at_goal_heading(text.strip())
        for part in parts:
            qq_msg = self._peek_qq_msg(sid)
            if qq_msg and part.strip():
                self._reply_seq += 1
                seq = self._reply_seq
                qsize = self._reply_queue.qsize()
                log.info(f"[{self.agent_name}] 📥 入队 #{seq} (队列{qsize}) [{sid[:12]}...]: {part[:40]}")
                self._reply_queue.put_nowait((self.name, self.agent_name, part, qq_msg, seq))

    def _enqueue_reply_direct(self, text: str, qq_msg: dict):
        """直接用给定的 qq_msg 推入统一发送队列（用于忙碌回复等场景）"""
        parts = self._split_at_goal_heading(text.strip())
        for part in parts:
            if qq_msg and part.strip():
                self._reply_seq += 1
                seq = self._reply_seq
                qsize = self._reply_queue.qsize()
                log.info(f"[{self.agent_name}] 📥 入队 #{seq} (队列{qsize}): {part[:40]}")
                self._reply_queue.put_nowait((self.name, self.agent_name, part, qq_msg, seq))

    def _flush_raw_buf(self, sid: str, message_only: bool = False):
        """发送 _session_raw_buf 积攒的内容。
        message_only=True: 只取 <message> 块内文本（缺头尾时兜底），给普通模式用
        message_only=False: 去首尾 message 标签后发送原始文本（管理员私聊模式）"""
        if not self._session_admin_private.get(sid):
            return
        raw = self._session_raw_buf.get(sid, "")
        if not raw.strip():
            return
        import re as _re
        if message_only:
            text = self._extract_msg_content(raw)
        else:
            text = raw.strip()
            text = _re.sub(r'^<message>\s*', '', text)
            text = _re.sub(r'\s*</\s*message>\s*$', '', text)
        if text:
            qq_msg = self._peek_qq_msg(sid)
            if qq_msg:
                self._enqueue_reply_direct(text, qq_msg)
        self._session_raw_buf[sid] = ""

    def _flush_buf_message(self, sid: str):
        """发送 _session_buf 积攒的内容（用 message_only=True 提取 <message> 块），用于普通模式"""
        raw = self._session_buf.pop(sid, "")
        if not raw.strip():
            return
        # 没有 <message> 标签 → 可能是 thought/plan 残留（模型误放在 message_chunk 中），不发
        if "<message>" not in raw and "</message>" not in raw:
            log.info(f"[{self.agent_name}] 🏁 跳过非消息残留: {raw.strip()[:80]}")
            return
        text = self._extract_msg_content(raw)
        if text:
            log.info(f"[{self.agent_name}] 📬 条: {text[:60]}")
            self._enqueue_reply(sid, text)

    def _handle_normal_mode_old(self, sid: str, update: dict, update_type: str, content: dict):
        """旧的普通模式处理逻辑（保留供参考，当前未被调用）"""
        if update_type == "tool_call":
            self._session_tool_running[sid] = True
        elif update_type == "tool_call_update":
            if update.get("status", "") == "in_progress":
                self._session_tool_running[sid] = True
        elif update_type == "agent_message_chunk":
            if content.get("type") == "text":
                text = content.get("text", "")
                clean = text
                for ch in ["┃", "╹", "▣", "■", "▌", "▐", "▀", "▄", "░", "▒", "▓",
                           "│", "║", "═", "╔", "╗", "╚", "╝", "╠", "╣", "╦", "╩", "╬"]:
                    clean = clean.replace(ch, "")
                if clean.strip():
                    self._session_buf.setdefault(sid, "")
                    self._session_buf[sid] += clean
                    if "</message>" in self._session_buf[sid]:
                        import re as _re
                        full = self._session_buf.pop(sid, "")
                        msgs = _re.findall(r'<message>(.*?)</message>', full, _re.DOTALL)
                        for mtext in msgs:
                            mtext = mtext.strip()
                            if not mtext:
                                continue
                            mtext = _re.sub(r'</?message>', '', mtext).strip()
                            if not mtext:
                                continue
                            log.info(f"[{self.agent_name}] 📬 条: {mtext[:60]}")
                            self._enqueue_reply(sid, mtext)

    @staticmethod
    def _extract_msg_content(raw: str) -> str:
        """只提取 <message> 块内文本。缺标签时兜底全返回。"""
        import re as _re
        has_open = "<message>" in raw
        has_close = "</message>" in raw
        if not has_open and not has_close:
            return raw.strip()
        if has_open and has_close:
            msgs = _re.findall(r'<message>(.*?)</message>', raw, _re.DOTALL)
            return "\n".join(m.strip() for m in msgs if m.strip()).strip() or raw.strip()
        if has_open and not has_close:
            idx = raw.index("<message>") + len("<message>")
            return raw[idx:].strip()
        # has_close but no open
        idx = raw.index("</message>")
        return raw[:idx].strip()

    def _relpath(self, filepath: str) -> str:
        """将绝对路径转为相对于 HOME 的路径（~ 前缀）"""
        home = os.environ.get("HOME") or os.path.expanduser("~")
        if filepath.startswith(home):
            return "~" + filepath[len(home):]
        return filepath

    @staticmethod
    def _collect_tool_detail(update: dict) -> str:
        """从 tool_call_update in_progress 事件中提取工具详情"""
        kind = update.get("kind", "")
        ri = update.get("rawInput", {})
        if kind in ("read", "write", "edit"):
            fp = ri.get("filePath") or ri.get("filepath") or ""
            if not fp:
                locs = update.get("locations", [])
                fp = locs[0].get("path", "") if locs else ""
            return fp if fp else ""
        if kind == "fetch":
            url = ri.get("url", "")
            return url[:120] if url else ""
        if kind in ("other", "search"):
            q = ri.get("query", "")
            return q[:120] if q else ""
        if kind == "execute":
            cmd = ri.get("command") or ri.get("description", "")
            return cmd[:200] if cmd else ""
        return ""

    def _load_runtime_config(self) -> dict:
        """从运行时配置文件中读取所有配置，文件不存在或异常时返回空 dict"""
        try:
            if os.path.isfile(self._runtime_config_path):
                with open(self._runtime_config_path) as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _send_rate_limited(self, sid: str, key: str, msg: str):
        """频控发送：同一 key 下首条总是发，后续间隔 >= 60s 才发，否则积攒"""
        last = self._rate_limit_last.setdefault(key, {}).get(sid, 0)
        if not last or time.time() - last >= 60:
            buf = self._rate_limit_buf.get(key, {}).get(sid, "")
            if buf.strip():
                msg = buf + "\n" + msg
                self._rate_limit_buf.setdefault(key, {})[sid] = ""
            self._rate_limit_last[key][sid] = time.time()
            self._enqueue_reply_direct(msg, self._peek_qq_msg(sid))
        else:
            b = self._rate_limit_buf.setdefault(key, {}).setdefault(sid, "")
            self._rate_limit_buf[key][sid] = b + ("\n" + msg) if b else msg

    def _cleanup_rate_limit(self, sid: str):
        """清空频控缓存，同时把积压未发的消息先发送出去"""
        for key in self._rate_limit_buf:
            buf = self._rate_limit_buf[key].pop(sid, None)
            if buf and buf.strip():
                self._enqueue_reply_direct(buf.strip(), self._peek_qq_msg(sid))
        for key in self._rate_limit_last:
            self._rate_limit_last[key].pop(sid, None)

    def _try_forward_tool_result(self, sid: str, update: dict):
        """管理员私聊：转发工具执行结果。read/write/edit 只发文件路径摘要，其余工具发完整输出"""
        if not self._session_admin_private.get(sid):
            return
        status = update.get("status", "")
        if status != "completed":
            return
        kind = update.get("kind", "")
        if kind in ("read", "write", "edit"):
            filepath = (update.get("rawInput", {}).get("filepath", "") or
                        update.get("rawInput", {}).get("filePath", ""))
            if not filepath:
                locs = update.get("locations", [])
                filepath = locs[0].get("path", "") if locs else ""
            if filepath:
                self._enqueue_reply_direct(f"{kind} {self._relpath(filepath)}", self._peek_qq_msg(sid))
            return
        # 其他工具：发完整输出
        result_text = ""
        raw_out = update.get("rawOutput", {})
        if isinstance(raw_out, dict) and raw_out.get("output", "").strip():
            result_text = raw_out["output"]
        if not result_text:
            items = update.get("content", [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        inner = item.get("content", {})
                        if isinstance(inner, dict):
                            result_text += inner.get("text", "")
        if result_text.strip():
            self._enqueue_reply_direct(result_text, self._peek_qq_msg(sid))

    async def _reply_worker(self):
        """统一回复收口：从队列取回复，按序调用 on_reply 发送"""
        while True:
            try:
                item = await self._reply_queue.get()
                if item is None:  # shutdown
                    break
                wkey, aname, text, qq_msg, seq = item
                qsize = self._reply_queue.qsize()
                log.info(f"[{self.agent_name}] 📤 出队 #{seq} (剩余{qsize}): {text[:40]}")
                if self.on_reply:
                    await self.on_reply(wkey, aname, text, qq_msg)
                self._reply_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[{self.agent_name}] 回复发送失败: {e}")

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
                    result = await self._send_request("session/resume", {
                        "sessionId": sid,
                        "cwd": self.work_dir,
                    })
                    # Use returned sessionId (wrapper may create new one)
                    new_sid = result.get("sessionId", sid)
                    restored[route_key] = new_sid
                    log.info(f"[{self.agent_name}] 🔄 恢复 Session [{route_key}]: {new_sid}")
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

        # 启动统一回复收口
        self._reply_worker_task = asyncio.create_task(self._reply_worker())

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
            if ut in ("agent_message_chunk", "tool_call_start"):
                log.info(f"[{self.agent_name}] 📩 UPDATE type={ut}")

        # request_permission — 优先处理，自动允许（无头模式）
        # 必须在"id"分支之前，因为 request_permission 同时有 id 和 method
        if msg.get("method") == "session/request_permission":
            req_id = msg.get("id")
            if req_id is not None:
                request_id = msg.get("params", {}).get("toolCall", {}).get("toolCallId", "")
                options = msg.get("params", {}).get("options", [])
                tc = msg.get("params", {}).get("toolCall", {})
                log.info(f"[{self.agent_name}] 🔍 权限请求 id={req_id} requestID={request_id}"
                         f" tool={tc.get('title','?')} kind={tc.get('kind','?')}"
                         f" path={json.dumps(tc.get('rawInput',{}))[:120]}")
                outcome = "allow_once"
                for opt in options:
                    if opt.get("kind") == "allow_always":
                        outcome = "allow_always"
                        log.info(f"[{self.agent_name}] 🔓 自动允许权限 (always)")
                        break
                else:
                    log.info(f"[{self.agent_name}] 🔓 自动允许权限 (once)")
                reply = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "outcome": {
                            "outcome": "selected",
                            "optionId": "always" if outcome == "allow_always" else "once",
                        },
                    },
                }
                log.info(f"[{self.agent_name}] 🔍 权限响应: {json.dumps(reply)}")
                self.proc.stdin.write((json.dumps(reply) + "\n").encode())
                await self.proc.stdin.drain()
                log.info(f"[{self.agent_name}] 🔍 权限响应已写入 stdin, drain完成")
            return

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

                # 清除该 session 的工具运行状态
                sid_from_map = self._prompt_msg_map.get(msg_id)
                if sid_from_map:
                    self._session_tool_running.pop(sid_from_map, None)
                    self._session_progress_sent.pop(sid_from_map, None)
                    self._session_last_activity[sid_from_map] = time.time()

                # 兼容：检查 _session_buf 中是否有残留文本（agent 忘记用 <message> 标签的情况）
                sid_from_map = self._prompt_msg_map.pop(msg_id, None)
                if sid_from_map and sid_from_map in self._session_buf:
                    leftover = self._session_buf.pop(sid_from_map, "")
                    leftover = leftover.strip()
                    if leftover:
                        import re as _re
                        msgs = _re.findall(r'<message>(.*?)</message>', leftover, _re.DOTALL)
                        if msgs:
                            for mtext in msgs:
                                mtext = mtext.strip()
                                if not mtext:
                                    continue
                                log.info(f"[{self.agent_name}] 🏁 最终消息(标签): {mtext[:60]}")
                                self._enqueue_reply(sid_from_map, mtext)
                        else:
                            # 无闭合标签：尝试提取 <message> 后的内容
                            idx = leftover.find("<message>")
                            if idx >= 0:
                                after = leftover[idx + len("<message>"):].strip()
                                if after:
                                    log.info(f"[{self.agent_name}] 🏁 最终消息(缺闭合): {after[:60]}")
                                    self._enqueue_reply(sid_from_map, after)
                            else:
                                # 没有 <message> 标签 → 可能是 thought 残留，不发
                                log.info(f"[{self.agent_name}] 🏁 跳过非消息残留: {leftover[:80]}")

                # prompt 完成后先清空频控缓冲（工具进度消息），再发结论（独立消息）
                if sid_from_map:
                    self._cleanup_rate_limit(sid_from_map)
                    if self._session_admin_private.get(sid_from_map):
                        self._enqueue_reply_direct("任务执行完成：", self._peek_qq_msg(sid_from_map))
                    buf_was_empty = not self._session_raw_buf.get(sid_from_map, "").strip()
                    self._flush_raw_buf(sid_from_map)
                    # 兜底：raw buffer 空但子代理有输出时，发送子代理结果
                    if buf_was_empty and self._session_admin_private.get(sid_from_map):
                        task_out = self._session_task_output.pop(sid_from_map, "").strip()
                        if task_out:
                            self._enqueue_reply_direct(task_out, self._peek_qq_msg(sid_from_map))
                    self._session_runtime_config.pop(sid_from_map, None)
                    self._pop_qq_msg(sid_from_map)
                    # 检查排队队列，处理下一条
                    await self._process_pending_queue(sid_from_map)

            # 普通 RPC 回复
            fut = self._pending_requests.pop(msg_id, None)
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

        sid = params.get("sessionId", "")
        if not sid:
            return

        self._session_last_activity[sid] = time.time()

        is_admin = self._session_admin_private.get(sid)

        if is_admin:
            # ====== 管理员私聊模式 ======
            if update_type == "agent_thought_chunk":
                # 新 thought → flush 积压的 message 输出
                self._flush_raw_buf(sid)
                # 日志记录 thought 内容（不发送给用户）
                thought_text = content.get("text", "") or content.get("reasoning", "") or ""
                if thought_text.strip():
                    log.info(f"[{self.agent_name}] 💭 thought [{sid[:12]}]: {thought_text[:300]}")
                if not self._session_progress_sent.get(sid):
                    self._session_progress_sent[sid] = True
                    self._enqueue_reply_direct("思考中……", self._peek_qq_msg(sid))

            elif update_type == "tool_call":
                # 新 tool_call → flush 积压的 message 输出
                self._flush_raw_buf(sid)
                self._session_tool_running[sid] = True
                kind = update.get("kind", "")
                raw_title = update.get("title", "") or kind
                # read/write/edit/fetch/search/other 在 in_progress 阶段发详情
                # execute 在 tool_call 阶段就发（含命令详情），in_progress 不再发
                if kind not in ("read", "write", "edit", "fetch", "other", "search"):
                    prefix = "收到～" if not self._session_progress_sent.get(sid) else ""
                    self._session_progress_sent[sid] = True
                    if kind == "execute":
                        detail = self._collect_tool_detail(update)
                        self._send_rate_limited(sid, "tool_call", f"{prefix}正在执行 {raw_title} {detail}" if detail else f"{prefix}正在执行 {raw_title}")
                    else:
                        self._send_rate_limited(sid, "tool_call", f"{prefix}正在执行 {raw_title}")

            elif update_type == "tool_call_update":
                kind = update.get("kind", "")
                st = update.get("status", "")
                if st in ("in_progress", "completed", "failed"):
                    log.info(f"[{self.agent_name}] 🔍 tool_call_update kind={kind} status={st}"
                             f" title={update.get('title','')[:40]}")
                if st == "in_progress":
                    self._session_tool_running[sid] = True
                    # execute 已在 tool_call 阶段发消息，此处跳过
                    if kind != "execute":
                        detail = self._collect_tool_detail(update)
                        if detail:
                            prefix = "收到～" if not self._session_progress_sent.get(sid) else ""
                            self._session_progress_sent[sid] = True
                            tool_name = update.get("title", "") or kind
                            self._send_rate_limited(sid, "tool_call", f"{prefix}正在执行 {tool_name} {detail}")
                elif kind == "execute" and st == "completed":
                    cfg = self._session_runtime_config.get(sid, {})
                    mode = int(cfg.get("show_bash_msg", 0))
                    if mode == 1:
                        ri = update.get("rawInput", {})
                        msg = f"{kind} " + " ".join(ri.keys())
                        self._send_rate_limited(sid, "tool_call", msg)
                    elif mode == 2:
                        ri = update.get("rawInput", {})
                        lines = [f"{k}={v}" for k, v in ri.items()]
                        msg = "\n".join(lines)
                        self._send_rate_limited(sid, "tool_call", msg)
                    # mode 0: 不发
                elif st == "completed" and kind in ("task", "think", "other"):
                    # 子代理/task 输出积攒，raw buffer 空时兜底发送
                    result_text = ""
                    raw_out = update.get("rawOutput", {})
                    if isinstance(raw_out, dict) and raw_out.get("output", "").strip():
                        result_text = raw_out["output"]
                    if result_text.strip():
                        self._session_task_output.setdefault(sid, "")
                        self._session_task_output[sid] += result_text.strip() + "\n"

            elif update_type == "agent_message_chunk":
                if content.get("type") == "text":
                    text = content.get("text", "")
                    clean = text
                    for ch in ["┃", "╹", "▣", "■", "▌", "▐", "▀", "▄", "░", "▒", "▓",
                               "│", "║", "═", "╔", "╗", "╚", "╝", "╠", "╣", "╦", "╩", "╬"]:
                        clean = clean.replace(ch, "")
                    # 保留空行 chunk（\n\n），只跳过真正的空字符串
                    # clean.strip() 会丢弃 \n\n 使格式丢失，改用 if clean
                    if clean:
                        # messageId 变化 → 新消息开始，flush 前一条
                        curr_mid = content.get("messageId", "") or ""
                        last_mid = self._session_last_message_id.get(sid, "")
                        if curr_mid and curr_mid != last_mid and last_mid:
                            self._flush_raw_buf(sid)
                        if curr_mid:
                            self._session_last_message_id[sid] = curr_mid
                        self._session_raw_buf.setdefault(sid, "")
                        self._session_raw_buf[sid] += clean

        else:
            # ====== 普通模式（新版）======
            if update_type == "agent_thought_chunk":
                self._flush_buf_message(sid)

            elif update_type == "tool_call":
                self._flush_buf_message(sid)
                self._session_tool_running[sid] = True

            elif update_type == "tool_call_update":
                if update.get("status", "") == "in_progress":
                    self._session_tool_running[sid] = True

            elif update_type == "agent_message_chunk":
                if content.get("type") == "text":
                    text = content.get("text", "")
                    clean = text
                    for ch in ["┃", "╹", "▣", "■", "▌", "▐", "▀", "▄", "░", "▒", "▓",
                               "│", "║", "═", "╔", "╗", "╚", "╝", "╠", "╣", "╦", "╩", "╬"]:
                        clean = clean.replace(ch, "")
                    if clean:
                        # messageId 变化 → 新消息开始，flush 前一条
                        curr_mid = content.get("messageId", "") or ""
                        last_mid = self._session_last_message_id.get(sid, "")
                        if curr_mid and curr_mid != last_mid and last_mid:
                            self._flush_buf_message(sid)
                        if curr_mid:
                            self._session_last_message_id[sid] = curr_mid
                        self._session_buf.setdefault(sid, "")
                        self._session_buf[sid] += clean

    async def send_message(self, text: str, qq_msg: dict = None):
        """发送消息到 agent，立即返回，不等待回复"""
        if not self._connected:
            log.warning(f"[{self.agent_name}] ACP 未就绪")
            return

        # 检查是否为管理员私聊（该模式下所有输出直接转发，不等待 </message>）
        is_admin_private = False
        if qq_msg:
            is_private = qq_msg.get("type") == "private"
            route_key = f"qq:private:{qq_msg['user_id']}" if is_private else f"qq:group:{qq_msg['from_id']}"
            if is_private:
                route = config.get("routes", {}).get(route_key)
                is_admin_private = bool(route and route.get("admin"))
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

        self._session_buf.pop(sid, None)  # 清除上一轮残留

        # 检查 session 是否忙碌（有正在处理的 prompt）
        if sid in self._prompt_msg_map.values():
            last_act = self._session_last_activity.get(sid, 0)
            tool_running = self._session_tool_running.get(sid, False)
            idle_seconds = time.time() - last_act

            # 判断是否真的在工作中：
            # 无工具时 60s 无活动 → 异常；有工具时放宽到 300s（工具可能较慢）
            max_idle = 300 if tool_running else 60
            if idle_seconds < max_idle:
                self._pending_prompts.setdefault(sid, []).append((text, route_key, qq_msg))
                count = len(self._pending_prompts[sid])
                log.info(f"[{self.agent_name}] 🔒 Session [{route_key}] 上一条任务处理中"
                         f" (tool={tool_running}, idle={idle_seconds:.0f}s)，入队 #{count}")
                self._enqueue_reply_direct(f"上一条任务处理中，已加入排队（第 {count} 条）", qq_msg)
                return

            # 异常状态：无活动超过阈值 → 清理僵死 prompt 并重试
            log.warning(f"[{self.agent_name}] ⚠️ Session [{route_key}] 异常(无活动{idle_seconds:.0f}s,"
                        f" tool={tool_running})，清理 {sum(1 for s in self._prompt_msg_map.values() if s == sid)} 条僵死 prompt，重置重试")
            for mid in list(self._prompt_msg_map):
                if self._prompt_msg_map[mid] == sid:
                    self._prompt_msg_map.pop(mid, None)
            self._session_tool_running.pop(sid, None)
            self._session_progress_sent.pop(sid, None)
            self._session_admin_private.pop(sid, None)
            self._session_task_output.pop(sid, None)
            self._session_runtime_config.pop(sid, None)
            self._session_last_message_id.pop(sid, None)
            self._cleanup_rate_limit(sid)
            self._session_raw_buf.pop(sid, None)
            self._session_buf.pop(sid, None)
            self._session_last_activity.pop(sid, None)
            self._pending_qq_msgs.pop(sid, None)
            try:
                await self._send_request("session/close", {"sessionId": sid})
            except Exception:
                pass
            self._route_sessions.pop(route_key, None)

            # 创建新 session 重试
            try:
                result = await self._send_request("session/new", {
                    "cwd": self.work_dir,
                    "mcpServers": [],
                })
                sid = result["sessionId"]
            except Exception as e:
                log.error(f"[{self.agent_name}] ❌ session/new(重试) 失败: {e}", exc_info=True)
                return
            self._route_sessions[route_key] = sid
            self._save_sessions()
            log.info(f"[{self.agent_name}] 📝 重试新 Session [{route_key}]: {sid}")

        self._push_qq_msg(sid, qq_msg or {})
        self._session_buf.pop(sid, None)  # 清除上一轮残留
        self._session_progress_sent.pop(sid, None)  # 重置进度消息标记
        self._session_raw_buf.pop(sid, None)  # 清除上一轮原始缓冲
        self._session_last_message_id.pop(sid, None)  # 清除上一条 messageId
        self._session_task_output.pop(sid, None)  # 清除上一轮子代理输出
        self._session_admin_private[sid] = is_admin_private
        self._session_runtime_config[sid] = self._load_runtime_config()

        await self._do_send_prompt(sid, route_key, text, qq_msg)

    async def _process_pending_queue(self, sid: str):
        """当前 prompt 完成后，处理 session 的排队队列"""
        queue = self._pending_prompts.get(sid)
        if not queue:
            return
        text, route_key, qq_msg = queue.pop(0)
        if not queue:
            del self._pending_prompts[sid]

        is_admin_private = False
        if qq_msg:
            is_private = qq_msg.get("type") == "private"
            if is_private:
                route = config.get("routes", {}).get(route_key)
                is_admin_private = bool(route and route.get("admin"))

        self._push_qq_msg(sid, qq_msg or {})
        self._session_buf.pop(sid, None)
        self._session_progress_sent.pop(sid, None)
        self._session_raw_buf.pop(sid, None)
        self._session_admin_private[sid] = is_admin_private
        self._session_runtime_config[sid] = self._load_runtime_config()

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
        is_admin = self._session_admin_private.get(sid, False)
        ctx_lines.append(f"你正在通过QQ和用户对话。来源: {src}，发送者: {sender}。")
        # 群聊时附加群名片
        card = qq_msg.get("card_name", "") if qq_msg else ""
        if card and card != sender:
            ctx_lines.append(f"用户群名片: {card}")
        ctx_lines.append(f"此次消息视作{'管理员' if is_admin else '非管理员'}消息。")

        # 处理 CQ reply（引用消息）
        if qq_msg and qq_msg.get("reply_id"):
            from .onebot import send_api_action
            try:
                msg_type = qq_msg.get("type", "private")
                if msg_type == "group":
                    group_id = qq_msg.get("from_id", "")
                    if group_id and group_id not in ("TEST_USER_ID", ""):
                        result = await send_api_action("get_msg", {"message_id": int(qq_msg["reply_id"])})
                        if result and result.get("status") == "ok":
                            data = result.get("data", {})
                            reply_msg = data.get("message", [])
                            reply_sender = data.get("sender", {}).get("nickname", "用户")
                            ctx_lines.append(f"")
                            ctx_lines.append(f"【用户引用了之前的消息】")
                            ctx_lines.append(f"发送者: {reply_sender}")
                            # 提取回复消息中的文本和图片
                            reply_text_parts = []
                            reply_images = []
                            for seg in reply_msg if isinstance(reply_msg, list) else []:
                                if seg.get("type") == "text":
                                    reply_text_parts.append(seg.get("data", {}).get("text", ""))
                                elif seg.get("type") == "image":
                                    img_url = seg.get("data", {}).get("url", "")
                                    if img_url:
                                        reply_images.append(img_url)
                            if reply_text_parts:
                                ctx_lines.append(f"消息内容: {''.join(reply_text_parts).strip()}")
                            if reply_images:
                                for i, url in enumerate(reply_images):
                                    ctx_lines.append(f"引用的图片{i+1}: {url}")
                                    ctx_lines.append(f"  （你可以用 <img>{url}</img> 把这张图片再发出去）")
                            ctx_lines.append(f"")
            except Exception as e:
                log.warning(f"[{self.agent_name}] 获取引用消息失败: {e}")

        # 图片
        if qq_msg and qq_msg.get("has_image"):
            ctx_lines.append(f"用户同时发送了 {len(qq_msg.get('images', []))} 张图片:")
            for i, img in enumerate(qq_msg.get("images", [])):
                url = img.get("url", "")
                if url:
                    ctx_lines.append(f"  图片{i+1}: {url}")

        # 特殊格式指令
        ctx_lines.append("")
        if is_admin:
            ctx_lines.append("【回复规则】")
            ctx_lines.append("  1. 正文直接输出，无需用 XML/标签包裹")
            ctx_lines.append("  2. 思考过程、内部推理以及 read/execute 等工具执行结果不要输出给用户")
            ctx_lines.append("  3. 正文应当展示为适合在 QQ 端呈现的纯文本格式。禁止使用 markdown/XML 语法和表格格式。")
        else:
            ctx_lines.append("【回复规则】")
            ctx_lines.append("  1. 每条独立回复用 <message> 包裹，支持一次输出多条：")
            ctx_lines.append("     <message>")
            ctx_lines.append("     第一条回复")
            ctx_lines.append("     </message>")
            ctx_lines.append("     <message>")
            ctx_lines.append("     第二条回复")
            ctx_lines.append("     </message>")
            ctx_lines.append("  2. 仅允许使用 <message> 和 </message> 标签，禁止使用其他格式的 message 标签")
            ctx_lines.append("  3. 每条 <message> 输出后立即发送给用户，无需等待")
            ctx_lines.append("  4. 注意 <message> 标签的开闭状态，确保不嵌套、不遗漏闭合标签")
            ctx_lines.append("  5. 思考过程、内部推理以及 read 的原始内容、execute 的命令输出不要放在 <message> 块内发给用户")
            ctx_lines.append("  6. 正文应当展示为适合在 QQ 端呈现的纯文本格式。禁止使用 markdown/XML 语法和表格格式。")
        ctx_lines.append("")
        ctx_lines.append("【发送图片/文件/语音 - 标签格式】")
        ctx_lines.append("  1. 在 <message> 内的任意位置插入标签即可发送媒体：")
        ctx_lines.append("     <message>这是查询结果<img>https://example.com/result.png</img></message>")
        ctx_lines.append("  2. <img>URL</img> — 发送图片")
        ctx_lines.append("  3. <audio>URL</audio> — 发送语音")
        ctx_lines.append("  4. <file>URL</file> — 发送文件")
        ctx_lines.append("  5. 标签可以放在 message 内的任何位置，文字前后中间都行")
        ctx_lines.append("  6. 如果你用 webfetch/grab/curl 等工具下载了图片或生成了本地图片文件：")
        ctx_lines.append("     • <img>https://example.com/pic.png</img> — 远程 URL")
        ctx_lines.append("     • <img>/absolute/path/to/file.png</img> — 本地绝对路径（自动转 base64 发送）")
        ctx_lines.append("     • <img>base64://iVBORw0KGgo...</img> — base64 编码（太长不建议）")
        ctx_lines.append("")
        ctx_lines.append("【进度同步约定】")
        ctx_lines.append("  以下场景必须输出 <message> 告知用户进度：")
        ctx_lines.append('  1. 开始工作前，用 <message> 块输出一句简短确认，让用户知道你收到了消息、即将开始处理')
        ctx_lines.append("     例如: <message>收到～我来看看</message>")
        ctx_lines.append("  2. 开始执行搜索/重要工具/命令前，告诉用户你要做什么")
        ctx_lines.append("  3. 遇到问题需要用户决策时，询问意见")

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
        self._pending_requests[msg_id] = fut

        if self.proc and self.proc.stdin:
            self.proc.stdin.write((json.dumps(msg) + "\n").encode())
            await self.proc.stdin.drain()

        try:
            return await asyncio.wait_for(fut, timeout=30)
        except asyncio.TimeoutError:
            self._pending_requests.pop(msg_id, None)
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
        self._save_sessions()
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
            self._session_buf.pop(sid, None)
            self._session_last_activity.pop(sid, None)
            self._session_tool_running.pop(sid, None)
            self._session_progress_sent.pop(sid, None)
            self._session_admin_private.pop(sid, None)
            self._session_task_output.pop(sid, None)
            self._session_runtime_config.pop(sid, None)
            self._session_last_message_id.pop(sid, None)
            self._session_raw_buf.pop(sid, None)
            self._pending_qq_msgs.pop(sid, None)
        self._prompt_msg_map.clear()
        self._pending_prompts.clear()
        self._rate_limit_buf.clear()
        self._rate_limit_last.clear()
        self._save_sessions()
        log.info(f"[{self.agent_name}] 🧹 已重置 {closed} 个 session{'，' + str(errors) + ' 个失败' if errors else ''}")
        return {"closed": closed, "errors": errors}

    async def reset_route_session(self, route_key: str) -> tuple[bool, str]:
        """关闭指定 route_key 的 session 并创建新 session，返回 (成功与否, 新session_id)"""
        old_sid = self._route_sessions.pop(route_key, None)
        if old_sid:
            try:
                await self._send_request("session/close", {
                    "sessionId": old_sid,
                })
            except Exception as e:
                log.warning(f"[{self.agent_name}] session/close [{route_key}] 失败: {e}")
            self._session_buf.pop(old_sid, None)
            self._session_last_activity.pop(old_sid, None)
            self._session_tool_running.pop(old_sid, None)
            self._session_progress_sent.pop(old_sid, None)
            self._session_admin_private.pop(old_sid, None)
            self._session_task_output.pop(old_sid, None)
            self._session_runtime_config.pop(old_sid, None)
            self._session_last_message_id.pop(old_sid, None)
            self._cleanup_rate_limit(old_sid)
            self._session_raw_buf.pop(old_sid, None)
            self._pending_qq_msgs.pop(old_sid, None)
            self._pending_prompts.pop(old_sid, None)
            for mid in list(self._prompt_msg_map):
                if self._prompt_msg_map[mid] == old_sid:
                    self._prompt_msg_map.pop(mid, None)
            # 保存到历史
            hist = self._session_history.setdefault(route_key, [])
            if old_sid not in hist:
                hist.insert(0, old_sid)
            self._save_session_history()

        # 创建新 session
        try:
            result = await self._send_request("session/new", {
                "cwd": self.work_dir,
                "mcpServers": [],
            })
            new_sid = result["sessionId"]
        except Exception as e:
            log.error(f"[{self.agent_name}] ❌ session/new 失败: {e}", exc_info=True)
            return False, ""
        self._route_sessions[route_key] = new_sid
        self._save_sessions()
        log.info(f"[{self.agent_name}] 🧹 已重置 session [{route_key}] {old_sid or '(new)'} → {new_sid}")
        return True, new_sid

    async def resume_route_session(self, route_key: str, session_id: str) -> tuple[bool, str]:
        """切换到指定 session_id，返回 (成功与否, 信息)"""
        hist = self._session_history.get(route_key, [])
        if session_id not in hist:
            return False, f"会话 {session_id} 不存在于 {route_key} 的历史中"
        old_sid = self._route_sessions.get(route_key, "")
        self._route_sessions[route_key] = session_id
        self._save_sessions()
        log.info(f"[{self.agent_name}] 🔄 恢复 session [{route_key}] {old_sid} → {session_id}")
        return True, session_id

    @property
    def connected(self) -> bool:
        return self._connected

    async def stop(self):
        self._connected = False
        # 停止回复收口
        if self._reply_worker_task:
            self._reply_queue.put_nowait(None)  # 发送关闭信号
            self._reply_worker_task.cancel()
            try:
                await self._reply_worker_task
            except asyncio.CancelledError:
                pass
            self._reply_worker_task = None
        for task in [self._reader_task, self._stderr_task]:
            if task:
                task.cancel()
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.proc.kill()



