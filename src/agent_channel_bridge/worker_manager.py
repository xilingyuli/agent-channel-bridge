"""WorkerManager — manages lifecycle of all ACP workers."""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .acp_worker import AcpWorker
from .config import config

log = logging.getLogger("onebot-bridge")


class WorkerManager:
    def __init__(self):
        self.workers: dict[str, AcpWorker] = {}
        self._on_reply_cb = None

    def add_worker(self, key: str, worker: AcpWorker):
        self.workers[key] = worker

    def get(self, key: str) -> Optional[AcpWorker]:
        return self.workers.get(key)

    def is_ready(self, key: str) -> bool:
        w = self.workers.get(key)
        return w is not None and w._connected

    async def start_all(self, config: dict, on_reply_cb=None):
        self._on_reply_cb = on_reply_cb
        worker_cfgs = config.get("workers", {})
        for key, wc in worker_cfgs.items():
            work_dir = wc.get("work_dir", "")
            name = wc.get("name", key)
            start_command = wc.get("start_command", "")
            worker = AcpWorker(key, work_dir, name=name, start_command=start_command)
            worker.on_reply = self._on_reply_cb
            self.add_worker(key, worker)

        # Start all workers concurrently
        tasks = [self.workers[k].start() for k in self.workers]
        if tasks:
            await asyncio.gather(*tasks)

    async def send_message(self, worker_key: str, text: str, qq_msg: dict = None):
        w = self.workers.get(worker_key)
        if w:
            await w.send_message(text, qq_msg)

    async def reset_for_msg(self, msg: dict) -> str:
        from .config import config, get_route
        route = get_route(msg["from_id"], msg["type"] == "private",
                          msg.get("is_mention", False))
        if not route:
            return "❌ 未匹配到路由"
        worker_name = route.get("worker", "")
        route_key = f"qq:{'private' if msg['type'] == 'private' else 'group'}:{msg['from_id']}"
        w = self.workers.get(worker_name)
        if not w:
            return f"❌ Worker [{worker_name}] 不存在"
        ok, new_sid = await w.reset_route_session(route_key)
        if ok:
            return f"✅ [{route.get('name', '?')}] session 已重置\n新会话: {new_sid}"
        return "❌ session 创建失败"

    async def resume_for_msg(self, msg: dict, session_id: str) -> str:
        from .config import config, get_route
        route = get_route(msg["from_id"], msg["type"] == "private",
                          msg.get("is_mention", False))
        if not route:
            return "❌ 未匹配到路由"
        worker_name = route.get("worker", "")
        route_key = f"qq:{'private' if msg['type'] == 'private' else 'group'}:{msg['from_id']}"
        w = self.workers.get(worker_name)
        if not w:
            return f"❌ Worker [{worker_name}] 不存在"
        ok, info = await w.resume_route_session(route_key, session_id)
        if ok:
            return f"✅ [{route.get('name', '?')}] 已切换到会话 {session_id}"
        return f"❌ {info}"

    async def stop_all(self):
        for w in self.workers.values():
            if w.proc:
                w.proc.kill()

    async def restart_for_session(self, msg: dict, session_id: str) -> str:
        """用指定 session_id 重启 worker（完整 session 恢复降级路径）"""
        w = self.workers.get("opencode_agent")
        if not w:
            return "❌ Worker 不存在"
        try:
            await w.stop()
        except Exception as e:
            log.warning(f"[opencode_agent] stop 异常: {e}")
        await asyncio.sleep(1)

        # 修改 start_command 追加 --session
        old_cmd = w.start_command
        w.start_command = f"{old_cmd} --session {session_id}"
        try:
            await w.start()
        except Exception as e:
            w.start_command = old_cmd
            return f"❌ 启动失败: {e}"
        w.start_command = old_cmd

        # 恢复后更新路由的 session 映射
        from .config import get_route
        route = get_route(msg["from_id"], msg["type"] == "private",
                          msg.get("is_mention", False))
        route_key = f"qq:{'private' if msg['type'] == 'private' else 'group'}:{msg['from_id']}"
        w._route_sessions[route_key] = session_id
        w._save_sessions()
        hist = w._session_history.setdefault(route_key, [])
        if session_id not in hist:
            hist.insert(0, session_id)
        w._save_session_history()
        return f"✅ 已用完整 session 重启并恢复 {session_id}"

    async def restart_worker(self, worker_key: str) -> str:
        """停止并重启指定 worker，返回状态信息"""
        w = self.workers.get(worker_key)
        if not w:
            return f"❌ Worker [{worker_key}] 不存在"
        old_connected = w._connected
        old_sessions = len(w._route_sessions)
        try:
            await w.stop()
        except Exception as e:
            log.warning(f"[{worker_key}] stop 异常: {e}")
        await asyncio.sleep(1)
        try:
            await w.start()
        except Exception as e:
            log.error(f"[{worker_key}] restart 失败: {e}")
            return f"❌ Worker [{worker_key}] 重启失败: {e}"
        return f"🛑 [{w.name or worker_key}] 已重启 (原{'运行中' if old_connected else '离线'}, {old_sessions} 个路由)"

    def status_lines(self) -> list[str]:
        lines = []
        for key, w in self.workers.items():
            status = "✅" if w._connected else "❌"
            sid = w.session_id or "-"
            sessions = len(w._route_sessions)
            start_cmd = getattr(w, 'start_command', '?') or '?'
            lines.append(f"  {status} {w.name or key} (session={sid[:16]}..., routes={sessions}) cmd={start_cmd}")
        return lines
