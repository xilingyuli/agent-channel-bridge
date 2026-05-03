"""JSON-RPC file logging."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Optional

from .config import BASE_DIR

log = logging.getLogger("onebot-bridge")

_rpc_log_file: Optional[str] = None
_rpc_log_fh: Optional[object] = None


def init_rpc_log():
    global _rpc_log_file, _rpc_log_fh
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    _rpc_log_file = os.path.join(log_dir, "rpc.jsonl")
    _rpc_log_fh = open(_rpc_log_file, "a", buffering=1)
    log.info(f"RPC 日志: {_rpc_log_file}")


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
