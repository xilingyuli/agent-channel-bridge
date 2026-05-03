"""Agent Channel Bridge — 将 QQ 消息路由到 AI Coding Agent."""

__version__ = "1.0.0"

from .config import load_config, get_route
from .acp_worker import AcpWorker
from .worker_manager import WorkerManager
