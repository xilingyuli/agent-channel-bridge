"""Tests for Agent Channel Bridge."""
import sys
from pathlib import Path

# Ensure src/ is on the path (src-layout)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def test_import():
    """Verify all modules can be imported."""
    from agent_channel_bridge.config import load_config, get_route
    from agent_channel_bridge.rpc_log import init_rpc_log, log_rpc
    from agent_channel_bridge.acp_worker import AcpWorker
    from agent_channel_bridge.worker_manager import WorkerManager
    assert load_config is not None
    assert get_route is not None
    assert AcpWorker is not None
    assert WorkerManager is not None


def test_version():
    from agent_channel_bridge import __version__
    assert __version__ == "1.0.0"
