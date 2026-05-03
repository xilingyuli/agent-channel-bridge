# Agent Channel Bridge

通过 OneBot v11 协议连接 NapCat/QQ，将消息路由到 ACP 协议 agent worker（如 OpenCode、Claude Code 等）。

## 架构

```
QQ → NapCat(WS:3001) → bridge.py → 路由匹配
    ├── 私聊 → 默认全部回复
    └── 群聊 → 仅 @机器人 时回复
```

所有消息走**打断模式**：中断 worker 当前命令，发送新消息到 ACP agent，agent 通过 skill 直接 WS 回复。

## 快速开始

```bash
pip install websockets pyyaml
cp config.yaml config.yaml  # 编辑配置
python3 bridge.py
```

配置修改后需重启 bridge 生效。

## 路由规则

| 来源 | 行为 |
|------|------|
| 私聊 | 默认全部回复到 `default.worker` |
| 群聊 @机器人 | 回复到该群 |
| 群聊不 @ / @别人 | ❌ 不处理 |
| 精确路由 `qq:private:QQ号` | 覆盖默认规则 |
| 精确路由 `qq:group:群号` | 覆盖默认规则 |

## 管理命令

发给 admin QQ（配置中 `admin: true` 的用户）：

- `/status` — Worker 状态
- `/reset` — 重置当前会话 session
- `/help` — 帮助

## 配置说明

```yaml
default:
  worker: my_agent            # 默认 worker 名称

applications:
  qq:
    type: napcat              # 消息来源类型
    bot_qq: 'YOUR_BOT_QQ'     # 机器人 QQ 号
    bot_name: YourBot         # 机器人名字（用于 @ 识别）
    ws_url: ws://localhost:3001  # NapCat WebSocket 地址

routes:
  qq:private:ADMIN_QQ:        # 管理员私聊
    name: MyAgent
    worker: my_agent
    admin: true               # 可执行管理命令
  qq:group:GROUP_ID:          # 群聊路由
    name: MyAgent
    worker: my_agent

workers:
  my_agent:
    name: my_agent
    start_command: opencode acp   # ACP agent 启动命令
    work_dir: /path/to/workspace  # 工作目录
```

## Worker 支持

支持任何使用 **ACP 协议**（Agent Communication Protocol）的 CLI agent：

- [OpenCode](https://github.com/opencode-ai/opencode) — `opencode acp`
- [Claude Code](https://github.com/anthropics/claude-code) — `claude --acp --stdio`

## License

MIT
