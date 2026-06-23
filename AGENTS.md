# AGENTS.md — Agent Channel Bridge 工作指南

> 关于本机所有 QQ Bot 相关服务的完整介绍，见 [`SERVICES.md`](./SERVICES.md)。

**速览：** NapCatQQ（Docker, port 3001）→ 同时服务 Miao-Yunzai（群管）和 Bridge（AI 路由）→ Bridge 通过 ACP 调用 OpenCode（子进程）。三个服务由 `~/Documents/Code/startmybot.sh` 一键启动。详情及约束见 `SERVICES.md`。

---

## 📌 项目概览

Agent Channel Bridge 是一个将 QQ 消息路由到 AI Coding Agent（OpenCode、Claude Code 等）的桥梁。它通过 ACP 协议（JSON-RPC over stdio）与 Agent 子进程通信，NapCat 作为 QQ 协议层。

## 🏗 项目结构

```
agent-channel-bridge/
├── pyproject.toml              # 构建配置、依赖、CLI 入口
├── Makefile                    # make run / make test / make lint
├── bridge.sh                   # 进程管理（start/stop/restart/status）
├── Dockerfile                  # Docker 镜像构建
├── config.yaml                 # 本地配置文件（gitignored）
├── config.template.yaml        # 配置模板
├── AGENTS.md                   # 👈 本文件 — AI 工作指南
├── README.md                   # 用户文档（修改时同步更新）
├── src/
│   └── agent_channel_bridge/
│       ├── __init__.py         # 版本号 + logging 初始化
│       ├── __main__.py         # 入口、WS 主循环、管理命令
│       ├── config.py           # 配置加载、路由匹配（通配符）、聊天日志
│       ├── acp_worker.py       # ACP Worker — Agent 子进程管理
│       ├── acp_wrapper_claude.py # ACP 包装器 — Claude Code CLI 适配器
│       ├── worker_manager.py   # Worker 生命周期管理
│       ├── onebot.py           # OneBot v11 协议（消息构建/解析/API）
│       └── rpc_log.py          # JSON-RPC 日志
└── CHANGELOG.md                # [可选] 变更日志
```

## 🔧 核心工作流程

### 1. 新增功能/修复 Bug 的标准流程

```
分析需求 → 写测试 → 实现 → 运行全部测试 → 更新文档 → 提交 PR
```

**必须遵守：**
- ✅ **先写测试再写代码**（测试驱动）
- ✅ **代码提交后签名并推送**（`config.yaml` 已 gitignored，注意检查）
- ✅ **同步更新 README.md**（特别要检查：配置示例、路由规则、架构图）


### 3. 路由匹配规则（重要！）

`config.py` 中的 `get_route()` 是核心逻辑。匹配策略：

```
精确匹配 → 通配匹配（8 种通配组合按优先级依次尝试）
```

**候选顺序（以 `qq:group:123` 为例）：**
1. `qq:group:123` ← 精确
2. `qq:group:*`
3. `qq:*:123`
4. `qq:*:*`
5. `*:group:123`
6. `*:group:*`
7. `*:*:123`
8. `*:*:*`

**关键约束：** 群聊消息必须被 `@`（`is_mention=True`）才会匹配任意路由（包括 `*:*:*`）。私聊自动匹配。

**配置示例：**
```yaml
routes:
  qq:private:ADMIN_QQ:       # 精确
    worker: agent1
    admin: true
  qq:private:*:               # 所有私聊兜底
    worker: agent1
  *:*:*:                      # 全兜底
    worker: agent1
```

**修改 `get_route()` 后必须更新：**
1. 测试：补充精确/通配/优先级用例
2. README：更新"路由规则详解"章节的匹配示例
3. `config.template.yaml`：更新示例路由（如果新增了配置格式）

### 4. 消息段构建

Agent 回复中可嵌入媒体标签，`_build_message_segments()` 解析后转为 OneBot v11 消息段：

| 标签 | 对应 OneBot 类型 | 本地文件支持 |
|------|-----------------|------------|
| `<img>URL</img>` | `image` | ✅ 自动 base64 |
| `<audio>URL</audio>` | `record` | ❌ 仅 URL |
| `<file>URL</file>` | `file` | ✅ 自动 base64 |

URL 中的换行/空白会被自动清理，标签大小写不敏感。

### 4.5 进度同步约定（Agent 提示模板）

system prompt 中的「进度同步约定」要求 Agent 在以下场景**必须**输出 `<message>`：

1. **收到用户消息后，立即输出确认消息** `<message>` 再开始工作（如"收到～我来看看..."）
2. 开始执行重要工具/命令前 — 告诉用户要做什么
3. 用户指令执行完毕时 — 告知结果或下一步
4. 工具执行慢或卡住时 — 告知用户"正在执行，请稍候"
5. 遇到问题需要用户决策时 — 询问意见
6. 长时间无文字输出时（超过 10 秒）— 必须输出一条进度

### 5. 配置管理

- `config.yaml` 已加入 `.gitignore`，**切勿手动移除 gitignore**
- `config.template.yaml` 是配置模板，修改配置格式时必须同步更新模板
- 配置格式变更后，README 的配置示例必须同步更新

### 6. OneBot 协议解析关键点

- 群聊 `@` 检测：优先 CQ 码 `message[].type == "at"`，回退到文本 `@bot_name`
- `@` 前缀会在解析时剥离
- 发送者名称回退链：`nickname → card → "QQ{user_id}"`

### 7. 文档同步清单

修改以下任一代码后，必须检查并更新对应文档：

| 代码修改 | 需要更新的文档 |
|---------|--------------|
| `get_route()` / 路由逻辑 | README「路由规则详解」+ `config.template.yaml` |
| 配置格式（yaml 字段） | README「配置」示例 + `config.template.yaml` |
| ACP 方法/协议变更 | README「核心技术：ACP 协议」表格 |
| 消息格式/标签 | README「消息格式」+ Agent 系统提示模板（`_do_send_prompt`） |
| 新增管理命令 | README「管理命令」表格 |
| 项目结构变更 | README「项目结构」树形图 |


## ⚠️ 常见 Pitfalls

1. **`config.yaml` 不要提交！** 它已在 `.gitignore` 中，更新配置模板请改 `config.template.yaml`
2. **YAML 通配符引号：** `*:*:*` 在 YAML 中会被解析为锚点，必须加引号写成 `'*:*:*'`
3. **日志配置在 `__init__.py`：** 使用 `force=True`，可能覆盖其他模块的 logger
4. **`_ws_conn` 是模块级全局变量：** 在 `config.py` 中定义，`__main__.py` 和 `onebot.py` 共享引用，重连时会有窗口期
5. **`_prompt_msg_map` 无超时清理：** 后台 prompt 如果不返回，map 会泄漏（潜在风险）
6. **`auto_test()` 已移除：** 不要再添加自动测试消息，会消耗 API 额度

## 🤖 Claude Code 作为 Worker

`acp_wrapper_claude.py` 实现了 ACP ↔ Claude Code CLI 的协议转换，让不支持 ACP 的 Agent（如 Claude Code）也能接入 bridge。

**工作原理：** 包装器实现最小 ACP JSON-RPC 服务端，对 `session/prompt` 请求调用 `claude -p` 打印模式，通过 `session/update` 通知流式返回输出。

**配置：**
```yaml
workers:
  claude_agent:
    name: claude_agent
    start_command: python -m agent_channel_bridge.acp_wrapper_claude
    work_dir: /path/to/workspace
```

**环境要求：**
- `ANTHROPIC_API_KEY` — DeepSeek API key（或其他 Anthropic 兼容 API）
- `ANTHROPIC_BASE_URL` — API 端点（默认 https://api.deepseek.com/anthropic）
- `claude` CLI 已安装（`npm install -g @anthropic-ai/claude-code`）

**限制：** Claude Code `-p` 模式是 stateless 的，包装器通过维护会话历史来模拟持续对话。`max_history=10` 轮后旧上下文会被裁剪。
