# CHANGELOG

## 2026-06-21 — 桥接层大规模重构

### 会话管理

- **修复** `_last_qq_msg` 被并发消息覆盖的 bug：改为 `_pending_qq_msgs` FIFO 队列，每个 prompt push，回复时 peek，prompt 完成时 pop，多轮并发不再串上下文
- **修复** session 忙碌时新建临时 session 导致丢失对话历史的问题：改为检测真实活动状态（`_session_last_activity` + `_session_tool_running`），真正工作中回复"上一条任务处理中"，僵死 60s（有工具 300s）后自动关闭重建
- session 路由规则不变：`qq:private:{user_id}` 和 `qq:group:{group_id}` 粒度复用 session

### 统一回复收口

- 新增 `_reply_queue`（`asyncio.Queue`）+ `_reply_worker` 后台协程，所有回复按序发送，替代原来分散的 `asyncio.create_task(on_reply(...))`
- 每条消息带独立序号，日志记录入队/出队的队列深度，可核对配对

### 消息模式分离

- `_handle_session_update` 重构为 `if is_admin: ... else: ...` 两层分支，管理员私聊与普通模式代码完全隔离，互不干扰
- 普通模式（非管理员 + 群聊）：行为与主分支一致，仅 `<message>` 标签提取后发送
- 管理员私聊模式：所有 `agent_message_chunk` 全文捕获到 `_session_raw_buf`，在 `step_finish` 和 prompt 完成时 flush 发送

### 管理员私聊回调处理

| 回调 | 行为 |
|------|------|
| `agent_thought_chunk` | 首次触发发 "思考中……"（与 `tool_call` 互斥，谁先到谁发） |
| `tool_call` | 每次都发进度通知，首次加 "收到～" 前缀；read/write/edit 取 `locations[0].path` 用 `_relpath` 转相对路径显示；web_search title 截断到 100 字；bash 只显示 "正在执行 bash" |
| `tool_call_update` | bash 类结果由 `show_bash_msg` 动态控制（见下方），其余不发 |
| `step_finish` | 发 step 自身摘要 + `_flush_raw_buf(message_only=True)` 清空积攒 |
| prompt 完成 | `_flush_raw_buf(message_only=False)` 全量清空 |

### `_flush_raw_buf` 与 message 提取

- `_flush_raw_buf(sid, message_only)`：`message_only=True` 时只提取 `<message>` 块内文本（缺头尾兜底），`False` 时去标签全量发送
- 新增 `_extract_msg_content(raw)` 静态方法处理缺头尾标签的兜底逻辑
- `step_finish` 调用 `message_only=True`，prompt 完成调用 `message_only=False`

### 频控机制

- 统一 `_send_rate_limited(sid, key, msg)`，按 key 分类独立频控，首条立即发，后续 ≥60s 间隔才发，否则积攒（换行拼接）到下次到期时一起发出
- `tool_call` 通知使用 key=`"tool_call"`
- bash 结果通知使用 key=`"tool_call_update_bash"`
- prompt 完成时所有频控缓存清空，未发的丢弃

### 运行时动态配置

- 配置文件：`{work_dir}/.bridge_runtime.json`（JSON 格式）
- 每个新 prompt 开始时读取一次，缓存到 `_session_runtime_config[sid]`，prompt 内后续回调使用缓存值，无需重启
- 当前支持字段：

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `show_bash_msg` | int | 0 | 0=不显示；1=仅显示 `rawInput` key 名；2=显示完整 key=value（换行分隔） |

### 超长消息缓存

- `on_worker_reply` 中消息 >1500 字时缓存到 `MSG_CACHE`（内存 dict），发摘要（id + 字数 + 前 50 字 + 提示）
- 用户回复 `展示消息 <id>` 触发 `send_chunked` 分段发送（1800 字/段），发送成功后才清缓存
- id 支持带或不带空格、支持带 `「」` 引号

### 权限自动允许

- 收到 `session/request_permission` 时自动回复 JSON-RPC 响应 `{"reply": "allow_always", "message": ""}`

### 其他修复

- `bridge.sh` 改用项目 `.venv/bin/python` 执行，避免系统 python 缺失依赖
- `_build_message_segments` 超长文本 `[:2000]` 截断保留（由上层超长消息缓存机制兜底）

### 维护注意

- 新增字段或 cleanup 时，确保在以下位置同步处理：
  - `_handle_session_update` 管理员私聊分支
  - `_handle_message` prompt 完成分支
  - `send_message` 僵死 session 清理
  - `reset_sessions` / `reset_route_session`
- 所有回复必须走 `_enqueue_reply` / `_enqueue_reply_direct`，禁止直接调用 `self.on_reply`
- 运行时配置通过 `_session_runtime_config[sid]` 获取，不要直接读文件
- 频控新增 key 时只需调用 `_send_rate_limited(sid, "new_key", msg)`，无需额外基础设施
