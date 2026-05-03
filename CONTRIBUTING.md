# Contributing

感谢你考虑为 Agent Channel Bridge 贡献代码！

## 开发环境

```bash
git clone https://github.com/MrToy/agent-channel-bridge.git
cd agent-channel-bridge
make dev    # pip install -e ".[dev]"
```

## 代码规范

- Python 3.10+
- 使用 `ruff` 做代码检查
- 使用 `mypy` 做类型检查（可选）
- 使用 `pytest` 做测试

```bash
make lint   # ruff + mypy
make test   # pytest
```

## 提交 PR

1. Fork 本仓库
2. 创建特性分支
3. 提交改动
4. 确保通过 lint + test
5. 创建 Pull Request
