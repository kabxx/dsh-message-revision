# @kabxx/dsh-message-revision

面向 DeepSeek Harness Web（`0.2.2-rc.5`）的历史消息原位编辑与重发插件。

- 在已发送的用户消息旁提供「编辑消息」入口，原位复用原生输入框，支持修改正文、增删附件、切换模型与权限预设，并直接联动「语音输入」插件。
- 保留原会话 ID 及目标轮次之前的上下文，替换该消息与后续对话，并在 POSIX 环境下同步清理随旧轮次撤回的子智能体会话。
- 基于 v4 JSONL（含 Zstandard 压缩）事务日志实现崩溃自动回滚与多标签页重连同步；重发失败时自动保留编辑草稿与待传文件。

## 安装

需要 Node.js `>= 24.9.0`：

```bash
dsh plugin --profile web add @kabxx/dsh-message-revision
```

也可在 DSH 插件管理器中选择「添加插件」并输入 `@kabxx/dsh-message-revision`。本地调试源码时，直接填入克隆后的仓库目录绝对路径或执行 `dsh plugin --profile web add .` 即可。

## 开发

本包无外部依赖、无构建步骤，直接基于 Node.js 原生 ESM 运行：

```bash
npm run check
npm test
npm pack --dry-run
```

## 项目结构

| 路径 | 内容 |
| --- | --- |
| [`src/index.js`](src/index.js) | Host 入口，注册 `/api/message-revision` 接口与会话重开调度 |
| [`src/revision.js`](src/revision.js) | 轮次边界解析、v4 JSONL / Zstandard 重写与子智能体归属校验 |
| [`src/rewrite-journal.js`](src/rewrite-journal.js) | 事务意图持久化、子会话暂存移出与启动恢复 |
| [`src/client.js`](src/client.js) | 原位输入框挂载、草稿恢复与多标签页重连同步 |
| [`locale/`](locale/) | 插件管理页中英文标题与描述 |
| [`test/`](test/) | 多轮编辑、子智能体迁移、草稿恢复与崩溃回滚测试 |

插件入口与注入配置见 [package.json](package.json) 和 [cordis.patch.yml](cordis.patch.yml)。
