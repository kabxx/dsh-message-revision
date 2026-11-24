# @kabxx/dsh-message-revision

编辑 DeepSeek Harness 历史用户消息，自动回退该消息之后的对话轮次并重新发送。

- 在空闲主会话的用户消息气泡旁悬停显示编辑按钮，点击即可原位展开编辑器
- 直接复用 DSH 原生输入框组件，支持附件增删、模型选择、思考强度和权限模式，并在提交失败时于当前页面保留草稿
- 在受限子进程中流式扫描并准备候选日志，按编辑时间点一并截断或移除关联的子智能体会话，并自动同步多标签页子智能体会话视图
- 基于跨平台原生内核排他锁（Linux / macOS `flock`、Windows 命名内核信号量）与预写式事务日志原子替换会话记录，异常中断自动回滚并支持重发投递恢复

## 安装

支持 Linux / WSL、macOS 与 Windows 上的 DSH 宿主（Node.js >= 24.9.0）：

```bash
dsh plugin --profile web add @kabxx/dsh-message-revision
```

也可以在 DSH 插件管理器中填写 `@kabxx/dsh-message-revision` 安装并启用。

## 开发

```bash
npm run check
npm test
npm pack --dry-run
```

## 项目结构

| 路径 | 说明 |
| --- | --- |
| [package.json](package.json) | 包清单与 DSH 插件导出配置 |
| [cordis.patch.yml](cordis.patch.yml) | 默认插件预设注入规则 |
| [src/index.js](src/index.js) | 服务端插件入口、RPC 路由、会话重开与投递恢复协调 |
| [src/client.js](src/client.js) | 前端消息悬停编辑按钮、原位编辑器与视图同步 |
| [src/bounded-revision.js](src/bounded-revision.js) | 有界流式编辑规划与子智能体家族树核验 |
| [src/bounded-commit.js](src/bounded-commit.js) | 多会话排他锁获取、原子替换提交与失败回滚 |
| [src/revision-candidates.js](src/revision-candidates.js) | 候选会话文件暂存目录生命周期管理 |
| [src/revision-notifications.js](src/revision-notifications.js) | 改写提交后的跨标签页子智能体会话变更通知流 |
| [src/rollback-events.js](src/rollback-events.js) | 崩溃恢复期间可安全回退的临时恢复标记判定 |
| [src/platform.js](src/platform.js) | Linux / macOS / Windows 宿主原生锁、Job Object、原子替换与进程标识适配 |
| [src/native-lease.js](src/native-lease.js) | 跨平台内核会话排他写锁封装（与 DSH `SessionWriteLease` 互通） |
| [src/rollout-process.js](src/rollout-process.js) | 受内存与超时约束的日志准备子进程调度（Linux `systemd-run`/`prlimit`、Windows Job Object、macOS RSS 监控） |
| [src/rollout-worker.js](src/rollout-worker.js) | 流式日志扫描、截断边界计算与候选日志写出 |
| [src/rollout-stream.js](src/rollout-stream.js) | V4 JSONL / Zstandard 流式逐行读写与完整性校验 |
| [src/revision.js](src/revision.js) | 消息内容改写、子智能体目录切分与单文件校验辅助 |
| [src/rewrite-journal.js](src/rewrite-journal.js) | 预写式改写事务日志与启动崩溃恢复 |
| [locale/](locale/) | 插件管理页中英文多语言资源 |
| [test/](test/) | 前后端、流式子进程与崩溃恢复测试 |
