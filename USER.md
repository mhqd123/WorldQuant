# 用户期望

遇到问题时：
1. 先自己尝试解决（查日志、改配置、重试）
2. 用 exec 跑诊断命令（如 `openclaw gateway status`、`journalctl`、`ss -ltnp` 等）
3. 只有多次尝试仍失败时才向人求助

## 定时任务：寻找并提交 Alpha

当收到「去寻找并提交 Alpha」或类似指令时，按以下流程执行：
1. 查阅 `~/.openclaw/workspace/BRAIN_EXPERT.md`
2. 若无 `alphas.txt` 或为空，则根据策略模板生成表达式，写入 `~/.openclaw/workspace/alphas.txt`
3. 执行 `python ~/.openclaw/workspace/skills/worldquant-brain/scripts/discover_and_submit.py ~/.openclaw/workspace`（若技能在全局则用 `~/.openclaw/skills/`）
4. 合格表达式会记录到 `alphas_qualified.json`

## 执行风格（长期有效）

- 默认采用**自主闭环执行**，不是聊天陪跑。
- 默认：先思考、先决定、先执行，少询问。
- 对 Alpha 任务，我应自行完成：寻找候选、测试、淘汰、迭代、再次提交，除非遇到真实阻塞，否则不要把中间步骤频繁抛回给用户。
- 只有在以下情况才主动打断用户：
  1. 出现真正的权限/环境阻塞
  2. 需要用户做不可替代的偏好决策
  3. 已经拿到值得汇报的结果
  4. 发现异常风险需要用户知情
- 在 Alpha 语境里，用户说“提交”，默认指**提交到 WorldQuant BRAIN 平台**，不是 git。
- 监控页必须反映**真实运行态**，不能只做静态展示来冒充实时监控。
