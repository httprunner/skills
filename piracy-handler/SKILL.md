---
name: piracy-handler
description: 盗版检测与后置处理编排器。Use when (1) wechat-search-collector 综合页搜索任务成功完成后，需要从本地 sqlite 聚类筛选盗版、创建子任务并写入 webhook 推送计划；(2) 个人页/合集/锚点等子任务进入终态后，需要触发 webhook 推送或按日期补偿重试。核心脚本：piracy_detect → piracy_create_subtasks → piracy_upsert_webhook_plans → dispatch_webhook / reconcile_webhook。
---

# Piracy Handler

执行目录：`~/.agents/skills/piracy-handler/`

## 核心流程

### 综合页搜索后置（三步串行）

```bash
npx tsx scripts/piracy_detect.ts --task-id <TASK_ID>
npx tsx scripts/piracy_create_subtasks.ts --task-id <TASK_ID>
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id <TASK_ID>
```

1. **detect** — 从 sqlite `capture_results` 聚类，按采集时长/总时长比阈值筛选，输出 `~/.eval/<TaskID>/detect.json`
2. **create_subtasks** — 为命中分组创建子任务（个人页搜索/合集视频采集/视频锚点采集）
3. **upsert_webhook_plans** — 为命中分组写入 webhook 推送计划

### Webhook 触发（子任务终态后）

```bash
npx tsx scripts/dispatch_webhook.ts --task-id <TASK_ID>
```

按 TaskID 解析 GroupID，检查组内所有任务是否到达终态，就绪则推送 webhook。

### Webhook 补偿（按日期批量重试）

```bash
npx tsx scripts/reconcile_webhook.ts --date <YYYY-MM-DD>
```

扫描 pending/failed 状态的 webhook 计划，逐条重试。

### 豁免检查（可选）

```bash
npx tsx scripts/whitelist_check.ts --book-id <BOOK_ID> --account-id <ACCOUNT_ID> --has-short-play-tag true
```

所有命令支持 `--dry-run`。完整 CLI flags、输出结构与调试示例见 `references/commands.md`。

## Required Env

| 变量 | 用途 |
|---|---|
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | 飞书应用凭证 |
| `TASK_BITABLE_URL` | 任务状态表 |
| `DRAMA_BITABLE_URL` | 剧单元信息表（detect 用） |
| `WEBHOOK_BITABLE_URL` | 推送计划表 |
| `CRAWLER_SERVICE_BASE_URL` | webhook 推送与豁免检查服务 |
