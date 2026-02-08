---
name: group-webhook-dispatch
description: Dispatch, reconcile, and upsert group webhook pushes after tasks reach terminal states. Use to trigger by TaskID/GroupID, run one-shot backfill by date for pending/failed, and write webhook plans into WEBHOOK_BITABLE_URL (JSON/JSONL).
---

# Group Webhook Dispatch

用于替代常驻 webhook worker，采用“事件驱动 + 按需补偿”的运行方式：触发时检查是否就绪并推送；必要时按日期做一次补偿扫描。

## 路径约定

统一安装与执行目录：`~/.agents/skills/group-webhook-dispatch/`。执行前先进入该目录：

```bash
cd ~/.agents/skills/group-webhook-dispatch
```

## Quick start（事件触发，推荐）

在 `~/.agents/skills/group-webhook-dispatch/` 目录运行：

```bash
npx tsx scripts/dispatch_webhook.ts --task-id <TASK_ID>
```

## Entry points

- `scripts/dispatch_webhook.ts`: 按 `--task-id` 或 `--group-id` 触发单组检查与推送
- `scripts/reconcile_webhook.ts`: 按 `--date` 扫描 `pending/failed` 做单次补偿
- `scripts/upsert_webhook_plan.ts`: 向 `WEBHOOK_BITABLE_URL` 批量创建/更新 webhook 计划（upsert）
- `scripts/webhook_lib.ts`: Feishu/SQLite/状态机公共逻辑

## Webhook 计划 upsert（JSON/JSONL）

输入 item 约定：
- `group_id`（必填）
- `date`（必填，`YYYY-MM-DD`）
- `biz_type`（可选，默认 `piracy_general_search`）
- `task_ids`（必填，数组）
- `drama_info`（可选，JSON 字符串）

运行：

```bash
npx tsx scripts/upsert_webhook_plan.ts --input <JSON/JSONL_FILE>
```

## Required env

- `FEISHU_APP_ID`, `FEISHU_APP_SECRET`
- `TASK_BITABLE_URL`, `WEBHOOK_BITABLE_URL`
- `CRAWLER_SERVICE_BASE_URL`
- Optional: `TRACKING_STORAGE_DB_PATH`（默认 `~/.eval/records.sqlite`）

## Debugging

- 使用 `--dry-run` 只打印将要执行的动作，不写表、不发 webhook。

## Resources

- Read `references/commands.md` for full command examples (env, debug, reconcile).
