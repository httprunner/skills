# Commands

## 约定
- 所有命令默认在 `piracy-handler` 目录执行。

## 基本运行

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export TASK_BITABLE_URL="https://.../base/...?...table=tbl_task"
export DRAMA_BITABLE_URL="https://.../base/...?...table=tbl_drama"
export WEBHOOK_BITABLE_URL="https://.../base/...?...table=tbl_webhook"

npx tsx scripts/piracy_detect.ts --task-id 123456
npx tsx scripts/piracy_create_subtasks.ts --task-id 123456
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id 123456
```

## 调试模式（不写表）

```bash
npx tsx scripts/piracy_detect.ts --task-id 123456 --threshold 0.5 --output -
npx tsx scripts/piracy_create_subtasks.ts --task-id 123456 --dry-run
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id 123456 --dry-run
```

## 完整调试日志

```bash
npx tsx scripts/piracy_detect.ts --task-id 123456 --threshold 0.2 --log-level debug
```

## 指定 sqlite 路径

```bash
npx tsx scripts/piracy_detect.ts --task-id 123456 --db-path ~/.eval/records.sqlite
```

## 输出

`piracy_detect.ts` 输出 JSON（可保存为 `detect.json`）：
- `selected_groups[]`：命中阈值的分组（含 `ratio`、`collection_item_id`、`anchor_links`、`drama` 等）
- `summary.resolved_task_count / unresolved_task_ids`
- `summary.missing_drama_meta_book_ids / invalid_drama_duration_book_ids`
- `summary.groups_above_threshold`

`piracy_create_subtasks.ts` / `piracy_upsert_webhook_plans.ts` 会输出各自的 JSON summary（并透传底层 npx 命令 stdout）。

说明：
- 子任务 `Date` 默认继承父任务日期（非“当前日期”）。
- 若多维表视图过滤了 Today/场景/App，可能看不到新建记录。
