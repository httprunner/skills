# Scripts Layout

## Entry Commands

- `scripts/piracy_detect.ts`: detect 阶段入口（支持 sqlite/supabase）。
- `scripts/piracy_create_subtasks.ts`: 基于 `detect.json` 创建子任务。
- `scripts/piracy_upsert_webhook_plans.ts`: 基于 `detect.json` upsert webhook 计划。
- `scripts/piracy_pipeline_supabase.ts`: 兼容入口（detect + create + upsert）。
- `scripts/dispatch_webhook.ts`: 单 group 推送 webhook。
- `scripts/reconcile_webhook.ts`: 按日期补偿 pending/failed 计划。
- `scripts/upsert_webhook_plan.ts`: 通用 webhook plan upsert 工具。
- `scripts/whitelist_check.ts`: 调用 crawler exemption 接口。

## Shared Modules

- `scripts/shared/lib.ts`: 通用工具、时间/解析、子进程桥接。
- `scripts/shared/cli.ts`: 通用 CLI 参数解析辅助。

## Detect Modules

- `scripts/detect/task_units.ts`: detect 的任务分组与来源解析。
- `scripts/detect/core.ts`: detect 聚合、阈值和输出构建核心逻辑。
- `scripts/detect/runner.ts`: detect 执行编排和输出写入。

## Data Source Modules

- `scripts/data/result_source.ts`: `capture_results` 统一读取（sqlite/supabase）。
- `scripts/data/result_source_cli.ts`: 数据源 CLI 参数统一转换。

## Webhook Modules

- `scripts/webhook/lib.ts`: webhook 计划读写、状态聚合、dispatch/reconcile 逻辑。
