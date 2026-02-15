# Commands Reference

工作目录：`~/.agents/skills/piracy-handler/`

## 环境变量

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export TASK_BITABLE_URL="https://.../base/...?...table=tbl_task"
export DRAMA_BITABLE_URL="https://.../base/...?...table=tbl_drama"
export WEBHOOK_BITABLE_URL="https://.../base/...?...table=tbl_webhook"
export CRAWLER_SERVICE_BASE_URL="https://..."
export SUPABASE_URL="https://<project>.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="..."
export SUPABASE_RESULT_TABLE="capture_results"
```

## 1. piracy_detect.ts

生成 `detect.json`（支持 sqlite/supabase）。

```bash
# 指定 TaskID 列表（支持单个或多个）
npx tsx scripts/piracy_detect.ts --task-ids 123456 --data-source sqlite
npx tsx scripts/piracy_detect.ts --task-ids 123456,123457 --data-source supabase --table capture_results

# 从飞书筛选父任务并按 BookID 分组
npx tsx scripts/piracy_detect.ts \
  --from-feishu \
  --task-app com.tencent.mm \
  --task-scene 综合页搜索 \
  --task-status success \
  --task-date Today \
  --data-source supabase
```

主要参数：

- `--task-ids <csv>`：任务 ID 列表
- `--from-feishu`：启用飞书筛选模式
- `--task-app/--task-scene/--task-status/--task-date/--task-limit`：飞书筛选条件
- `--data-source sqlite|supabase`：结果读取源（默认 `sqlite`）
- `--db-path`：sqlite 路径（sqlite 模式）
- `--table --page-size --timeout-ms`：Supabase 参数（supabase 模式）
- `--threshold`：盗版阈值（默认 `0.5`）
- `--output`：输出文件路径；`-` 仅允许单 detect unit

兼容参数：`--task-id`（已废弃，等价 `--task-ids <id>`）。

检测语义说明：
- 仅纳入 `status=success` 的综合页任务结果行参与聚合；
- ratio 大于等于阈值（`ratio >= threshold`）即命中；
- 微信任务 GroupID 前缀统一为 `微信视频号`（对齐 TaskAgent）。

## 2. piracy_create_subtasks.ts

基于 detect.json 创建子任务。

```bash
npx tsx scripts/piracy_create_subtasks.ts --task-id 123456
npx tsx scripts/piracy_create_subtasks.ts --input ~/.eval/123456/detect.json
npx tsx scripts/piracy_create_subtasks.ts --task-id 123456 --dry-run
```

## 3. piracy_upsert_webhook_plans.ts

基于 detect.json 创建/更新 webhook 计划。

```bash
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id 123456
npx tsx scripts/piracy_upsert_webhook_plans.ts --input ~/.eval/123456/detect.json --biz-type piracy_general_search
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id 123456 --dry-run
```

## 4. piracy_pipeline_supabase.ts

兼容入口：一条命令跑 `detect + create_subtasks + upsert_webhook_plans`。

```bash
npx tsx scripts/piracy_pipeline_supabase.ts --task-ids 69111,69112,69113
npx tsx scripts/piracy_pipeline_supabase.ts --task-ids 69111,69112,69113 --dry-run
```

## 5. dispatch_webhook.ts

按 `task-id` 或 `group-id` 推送单组 webhook。

```bash
npx tsx scripts/dispatch_webhook.ts --task-id 123456 --data-source sqlite
npx tsx scripts/dispatch_webhook.ts --group-id "微信视频号_123_xxx" --date 2026-02-15 --data-source supabase --table capture_results
npx tsx scripts/dispatch_webhook.ts --task-id 123456 --dry-run
```

## 6. reconcile_webhook.ts

按日期批量重试 `pending/failed` webhook 计划。

```bash
npx tsx scripts/reconcile_webhook.ts --date 2026-02-15 --data-source sqlite
npx tsx scripts/reconcile_webhook.ts --date 2026-02-15 --data-source supabase --table capture_results --limit 100
npx tsx scripts/reconcile_webhook.ts --date 2026-02-15 --dry-run
```

主要参数：

- `--date`：扫描日期
- `--biz-type`：业务类型（默认 `piracy_general_search`）
- `--limit`：最大处理条数（默认 `50`）
- `--data-source sqlite|supabase`：payload 采集结果来源
- `--db-path`：sqlite 路径（sqlite 模式）
- `--table --page-size --timeout-ms`：Supabase 参数（supabase 模式）
- `--max-retries`：最大重试次数

ready 语义说明：
- 组内任务全部为 `success|error` 才会触发 webhook；
- `failed` 任务不会触发 ready，仅进入后续 reconcile 重试路径。

## 7. upsert_webhook_plan.ts

通用 webhook 计划 upsert。

```bash
npx tsx scripts/upsert_webhook_plan.ts --input plans.jsonl
npx tsx scripts/upsert_webhook_plan.ts --group-id "微信视频号_123_xxx" --task-id 111,222 --date 2026-02-15
```

输入字段：`group_id`、`date`、`task_ids`（必填）；`biz_type`、`drama_info`（可选）。

## 8. whitelist_check.ts

豁免检查（调用 `GET /drama/exemption`）。

```bash
npx tsx scripts/whitelist_check.ts --book-id 7386963157631110169 --account-id 123 --has-short-play-tag true
```
