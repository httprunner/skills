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
npx tsx scripts/piracy_detect.ts --task-ids 123456,123457 --data-source supabase --supabase-table capture_results

# 不传 --task-ids 时，自动按飞书条件筛选父任务并按 BookID 分组
npx tsx scripts/piracy_detect.ts \
  --task-app com.tencent.mm \
  --task-date Today,Yesterday \
  --data-source supabase

# 不传 --task-app 时默认 com.tencent.mm
npx tsx scripts/piracy_detect.ts --task-date Today --data-source supabase
```

主要参数：

- `--task-ids <csv>`：任务 ID 列表
- `--task-app/--task-date/--task-limit`：飞书筛选条件（`--task-app` 支持 CSV，默认 `com.tencent.mm`；`--task-date` 支持 CSV，默认 `Today`；Scene 固定为`综合页搜索`）
- `--data-source sqlite|supabase`：结果读取源（默认 `sqlite`）
- `--sqlite-path`：sqlite 路径（sqlite 模式）
- `--supabase-table --supabase-page-size --supabase-timeout-ms`：Supabase 参数（supabase 模式）
- `--threshold`：盗版阈值（默认 `0.5`）
- `--output`：输出文件路径；`-` 仅允许单 detect unit

筛选规则：
- `--task-ids` 与 `--task-app/--task-date` 互斥；
- 指定 `--task-ids` 时，按 TaskID 检测；
- 未指定 `--task-ids` 时，按飞书筛选条件检测（未传 `--task-app` 时默认 `com.tencent.mm`，未传 `--task-date` 时默认 `Today`）。
- 传入多个 `--task-date` 值时，按输入顺序作为优先级依次处理（例如 `Today,Yesterday` 先处理 `Today`）。

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

## 3. upsert_webhook_plan.ts

统一 webhook plan 入口：支持 detect.json 与通用 plan 两种输入。

```bash
# detect 输入（推荐用于 piracy 主流程）
npx tsx scripts/upsert_webhook_plan.ts --source detect --input ~/.eval/123456/detect.json --biz-type piracy_general_search
npx tsx scripts/upsert_webhook_plan.ts --source detect --input ~/.eval/123456/detect.json --dry-run

# 通用 plan 输入（JSON/JSONL）
npx tsx scripts/upsert_webhook_plan.ts --source plan --input plans.jsonl
npx tsx scripts/upsert_webhook_plan.ts --source plan --group-id "微信视频号_123_xxx" --task-id 111,222 --date 2026-02-15
```

## 4. piracy_pipeline.ts

兼容入口：一条命令跑 `detect + create_subtasks + upsert_webhook_plan`。

```bash
npx tsx scripts/piracy_pipeline.ts --task-ids 69111,69112,69113
npx tsx scripts/piracy_pipeline.ts --task-ids 69111,69112,69113 --dry-run

# 默认扫描：task-app=com.tencent.mm, task-date=Today
npx tsx scripts/piracy_pipeline.ts --dry-run

# 多 App + 多日期扫描
npx tsx scripts/piracy_pipeline.ts \
  --task-app com.tencent.mm,com.smile.gifmaker \
  --task-date Today,Yesterday \
  --dry-run

```

主要参数：

- `--task-ids <csv>`：指定任务 ID 列表（与 `--task-app/--task-date` 互斥）
- `--task-app <csv>`：任务 App 过滤（支持单值/多值 CSV，默认 `com.tencent.mm`）
- `--task-date <csv>`：任务日期过滤（支持单值/多值 CSV，默认 `Today`）
- `--task-limit <n>`：每轮 fetch 上限（0 表示不限制）
- `--threshold <num>`：盗版命中阈值（默认 `0.5`）

前置校验语义：

- 任务分组键为 `App + BookID + Date`
- 多日期输入时，按 `--task-date` 提供顺序优先处理
- 若分组内存在 `status != success|error`，整组跳过
- 若分组内任一任务 `ItemsCollected` 为空或非数字，整组跳过
- 从 `capture_results` 按分组 `task_ids` 拉取结果后，按 `task_id` 分别统计 `distinct item_id`（空 `item_id` 使用行级 fallback）
- 仅当分组内每个任务都满足 `observed_distinct_item_count == ItemsCollected` 时分组进入 detect
- 仅 detect 命中组会创建个人页任务并 upsert webhook plan

## 5. webhook.ts

统一 webhook 入口，支持单组触发与批量补偿。

```bash
# 单组触发（按 task-id 或 group-id）
npx tsx scripts/webhook.ts --mode single --task-id 123456 --data-source sqlite
npx tsx scripts/webhook.ts --mode single --group-id "微信视频号_123_xxx" --date 2026-02-15 --data-source supabase --table capture_results

# 批量补偿（按日期扫描 pending/failed）
npx tsx scripts/webhook.ts --mode reconcile --date 2026-02-15 --data-source sqlite
npx tsx scripts/webhook.ts --mode reconcile --date 2026-02-15 --data-source supabase --table capture_results --limit 100
npx tsx scripts/webhook.ts --mode reconcile --date Today,Yesterday --data-source supabase --table capture_results --limit 100

# 自动模式（默认）：传 task/group 走 single，否则走 reconcile
npx tsx scripts/webhook.ts --task-id 123456 --dry-run
npx tsx scripts/webhook.ts --date 2026-02-15 --dry-run
```

主要参数：

- `--mode auto|single|reconcile`：运行模式（默认 `auto`）
- `--task-id/--group-id`：single 模式入口参数
- `--date`：single/reconcile 的日期（支持单值/多值 CSV 与 `Today/Yesterday`，single 不传时默认今天）
- `--biz-type`：业务类型（默认 `piracy_general_search`）
- `--limit`：reconcile 最大处理条数（默认 `50`）
- `--data-source sqlite|supabase`：payload 采集结果来源
- `--sqlite-path`：sqlite 路径（sqlite 模式）
- `--table --page-size --timeout-ms`：Supabase 参数（supabase 模式）
- `--max-retries`：最大重试次数

ready 语义说明：
- 组内任务全部为 `success|error` 才会触发 webhook；
- `failed` 任务不会触发 ready，仅进入后续 reconcile 重试路径。

## 6. whitelist_check.ts

豁免检查（调用 `GET /drama/exemption`）。

```bash
npx tsx scripts/whitelist_check.ts --book-id 7386963157631110169 --account-id 123 --has-short-play-tag true
```
