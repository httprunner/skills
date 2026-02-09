# Commands Reference

所有命令在 `~/.agents/skills/piracy-handler/` 目录执行。

## 环境变量

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export TASK_BITABLE_URL="https://.../base/...?...table=tbl_task"
export DRAMA_BITABLE_URL="https://.../base/...?...table=tbl_drama"
export WEBHOOK_BITABLE_URL="https://.../base/...?...table=tbl_webhook"
export CRAWLER_SERVICE_BASE_URL="https://..."   # webhook 推送 + 豁免检查
```

## piracy_detect.ts

从 sqlite 聚类 + 阈值筛选盗版分组（只读）。

```bash
npx tsx scripts/piracy_detect.ts --task-id 123456
npx tsx scripts/piracy_detect.ts --task-id 123456 --output -          # stdout
npx tsx scripts/piracy_detect.ts --task-id 123456 --threshold 0.3     # 自定义阈值
npx tsx scripts/piracy_detect.ts --task-id 123456 --log-level debug   # 调试
npx tsx scripts/piracy_detect.ts --task-id 123456 --db-path ~/.eval/records.sqlite
```

| Flag | 说明 | 默认值 |
|---|---|---|
| `--task-id` (必填) | 父任务 TaskID | - |
| `--db-path` | SQLite 路径 | `~/.eval/records.sqlite` |
| `--threshold` | 采集/总时长阈值 | `0.5` |
| `--log-level` | `silent\|error\|info\|debug` | `info` |
| `--output` | 输出路径；`-` 为 stdout | `~/.eval/<TaskID>/detect.json` |
| `--app` | 覆盖父任务 App | - |
| `--book-id` | 覆盖父任务 BookID | - |
| `--date` | 覆盖采集日期 | - |

**输出结构 (`detect.json`):**

- `selected_groups[]`：命中分组
  - `group_id`, `app`, `book_id`, `user_id`, `user_name`, `params`
  - `capture_duration_sec`, `ratio`
  - `collection_item_id`, `anchor_links[]`
  - `drama { name, episode_count, rights_protection_scenario, priority, total_duration_sec }`
- `summary`：`resolved_task_count`, `unresolved_task_ids[]`, `missing_drama_meta_book_ids[]`, `invalid_drama_duration_book_ids[]`, `groups_above_threshold`

## piracy_create_subtasks.ts

为命中分组创建子任务（写 `TASK_BITABLE_URL`）。

```bash
npx tsx scripts/piracy_create_subtasks.ts --task-id 123456
npx tsx scripts/piracy_create_subtasks.ts --input path/to/detect.json
npx tsx scripts/piracy_create_subtasks.ts --task-id 123456 --dry-run
```

**子任务创建规则：**

- 每个命中分组必建 `个人页搜索` 子任务
- 有 `collection_item_id` 时建 `合集视频采集` 子任务
- 有 `anchor_links` 时按链接建 `视频锚点采集` 子任务
- 子任务 `Date` 继承父任务日期（非当前日期）
- 已存在相同 GroupID+Day 的子任务不会重复创建

## piracy_upsert_webhook_plans.ts

为检测到的分组创建/更新 webhook 推送计划（写 `WEBHOOK_BITABLE_URL`）。

```bash
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id 123456
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id 123456 --dry-run
npx tsx scripts/piracy_upsert_webhook_plans.ts --input detect.json --biz-type custom_type
```

## dispatch_webhook.ts

按 TaskID 或 GroupID 触发单组 webhook 推送。组内所有任务达到终态时才发送。

```bash
npx tsx scripts/dispatch_webhook.ts --task-id 123456
npx tsx scripts/dispatch_webhook.ts --group-id "视频号_12345_user1" --date 2025-01-15
npx tsx scripts/dispatch_webhook.ts --task-id 123456 --dry-run
```

## reconcile_webhook.ts

按日期扫描 pending/failed 状态的 webhook 计划，逐条重试。

```bash
npx tsx scripts/reconcile_webhook.ts --date 2025-01-15
npx tsx scripts/reconcile_webhook.ts --date 2025-01-15 --limit 100 --dry-run
```

| Flag | 说明 | 默认值 |
|---|---|---|
| `--date` | 扫描日期 | 今天 |
| `--biz-type` | BizType | `piracy_general_search` |
| `--limit` | 最大处理条数 | `50` |
| `--dry-run` | 不实际推送/写表 | - |
| `--max-retries` | 最大重试次数 | `3` |

## upsert_webhook_plan.ts

通用 webhook 计划 upsert，支持 JSON/JSONL 输入。通常由 `piracy_upsert_webhook_plans.ts` 内部调用，也可直接使用。

```bash
npx tsx scripts/upsert_webhook_plan.ts --input plans.jsonl
npx tsx scripts/upsert_webhook_plan.ts --group-id "视频号_12345_user1" --task-id 111,222 --date 2025-01-15
```

输入 item 字段：`group_id`(必填), `date`(必填), `task_ids`(必填), `biz_type`(默认 `piracy_general_search`), `drama_info`(JSON 字符串)。

## whitelist_check.ts

豁免检查，调用 `GET /drama/exemption`（与 webhook 推送同一服务 `CRAWLER_SERVICE_BASE_URL`）。

```bash
# 必传三参数
npx tsx scripts/whitelist_check.ts --book-id 7386963157631110169 --account-id 123 --has-short-play-tag true

# JSON 输出
npx tsx scripts/whitelist_check.ts --book-id 7386963157631110169 --account-id 123 --has-short-play-tag false --format json

# 自定义可选参数
npx tsx scripts/whitelist_check.ts --book-id 7386963157631110169 --account-id 123 --has-short-play-tag true \
  --platform 快手 --viewing-mode free --infringement-ratio 0.8
```

| Flag | 说明 | 默认值 |
|---|---|---|
| `--book-id` (必填) | BookID | - |
| `--account-id` (必填) | AccountID | - |
| `--has-short-play-tag` (必填) | 是否短剧标签：`true\|false` | - |
| `--platform` | `快手` 或 `微信视频号` | `微信视频号` |
| `--viewing-mode` | `free` 或 `need_pay` | `free` |
| `--infringement-ratio` | 匹配豁免比例 | `1` |
| `--format` | `json\|text` | `text` |

text 模式输出 `EXEMPT` 或 `NOT_EXEMPT`。

## 字段名覆盖

Webhook 表和任务表的字段名可通过 `WEBHOOK_FIELD_*` 和 `TASK_FIELD_*` 环境变量覆盖。例如 `WEBHOOK_FIELD_STATUS=Status`、`TASK_FIELD_GROUPID=GroupID`。详见 `webhook_lib.ts` 中 `webhookFields()` / `taskFields()` 函数。
