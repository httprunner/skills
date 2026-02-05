---
name: feishu-bitable-task-manager
description: Manage tasks in Feishu Bitable (multi-dimensional table): fetch, update, and create flows using a fixed schema, filters, pagination, and status update rules. Use when building or running task pullers/reporters that must match a specific task status table and its field mapping, status presets, and date presets.
---

# Feishu Bitable Task Manager (TypeScript)

Follow the task table conventions when pulling and updating tasks in Feishu Bitable.

## Workflow

1) Load env and field mappings.
- Require `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `TASK_BITABLE_URL`.
- Apply `TASK_FIELD_*` overrides if the table uses custom column names.

2) Resolve Bitable identity.
- Parse the Bitable URL to get `app_token`/`wiki_token`, `table_id`, and optional `view_id`.
- If the URL is wiki-based, call `wiki/v2/spaces/get_node` to resolve the app token.

3) Build table filters (fetch/resolve paths).
- Always filter by `App`, `Scene`, `Status`, and `Date` presets.
- Date presets are **literal strings**: `Today`, `Yesterday`, `Any`.
- For explicit dates (`YYYY-MM-DD`), use `ExactDate` filter payload when the table Date column is a datetime type. If the column is plain text, use the literal date string.
- Default status is `pending` when omitted.

4) Call Feishu Bitable search.
- `POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search`
- Use `page_size` + `page_token` for pagination.
- Respect `view_id` only when explicitly enabled (`--use-view`); default ignores view.

5) Validate decoded tasks.
- Keep only rows with `TaskID != 0` and at least one of `Params`, `ItemID`, `BookID`, `URL`, `UserID`, `UserName`.

6) Update task status/metadata.
- Resolve `record_id` from `TaskID` or `BizTaskID` when needed.
- Use `records/batch_update` for multiple updates, `records/{record_id}` for single updates.
- Apply status + timing + metrics updates using the task table field mapping.
- For JSONL ingestion, update any fields whose keys match column names, and map `CDNURL`/`cdn_url` to `Extra`.
- Use `--skip-status` to skip updates for tasks already in a given status (comma-separated).

7) Create tasks.
- Use `records/batch_create` for multiple tasks, `records` for single create.
- Accept JSON/JSONL input (same key conventions as update); map `CDNURL`/`cdn_url` to `Extra`.
- Use `--skip-existing <fields>` to skip creation when existing records match on the given fields (all must match).

8) Derive tasks from a source Bitable (原始多维表格).
- Source core fields: `BID`, `短剧名`, `维权场景`, `主角名`, `付费剧名`.
- Filter source records: `BID` non-empty AND `BID` != `暂无` AND `短剧名` non-empty AND `维权场景` non-empty.
- Before creating, query today’s tasks once and build a `BookID` set. Use task-table filter `App=com.smile.gifmaker`, `Scene=综合页搜索`, `Date=Today`, then skip any source record whose `BID` exists in the fetched `BookID` set.
- For each source record not skipped, create tasks with base fields `BookID=<BID>`, `Scene=综合页搜索`, `Date=Today`, `Status=pending`, `App=com.smile.gifmaker`, `Extra=春节档专项`.
- Task 1 (短剧名): set `Params=<短剧名>`.
- Task 2 (主角名): only if non-empty. Replace commas with spaces before setting `Params=<主角名>`.
- Task 3 (付费剧名): only if `付费剧名` is non-empty and `付费剧名` != `短剧名`; set `Params=<付费剧名>`.

## Run (TypeScript)

Use `npx tsx` so you can execute without building a binary:

```bash
npx tsx scripts/bitable_task.ts <subcommand> [flags]
```

## Examples

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID&view=VIEW_ID"
npx tsx scripts/bitable_task.ts fetch --app com.smile.gifmaker --scene 综合页搜索 --status pending --date Today --limit 10
npx tsx scripts/bitable_task.ts fetch --app com.smile.gifmaker --scene 综合页搜索 --status pending --date 2026-02-05 --limit 10
```

```bash
npx tsx scripts/bitable_task.ts update \
  --task-id 180413 \
  --status running \
  --device-serial 1fa20bb \
  --dispatched-at now
```

Update single task by BizTaskID:

```bash
npx tsx scripts/bitable_task.ts update \
  --biz-task-id ext-20240101-001 \
  --status success \
  --completed-at now
```

Update from JSONL output (per-line task updates):

```bash
npx tsx scripts/bitable_task.ts update --input output.jsonl
```

Update from JSONL with CLI defaults for missing fields:

```bash
npx tsx scripts/bitable_task.ts update \
  --input tasks.jsonl \
  --status ready \
  --date 2026-01-27
```

Create tasks from JSONL with defaults:

```bash
npx tsx scripts/bitable_task.ts create \
  --input tasks.jsonl \
  --app com.smile.gifmaker \
  --scene 单个链接采集 \
  --status pending \
  --date 2026-01-27
```

Create from JSONL and skip when BizTaskID already exists:

```bash
npx tsx scripts/bitable_task.ts create \
  --input tasks.jsonl \
  --app com.smile.gifmaker \
  --scene 单个链接采集 \
  --status ready \
  --date 2026-01-27 \
  --skip-existing BizTaskID
```

Create from JSONL and skip when both BookID and UserID match existing records:

```bash
npx tsx scripts/bitable_task.ts create \
  --input tasks.jsonl \
  --skip-existing BookID,UserID
```

Create a single task with explicit fields:

```bash
npx tsx scripts/bitable_task.ts create \
  --biz-task-id GYS2601290001 \
  --app com.smile.gifmaker \
  --scene 单个链接采集 \
  --status pending \
  --date 2026-01-27 \
  --book-id 7591421623471705150 \
  --user-id 5891321132 \
  --url https://www.kuaishou.com/short-video/3xcx7sk3yi583je
```

Derive tasks from source Bitable (原始多维表格):

Fetch source and create tasks:

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID"
npx tsx scripts/bitable_derive.ts sync \
  --bitable-url "https://.../base/SOURCE_APP?table=SOURCE_TABLE" \
  --task-url "https://.../base/TASK_APP?table=TASK_TABLE" \
  --app com.smile.gifmaker \
  --extra 春节档专项 \
  --skip-existing
```

Create one task per source row and store `Params` as a JSON list `[短剧名, 主角名, 付费剧名]`:

```bash
npx tsx scripts/bitable_derive.ts sync \
  --bitable-url "https://.../base/SOURCE_APP?table=SOURCE_TABLE" \
  --task-url "https://.../base/TASK_APP?table=TASK_TABLE" \
  --app com.smile.gifmaker \
  --extra 春节档专项 \
  --params-list \
  --skip-existing
```

Fetch source table to JSONL (fields are simplified to raw values, not typed objects):

```bash
npx tsx scripts/bitable_derive.ts fetch \
  --bitable-url "https://.../base/SOURCE_APP?table=SOURCE_TABLE" \
  --output source.jsonl
```

Create tasks from JSONL:

```bash
npx tsx scripts/bitable_derive.ts create \
  --input source.jsonl \
  --skip-existing \
  --app com.smile.gifmaker \
  --extra 春节档专项
```

Notes:
- `bitable_derive.ts create --skip-existing` skips when a task already exists for the same `App + Scene + Date + BookID`.
- `bitable_task.ts fetch --date YYYY-MM-DD` uses `ExactDate` filtering when the task table Date field is a datetime type (text Date columns should use the literal date string).

## Resources

- Read `references/task-fetch.md` for filters, pagination, validation, and field mapping.
- Read `references/task-update.md` for status updates, timing fields, and batch update rules.
- Read `references/task-create.md` for create payload rules and batch create behavior.
- Read `references/feishu-integration.md` for Feishu API endpoints and request/response payloads.
- `scripts/bitable_task.ts`: single CLI entrypoint (`fetch`/`update`/`create`).
- `scripts/bitable_derive.ts`: derive tasks from a source Bitable and create tasks in the task table.
- `scripts/bitable_common.ts`: Feishu OpenAPI HTTP + token/wiki helpers + value/timestamp coercion + env field mapping.
