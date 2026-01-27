---
name: feishu-bitable-task-manager
description: Manage tasks in Feishu Bitable (multi-dimensional table): fetch, update, and create flows using a fixed schema, filters, pagination, and status update rules. Use when building or running task pullers/reporters that must match a specific task status table and its field mapping, status presets, and date presets.
---

# Feishu Bitable Task Manager

Follow the task table conventions when pulling and updating tasks in Feishu Bitable.

## Workflow

1) Load env and field mappings.
- Require `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `TASK_BITABLE_URL`.
- Apply `TASK_FIELD_*` overrides if the table uses custom column names.

2) Resolve Bitable identity.
- Parse the Bitable URL to get `app_token`/`wiki_token`, `table_id`, and optional `view_id`.
- If the URL is wiki-based, call `wiki/v2/spaces/get_node` to resolve the app token.

3) Build table filters.
- Always filter by `App`, `Scene`, `Status`, and `Date` presets.
- Date presets are **literal strings**: `Today`, `Yesterday`, `Any`.
- Default status is `pending` when omitted.

4) Call Feishu Bitable search.
- `POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search`
- Use `page_size` + `page_token` for pagination.
- Respect `view_id` unless `ignore_view` is true.

5) Validate decoded tasks.
- Keep only rows with `TaskID != 0` and at least one of `Params`, `ItemID`, `BookID`, `URL`, `UserID`, `UserName`.

6) Update task status/metadata.
- Resolve record_id from `TaskID` or `BizTaskID` when needed.
- Use `records/batch_update` for multiple updates, `records/{record_id}` for single updates.
- Apply status + timing + metrics updates using the task table field mapping.
- For JSONL ingestion, update any fields whose keys match column names, and map `CDNURL` to `Extra`.
- Use `--skip-status` to skip updates for tasks already in a given status (comma-separated).

7) Create tasks.
- Use `records/batch_create` for multiple tasks, `records` for single create.
- Accept JSON/JSONL input (same key conventions as update); map `CDNURL` to `Extra`.

## Minimal Python example (standalone)

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID&view=VIEW_ID"
python scripts/fetch_tasks.py --app com.smile.gifmaker --scene 综合页搜索 --status pending --date Today --limit 10
```

```bash
python scripts/update_tasks.py \
  --task-id 180413 \
  --status running \
  --device-serial 1fa20bb \
  --dispatched-at now
```

```bash
python scripts/update_tasks.py \
  --biz-task-id ext-20240101-001 \
  --status success \
  --completed-at now
```

```bash
python scripts/update_tasks.py \
  --input output.jsonl
```

```bash
python scripts/update_tasks.py \
  --input tasks.jsonl \
  --status ready \
  --date 2026-01-27
```

```bash
python scripts/create_tasks.py \
  --input tasks.jsonl \
  --status pending \
  --date 2026-01-27
```

```bash
python scripts/create_tasks.py \
  --biz-task-id GYS2601290001 \
  --app com.smile.gifmaker \
  --scene 单个链接采集 \
  --status pending \
  --date 2026-01-27 \
  --book-id 7591421623471705150 \
  --user-id 5891321132 \
  --url https://www.kuaishou.com/short-video/3xcx7sk3yi583je
```

## Resources

- Read `references/task-fetch.md` for filters, pagination, validation, and field mapping.
- Read `references/task-update.md` for status updates, timing fields, and batch update rules.
- Read `references/task-create.md` for create payload rules and batch create behavior.
- Read `references/feishu-integration.md` for Feishu API endpoints and request/response payloads.
- `scripts/fetch_tasks.py`: HTTP-based Python implementation that hits `/records/search` and decodes tasks (including wiki URL support).
- `scripts/update_tasks.py`: HTTP-based Python implementation that updates task rows via `/records/batch_update` or `/records/{record_id}`.
- `scripts/create_tasks.py`: HTTP-based Python implementation that creates task rows via `/records/batch_create` or `/records`.
