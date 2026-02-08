---
name: feishu-bitable-task-manager-go
description: Manage tasks in Feishu Bitable (multi-dimensional table), fetch, update, and create flows using a fixed schema, filters, pagination, and status update rules. Use when building or running task pullers/reporters that must match a specific task status table and its field mapping, status presets, and date presets.
---

# Feishu Bitable Task Manager (Go)

Follow the task table conventions when pulling and updating tasks in Feishu Bitable.

## Path Convention

Canonical install and execution directory: `~/.agents/skills/feishu-bitable-task-manager-go/`. Run commands from this directory:

```bash
cd ~/.agents/skills/feishu-bitable-task-manager-go
```

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

## Run (Use `go run`)

Use `go run` so you can execute without building a binary:

```bash
go run ./cmd/bitable-task <subcommand> [flags]
```

If `go` is not available, install a Go toolchain first using the `go-installer` skill. If that skill is not available, install it with `npx skills add httprunner/skills@go-installer`. Re-run the `go run` command above.

## Examples

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID&view=VIEW_ID"
go run ./cmd/bitable-task fetch --app com.smile.gifmaker --scene 综合页搜索 --status pending --date Today --limit 10
```

```bash
go run ./cmd/bitable-task update \
  --task-id 180413 \
  --status running \
  --device-serial 1fa20bb \
  --dispatched-at now
```

Update single task by BizTaskID:

```bash
go run ./cmd/bitable-task update \
  --biz-task-id ext-20240101-001 \
  --status success \
  --completed-at now
```

Update from JSONL output (per-line task updates):

```bash
go run ./cmd/bitable-task update --input output.jsonl
```

Update from JSONL with CLI defaults for missing fields:

```bash
go run ./cmd/bitable-task update \
  --input tasks.jsonl \
  --status ready \
  --date 2026-01-27
```

Create tasks from JSONL with defaults:

```bash
go run ./cmd/bitable-task create \
  --input tasks.jsonl \
  --app com.smile.gifmaker \
  --scene 单个链接采集 \
  --status pending \
  --date 2026-01-27
```

Create from JSONL and skip when BizTaskID already exists:

```bash
go run ./cmd/bitable-task create \
  --input tasks.jsonl \
  --app com.smile.gifmaker \
  --scene 单个链接采集 \
  --status ready \
  --date 2026-01-27 \
  --skip-existing BizTaskID
```

Create from JSONL and skip when both BookID and UserID match existing records:

```bash
go run ./cmd/bitable-task create \
  --input tasks.jsonl \
  --skip-existing BookID,UserID
```

Create a single task with explicit fields:

```bash
go run ./cmd/bitable-task create \
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
- `cmd/bitable-task`: single CLI entrypoint (`fetch`/`update`/`create`).
- `internal/common/common.go`: Feishu OpenAPI HTTP + token/wiki helpers + value/timestamp coercion + env field mapping.
- `internal/cli/*.go`: CLI implementation for fetch/update/create, JSON/JSONL ingestion, and skip rules.
