---
name: result-bitable-reporter
description: "Run real-time eval collection and result reporting workflows with SQLite + Feishu Bitable. Use this skill when you need to (1) collect app event results via evalpkgs with TaskID-scoped artifacts, (2) filter pending/failed rows from capture_results, (3) report rows to a Feishu result table, and (4) reset or update reported/reported_at/report_error for retry-safe pipelines."
---

# Result Bitable Reporter

Use this skill to run an explicit SQLite -> Feishu result reporting pipeline.

## Workflow

1) Prepare env and target table.
- Require `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `RESULT_BITABLE_URL`.
- Optional: `FEISHU_BASE_URL` (default `https://open.feishu.cn`).
- Optional SQLite knobs: `TRACKING_STORAGE_DB_PATH`, `RESULT_SQLITE_TABLE`.

2) (Optional) Run real-time collection into shared sqlite.
- Use `collect` to start `evalpkgs run --log-level debug` in background.
- Require env: `BUNDLE_ID`, `SerialNumber`.
- Require `--task-id`; the command injects `TaskID=<task-id>` into evalpkgs.
- `collect` enforces at most one collector process per `SerialNumber`.
- If a collector already exists for this device, `collect` stops it first and then starts a new one.
- Use `collect-stop` to stop background collection and print summary metrics:
  `before_count/after_count/delta/task_delta/jsonl_lines/runtime_sec`.
- `collect-stop` supports fast/slow settle tuning via `--wait-ms` and `--stable-ms`.

3) Preview rows before upload.
- Use `filter` first to confirm selected rows.
- Default selection: `reported IN (0, -1)`.

4) Run batch report and writeback.
- Use `report` to fetch rows from sqlite, batch-create Feishu records, and update sqlite status.
- Success writeback: `reported=1`, `reported_at=now_ms`, `report_error=NULL`.
- Failure writeback: `reported=-1`, `reported_at=now_ms`, `report_error=<error (<=512 chars)>`.

5) Retry failed rows.
- Use `retry-reset` to reset failed rows: `reported=-1 -> 0`.
- Re-run `report` with the same filters.

## Run

Run commands with `npx tsx`:

```bash
npx tsx scripts/result_reporter.ts <subcommand> [flags]
```

## Commands

### collect
Start eval collection in background.

```bash
export BUNDLE_ID=com.tencent.mm
export SerialNumber=1fa20bb
npx tsx scripts/result_reporter.ts collect \
  --task-id 20260206001 \
  --db-path ~/.eval/records.sqlite \
  --table capture_results
```

### collect-stop
Stop background collection and print collected row delta.

```bash
export SerialNumber=1fa20bb
npx tsx scripts/result_reporter.ts collect-stop
```

Prefer faster stop/return:

```bash
SerialNumber=1fa20bb npx tsx scripts/result_reporter.ts collect-stop --wait-ms 3000 --stable-ms 800
```

Prefer steadier sqlite settle before counting:

```bash
SerialNumber=1fa20bb npx tsx scripts/result_reporter.ts collect-stop --wait-ms 15000 --stable-ms 3000
```

### filter
Print selected sqlite rows as JSONL.

```bash
npx tsx scripts/result_reporter.ts filter \
  --db-path ~/.eval/records.sqlite \
  --table capture_results \
  --status 0,-1 \
  --app com.tencent.mm \
  --scene onSearch \
  --limit 20
```

### report
Select rows, upload to Feishu result table via `records/batch_create`, then writeback status.

```bash
npx tsx scripts/result_reporter.ts report \
  --db-path ~/.eval/records.sqlite \
  --table capture_results \
  --bitable-url "$RESULT_BITABLE_URL" \
  --status 0,-1 \
  --batch-size 30 \
  --limit 100
```

Dry run (no Feishu call, no sqlite writeback):

```bash
npx tsx scripts/result_reporter.ts report --dry-run --limit 10
```

### retry-reset
Reset failed rows for retry.

```bash
npx tsx scripts/result_reporter.ts retry-reset \
  --db-path ~/.eval/records.sqlite \
  --table capture_results
```

## Filter Flags

- `--status <csv>`: default `0,-1`
- `--app <value>`
- `--scene <value>`
- `--params-like <value>`
- `--item-id <value>`
- `--date-from <ISO date/datetime>`
- `--date-to <ISO date/datetime>`
- `--where <SQL expr>`: optional additional SQL predicate
- `--where-arg <value>`: repeatable bound values for `--where`
- `--limit <n>`: default `30`

## Result Field Mapping

Payload field names default to the standard result-table field set and can be overridden by env:
- `RESULT_FIELD_DATETIME` (default `Datetime`)
- `RESULT_FIELD_DEVICE_SERIAL` (default `DeviceSerial`)
- `RESULT_FIELD_APP` (default `App`)
- `RESULT_FIELD_SCENE` (default `Scene`)
- `RESULT_FIELD_PARAMS` (default `Params`)
- `RESULT_FIELD_ITEMID` (default `ItemID`)
- `RESULT_FIELD_ITEMCAPTION` (default `ItemCaption`)
- `RESULT_FIELD_ITEMCDNURL` (default `ItemCDNURL`)
- `RESULT_FIELD_ITEMURL` (default `ItemURL`)
- `RESULT_FIELD_DURATION` (default `ItemDuration`)
- `RESULT_FIELD_USERNAME` (default `UserName`)
- `RESULT_FIELD_USERID` (default `UserID`)
- `RESULT_FIELD_USERALIAS` (default `UserAlias`)
- `RESULT_FIELD_USERAUTHENTITY` (default `UserAuthEntity`)
- `RESULT_FIELD_TAGS` (default `Tags`)
- `RESULT_FIELD_TASKID` (default `TaskID`)
- `RESULT_FIELD_EXTRA` (default `Extra`)
- `RESULT_FIELD_LIKECOUNT` (default `LikeCount`)
- `RESULT_FIELD_VIEWCOUNT` (default `ViewCount`)
- `RESULT_FIELD_ANCHORPOINT` (default `AnchorPoint`)
- `RESULT_FIELD_COMMENTCOUNT` (default `CommentCount`)
- `RESULT_FIELD_COLLECTCOUNT` (default `CollectCount`)
- `RESULT_FIELD_FORWARDCOUNT` (default `ForwardCount`)
- `RESULT_FIELD_SHARECOUNT` (default `ShareCount`)
- `RESULT_FIELD_PAYMODE` (default `PayMode`)
- `RESULT_FIELD_COLLECTION` (default `Collection`)
- `RESULT_FIELD_EPISODE` (default `Episode`)
- `RESULT_FIELD_PUBLISHTIME` (default `PublishTime`)

## Resources

- `scripts/result_reporter.ts`: CLI entrypoint for collect/collect-stop/filter/report/retry-reset.
- `references/sqlite-and-field-mapping.md`: sqlite schema and writeback semantics.
- `references/feishu-api-and-errors.md`: Feishu APIs and common failure handling.
