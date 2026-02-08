# SQLite and Field Mapping

## SQLite Defaults

- DB path default: `$HOME/.eval/records.sqlite`
- Table default: `capture_results`
- `collect-start` mode also writes to the same shared sqlite path unless `--db-path` overrides it.
- Override via env:
  - `TRACKING_STORAGE_DB_PATH`
  - `RESULT_SQLITE_TABLE`

## Expected Columns

The script expects a standard capture-result schema in the result table:

- Business columns: `Datetime`, `DeviceSerial`, `App`, `Scene`, `Params`, `ItemID`, `ItemCaption`, `ItemCDNURL`, `ItemURL`, `ItemDuration`, `UserName`, `UserID`, `UserAlias`, `UserAuthEntity`, `Tags`, `TaskID`, `Extra`, `LikeCount`, `ViewCount`, `AnchorPoint`, `CommentCount`, `CollectCount`, `ForwardCount`, `ShareCount`, `PayMode`, `Collection`, `Episode`, `PublishTime`.
- Reporter bookkeeping columns: `reported`, `reported_at`, `report_error`.

## Report Status Semantics

Status semantics:

- `reported = 0`: pending (never reported or manually reset)
- `reported = -1`: previous report failed
- `reported = 1`: successfully reported

Writeback columns:

- `reported_at`: unix milliseconds of latest attempt
- `report_error`: truncated error string (<= 512 chars), or `NULL` on success

## Filtering Rules

Default query picks rows where:

- `reported IN (0, -1)`
- Ordered by `id ASC`
- `filter`: limited by `--limit` (default 30)
- `report`: uses fixed sqlite page size per fetch and loops until no rows remain for current filters
- `report --max-rows <n>`: optional total cap for current run

Optional filters can narrow selection by app/scene/params/item/date/custom SQL predicate.
For task-scoped workflows, pass `--task-id <TASK_ID>` so only current-task rows are selected.
`TASK_ID` must be digits only.

## Quick Stat (`stat`)

Use `stat` to print total row count in `capture_results` for one TaskID.

Examples:

```bash
npx tsx scripts/result_reporter.ts stat --task-id 20260206001
```

## Collect Mode Validation

`collect-start` and `collect-stop` perform a non-writeback validation around `capture_results`:

- `collect-start` records `before_count` and starts eval collection in background
- `collect-start` enforces one active collector process per `SerialNumber` (old one will be stopped first)
- `collect-stop` terminates collector by `SerialNumber`
- `collect-stop` queries `after_count` and prints:
  - `delta`: total row increment in table
  - `task_delta`: new rows for current `TaskID` after start snapshot
  - `records_jsonl`: line count in `~/.eval/<TaskID>/*_records.jsonl`
  - `tracking_events`: line count in `~/.eval/<TaskID>/*_track.jsonl`
  - `runtime_sec`: collection runtime in seconds

`collect-start` does not update `reported/reported_at/report_error`; those fields remain managed by `report` and `retry-reset`.

## Command Examples

Run real-time collection in background:

```bash
export BUNDLE_ID=com.tencent.mm
export SerialNumber=1fa20bb
npx tsx scripts/result_reporter.ts collect-start \
  --task-id 20260206001 \
  --db-path ~/.eval/records.sqlite \
  --table capture_results
```

Stop one device collector and print collected delta:

```bash
SerialNumber=1fa20bb npx tsx scripts/result_reporter.ts collect-stop
```

Inspect one pending/failed row:

```bash
npx tsx scripts/result_reporter.ts filter \
  --db-path ~/.eval/records.sqlite \
  --table capture_results \
  --task-id 20260206001 \
  --status 0,-1 \
  --limit 1
```

Inspect by app + scene:

```bash
npx tsx scripts/result_reporter.ts filter \
  --app com.tencent.mm \
  --scene onSearch \
  --status 0,-1 \
  --limit 20
```

## Retry Operations

Reset failed rows for re-upload:

```bash
sqlite3 ~/.eval/records.sqlite "UPDATE capture_results SET reported = 0, reported_at = NULL, report_error = NULL WHERE reported = -1;"
```

Inspect latest failures:

```bash
sqlite3 ~/.eval/records.sqlite "SELECT id, TaskID, Params, report_error, reported_at FROM capture_results WHERE reported = -1 ORDER BY reported_at DESC LIMIT 20;"
```
