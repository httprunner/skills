# SQLite Schema and Column Mapping

## SQLite Defaults

- DB path default: `$HOME/.eval/records.sqlite`
- Table default: `capture_results`
- `collect-start` mode writes to the same shared sqlite path unless `--db-path` overrides it.
- Override via env:
  - `TRACKING_STORAGE_DB_PATH`
  - `RESULT_SQLITE_TABLE`

## Expected SQLite Columns

Business columns: `Datetime`, `DeviceSerial`, `App`, `Scene`, `Params`, `ItemID`, `ItemCaption`, `ItemCDNURL`, `ItemURL`, `ItemDuration`, `UserName`, `UserID`, `UserAlias`, `UserAuthEntity`, `Tags`, `TaskID`, `Extra`, `LikeCount`, `ViewCount`, `AnchorPoint`, `CommentCount`, `CollectCount`, `ForwardCount`, `ShareCount`, `PayMode`, `Collection`, `Episode`, `PublishTime`.

Reporter bookkeeping columns: `reported`, `reported_at`, `report_error`.

## Column Mapping: SQLite -> Supabase

| SQLite (PascalCase) | Supabase (snake_case) | Type |
|---|---|---|
| Datetime | datetime | bigint (unix ms) |
| DeviceSerial | device_serial | text |
| App | app | text |
| Scene | scene | text |
| Params | params | text |
| ItemID | item_id | text |
| ItemCaption | item_caption | text |
| ItemCDNURL | item_cdn_url | text |
| ItemURL | item_url | text |
| ItemDuration | item_duration | numeric |
| UserName | user_name | text |
| UserID | user_id | text |
| UserAlias | user_alias | text |
| UserAuthEntity | user_auth_entity | text |
| Tags | tags | text |
| TaskID | task_id | bigint |
| Extra | extra | text |
| LikeCount | like_count | integer |
| ViewCount | view_count | integer |
| AnchorPoint | anchor_point | text |
| CommentCount | comment_count | integer |
| CollectCount | collect_count | integer |
| ForwardCount | forward_count | integer |
| ShareCount | share_count | integer |
| PayMode | pay_mode | text |
| Collection | collection | text |
| Episode | episode | text |
| PublishTime | publish_time | text |

## Report Status Semantics

Note: uploader uses a stable `item_id` key for dedup.
- Prefer sqlite `ItemID` when present.
- Fallback to `ItemURL` when `ItemID` is empty/null.

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
- `report`: pages through all matching rows using cursor-based pagination
- `report --max-rows <n>`: optional total cap for current run

Optional filters narrow selection by app/scene/params/item/date/custom SQL predicate.
For task-scoped workflows, pass `--task-id <TASK_ID>` to select only current-task rows.
`TASK_ID` must be digits only.

## Quick Stat (`stat`)

Print total row count in `capture_results` for one TaskID:

```bash
npx tsx scripts/result_reporter.ts stat --task-id 20260206001
```

## Collect Mode

- `collect-start` records `before_count` and starts eval collection in background
- `collect-start` enforces one active collector per `SerialNumber`
- `collect-stop` terminates collector by `SerialNumber` and prints delta metrics:
  - `delta`, `task_delta`, `records_jsonl`, `tracking_events`, `runtime_sec`

## Command Examples

Start collection:

```bash
export BUNDLE_ID=com.tencent.mm
export SerialNumber=1fa20bb
npx tsx scripts/result_reporter.ts collect-start --task-id 20260206001
```

Stop collection:

```bash
SerialNumber=1fa20bb npx tsx scripts/result_reporter.ts collect-stop
```

Filter pending/failed rows:

```bash
npx tsx scripts/result_reporter.ts filter --task-id 20260206001 --status 0,-1 --limit 10
```

Report to Supabase:

```bash
export SUPABASE_URL=https://xxx.supabase.co
export SUPABASE_SERVICE_ROLE_KEY=eyJ...
npx tsx scripts/result_reporter.ts report --task-id 20260206001 --batch-size 100
```

Retry failed rows:

```bash
npx tsx scripts/result_reporter.ts retry-reset
npx tsx scripts/result_reporter.ts report --task-id 20260206001
```
