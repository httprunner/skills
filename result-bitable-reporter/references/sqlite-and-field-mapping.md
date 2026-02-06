# SQLite and Field Mapping

## SQLite Defaults

- DB path default: `$HOME/.eval/records.sqlite`
- Table default: `capture_results`
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
- Limited by `--limit` (default 30)

Optional filters can narrow selection by app/scene/params/item/date/custom SQL predicate.

## Retry Operations

Reset failed rows for re-upload:

```sql
UPDATE capture_results
SET reported = 0, reported_at = NULL, report_error = NULL
WHERE reported = -1;
```

Inspect latest failures:

```sql
SELECT id, TaskID, Params, report_error, reported_at
FROM capture_results
WHERE reported = -1
ORDER BY reported_at DESC
LIMIT 20;
```
