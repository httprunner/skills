---
name: result-supabase-reporter
description: "Collect app events via evalpkgs into sqlite, then filter/report capture_results to Supabase with retry-safe writeback. Use for collect-start/collect-stop/stat/filter/report/retry-reset workflows, especially task-scoped reporting with --task-id."
---

# Result Supabase Reporter

Use this skill for a deterministic `sqlite -> Supabase` result pipeline around `capture_results`.

## Path Convention

Canonical install and execution directory:

```bash
cd ~/.agents/skills/result-supabase-reporter
```

One-off invocation from any directory:

```bash
(cd ~/.agents/skills/result-supabase-reporter && npx tsx scripts/result_reporter.ts --help)
```

## Workflow

1) Optional data collection
- `collect-start`: start background `evalpkgs run` for one device (`SerialNumber`) with one `TaskID` (digits only).
- `collect-stop`: stop collector and print summary metrics (`delta`, `task_delta`, `records_jsonl`, `tracking_events`, `runtime_sec`).

2) Data inspection / selection
- `stat`: print total sqlite row count for one `--task-id`.
- `filter`: preview upload candidates from `capture_results`.
- Default status filter is pending+failed (`reported IN (0,-1)`).

3) Data reporting
- `report`: batch upsert to Supabase and write back sqlite status.
- `--max-rows <n>` sets total cap for one report run.
- For per-task workflows, always pass `--task-id <TASK_ID>` to avoid cross-task uploads.
- Success writeback: `reported=1`, `reported_at=now_ms`, `report_error=NULL`.
- Failure writeback: `reported=-1`, `reported_at=now_ms`, `report_error=<truncated error>`.

4) Retry
- `retry-reset`: move failed rows (`reported=-1`) back to pending (`reported=0`), then rerun `report`.

## Run

Install dependencies once:

```bash
npm install
```

CLI entry:

```bash
npx tsx scripts/result_reporter.ts <subcommand> [flags]
```

Subcommands:
- `collect-start`
- `collect-stop`
- `stat`
- `filter`
- `report`
- `retry-reset`

## Environment Variables

Required by phase:
- Collection: `BUNDLE_ID`, `SerialNumber`
- Supabase report: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`

Optional overrides:
- `TRACKING_STORAGE_DB_PATH` (default `$HOME/.eval/records.sqlite`)
- `RESULT_SQLITE_TABLE` (default `capture_results`)
- `SUPABASE_RESULT_TABLE` (default `capture_results`)

## Recommended Command Patterns

Task-scoped preview:

```bash
npx tsx scripts/result_reporter.ts filter --task-id <TASK_ID> --status 0,-1 --limit 20
```

Task-scoped report:

```bash
export SUPABASE_URL=https://<project>.supabase.co
export SUPABASE_SERVICE_ROLE_KEY=<service_role_key>
npx tsx scripts/result_reporter.ts report --task-id <TASK_ID> --batch-size 100 --max-rows 500
```

Retry failed rows:

```bash
npx tsx scripts/result_reporter.ts retry-reset --app com.tencent.mm --scene onSearch
npx tsx scripts/result_reporter.ts report --task-id <TASK_ID> --status 0,-1
```

## Failure Handling

- Use `--dry-run` with `report` to validate selection size before network writes.
- Check sqlite `report_error` for root cause when `reported=-1`.
- Typical issues: invalid `SUPABASE_URL`, expired/incorrect service role key, target table missing required columns, or missing unique key for upsert conflict columns.
- Fix issue first, then run `retry-reset` and `report` again.

## Resources

- `scripts/result_reporter.ts`: executable source of truth for flags and behavior.
- `references/init.sql`: Supabase table DDL for `capture_results`.
- `references/sqlite-and-field-mapping.md`: sqlite schema expectations, mapping, and command examples.
- `references/supabase-api-and-errors.md`: Supabase API usage, upsert conflict key, and error triage.
