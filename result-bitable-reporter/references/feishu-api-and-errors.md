# Feishu API and Error Handling

## APIs Used

1. Tenant token
- `POST /open-apis/auth/v3/tenant_access_token/internal/`
- Request body:
  - `app_id`
  - `app_secret`

2. Wiki token resolve (only for wiki URLs)
- `GET /open-apis/wiki/v2/spaces/get_node?token=<wiki_token>`
- Read `data.node.obj_token` as bitable app token.

3. Result row batch create
- `POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create`
- Request body:
  - `records`: list of `{ "fields": { ... } }`

## Input URL Requirements

`RESULT_BITABLE_URL` (or `--bitable-url`) must include:

- app token URL: `/base/<app_token>?table=<table_id>`
- or wiki URL: `/wiki/<wiki_token>?table=<table_id>`

## Batch Size

Feishu bitable batch_create supports up to 500 records per request.

Skill default:
- `--batch-size 30`
- script auto-splits large selections into chunks.

## Failure Handling

- API call failure marks each row in the failed chunk as `reported=-1`.
- `report_error` stores a truncated string for later inspection.
- Common causes:
  - Auth failure (`FEISHU_APP_ID`/`FEISHU_APP_SECRET` mismatch)
  - Schema mismatch (`FieldNameNotFound`)
  - Rate limit or gateway errors

## Practical Triage

1. Use `--dry-run` to validate selection first.
2. In task workflow, include `--task-id <TASK_ID>` to avoid cross-task uploads.
3. Confirm result table URL and field names.
4. Inspect sqlite `report_error` values and repair mapping/env.
5. Run `retry-reset`, then retry `report`.

## Field Mapping Env Overrides

Default Feishu field names come from sqlite capture columns and can be overridden:

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

## Command Examples

Report pending/failed rows to Feishu:

```bash
export FEISHU_APP_ID=...
export FEISHU_APP_SECRET=...
export RESULT_BITABLE_URL="https://.../wiki/...?...table=tbl_xxx&view=vew_xxx"
npx tsx scripts/result_reporter.ts report \
  --db-path ~/.eval/records.sqlite \
  --table capture_results \
  --task-id 20260206001 \
  --status 0,-1 \
  --batch-size 30 \
  --max-rows 500
```

Dry-run without Feishu write:

```bash
npx tsx scripts/result_reporter.ts report \
  --dry-run \
  --task-id 20260206001 \
  --status 0,-1 \
  --max-rows 20
```

Reset failed rows and retry:

```bash
npx tsx scripts/result_reporter.ts retry-reset \
  --db-path ~/.eval/records.sqlite \
  --table capture_results

npx tsx scripts/result_reporter.ts report \
  --task-id 20260206001 \
  --status 0,-1 \
  --batch-size 30 \
  --max-rows 500
```
