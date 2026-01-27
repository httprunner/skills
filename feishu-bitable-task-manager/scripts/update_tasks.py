#!/usr/bin/env python3
"""Update tasks in Feishu Bitable via HTTP."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlencode

import bitable_common as common

MAX_BATCH_SIZE = 500
MAX_FILTER_VALUES = 50


def chunked(values: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [values]
    return [values[i : i + size] for i in range(0, len(values), size)]


def build_id_filter(field_name: str, values: Iterable[str]) -> Optional[dict]:
    field_name = (field_name or "").strip()
    if not field_name:
        return None
    seen = set()
    conditions = []
    for value in values:
        if not isinstance(value, str):
            continue
        value = value.strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        conditions.append({"field_name": field_name, "operator": "is", "value": [value]})
    if not conditions:
        return None
    return {"conjunction": "or", "conditions": conditions}


def fetch_records(
    base_url: str,
    token: str,
    ref: common.BitableRef,
    filter_obj: Optional[dict],
    page_size: int,
    ignore_view: bool,
    view_id: str,
) -> List[dict]:
    page_size = common.clamp_page_size(page_size)
    query = {"page_size": str(page_size)}
    url = (
        f"{base_url}/open-apis/bitable/v1/apps/{ref.app_token}"
        f"/tables/{ref.table_id}/records/search?{urlencode(query)}"
    )
    body = None
    if (not ignore_view and view_id) or filter_obj:
        body = {}
        if not ignore_view and view_id:
            body["view_id"] = view_id
        if filter_obj:
            body["filter"] = filter_obj
    resp = common.request_json("POST", url, token, body)
    if resp.get("code") != 0:
        raise RuntimeError(f"search records failed: code={resp.get('code')} msg={resp.get('msg')}")
    data = resp.get("data") or {}
    return data.get("items") or []


def fetch_record_statuses(
    base_url: str,
    token: str,
    ref: common.BitableRef,
    record_ids: List[str],
    status_field: str,
) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for record_id in record_ids:
        record_id = (record_id or "").strip()
        if not record_id or record_id in result:
            continue
        url = (
            f"{base_url}/open-apis/bitable/v1/apps/{ref.app_token}"
            f"/tables/{ref.table_id}/records/{record_id}"
        )
        resp = common.request_json("GET", url, token, None)
        if resp.get("code") != 0:
            raise RuntimeError(
                f"get record failed: code={resp.get('code')} msg={resp.get('msg')}"
            )
        data = resp.get("data") or {}
        record = data.get("record") or {}
        fields = record.get("fields") or {}
        status = common.bitable_value_to_string(fields.get(status_field, ""))
        if status:
            result[record_id] = status
    return result


def resolve_record_ids_by_task_id(
    base_url: str,
    token: str,
    ref: common.BitableRef,
    fields: Dict[str, str],
    task_ids: List[int],
    ignore_view: bool,
    view_id: str,
) -> tuple[Dict[int, str], Dict[str, str]]:
    result: Dict[int, str] = {}
    statuses: Dict[str, str] = {}
    if not task_ids:
        return result, statuses
    values = [str(task_id) for task_id in task_ids if isinstance(task_id, int) and task_id > 0]
    if not values:
        return result, statuses
    task_field = fields.get("TaskID", "TaskID")
    status_field = fields.get("Status", "Status")
    for batch in chunked(values, MAX_FILTER_VALUES):
        filter_obj = build_id_filter(task_field, batch)
        if not filter_obj:
            continue
        items = fetch_records(
            base_url=base_url,
            token=token,
            ref=ref,
            filter_obj=filter_obj,
            page_size=min(common.MAX_PAGE_SIZE, max(len(batch), 1)),
            ignore_view=ignore_view,
            view_id=view_id,
        )
        for item in items:
            record_id = (item.get("record_id") or "").strip()
            raw_fields = item.get("fields") or {}
            task_id = common.field_int(raw_fields, task_field)
            if record_id and task_id > 0 and task_id not in result:
                result[task_id] = record_id
        statuses.update(extract_statuses_from_items(items, status_field))
    return result, statuses


def resolve_record_ids_by_biz_task_id(
    base_url: str,
    token: str,
    ref: common.BitableRef,
    fields: Dict[str, str],
    biz_task_ids: List[str],
    ignore_view: bool,
    view_id: str,
) -> tuple[Dict[str, str], Dict[str, str]]:
    result: Dict[str, str] = {}
    statuses: Dict[str, str] = {}
    values = [str(value).strip() for value in biz_task_ids if isinstance(value, str) and value.strip()]
    if not values:
        return result, statuses
    biz_field = fields.get("BizTaskID", "BizTaskID")
    status_field = fields.get("Status", "Status")
    for batch in chunked(values, MAX_FILTER_VALUES):
        filter_obj = build_id_filter(biz_field, batch)
        if not filter_obj:
            continue
        items = fetch_records(
            base_url=base_url,
            token=token,
            ref=ref,
            filter_obj=filter_obj,
            page_size=min(common.MAX_PAGE_SIZE, max(len(batch), 1)),
            ignore_view=ignore_view,
            view_id=view_id,
        )
        for item in items:
            record_id = (item.get("record_id") or "").strip()
            raw_fields = item.get("fields") or {}
            biz_task_id = common.bitable_value_to_string(raw_fields.get(biz_field, ""))
            if record_id and biz_task_id and biz_task_id not in result:
                result[biz_task_id] = record_id
        statuses.update(extract_statuses_from_items(items, status_field))
    return result, statuses


def extract_statuses_from_items(
    items: List[dict],
    status_field: str,
) -> Dict[str, str]:
    statuses: Dict[str, str] = {}
    for item in items:
        record_id = (item.get("record_id") or "").strip()
        raw_fields = item.get("fields") or {}
        status = common.bitable_value_to_string(raw_fields.get(status_field, ""))
        if record_id and status:
            statuses[record_id] = status
    return statuses


def has_cdn_url(extra: Any) -> bool:
    raw = common.normalize_extra(extra)
    if not raw:
        return False
    try:
        payload = json.loads(raw)
    except Exception:
        return False
    cdn = payload.get("cdn_url")
    if not isinstance(cdn, str):
        return False
    return cdn.strip() != ""


def build_update_fields(fields_map: Dict[str, str], update: Dict[str, Any]) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}

    status = (update.get("status") or "").strip()
    if status:
        fields[fields_map["Status"]] = status

    date_value = update.get("date")
    if date_value is not None and fields_map.get("Date"):
        payload = common.coerce_date_payload(date_value)
        if payload is not None:
            fields[fields_map["Date"]] = payload

    device_serial = (update.get("device_serial") or "").strip()
    if device_serial and fields_map.get("DispatchedDevice"):
        fields[fields_map["DispatchedDevice"]] = device_serial

    dispatched_ms = common.coerce_millis(update.get("dispatched_at"))
    start_ms = common.coerce_millis(update.get("start_at"))
    if dispatched_ms is not None and fields_map.get("DispatchedAt"):
        fields[fields_map["DispatchedAt"]] = dispatched_ms
    if start_ms is None and dispatched_ms is not None:
        start_ms = dispatched_ms
    if start_ms is not None and fields_map.get("StartAt"):
        fields[fields_map["StartAt"]] = start_ms

    completed_ms = common.coerce_millis(update.get("completed_at"))
    end_ms = common.coerce_millis(update.get("end_at"))
    if completed_ms is not None:
        end_ms = completed_ms
    if end_ms is not None and fields_map.get("EndAt"):
        fields[fields_map["EndAt"]] = end_ms

    elapsed = common.coerce_int(update.get("elapsed_seconds"))
    if elapsed is None and start_ms is not None and end_ms is not None:
        elapsed = max(0, int((end_ms - start_ms) / 1000))
    if elapsed is not None and fields_map.get("ElapsedSeconds"):
        fields[fields_map["ElapsedSeconds"]] = elapsed

    items_collected = common.coerce_int(update.get("items_collected"))
    if items_collected is not None and fields_map.get("ItemsCollected"):
        fields[fields_map["ItemsCollected"]] = items_collected

    logs = (update.get("logs") or "").strip()
    if logs and fields_map.get("Logs"):
        fields[fields_map["Logs"]] = logs

    retry_count = common.coerce_int(update.get("retry_count"))
    if retry_count is not None and fields_map.get("RetryCount"):
        fields[fields_map["RetryCount"]] = retry_count

    extra = update.get("extra")
    force_extra = bool(update.get("force_extra"))
    if fields_map.get("Extra") and extra is not None:
        if force_extra or (status == "success" and has_cdn_url(extra)):
            extra_payload = common.normalize_extra(extra)
            if extra_payload:
                fields[fields_map["Extra"]] = extra_payload

    extra_fields = update.get("fields")
    if isinstance(extra_fields, dict):
        for key, value in extra_fields.items():
            if key and value is not None:
                fields[key] = value

    return fields


def batch_update_records(base_url: str, token: str, ref: common.BitableRef, records: List[dict]) -> None:
    url = (
        f"{base_url}/open-apis/bitable/v1/apps/{ref.app_token}"
        f"/tables/{ref.table_id}/records/batch_update"
    )
    payload = {"records": records}
    resp = common.request_json("POST", url, token, payload)
    if resp.get("code") != 0:
        raise RuntimeError(
            f"batch update failed: code={resp.get('code')} msg={resp.get('msg')}"
        )


def update_record(base_url: str, token: str, ref: common.BitableRef, record_id: str, fields: Dict[str, Any]) -> None:
    url = (
        f"{base_url}/open-apis/bitable/v1/apps/{ref.app_token}"
        f"/tables/{ref.table_id}/records/{record_id}"
    )
    payload = {"fields": fields}
    resp = common.request_json("PUT", url, token, payload)
    if resp.get("code") != 0:
        raise RuntimeError(
            f"update record failed: code={resp.get('code')} msg={resp.get('msg')}"
        )


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update tasks in Feishu Bitable (HTTP).")
    parser.add_argument("--task-url", default=common.env("TASK_BITABLE_URL"), help="Bitable task table URL")
    parser.add_argument("--input", default="", help="Input JSON or JSONL file (use - for stdin)")
    parser.add_argument("--task-id", type=int, default=0, help="Single task id to update")
    parser.add_argument("--biz-task-id", default="", help="Single biz task id to update")
    parser.add_argument("--record-id", default="", help="Single record id to update")
    parser.add_argument("--status", default="", help="Status to set")
    parser.add_argument("--date", default="", help="Date to set (string, e.g. 2026-01-27)")
    parser.add_argument("--device-serial", default="", help="Dispatched device serial")
    parser.add_argument("--dispatched-at", default="", help="Dispatch time (ms/seconds/ISO/now)")
    parser.add_argument("--start-at", default="", help="Start time (ms/seconds/ISO)")
    parser.add_argument("--completed-at", default="", help="Completion time (ms/seconds/ISO)")
    parser.add_argument("--end-at", default="", help="End time (ms/seconds/ISO)")
    parser.add_argument("--elapsed-seconds", default="", help="Elapsed seconds (int)")
    parser.add_argument("--items-collected", default="", help="Items collected (int)")
    parser.add_argument("--logs", default="", help="Logs path or identifier")
    parser.add_argument("--retry-count", default="", help="Retry count (int)")
    parser.add_argument("--extra", default="", help="Extra JSON string")
    parser.add_argument("--skip-status", default="", help="Skip updates when current status matches (comma-separated)")
    parser.add_argument("--ignore-view", action="store_true", default=True, help="Ignore view_id when searching")
    parser.add_argument("--use-view", dest="ignore_view", action="store_false", help="Use view_id from URL")
    parser.add_argument("--view-id", default="", help="Override view_id when searching")
    return parser.parse_args(argv)


def load_updates(args: argparse.Namespace, fields_map: Dict[str, str]) -> List[dict]:
    updates: List[dict] = []
    if args.input:
        if args.input == "-":
            raw = sys.stdin.read()
        else:
            with open(args.input, "r", encoding="utf-8") as handle:
                raw = handle.read()
        mode = common.detect_input_format(args.input, raw)
        updates = common.parse_jsonl_input(raw) if mode == "jsonl" else common.parse_json_input(raw)
    else:
        base = {
            "task_id": args.task_id,
            "biz_task_id": args.biz_task_id,
            "record_id": args.record_id,
            "status": args.status,
            "device_serial": args.device_serial,
            "dispatched_at": args.dispatched_at,
            "start_at": args.start_at,
            "completed_at": args.completed_at,
            "end_at": args.end_at,
            "elapsed_seconds": args.elapsed_seconds,
            "items_collected": args.items_collected,
            "logs": args.logs,
            "retry_count": args.retry_count,
            "extra": args.extra,
        }
        updates = [base]

    known_keys = {
        "task_id",
        "taskID",
        "TaskID",
        "biz_task_id",
        "bizTaskId",
        "BizTaskID",
        "record_id",
        "recordId",
        "RecordID",
        "status",
        "date",
        "Date",
        "device_serial",
        "dispatched_at",
        "start_at",
        "completed_at",
        "end_at",
        "elapsed_seconds",
        "items_collected",
        "logs",
        "retry_count",
        "extra",
        "fields",
        "CDNURL",
        "cdn_url",
        "cdnUrl",
        "cdnurl",
    }
    allowed_field_names = {value for value in fields_map.values() if value}

    def pick(item: dict, key: str, fallback: Any) -> Any:
        if key in item and item[key] is not None:
            return item[key]
        return fallback

    normalized: List[dict] = []
    for item in updates:
        if not isinstance(item, dict):
            continue
        cdn_url = ""
        for key in ("CDNURL", "cdn_url", "cdnUrl", "cdnurl"):
            raw = item.get(key)
            if isinstance(raw, str) and raw.strip():
                cdn_url = raw.strip()
                break
        extra = pick(item, "extra", args.extra)
        force_extra = False
        if cdn_url:
            extra = {"cdn_url": cdn_url}
            force_extra = True

        extra_fields: Dict[str, Any] = {}
        for key, value in item.items():
            if key in known_keys:
                continue
            if key in allowed_field_names and value is not None:
                extra_fields[key] = value
        raw_fields = item.get("fields")
        if isinstance(raw_fields, dict):
            for key, value in raw_fields.items():
                if key and value is not None:
                    extra_fields[key] = value

        merged = {
            "task_id": item.get("task_id") or item.get("taskID") or item.get("TaskID"),
            "biz_task_id": item.get("biz_task_id") or item.get("bizTaskId") or item.get("BizTaskID"),
            "record_id": item.get("record_id") or item.get("recordId") or item.get("RecordID"),
            "status": pick(item, "status", args.status),
            "date": pick(item, "date", args.date) or item.get("Date"),
            "device_serial": pick(item, "device_serial", args.device_serial),
            "dispatched_at": pick(item, "dispatched_at", args.dispatched_at),
            "start_at": pick(item, "start_at", args.start_at),
            "completed_at": pick(item, "completed_at", args.completed_at),
            "end_at": pick(item, "end_at", args.end_at),
            "elapsed_seconds": pick(item, "elapsed_seconds", args.elapsed_seconds),
            "items_collected": pick(item, "items_collected", args.items_collected),
            "logs": pick(item, "logs", args.logs),
            "retry_count": pick(item, "retry_count", args.retry_count),
            "extra": extra,
            "force_extra": force_extra,
            "fields": extra_fields,
        }
        normalized.append(merged)
    return normalized


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    task_url = (args.task_url or "").strip()
    if not task_url:
        print("TASK_BITABLE_URL is required", file=sys.stderr)
        return 2

    app_id = common.env("FEISHU_APP_ID")
    app_secret = common.env("FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        print("FEISHU_APP_ID/FEISHU_APP_SECRET are required", file=sys.stderr)
        return 2

    base_url = common.env("FEISHU_BASE_URL", common.DEFAULT_BASE_URL)
    fields_map = common.load_task_fields_from_env()

    updates = load_updates(args, fields_map)
    if not updates:
        print("no updates provided", file=sys.stderr)
        return 2

    try:
        ref = common.parse_bitable_url(task_url)
        token = common.get_tenant_access_token(base_url, app_id, app_secret)
        if not ref.app_token:
            if ref.wiki_token:
                ref.app_token = common.resolve_wiki_app_token(base_url, token, ref.wiki_token)
            else:
                print("bitable URL missing app_token and wiki_token", file=sys.stderr)
                return 2
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    view_id = args.view_id.strip() if args.view_id.strip() else ref.view_id

    task_ids_to_resolve: List[int] = []
    biz_task_ids_to_resolve: List[str] = []
    for upd in updates:
        record_id = str(upd.get("record_id") or "").strip()
        task_id = common.coerce_int(upd.get("task_id")) or 0
        biz_task_id = str(upd.get("biz_task_id") or "").strip()
        if not record_id and task_id > 0:
            task_ids_to_resolve.append(task_id)
        if not record_id and not task_id and biz_task_id:
            biz_task_ids_to_resolve.append(biz_task_id)

    resolved: Dict[int, str] = {}
    resolved_biz: Dict[str, str] = {}
    status_by_record: Dict[str, str] = {}
    if task_ids_to_resolve:
        try:
            resolved, statuses = resolve_record_ids_by_task_id(
                base_url=base_url,
                token=token,
                ref=ref,
                fields=fields_map,
                task_ids=task_ids_to_resolve,
                ignore_view=args.ignore_view,
                view_id=view_id,
            )
            status_by_record.update(statuses)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    if biz_task_ids_to_resolve:
        try:
            resolved_biz, statuses = resolve_record_ids_by_biz_task_id(
                base_url=base_url,
                token=token,
                ref=ref,
                fields=fields_map,
                biz_task_ids=biz_task_ids_to_resolve,
                ignore_view=args.ignore_view,
                view_id=view_id,
            )
            status_by_record.update(statuses)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    skip_statuses = {
        status.strip().lower()
        for status in args.skip_status.split(",")
        if status.strip()
    }
    if skip_statuses:
        record_ids_needed: List[str] = []
        for upd in updates:
            record_id = str(upd.get("record_id") or "").strip()
            if not record_id:
                task_id = common.coerce_int(upd.get("task_id")) or 0
                if task_id > 0:
                    record_id = resolved.get(task_id, "")
                else:
                    biz_task_id = str(upd.get("biz_task_id") or "").strip()
                    if biz_task_id:
                        record_id = resolved_biz.get(biz_task_id, "")
            if record_id and record_id not in status_by_record:
                record_ids_needed.append(record_id)
        if record_ids_needed:
            try:
                fetched = fetch_record_statuses(
                    base_url=base_url,
                    token=token,
                    ref=ref,
                    record_ids=record_ids_needed,
                    status_field=fields_map.get("Status", "Status"),
                )
                status_by_record.update(fetched)
            except RuntimeError as exc:
                print(str(exc), file=sys.stderr)
                return 2

    records: List[dict] = []
    errors: List[str] = []
    skipped = 0
    for upd in updates:
        record_id = str(upd.get("record_id") or "").strip()
        if not record_id:
            task_id = common.coerce_int(upd.get("task_id")) or 0
            if task_id > 0:
                record_id = resolved.get(task_id, "")
            else:
                biz_task_id = str(upd.get("biz_task_id") or "").strip()
                if biz_task_id:
                    record_id = resolved_biz.get(biz_task_id, "")
        if not record_id:
            errors.append("missing record_id for update")
            continue
        if skip_statuses:
            status = status_by_record.get(record_id, "").strip().lower()
            if status in skip_statuses:
                skipped += 1
                continue

        fields = build_update_fields(fields_map, upd)
        if not fields:
            errors.append(f"record {record_id}: no fields to update")
            continue
        records.append({"record_id": record_id, "fields": fields})

    start = time.time()
    updated = 0
    try:
        if len(records) == 1:
            update_record(base_url, token, ref, records[0]["record_id"], records[0]["fields"])
            updated = 1
        else:
            for idx in range(0, len(records), MAX_BATCH_SIZE):
                batch = records[idx : idx + MAX_BATCH_SIZE]
                batch_update_records(base_url, token, ref, batch)
                updated += len(batch)
    except RuntimeError as exc:
        errors.append(str(exc))

    duration = time.time() - start
    payload = {
        "updated": updated,
        "requested": len(records),
        "skipped": skipped,
        "failed": len(errors),
        "errors": errors,
        "elapsed_seconds": round(duration, 3),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
