#!/usr/bin/env python3
"""Fetch tasks from Feishu Bitable via HTTP (table filters)."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import bitable_common as common


@dataclass
class PageInfo:
    has_more: bool
    next_page_token: str
    pages: int


def build_filter(fields: Dict[str, str], app: str, scene: str, status: str, date_preset: str) -> Optional[dict]:
    conds = []

    def add(field_key: str, value: str) -> None:
        name = fields.get(field_key, "").strip()
        val = (value or "").strip()
        if name and val:
            conds.append({"field_name": name, "operator": "is", "value": [val]})

    add("App", app)
    add("Scene", scene)
    add("Status", status)
    if date_preset and date_preset != "Any":
        add("Date", date_preset)

    if not conds:
        return None
    return {"conjunction": "and", "conditions": conds}


def fetch_records(
    base_url: str,
    token: str,
    ref: common.BitableRef,
    page_size: int,
    limit: int,
    ignore_view: bool,
    view_id: str,
    filter_obj: Optional[dict],
    max_pages: int,
) -> Tuple[List[dict], PageInfo]:
    page_size = common.clamp_page_size(page_size)
    if limit > 0 and limit < page_size:
        page_size = limit

    items: List[dict] = []
    page_token = ""
    pages = 0

    while True:
        query = {"page_size": str(page_size)}
        if page_token:
            query["page_token"] = page_token
        qs = urlencode(query)

        url = (
            f"{base_url}/open-apis/bitable/v1/apps/{ref.app_token}"
            f"/tables/{ref.table_id}/records/search?{qs}"
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
            raise RuntimeError(
                f"search records failed: code={resp.get('code')} msg={resp.get('msg')}"
            )

        data = resp.get("data") or {}
        batch = data.get("items") or []
        items.extend(batch)
        pages += 1

        has_more = bool(data.get("has_more"))
        page_token = (data.get("page_token") or "").strip()

        if limit > 0 and len(items) >= limit:
            items = items[:limit]
            break
        if max_pages > 0 and pages >= max_pages:
            break
        if not has_more or not page_token:
            break

    return items, PageInfo(has_more=bool(page_token), next_page_token=page_token, pages=pages)


def field_string(fields: Dict[str, Any], name: str) -> str:
    if not fields or not name:
        return ""
    return common.bitable_value_to_string(fields.get(name))


def field_int(fields: Dict[str, Any], name: str) -> int:
    return common.field_int(fields, name)


def decode_task(fields: Dict[str, Any], mapping: Dict[str, str]) -> Optional[Dict[str, Any]]:
    if not fields:
        return None

    task_id = field_int(fields, mapping["TaskID"])
    if task_id == 0:
        return None

    def get(name: str) -> str:
        return field_string(fields, mapping[name])

    task = {
        "task_id": task_id,
        "biz_task_id": get("BizTaskID"),
        "parent_task_id": get("ParentTaskID"),
        "app": get("App"),
        "scene": get("Scene"),
        "params": get("Params"),
        "item_id": get("ItemID"),
        "book_id": get("BookID"),
        "url": get("URL"),
        "user_id": get("UserID"),
        "user_name": get("UserName"),
        "date": get("Date"),
        "status": get("Status"),
        "extra": get("Extra"),
        "logs": get("Logs"),
        "last_screenshot": get("LastScreenShot"),
        "group_id": get("GroupID"),
        "device_serial": get("DeviceSerial"),
        "dispatched_device": get("DispatchedDevice"),
        "dispatched_at": get("DispatchedAt"),
        "start_at": get("StartAt"),
        "end_at": get("EndAt"),
        "elapsed_seconds": get("ElapsedSeconds"),
        "items_collected": get("ItemsCollected"),
        "retry_count": get("RetryCount"),
    }

    if not any(
        task.get(key)
        for key in (
            "params",
            "item_id",
            "book_id",
            "url",
            "user_id",
            "user_name",
        )
    ):
        return None
    return task


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch tasks from Feishu Bitable (HTTP + table filters).")
    parser.add_argument("--task-url", default=common.env("TASK_BITABLE_URL"), help="Bitable task table URL")
    parser.add_argument("--app", required=True, help="App value for filter (e.g. com.smile.gifmaker)")
    parser.add_argument("--scene", required=True, help="Scene value for filter")
    parser.add_argument("--status", default="pending", help="Task status filter (default: pending)")
    parser.add_argument("--date", default="Today", help="Date preset: Today/Yesterday/Any")
    parser.add_argument("--limit", type=int, default=0, help="Max tasks to return (0 = no cap)")
    parser.add_argument("--page-size", type=int, default=common.DEFAULT_PAGE_SIZE, help="Page size (max 500)")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages to fetch (0 = no cap)")
    parser.add_argument("--ignore-view", action="store_true", default=True, help="Ignore view_id when searching")
    parser.add_argument("--use-view", dest="ignore_view", action="store_false", help="Use view_id from URL")
    parser.add_argument("--view-id", default="", help="Override view_id when searching")
    parser.add_argument("--jsonl", action="store_true", help="Output JSONL (one task per line)")
    parser.add_argument("--raw", action="store_true", help="Include raw fields in output")
    return parser.parse_args(argv)


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

    ref = common.parse_bitable_url(task_url)
    fields = common.load_task_fields_from_env()
    filter_obj = build_filter(fields, args.app, args.scene, args.status, args.date)

    try:
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

    start = time.time()
    try:
        items, page_info = fetch_records(
            base_url=base_url,
            token=token,
            ref=ref,
            page_size=args.page_size,
            limit=args.limit,
            ignore_view=args.ignore_view,
            view_id=view_id,
            filter_obj=filter_obj,
            max_pages=args.max_pages,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    duration = time.time() - start

    tasks: List[Dict[str, Any]] = []
    for item in items:
        record_id = (item.get("record_id") or "").strip()
        fields_raw = item.get("fields") or {}
        task = decode_task(fields_raw, fields)
        if task is None:
            continue
        task["record_id"] = record_id
        if args.raw:
            task["raw_fields"] = fields_raw
        tasks.append(task)

    if args.jsonl:
        for task in tasks:
            print(json.dumps(task, ensure_ascii=False))
    else:
        payload = {
            "tasks": tasks,
            "count": len(tasks),
            "elapsed_seconds": round(duration, 3),
            "page_info": {
                "has_more": page_info.has_more,
                "next_page_token": page_info.next_page_token,
                "pages": page_info.pages,
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
