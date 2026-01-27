"""Shared helpers for Feishu Bitable HTTP scripts."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://open.feishu.cn"
DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 500

TASK_FIELD_ENV_MAP = {
    "TASK_FIELD_TASKID": "TaskID",
    "TASK_FIELD_BIZ_TASK_ID": "BizTaskID",
    "TASK_FIELD_PARENT_TASK_ID": "ParentTaskID",
    "TASK_FIELD_APP": "App",
    "TASK_FIELD_SCENE": "Scene",
    "TASK_FIELD_PARAMS": "Params",
    "TASK_FIELD_ITEMID": "ItemID",
    "TASK_FIELD_BOOKID": "BookID",
    "TASK_FIELD_URL": "URL",
    "TASK_FIELD_USERID": "UserID",
    "TASK_FIELD_USERNAME": "UserName",
    "TASK_FIELD_DATE": "Date",
    "TASK_FIELD_STATUS": "Status",
    "TASK_FIELD_LOGS": "Logs",
    "TASK_FIELD_LAST_SCREEN_SHOT": "LastScreenShot",
    "TASK_FIELD_GROUPID": "GroupID",
    "TASK_FIELD_DEVICE_SERIAL": "DeviceSerial",
    "TASK_FIELD_DISPATCHED_DEVICE": "DispatchedDevice",
    "TASK_FIELD_DISPATCHED_AT": "DispatchedAt",
    "TASK_FIELD_START_AT": "StartAt",
    "TASK_FIELD_END_AT": "EndAt",
    "TASK_FIELD_ELAPSED_SECONDS": "ElapsedSeconds",
    "TASK_FIELD_ITEMS_COLLECTED": "ItemsCollected",
    "TASK_FIELD_EXTRA": "Extra",
    "TASK_FIELD_RETRYCOUNT": "RetryCount",
}


@dataclass
class BitableRef:
    raw_url: str
    app_token: str
    table_id: str
    view_id: str
    wiki_token: str


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def clamp_page_size(size: int) -> int:
    if size <= 0:
        return DEFAULT_PAGE_SIZE
    if size > MAX_PAGE_SIZE:
        return MAX_PAGE_SIZE
    return size


def first_query_value(qs: Dict[str, List[str]], *keys: str) -> str:
    for key in keys:
        vals = qs.get(key) or []
        for val in vals:
            if val and val.strip():
                return val.strip()
    return ""


def parse_bitable_url(raw: str) -> BitableRef:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("bitable url is empty")
    u = urlparse(raw)
    if not u.scheme:
        raise ValueError("bitable url missing scheme")

    segments = [s for s in u.path.strip("/").split("/") if s]
    app_token = ""
    wiki_token = ""
    for i in range(len(segments) - 1):
        if segments[i] == "base":
            app_token = segments[i + 1]
        elif segments[i] == "wiki":
            wiki_token = segments[i + 1]
        if app_token:
            break
    if not app_token and not wiki_token:
        if segments:
            app_token = segments[-1]
    qs = parse_qs(u.query)
    table_id = first_query_value(qs, "table", "tableId", "table_id")
    view_id = first_query_value(qs, "view", "viewId", "view_id")
    if not table_id:
        raise ValueError("missing table_id in bitable url query")
    return BitableRef(raw, app_token, table_id, view_id, wiki_token)


def load_task_fields_from_env() -> Dict[str, str]:
    fields = {v: v for v in TASK_FIELD_ENV_MAP.values()}
    for env_name, default_name in TASK_FIELD_ENV_MAP.items():
        override = env(env_name, "")
        if override:
            fields[default_name] = override
    return fields


def request_json(method: str, url: str, token: Optional[str], payload: Optional[dict]) -> dict:
    data = None
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(url, data=data, method=method, headers=headers)
    with urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def get_tenant_access_token(base_url: str, app_id: str, app_secret: str) -> str:
    url = f"{base_url}/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": app_id, "app_secret": app_secret}
    resp = request_json("POST", url, None, payload)
    if resp.get("code") != 0:
        raise RuntimeError(f"tenant token error: code={resp.get('code')} msg={resp.get('msg')}")
    token = (resp.get("tenant_access_token") or "").strip()
    if not token:
        raise RuntimeError("tenant token missing in response")
    return token


def resolve_wiki_app_token(base_url: str, token: str, wiki_token: str) -> str:
    wiki_token = (wiki_token or "").strip()
    if not wiki_token:
        raise RuntimeError("wiki token is empty")
    url = f"{base_url}/open-apis/wiki/v2/spaces/get_node?token={wiki_token}"
    resp = request_json("GET", url, token, None)
    if resp.get("code") != 0:
        raise RuntimeError(f"wiki node error: code={resp.get('code')} msg={resp.get('msg')}")
    data = resp.get("data") or {}
    node = data.get("node") or {}
    obj_type = (node.get("obj_type") or "").strip()
    obj_token = (node.get("obj_token") or "").strip()
    if obj_type != "bitable":
        raise RuntimeError(f"wiki node obj_type is {obj_type}, not bitable")
    if not obj_token:
        raise RuntimeError("wiki node obj_token missing")
    return obj_token


def bitable_value_to_string(value: Any) -> str:
    return normalize_bitable_value(value).strip()


def normalize_bitable_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bytes):
        return value.decode("utf-8").strip()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        if _is_rich_text_array(value):
            return _join_rich_text(value)
        parts = [normalize_bitable_value(item) for item in value]
        parts = [p for p in parts if p]
        return ",".join(parts) if parts else ""
    if isinstance(value, dict):
        for key in ("value", "values", "elements", "content"):
            if key in value:
                text = normalize_bitable_value(value[key])
                if text:
                    return text
        if isinstance(value.get("text"), str) and value["text"].strip():
            return value["text"].strip()
        for key in ("link", "name", "en_name", "email", "id", "user_id", "url", "tmp_url", "file_token"):
            text = normalize_bitable_value(value.get(key))
            if text:
                return text
        if any(k in value for k in ("address", "location", "pname", "cityname", "adname")):
            parts = [
                normalize_bitable_value(value.get("location")),
                normalize_bitable_value(value.get("pname")),
                normalize_bitable_value(value.get("cityname")),
                normalize_bitable_value(value.get("adname")),
            ]
            parts = [p for p in parts if p]
            if parts:
                return ",".join(parts)
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return ""
    return str(value).strip()


def _is_rich_text_array(items: List[Any]) -> bool:
    for item in items:
        if isinstance(item, dict) and "text" in item:
            return True
    return False


def _join_rich_text(items: List[Any]) -> str:
    parts: List[str] = []
    for item in items:
        if isinstance(item, dict):
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
                continue
            nested = item.get("value")
            nested_text = normalize_bitable_value(nested)
            if nested_text:
                parts.append(nested_text)
        else:
            text = normalize_bitable_value(item)
            if text:
                parts.append(text)
    return " ".join(parts) if parts else ""


def field_int(fields: Dict[str, Any], name: str) -> int:
    raw = bitable_value_to_string(fields.get(name))
    if not raw:
        return 0
    try:
        return int(float(raw))
    except ValueError:
        return 0


def parse_json_input(raw: str) -> List[dict]:
    raw = raw.strip()
    if not raw:
        return []
    if raw.startswith("["):
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        return []
    data = json.loads(raw)
    if isinstance(data, dict):
        if "tasks" in data and isinstance(data["tasks"], list):
            return data["tasks"]
        return [data]
    return []


def parse_jsonl_input(raw: str) -> List[dict]:
    items = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        items.append(json.loads(line))
    return items


def detect_input_format(input_path: str, raw: str) -> str:
    if input_path and input_path != "-":
        suffix = Path(input_path).suffix.lower()
        if suffix == ".jsonl":
            return "jsonl"
    stripped = raw.lstrip()
    if stripped.startswith("[") or stripped.startswith("{"):
        return "json"
    return "jsonl"


def coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def parse_datetime(raw: str) -> Optional[datetime]:
    raw = raw.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def coerce_millis(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        num = int(value)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.lower() == "now":
            return int(time.time() * 1000)
        if raw.isdigit():
            num = int(raw)
        else:
            parsed = parse_datetime(raw)
            return int(parsed.timestamp() * 1000) if parsed else None
    else:
        return None

    if num < 100000000000:
        return int(num * 1000)
    return num


def coerce_date_payload(value: Any) -> Optional[Any]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        num = int(value)
        if num < 100000000000:
            return int(num * 1000)
        return num
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.lower() == "now":
            return int(time.time() * 1000)
        if raw.isdigit():
            num = int(raw)
            if num < 100000000000:
                return int(num * 1000)
            return num
        parsed = parse_datetime(raw)
        if parsed:
            return int(parsed.timestamp() * 1000)
        return raw
    return None


def normalize_extra(extra: Any) -> str:
    if extra is None:
        return ""
    if isinstance(extra, str):
        return extra.strip()
    try:
        return json.dumps(extra, ensure_ascii=False)
    except Exception:
        return ""
