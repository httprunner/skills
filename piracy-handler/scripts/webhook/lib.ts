import {
  chunk,
  dayStartMs,
  env,
  expandHome,
  firstText,
  must,
  pickField,
  readInput,
  todayLocal,
  toDay,
} from "../shared/lib";
import { createResultSource, type ResultDataSource } from "../data/result_source";
import { createHmac } from "crypto";

// Re-export for consumers that import from webhook_lib
export { dayStartMs, env, expandHome, must, readInput, toDay };

export type DispatchOptions = {
  groupID: string;
  day: string;
  bizType: string;
  planRecordID?: string;
  planFields?: Record<string, any>;
  dryRun?: boolean;
  dataSource?: ResultDataSource;
  dbPath?: string;
  table?: string;
  pageSize?: number;
  timeoutMs?: number;
  maxRetries?: number;
};

export type DispatchResult = {
  group_id: string;
  day: string;
  biz_type: string;
  ready: boolean;
  pushed: boolean;
  status: string;
  retry_count: number;
  task_ids: number[];
  task_ids_by_status: Record<string, number[]>;
  reason?: string;
  // Debug-only: present in dry-run outputs.
  records_by_task_id?: Record<string, { total: number; items: string[] }>;
  payload_record_count?: number;
  debug?: Record<string, unknown>;
};

export type PendingOrFailedPlanRow = {
  recordID: string;
  groupID: string;
  day: string;
  bizType: string;
  fields: Record<string, any>;
};

const TASK_ID_FIELDS = ["task_id", "TaskID"] as const;
const ITEM_ID_FIELDS = ["ItemID", "item_id", "id", "ID"] as const;

const SCENE_GENERAL_SEARCH = "综合页搜索";
const VEDEM_SIGNATURE_EXPIRATION = 1800;

type ResultFieldSchema = {
  Datetime: string;
  App: string;
  Scene: string;
  Params: string;
  ItemID: string;
  ItemCaption: string;
  ItemCDNURL: string;
  ItemURL: string;
  ItemDuration: string;
  UserName: string;
  UserID: string;
  UserAlias: string;
  UserAuthEntity: string;
  Tags: string;
  LikeCount: string;
  ViewCount: string;
  CommentCount: string;
  CollectCount: string;
  ForwardCount: string;
  ShareCount: string;
  AnchorPoint: string;
  PayMode: string;
  Collection: string;
  Episode: string;
  PublishTime: string;
  TaskID: string;
  DeviceSerial: string;
  Extra: string;
};

type SourceFieldSchema = {
  TaskID: string;
  DramaID: string;
  DramaName: string;
  TotalDuration: string;
  EpisodeCount: string;
  Priority: string;
  RightsProtectionScenario: string;
  BizTaskID: string;
  AccountID: string;
  AccountName: string;
  SearchKeywords: string;
  Platform: string;
  CaptureDate: string;
};

function envField(name: string, def: string) {
  return String(env(name, def)).trim() || def;
}

function resultFieldSchema(): ResultFieldSchema {
  return {
    Datetime: envField("RESULT_FIELD_DATETIME", "Datetime"),
    App: envField("RESULT_FIELD_APP", "App"),
    Scene: envField("RESULT_FIELD_SCENE", "Scene"),
    Params: envField("RESULT_FIELD_PARAMS", "Params"),
    ItemID: envField("RESULT_FIELD_ITEMID", "ItemID"),
    ItemCaption: envField("RESULT_FIELD_ITEMCAPTION", "ItemCaption"),
    ItemCDNURL: envField("RESULT_FIELD_ITEMCDNURL", "ItemCDNURL"),
    ItemURL: envField("RESULT_FIELD_ITEMURL", "ItemURL"),
    ItemDuration: envField("RESULT_FIELD_DURATION", "ItemDuration"),
    UserName: envField("RESULT_FIELD_USERNAME", "UserName"),
    UserID: envField("RESULT_FIELD_USERID", "UserID"),
    UserAlias: envField("RESULT_FIELD_USERALIAS", "UserAlias"),
    UserAuthEntity: envField("RESULT_FIELD_USERAUTHENTITY", "UserAuthEntity"),
    Tags: envField("RESULT_FIELD_TAGS", "Tags"),
    LikeCount: envField("RESULT_FIELD_LIKECOUNT", "LikeCount"),
    ViewCount: envField("RESULT_FIELD_VIEWCOUNT", "ViewCount"),
    CommentCount: envField("RESULT_FIELD_COMMENTCOUNT", "CommentCount"),
    CollectCount: envField("RESULT_FIELD_COLLECTCOUNT", "CollectCount"),
    ForwardCount: envField("RESULT_FIELD_FORWARDCOUNT", "ForwardCount"),
    ShareCount: envField("RESULT_FIELD_SHARECOUNT", "ShareCount"),
    AnchorPoint: envField("RESULT_FIELD_ANCHORPOINT", "AnchorPoint"),
    PayMode: envField("RESULT_FIELD_PAYMODE", "PayMode"),
    Collection: envField("RESULT_FIELD_COLLECTION", "Collection"),
    Episode: envField("RESULT_FIELD_EPISODE", "Episode"),
    PublishTime: envField("RESULT_FIELD_PUBLISHTIME", "PublishTime"),
    TaskID: envField("RESULT_FIELD_TASKID", "TaskID"),
    DeviceSerial: envField("RESULT_FIELD_DEVICE_SERIAL", "DeviceSerial"),
    Extra: envField("RESULT_FIELD_EXTRA", "Extra"),
  };
}

function sourceFieldSchema(): SourceFieldSchema {
  return {
    TaskID: envField("SOURCE_FIELD_TASK_ID", "TaskID"),
    DramaID: envField("SOURCE_FIELD_DRAMA_ID", "DramaID"),
    DramaName: envField("SOURCE_FIELD_DRAMA_NAME", "DramaName"),
    TotalDuration: envField("SOURCE_FIELD_TOTAL_DURATION", "TotalDuration"),
    EpisodeCount: envField("SOURCE_FIELD_EPISODE_COUNT", "EpisodeCount"),
    Priority: envField("SOURCE_FIELD_PRIORITY", "Priority"),
    RightsProtectionScenario: envField("SOURCE_FIELD_RIGHTS_SCENARIO", "RightsProtectionScenario"),
    BizTaskID: envField("SOURCE_FIELD_BIZ_TASK_ID", "BizTaskID"),
    AccountID: envField("SOURCE_FIELD_ACCOUNT_ID", "AccountID"),
    AccountName: envField("SOURCE_FIELD_ACCOUNT_NAME", "AccountName"),
    SearchKeywords: envField("SOURCE_FIELD_SEARCH_KEYWORDS", "SearchKeywords"),
    Platform: envField("SOURCE_FIELD_PLATFORM", "Platform"),
    CaptureDate: envField("SOURCE_FIELD_CAPTURE_DATE", "CaptureDate"),
  };
}

function mapAppValue(app: string) {
  const s = String(app || "").trim();
  switch (s) {
    case "com.smile.gifmaker":
    case "com.jiangjia.gif":
      return "快手";
    case "com.tencent.mm":
    case "com.tencent.xin":
      return "微信视频号";
    default:
      return s;
  }
}

function hasOwn(obj: Record<string, any> | null | undefined, key: string) {
  return Boolean(obj) && Object.prototype.hasOwnProperty.call(obj, key);
}

function toSnakeCase(input: string) {
  const s = String(input || "").trim();
  if (!s) return "";
  const runes = Array.from(s);
  let out = "";
  for (let i = 0; i < runes.length; i++) {
    const ch = runes[i];
    const code = ch.charCodeAt(0);
    const isUpper = code >= 65 && code <= 90;
    if (!isUpper) {
      out += ch;
      continue;
    }
    if (i > 0) {
      const prev = runes[i - 1];
      const prevCode = prev.charCodeAt(0);
      const next = i + 1 < runes.length ? runes[i + 1] : "";
      const nextCode = next ? next.charCodeAt(0) : 0;
      const prevIsLowerOrDigit = (prevCode >= 97 && prevCode <= 122) || (prevCode >= 48 && prevCode <= 57);
      const nextIsLower = nextCode >= 97 && nextCode <= 122;
      if (prevIsLowerOrDigit || nextIsLower) out += "_";
    }
    out += ch.toLowerCase();
  }
  return out;
}

function bitableValueToString(value: any): string {
  if (value == null) return "";
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value).trim();
  if (Array.isArray(value)) {
    const isRichText = value.some((it) => it && typeof it === "object" && !Array.isArray(it) && "text" in it);
    const parts = value
      .map((it) => {
        if (isRichText && it && typeof it === "object" && typeof (it as any).text === "string") {
          return String((it as any).text || "").trim();
        }
        return bitableValueToString(it);
      })
      .filter(Boolean);
    return (isRichText ? parts.join(" ") : parts.join(",")).trim();
  }
  if (typeof value === "object") {
    const obj = value as Record<string, any>;
    for (const k of ["value", "values", "elements", "content"]) {
      if (hasOwn(obj, k)) {
        const nested = bitableValueToString(obj[k]);
        if (nested) return nested;
      }
    }
    if (typeof obj.text === "string" && obj.text.trim()) return obj.text.trim();
    for (const k of ["link", "name", "en_name", "email", "id", "user_id", "url", "tmp_url", "file_token", "address"]) {
      const v = bitableValueToString(obj[k]);
      if (v) return v;
    }
    try {
      return JSON.stringify(obj).trim();
    } catch {
      return "";
    }
  }
  return "";
}

function bitableFieldString(fields: Record<string, any>, name: string) {
  const key = String(name || "").trim();
  if (!key || !hasOwn(fields, key)) return "";
  return bitableValueToString(fields[key]);
}

function bitableFieldStringCompat(fields: Record<string, any>, rawKey: string): [string, boolean] {
  const key = String(rawKey || "").trim();
  if (!key || !fields) return ["", false];
  if (hasOwn(fields, key)) return [bitableFieldString(fields, key), true];
  const snake = toSnakeCase(key);
  if (snake && hasOwn(fields, snake)) return [bitableFieldString(fields, snake), true];
  return ["", false];
}

function flattenDramaFields(raw: Record<string, any>, schema: SourceFieldSchema) {
  const payload: Record<string, any> = {};
  for (const [engName, rawKey] of Object.entries(schema)) {
    if (raw && hasOwn(raw, engName)) {
      payload[engName] = bitableFieldString(raw, engName);
      continue;
    }
    if (raw) {
      payload[engName] = bitableFieldString(raw, rawKey);
      continue;
    }
    payload[engName] = "";
  }
  return payload;
}

function flattenRecordsAndCollectItemIDs(records: Array<Record<string, any>>, schema: ResultFieldSchema) {
  const result: Array<Record<string, any>> = [];
  const seenItem = new Set<string>();
  const rawItemKey = schema.ItemID;
  const rawAppKey = schema.App;
  for (const rec of records) {
    if (rawItemKey) {
      const [itemID, ok] = bitableFieldStringCompat(rec, rawItemKey);
      if (ok && itemID) {
        if (seenItem.has(itemID)) continue;
        seenItem.add(itemID);
      }
    }
    const recID = String(rec.record_id ?? rec._record_id ?? rec.id ?? "").trim();
    const entry: Record<string, any> = { _record_id: recID };
    for (const [engName, rawKey] of Object.entries(schema)) {
      const [fieldValue, ok] = bitableFieldStringCompat(rec, rawKey);
      if (!ok) {
        entry[engName] = null;
        continue;
      }
      entry[engName] = engName === "App" && rawKey === rawAppKey ? mapAppValue(fieldValue) : fieldValue;
    }
    result.push(entry);
  }
  return result;
}

function buildWebhookResultPayload(
  dramaRaw: Record<string, any>,
  records: Array<Record<string, any>>,
  sourceSchema: SourceFieldSchema,
  resultSchema: ResultFieldSchema,
) {
  const payload = flattenDramaFields(dramaRaw || {}, sourceSchema);
  for (const [key, val] of Object.entries(dramaRaw || {})) {
    const trimmed = String(key || "").trim();
    if (!trimmed) continue;
    if (hasOwn(payload, trimmed)) continue;
    if (val == null) continue;
    if (typeof val === "string" && !val.trim()) continue;
    payload[trimmed] = val;
  }
  payload.records = flattenRecordsAndCollectItemIDs(records, resultSchema);
  return payload;
}

function parseGroupUserKey(groupID: string) {
  const trimmed = String(groupID || "").trim();
  if (!trimmed) return "";
  const parts = trimmed.split("_");
  if (parts.length < 3) return "";
  return String(parts[parts.length - 1] || "").trim();
}

function equalFold(a: string, b: string) {
  return String(a || "").trim().toLowerCase() === String(b || "").trim().toLowerCase();
}

function decodeCommonEscapes(input: string): string {
  if (!input) return "";
  return input
    .replace(/\\u([0-9a-fA-F]{4})/g, (_, hex: string) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex: string) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/&#([0-9]+);/g, (_, dec: string) => String.fromCharCode(parseInt(dec, 10)))
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&nbsp;/g, " ");
}

function normalizeUserName(raw: string) {
  const trimmed = String(raw || "").trim();
  if (!trimmed) return "";
  const withoutTags = trimmed.replace(/<[^>]*>/g, "");
  const unescaped = decodeCommonEscapes(withoutTags);
  return normalizeKey(unescaped);
}

function deriveUserKeyFromTaskValues(userID: string, userName: string) {
  // Match fox/search/main.go deriveUserKeyFromTask: prefer UserID, else normalized UserName.
  const uid = normalizeKey(userID);
  if (uid) return uid;
  return normalizeUserName(userName);
}

function pickDominant(values: string[]) {
  const counts = new Map<string, number>();
  for (const v of values) {
    const s = normalizeKey(v);
    if (!s) continue;
    counts.set(s, (counts.get(s) || 0) + 1);
  }
  let best = "";
  let bestCount = 0;
  for (const [k, c] of counts.entries()) {
    if (c > bestCount || (c === bestCount && k < best)) {
      best = k;
      bestCount = c;
    }
  }
  return best;
}

function resolveUserKey(userInfo: { UserAlias?: string; UserID?: string; UserName?: string }) {
  // Match fox ResolveUserKey priority: alias > id > name.
  return normalizeKey(userInfo.UserAlias) || normalizeKey(userInfo.UserID) || normalizeKey(userInfo.UserName);
}

// Keep the implementation aligned with fox (alias > id > name) and avoid
// heuristics here. Grouping should already be correct via Task table.

export function parseTaskIDs(v: any): number[] {
  if (v == null) return [];

  const collectFromStatusObject = (obj: Record<string, any>) => {
    const out: number[] = [];
    for (const val of Object.values(obj)) {
      if (Array.isArray(val)) {
        for (const it of val) {
          const n = Math.trunc(Number(it));
          if (Number.isFinite(n) && n > 0) out.push(n);
        }
        continue;
      }
      const n = Math.trunc(Number(val));
      if (Number.isFinite(n) && n > 0) out.push(n);
    }
    return Array.from(new Set(out)).sort((a, b) => a - b);
  };

  if (typeof v === "object" && !Array.isArray(v) && v) {
    const maybeRichTextCell = "text" in v || "value" in v || "type" in v;
    if (!maybeRichTextCell) return collectFromStatusObject(v as Record<string, any>);
  }

  const s = firstText(v).trim();
  if (!s) return [];

  let obj: any;
  try {
    obj = JSON.parse(s);
  } catch {
    return [];
  }

  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return [];
  return collectFromStatusObject(obj as Record<string, any>);
}

// ---------- Feishu Bitable API ----------

export type BitableRef = { appToken: string; tableID: string; viewID: string; wikiToken: string };

export function parseBitableURL(raw: string): BitableRef {
  const normalized = String(raw || "")
    .trim()
    .replace(/^['"]|['"]$/g, "")
    .replace(/\\([?=&])/g, "$1");
  const u = new URL(normalized);
  const seg = u.pathname.split("/").filter(Boolean);
  let appToken = "";
  let wikiToken = "";
  for (let i = 0; i < seg.length - 1; i++) {
    if (seg[i] === "base") appToken = seg[i + 1];
    if (seg[i] === "wiki") wikiToken = seg[i + 1];
  }
  const tableID = u.searchParams.get("table") || u.searchParams.get("table_id") || "";
  const viewID = u.searchParams.get("view") || u.searchParams.get("view_id") || "";
  if (!tableID) throw new Error(`missing table id in url: ${raw}`);
  return { appToken, tableID, viewID, wikiToken };
}

export async function requestJSON(method: string, url: string, token: string, body: any) {
  const res = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body == null ? undefined : JSON.stringify(body),
  });
  const txt = await res.text();
  if (!res.ok) throw new Error(`http ${res.status}: ${txt}`);
  const data = txt ? JSON.parse(txt) : {};
  if (data.code !== 0) throw new Error(`api error code=${data.code} msg=${data.msg || ""}`);
  return data;
}

export async function getTenantToken(baseURL: string, appID: string, appSecret: string) {
  const url = `${baseURL}/open-apis/auth/v3/tenant_access_token/internal`;
  const data = await requestJSON("POST", url, "", { app_id: appID, app_secret: appSecret });
  const token = String(data.tenant_access_token || "").trim();
  if (!token) throw new Error("tenant_access_token missing");
  return token;
}

export async function resolveWikiToken(baseURL: string, token: string, wikiToken: string) {
  const url = `${baseURL}/open-apis/wiki/v2/spaces/get_node?token=${encodeURIComponent(wikiToken)}`;
  const data = await requestJSON("GET", url, token, null);
  const objToken = String(data?.data?.node?.obj_token || "").trim();
  if (!objToken) throw new Error("wiki obj_token missing");
  return objToken;
}

export function condition(field: string, op: string, ...vals: any[]) {
  return { field_name: field, operator: op, value: vals };
}
export function andFilter(conditions: any[], children: any[] = []) {
  return { conjunction: "and", conditions, children };
}
export function orFilter(conditions: any[]) {
  return { conjunction: "or", conditions };
}

export type FeishuCtx = { baseURL: string; token: string };

function useWebhookView() {
  // Default to ignoring view_id so upsert/query can see full table and avoid duplicates.
  const raw = env("WEBHOOK_USE_VIEW", "false").trim().toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

export async function searchRecords(ctx: FeishuCtx, bitableURL: string, filter: any, pageSize = 200, limit = 200) {
  const ref = parseBitableURL(bitableURL);
  if (!ref.appToken && ref.wikiToken) ref.appToken = await resolveWikiToken(ctx.baseURL, ctx.token, ref.wikiToken);
  if (!ref.appToken) throw new Error("bitable app token missing");

  const out: any[] = [];
  let pageToken = "";
  while (true) {
    const q = new URLSearchParams();
    q.set("page_size", String(pageSize));
    if (pageToken) q.set("page_token", pageToken);
    const url = `${ctx.baseURL}/open-apis/bitable/v1/apps/${ref.appToken}/tables/${ref.tableID}/records/search?${q.toString()}`;
    const body: any = {};
    if (filter) body.filter = filter;
    if (ref.viewID && useWebhookView()) body.view_id = ref.viewID;
    const data = await requestJSON("POST", url, ctx.token, body);
    const items = Array.isArray(data?.data?.items) ? data.data.items : [];
    out.push(...items);
    if (out.length >= limit) return out.slice(0, limit);
    if (!data?.data?.has_more || !data?.data?.page_token) break;
    pageToken = String(data.data.page_token);
  }
  return out;
}

export async function batchCreate(ctx: FeishuCtx, bitableURL: string, records: Array<{ fields: Record<string, any> }>) {
  if (!records.length) return { recordIDs: [] as string[] };
  const ref = parseBitableURL(bitableURL);
  if (!ref.appToken && ref.wikiToken) ref.appToken = await resolveWikiToken(ctx.baseURL, ctx.token, ref.wikiToken);
  if (!ref.appToken) throw new Error("bitable app token missing");
  const url = `${ctx.baseURL}/open-apis/bitable/v1/apps/${ref.appToken}/tables/${ref.tableID}/records/batch_create`;
  const recordIDs: string[] = [];
  for (let i = 0; i < records.length; i += 500) {
    const data = await requestJSON("POST", url, ctx.token, { records: records.slice(i, i + 500) });
    const created = Array.isArray(data?.data?.records) ? data.data.records : [];
    for (const row of created) {
      const id = String(row?.record_id || "").trim();
      if (id) recordIDs.push(id);
    }
  }
  return { recordIDs };
}

export async function batchUpdate(ctx: FeishuCtx, bitableURL: string, records: Array<{ record_id: string; fields: Record<string, any> }>) {
  if (!records.length) return;
  const ref = parseBitableURL(bitableURL);
  if (!ref.appToken && ref.wikiToken) ref.appToken = await resolveWikiToken(ctx.baseURL, ctx.token, ref.wikiToken);
  if (!ref.appToken) throw new Error("bitable app token missing");
  const url = `${ctx.baseURL}/open-apis/bitable/v1/apps/${ref.appToken}/tables/${ref.tableID}/records/batch_update`;
  for (let i = 0; i < records.length; i += 500) {
    await requestJSON("POST", url, ctx.token, { records: records.slice(i, i + 500) });
  }
}

// ---------- field name configs ----------

function taskFields() {
  return {
    TaskID: env("TASK_FIELD_TASKID", "TaskID"),
    App: env("TASK_FIELD_APP", "App"),
    Scene: env("TASK_FIELD_SCENE", "Scene"),
    Params: env("TASK_FIELD_PARAMS", "Params"),
    Date: env("TASK_FIELD_DATE", "Date"),
    Status: env("TASK_FIELD_STATUS", "Status"),
    GroupID: env("TASK_FIELD_GROUPID", "GroupID"),
    UserID: env("TASK_FIELD_USERID", "UserID"),
    UserName: env("TASK_FIELD_USERNAME", "UserName"),
  };
}

export function webhookFields() {
  return {
    App: env("WEBHOOK_FIELD_APP", "App"),
    BizType: env("WEBHOOK_FIELD_BIZTYPE", "BizType"),
    GroupID: env("WEBHOOK_FIELD_GROUPID", "GroupID"),
    Status: env("WEBHOOK_FIELD_STATUS", "Status"),
    TaskIDs: env("WEBHOOK_FIELD_TASKIDS", "TaskIDs"),
    TaskIDsByStatus: env("WEBHOOK_FIELD_TASK_IDS_BY_STATUS", ""),
    DramaInfo: env("WEBHOOK_FIELD_DRAMAINFO", "DramaInfo"),
    Date: env("WEBHOOK_FIELD_DATE", "Date"),
    RetryCount: env("WEBHOOK_FIELD_RETRYCOUNT", "RetryCount"),
    LastError: env("WEBHOOK_FIELD_LAST_ERROR", "LastError"),
    Records: env("WEBHOOK_FIELD_RECORDS", "Records"),
    UserInfo: env("WEBHOOK_FIELD_USERINFO", "UserInfo"),
    StartAt: env("WEBHOOK_FIELD_STARTAT", "StartAt"),
    EndAt: env("WEBHOOK_FIELD_ENDAT", "EndAt"),
    UpdateAt: env("WEBHOOK_FIELD_UPDATEAT", "UpdateAt"),
  };
}

// ---------- dispatch logic ----------

function classifyStatuses(rows: any[], statusField: string, taskIDField: string) {
  const by: Record<string, number[]> = {};
  for (const r of rows) {
    const f = r.fields || {};
    const status = firstText(f[statusField]).toLowerCase() || "unknown";
    const id = Math.trunc(Number(firstText(f[taskIDField])));
    if (!Number.isFinite(id) || id <= 0) continue;
    if (!by[status]) by[status] = [];
    by[status].push(id);
  }
  for (const k of Object.keys(by)) by[k] = Array.from(new Set(by[k])).sort((a, b) => a - b);
  return by;
}

function isTerminalStatus(s: string) {
  const x = s.toLowerCase();
  return x === "success" || x === "error";
}

function allTerminal(taskRows: any[], statusField: string) {
  return taskRows.every((r) => isTerminalStatus(firstText((r.fields || {})[statusField])));
}

async function fetchTaskRowsByTaskIDs(ctx: FeishuCtx, taskURL: string, taskIDField: string, taskIDs: number[]) {
  const ids = uniqInts(taskIDs);
  if (!ids.length) return [] as any[];

  const out: any[] = [];
  // Feishu Bitable filter does not support a compact `in` operator for all field types.
  // Use OR of `is` conditions, chunked to keep request bodies reasonable.
  const chunkSize = 100;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const part = ids.slice(i, i + chunkSize);
    const or = orFilter(part.map((id) => condition(taskIDField, "is", String(id))));
    const filter = andFilter([], [or]);
    const rows = await searchRecords(ctx, taskURL, filter, 200, part.length + 50);
    out.push(...rows);
  }
  return out;
}

function mergeTaskIDsByStatus(allIDs: number[], taskRows: any[], statusField: string, taskIDField: string) {
  const found = new Set<number>();
  const by = classifyStatuses(taskRows, statusField, taskIDField);
  for (const ids of Object.values(by)) {
    for (const id of ids) found.add(id);
  }
  const missing = allIDs.filter((id) => !found.has(id));
  if (missing.length) {
    // Keep missing task IDs under pending to avoid silently dropping them.
    by.pending = uniqInts([...(by.pending || []), ...missing]);
  }
  // Ensure every task id appears somewhere.
  const flattened = new Set<number>();
  for (const ids of Object.values(by)) for (const id of ids) flattened.add(id);
  const leftovers = allIDs.filter((id) => !flattened.has(id));
  if (leftovers.length) by.unknown = uniqInts([...(by.unknown || []), ...leftovers]);
  return { byStatus: by, missingTaskIDs: missing };
}

function uniqInts(vals: number[]) {
  return Array.from(new Set(vals.filter((n) => Number.isFinite(n) && n > 0))).sort((a, b) => a - b);
}

function parseDramaInfo(raw: any) {
  const s = firstText(raw);
  if (!s) return {};
  try {
    const j = JSON.parse(s);
    return typeof j === "object" && j ? j : {};
  } catch {
    return {};
  }
}

async function collectPayloadFromResultSource(opts: DispatchOptions, taskIDs: number[]) {
  if (!taskIDs.length) {
    return { records: [] };
  }
  const source = createResultSource({
    dataSource: opts.dataSource || "sqlite",
    dbPath: opts.dbPath,
    supabaseTable: opts.table,
    pageSize: opts.pageSize,
    timeoutMs: opts.timeoutMs,
  });
  const rows = await source.fetchByTaskIDs(taskIDs);

  return { records: rows };
}

function normalizeKey(v: any) {
  return String(v || "")
    .trim()
    .replace(/\s+/g, " ");
}

function normalizeTaskIDsByStatus(input: any): Record<string, number[]> {
  const obj = input && typeof input === "object" && !Array.isArray(input) ? (input as Record<string, any>) : {};
  const out: Record<string, number[]> = {};
  for (const [k, v] of Object.entries(obj)) {
    const key = String(k || "").trim().toLowerCase() || "unknown";
    const ids: number[] = [];
    if (Array.isArray(v)) {
      for (const it of v) {
        const n = Math.trunc(Number(it));
        if (Number.isFinite(n) && n > 0) ids.push(n);
      }
    } else {
      const n = Math.trunc(Number(v));
      if (Number.isFinite(n) && n > 0) ids.push(n);
    }
    if (!out[key]) out[key] = [];
    out[key].push(...ids);
  }
  for (const k of Object.keys(out)) out[k] = uniqInts(out[k]);
  return out;
}

function parseTaskIDsByStatusCell(v: any): Record<string, number[]> {
  if (v == null) return {};
  if (typeof v === "object" && !Array.isArray(v) && v) {
    const maybeRichTextCell = "text" in (v as any) || "value" in (v as any) || "type" in (v as any);
    if (!maybeRichTextCell) return normalizeTaskIDsByStatus(v);
  }
  const s = firstText(v).trim();
  if (!s) return {};
  try {
    const j = JSON.parse(s);
    return normalizeTaskIDsByStatus(j);
  } catch {
    return {};
  }
}

function equalTaskIDsByStatus(a: any, b: any) {
  const aa = normalizeTaskIDsByStatus(parseTaskIDsByStatusCell(a));
  const bb = normalizeTaskIDsByStatus(parseTaskIDsByStatusCell(b));
  const ak = Object.keys(aa).sort();
  const bk = Object.keys(bb).sort();
  if (ak.length !== bk.length) return false;
  for (let i = 0; i < ak.length; i++) {
    if (ak[i] !== bk[i]) return false;
    const av = aa[ak[i]] || [];
    const bv = bb[bk[i]] || [];
    if (av.length !== bv.length) return false;
    for (let j = 0; j < av.length; j++) if (av[j] !== bv[j]) return false;
  }
  return true;
}

function pickTaskMetaFromRows(taskRows: any[], userIDField: string, userNameField: string, paramsField: string) {
  let userID = "";
  let userName = "";
  let params = "";
  for (const r of taskRows) {
    const f = r?.fields || {};
    if (!userID) userID = String(firstText(f[userIDField]) || "").trim();
    if (!userName) userName = String(firstText(f[userNameField]) || "").trim();
    if (!params) params = String(firstText(f[paramsField]) || "").trim();
    if (userID && userName && params) break;
  }
  return { UserID: userID, UserName: userName, Params: params };
}


function readRecordTaskID(fields: Record<string, unknown>, resultSchema: ResultFieldSchema) {
  const tid = Math.trunc(Number(pickField(fields, ["TaskID", resultSchema.TaskID, "task_id"]) || 0));
  return Number.isFinite(tid) && tid > 0 ? tid : 0;
}

function readRecordUserKey(fields: Record<string, unknown>, resultSchema: ResultFieldSchema) {
  const alias = normalizeKey(pickField(fields, ["UserAlias", resultSchema.UserAlias, "user_alias"]));
  if (alias) return alias;
  const uid = normalizeKey(pickField(fields, ["UserID", resultSchema.UserID, "user_id"]));
  if (uid) return uid;
  return normalizeKey(pickField(fields, ["UserName", resultSchema.UserName, "user_name"]));
}

function pickFirstNonEmptyCaptureFieldByTaskIDs(
  records: Array<Record<string, unknown>>,
  taskIDs: number[],
  resultSchema: ResultFieldSchema,
  ...fieldNames: string[]
) {
  const ids = uniqInts(taskIDs);
  if (!ids.length || !records.length) return "";
  for (const id of ids) {
    for (const r of records) {
      const tid = readRecordTaskID(r, resultSchema);
      if (tid !== id) continue;
      const v = normalizeKey(pickField(r, fieldNames));
      if (v) return v;
    }
  }
  return "";
}

function buildRecordsByTaskID(taskIDs: number[], records: Array<Record<string, unknown>>, resultSchema: ResultFieldSchema) {
  const itemsByTaskID = new Map<number, Set<string>>();
  for (const r of records) {
    const tid = readRecordTaskID(r, resultSchema);
    if (!Number.isFinite(tid) || tid <= 0) continue;
    const itemID = normalizeKey(pickField(r, ["ItemID", resultSchema.ItemID, "item_id", "id", "ID"]));
    if (!itemID) continue;
    if (!itemsByTaskID.has(tid)) itemsByTaskID.set(tid, new Set());
    itemsByTaskID.get(tid)!.add(itemID);
  }

  const recordsByTaskID: Record<string, { total: number; items: string[] }> = {};
  const missingRecordTaskIDs: number[] = [];
  for (const tid of uniqInts(taskIDs)) {
    const set = itemsByTaskID.get(tid) || new Set<string>();
    const items = Array.from(set).sort();
    recordsByTaskID[String(tid)] = { total: items.length, items };
    if (!items.length) missingRecordTaskIDs.push(tid);
  }
  return { recordsByTaskID, missingRecordTaskIDs };
}

function truncateRecordsByTaskID(
  recordsByTaskID: Record<string, { total: number; items: string[] }>,
  maxItemsPerTask = 20,
) {
  const out: Record<string, { total: number; items: string[] }> = {};
  for (const [k, v] of Object.entries(recordsByTaskID || {})) {
    const total = Math.trunc(Number((v as any)?.total ?? 0));
    const items = Array.isArray((v as any)?.items) ? ((v as any).items as string[]) : [];
    out[k] = { total: Number.isFinite(total) && total >= 0 ? total : items.length, items: items.slice(0, maxItemsPerTask) };
  }
  return out;
}

async function postWebhook(baseURL: string, payload: any) {
  const url = `${baseURL.replace(/\/+$/, "")}/drama/webhook/grab-finished`;
  const body = JSON.stringify(payload);
  const auth = buildVedemAgwTokenSigned(body);
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (auth) headers["Agw-Auth"] = auth;
  if (isTraceLogEnabled()) {
    console.error(`[webhook][trace] request url=${url}`);
    console.error(`[webhook][trace] request headers=${JSON.stringify(headers)}`);
    console.error(`[webhook][trace] request body=${JSON.stringify(payload, null, 2)}`);
  }
  const res = await fetch(url, {
    method: "POST",
    headers,
    body,
  });
  const txt = await res.text();
  if (!res.ok) throw new Error(`webhook http ${res.status}: ${txt}`);
}

function sha256HMACHex(key: string, data: string) {
  return createHmac("sha256", Buffer.from(String(key || ""), "utf-8"))
    .update(Buffer.from(String(data || ""), "utf-8"))
    .digest("hex");
}

function buildVedemAgwTokenSigned(body: string) {
  const ak = String(env("VEDEM_DRAMA_AK", "") || "").trim();
  const sk = String(env("VEDEM_DRAMA_SK", "") || "").trim();
  if (!ak || !sk) return "";
  const nowUnix = Math.trunc(Date.now() / 1000);
  const signKeyInfo = `auth-v2/${ak}/${nowUnix}/${VEDEM_SIGNATURE_EXPIRATION}`;
  const signKey = sha256HMACHex(sk, signKeyInfo);
  const signResult = sha256HMACHex(signKey, body);
  return `${signKeyInfo}/${signResult}`;
}

function isTraceLogEnabled() {
  const level = String(env("LOG_LEVEL", "info") || "info").trim().toLowerCase();
  return level === "trace";
}

export async function resolveGroupFromTaskID(taskID: number) {
  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const taskURL = must("TASK_BITABLE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const tf = taskFields();

  const rows = await searchRecords(ctx, taskURL, andFilter([condition(tf.TaskID, "is", String(taskID))]), 10, 10);
  if (!rows.length) return { groupID: "", day: "", bizType: "piracy_general_search" };
  const f = rows[0].fields || {};
  const groupID = firstText(f[tf.GroupID]);
  const day = toDay(firstText(f[tf.Date]));
  return { groupID, day, bizType: "piracy_general_search" };
}

export async function processOneGroup(opts: DispatchOptions): Promise<DispatchResult> {
  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const taskURL = must("TASK_BITABLE_URL");
  const webhookURL = must("WEBHOOK_BITABLE_URL");
  const crawlerBaseURL = must("CRAWLER_SERVICE_BASE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const maxRetries = opts.maxRetries ?? 3;

  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const tf = taskFields();
  const wf = webhookFields();
  const resultSchema = resultFieldSchema();
  const sourceSchema = sourceFieldSchema();

  const day = opts.day || todayLocal();

  const providedRecordID = String(opts.planRecordID || "").trim();
  const providedFields = opts.planFields && typeof opts.planFields === "object" ? opts.planFields : null;

  const planRows: any[] =
    providedRecordID && providedFields
      ? [{ record_id: providedRecordID, fields: providedFields }]
      : await searchRecords(
          ctx,
          webhookURL,
          andFilter([
            condition(wf.BizType, "is", opts.bizType),
            condition(wf.GroupID, "is", opts.groupID),
            condition(wf.Date, "is", "ExactDate", String(dayStartMs(day))),
          ]),
          20,
          20,
        );

  if (!planRows.length) {
    return {
      group_id: opts.groupID, day, biz_type: opts.bizType,
      ready: false, pushed: false, status: "missing_plan",
      retry_count: 0, task_ids: [], task_ids_by_status: {},
      reason: "webhook plan not found",
    };
  }

  const chosenPlan =
    planRows.find((row) => uniqInts(parseTaskIDs((row.fields || {})[wf.TaskIDs])).length > 0) || planRows[0];

  const planFields = chosenPlan.fields || {};
  const recordID = String(chosenPlan.record_id || "").trim();
  const currentStatus = firstText(planFields[wf.Status]).toLowerCase() || "pending";
  const retryCount = Math.trunc(Number(firstText(planFields[wf.RetryCount]) || "0")) || 0;
  const taskIDs = uniqInts(parseTaskIDs(planFields[wf.TaskIDs]));

  if (!taskIDs.length) {
    return {
      group_id: opts.groupID, day, biz_type: opts.bizType,
      ready: false, pushed: false, status: "invalid_plan",
      retry_count: retryCount, task_ids: [], task_ids_by_status: {},
      reason: "empty TaskIDs",
    };
  }

  const allTaskRows = await fetchTaskRowsByTaskIDs(ctx, taskURL, tf.TaskID, taskIDs);
  const { byStatus, missingTaskIDs } = mergeTaskIDsByStatus(taskIDs, allTaskRows, tf.Status, tf.TaskID);
  const ready = allTaskRows.length > 0 && allTerminal(allTaskRows, tf.Status);
  const updateAtField = typeof wf.UpdateAt === "string" && wf.UpdateAt.trim() ? wf.UpdateAt.trim() : "";
  const withUpdateAt = (fields: Record<string, any>) => {
    if (!Object.keys(fields).length) return fields;
    return updateAtField ? { ...fields, [updateAtField]: Date.now() } : fields;
  };

  const updateBase: Record<string, any> = {};
  const nextTaskIDsByStatusStr = JSON.stringify(byStatus);
  const taskIDsByStatusField = typeof wf.TaskIDsByStatus === "string" && wf.TaskIDsByStatus.trim() ? wf.TaskIDsByStatus.trim() : "";
  if (taskIDsByStatusField) {
    const cur = (planFields || {})[taskIDsByStatusField];
    if (!equalTaskIDsByStatus(cur, byStatus)) updateBase[taskIDsByStatusField] = nextTaskIDsByStatusStr;
  }
  if (wf.TaskIDs) {
    // Keep TaskIDs reflecting current status buckets for human readability.
    updateBase[wf.TaskIDs] = nextTaskIDsByStatusStr;
  }

  const nowMs = Date.now();
  const startAtMs = Math.trunc(Number(firstText(planFields[wf.StartAt]) || 0)) || 0;
  if (wf.StartAt && !startAtMs) {
    updateBase[wf.StartAt] = nowMs;
  }

  if (!ready) {
    if (!opts.dryRun && Object.keys(updateBase).length > 0) {
      await batchUpdate(ctx, webhookURL, [{ record_id: recordID, fields: withUpdateAt(updateBase) }]);
    }
    return {
      group_id: opts.groupID, day, biz_type: opts.bizType,
      ready: false, pushed: false, status: currentStatus,
      retry_count: retryCount, task_ids: taskIDs, task_ids_by_status: byStatus,
      reason: missingTaskIDs.length ? `tasks_missing:${missingTaskIDs.join(",")}` : "tasks_not_ready",
    };
  }

  const dramaInfo = parseDramaInfo(planFields[wf.DramaInfo]);
  const { records: sourceRecords } = await collectPayloadFromResultSource(opts, taskIDs);

  const meta = pickTaskMetaFromRows(allTaskRows, tf.UserID, tf.UserName, tf.Params);
  const groupUserKey = normalizeKey(parseGroupUserKey(opts.groupID)) || normalizeKey(meta.UserID);

  const sceneByTaskID = new Map<number, string>();
  for (const r of allTaskRows) {
    const f = r?.fields || {};
    const tid = Math.trunc(Number(firstText(f[tf.TaskID]) || 0));
    if (!Number.isFinite(tid) || tid <= 0) continue;
    const scene = String(firstText(f[tf.Scene]) || "").trim();
    if (scene) sceneByTaskID.set(tid, scene);
  }

  const allowedTaskIDs = new Set<number>(uniqInts(taskIDs));
  const strictSceneFilter = String(opts.bizType || "").trim() === "piracy_general_search";
  let strictTotal = 0;
  let strictKept = 0;
  let strictMissingUserKey = 0;

  const filteredRecords: Array<Record<string, unknown>> = [];
  for (const rec of sourceRecords) {
    const tid = readRecordTaskID(rec, resultSchema);
    if (tid <= 0 || !allowedTaskIDs.has(tid)) continue;
    const scene = String(sceneByTaskID.get(tid) || "").trim();
    if (strictSceneFilter && scene === SCENE_GENERAL_SEARCH) {
      strictTotal++;
      if (!groupUserKey) continue;
      const userKey = normalizeKey(readRecordUserKey(rec, resultSchema));
      if (!userKey) {
        strictMissingUserKey++;
        continue;
      }
      if (!equalFold(userKey, groupUserKey)) continue;
      strictKept++;
    }
    filteredRecords.push(rec);
  }

  const userAlias = pickFirstNonEmptyCaptureFieldByTaskIDs(
    filteredRecords,
    taskIDs,
    resultSchema,
    "UserAlias",
    resultSchema.UserAlias,
    "user_alias",
  );
  const userAuthEntity = pickFirstNonEmptyCaptureFieldByTaskIDs(
    filteredRecords,
    taskIDs,
    resultSchema,
    "UserAuthEntity",
    resultSchema.UserAuthEntity,
    "user_auth_entity",
  );
  const finalUserInfo = {
    UserID: String(meta.UserID || "").trim(),
    UserName: String(meta.UserName || "").trim(),
    UserAlias: String(userAlias || "").trim(),
    UserAuthEntity: String(userAuthEntity || "").trim(),
  };

  const { recordsByTaskID, missingRecordTaskIDs } = buildRecordsByTaskID(taskIDs, filteredRecords, resultSchema);
  const payload = buildWebhookResultPayload(dramaInfo, filteredRecords, sourceSchema, resultSchema) as Record<string, any>;
  if (!String(payload.DramaName || "").trim()) payload.DramaName = String(meta.Params || "").trim();
  payload.UserInfo = finalUserInfo;
  const payloadRecords = Array.isArray(payload.records) ? payload.records : [];

  if (missingRecordTaskIDs.length) {
    console.error(
       `[webhook] warn: group=${opts.groupID} day=${day} missing records for task_ids=${missingRecordTaskIDs.join(",")}; source=${opts.dataSource || "sqlite"} user_id=${finalUserInfo.UserID || ""}`,
     );
   }

  if (opts.dryRun) {
    return {
      group_id: opts.groupID, day, biz_type: opts.bizType,
      ready: true, pushed: false, status: currentStatus,
      retry_count: retryCount, task_ids: taskIDs, task_ids_by_status: byStatus,
      reason: "dry_run",
      payload_record_count: payloadRecords.length,
      records_by_task_id: truncateRecordsByTaskID(recordsByTaskID, 30),
      debug: {
        data_source: opts.dataSource || "sqlite",
        user_info: finalUserInfo,
        group_user_key: groupUserKey,
        strict_total: strictTotal,
        strict_kept: strictKept,
        strict_missing_user_key: strictMissingUserKey,
        fetched_row_count: sourceRecords.length,
        final_row_count: filteredRecords.length,
        payload_unique_item_count: payloadRecords.length,
      },
    };
  }

  try {
    // Persist artifacts before webhook delivery for easier debugging.
    if (!opts.dryRun) {
      await batchUpdate(ctx, webhookURL, [
        {
          record_id: recordID,
          fields: withUpdateAt({
            ...updateBase,
            [wf.Records]: JSON.stringify(recordsByTaskID),
            [wf.UserInfo]: JSON.stringify(finalUserInfo),
          }),
        },
      ]);
    }
    await postWebhook(crawlerBaseURL, payload);
    await batchUpdate(ctx, webhookURL, [
      {
        record_id: recordID,
        fields: withUpdateAt({
          ...updateBase,
          [wf.Status]: "success",
          [wf.RetryCount]: 0,
          [wf.LastError]: "",
           [wf.EndAt]: nowMs,
           // Records is a per-task view of what we actually sent.
           [wf.Records]: JSON.stringify(recordsByTaskID),
           [wf.UserInfo]: JSON.stringify(finalUserInfo),
         }),
       },
     ]);
    return {
      group_id: opts.groupID, day, biz_type: opts.bizType,
      ready: true, pushed: true, status: "success",
      retry_count: 0, task_ids: taskIDs, task_ids_by_status: byStatus,
    };
  } catch (err) {
    const next = retryCount + 1;
    const failStatus = next >= maxRetries ? "error" : "failed";
    await batchUpdate(ctx, webhookURL, [
      {
        record_id: recordID,
        fields: withUpdateAt({
          ...updateBase,
          [wf.Status]: failStatus,
          [wf.RetryCount]: next,
          [wf.LastError]: String(err instanceof Error ? err.message : err),
           [wf.EndAt]: Date.now(),
           // Records is a per-task view of what we actually sent.
           [wf.Records]: JSON.stringify(recordsByTaskID),
           [wf.UserInfo]: JSON.stringify(finalUserInfo),
         }),
       },
     ]);
    return {
      group_id: opts.groupID, day, biz_type: opts.bizType,
      ready: true, pushed: false, status: failStatus,
      retry_count: next, task_ids: taskIDs, task_ids_by_status: byStatus,
      reason: String(err instanceof Error ? err.message : err),
    };
  }
}

export async function listPendingOrFailedRows(day: string, bizType: string, limit: number): Promise<PendingOrFailedPlanRow[]> {
  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const webhookURL = must("WEBHOOK_BITABLE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const wf = webhookFields();

  const statusOr = {
    conjunction: "or",
    conditions: [condition(wf.Status, "is", "pending"), condition(wf.Status, "is", "failed")],
  };
  const conditions = [condition(wf.BizType, "is", bizType)];
  if (day) conditions.push(condition(wf.Date, "is", "ExactDate", String(dayStartMs(day))));
  const filter = andFilter(conditions, [statusOr]);

  const rows = await searchRecords(ctx, webhookURL, filter, 200, limit);
  return rows.map((r) => {
    const f = r.fields || {};
    return {
      recordID: String(r.record_id || "").trim(),
      groupID: firstText(f[wf.GroupID]),
      day: day || toDay(firstText(f[wf.Date])),
      bizType: firstText(f[wf.BizType]) || bizType,
      fields: f,
    };
  });
}
