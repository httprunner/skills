#!/usr/bin/env node
import {
  BitableRef,
  ClampPageSize,
  CoerceDatePayload,
  CoerceInt,
  CoerceMillis,
  DefaultBaseURL,
  Env,
  FieldInt,
  GetTenantAccessToken,
  LoadTaskFieldsFromEnv,
  NormalizeExtra,
  NormalizeBitableValue,
  ParseBitableURL,
  RequestJSON,
  ResolveWikiAppToken,
  BitableValueToString,
  MaxPageSize,
  readAllInput,
  detectInputFormat,
  parseJSONItems,
  parseJSONLItems,
} from "./bitable_common";
import chalk from "chalk";

const updateMaxBatchSize = 500;
const updateMaxFilterValues = 50;
const createMaxBatchSize = 500;
const createMaxFilterValues = 50;

const appGroupLabels: Record<string, string> = {
  "com.smile.gifmaker": "快手",
};

type LogLevel = "info" | "error";

function createLogger(json: boolean, stream: NodeJS.WriteStream, color: boolean) {
  const useColor = color && !json;
  const levelColor = (value: string, level: LogLevel) => {
    if (!useColor) return value;
    return level === "error" ? chalk.red(value) : chalk.green(value);
  };
  const keyColor = (value: string) => (useColor ? chalk.blue(value) : value);
  const msgColor = (value: string) => (useColor ? chalk.green(value) : value);
  const valueColor = (value: string) => (useColor ? chalk.dim(value) : value);
  function formatValue(value: unknown): string {
    if (typeof value === "string") {
      if (value === "") return '""';
      if (/\s/.test(value) || value.includes("=") || value.includes("\"")) return JSON.stringify(value);
      return value;
    }
    if (value === null || value === undefined) return "null";
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return JSON.stringify(value);
  }
  function write(level: LogLevel, msg: string, fields?: Record<string, unknown>) {
    const time = new Date().toISOString();
    if (json) {
      const payload: Record<string, unknown> = { time, level: level.toUpperCase(), msg };
      if (fields) for (const [k, v] of Object.entries(fields)) payload[k] = v;
      stream.write(`${JSON.stringify(payload)}\n`);
      return;
    }
    const parts = [
      `${keyColor("time")}=${valueColor(time)}`,
      `${keyColor("level")}=${levelColor(level.toUpperCase(), level)}`,
      `${keyColor("msg")}=${msgColor(msg)}`,
    ];
    if (fields) {
      for (const [k, v] of Object.entries(fields)) {
        parts.push(`${keyColor(k)}=${valueColor(formatValue(v))}`);
      }
    }
    stream.write(parts.join(" ") + "\n");
  }
  return {
    info: (msg: string, fields?: Record<string, unknown>) => write("info", msg, fields),
    error: (msg: string, fields?: Record<string, unknown>) => write("error", msg, fields),
  };
}

let logger = createLogger(false, process.stdout, Boolean(process.stdout.isTTY));
let errLogger = createLogger(false, process.stderr, Boolean(process.stderr.isTTY));

function setLoggerJSON(enabled: boolean) {
  const outColor = Boolean(process.stdout.isTTY) && !enabled;
  const errColor = Boolean(process.stderr.isTTY) && !enabled;
  logger = createLogger(enabled, process.stdout, outColor);
  errLogger = createLogger(enabled, process.stderr, errColor);
}

function printUsage(out: NodeJS.WriteStream) {
  out.write("Usage:\n");
  out.write("  bitable-task [--log-json] <command> [flags]\n\n");
  out.write("Commands:\n");
  out.write("  fetch   Fetch tasks from Bitable\n");
  out.write("  update  Update tasks in Bitable\n");
  out.write("  create  Create tasks in Bitable\n\n");
  out.write("Global Flags:\n");
  out.write("  --log-json   Output logs in JSON\n\n");
  out.write("Environment:\n");
  out.write("  FEISHU_APP_ID, FEISHU_APP_SECRET, TASK_BITABLE_URL (required)\n");
  out.write("  FEISHU_BASE_URL (optional, default: https://open.feishu.cn)\n");
  out.write("  TASK_FIELD_* overrides (optional)\n");
}

function parseGlobalFlags(args: string[]) {
  let logJson = false;
  const rest: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--log-json") {
      logJson = true;
      continue;
    }
    if (arg.startsWith("--log-json=")) {
      const raw = arg.split("=", 2)[1] ?? "";
      const lowered = raw.trim().toLowerCase();
      logJson = !(lowered === "false" || lowered === "0" || lowered === "no");
      continue;
    }
    rest.push(arg);
  }
  return { logJson, rest };
}

function parseFlags(args: string[]) {
  const flags: Record<string, string | boolean> = {};
  const positionals: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "-h" || arg === "--help") {
      flags.help = true;
      continue;
    }
    if (arg.startsWith("--")) {
      const raw = arg.slice(2);
      const [name, value] = raw.includes("=") ? raw.split("=", 2) : [raw, ""];
      const key = name.replace(/-/g, "_");
      if (value !== "") {
        flags[key] = value;
        continue;
      }
      if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
        flags[key] = args[i + 1];
        i++;
        continue;
      }
      flags[key] = true;
      continue;
    }
    if (!arg.startsWith("-") || arg === "-") {
      positionals.push(...args.slice(i));
      break;
    }
  }
  return { flags, positionals } as const;
}

function coerceBoolFlags(flags: Record<string, any>, keys: string[]) {
  for (const key of keys) {
    if (!(key in flags)) continue;
    const v = flags[key];
    if (typeof v === "boolean") continue;
    if (typeof v === "string") {
      const lowered = v.trim().toLowerCase();
      flags[key] = !(lowered === "false" || lowered === "0" || lowered === "no");
    } else {
      flags[key] = Boolean(v);
    }
  }
}

type Task = {
  task_id: number;
  biz_task_id: string;
  parent_task_id: string;
  app: string;
  scene: string;
  params: string;
  item_id: string;
  book_id: string;
  url: string;
  user_id: string;
  user_name: string;
  date: string;
  status: string;
  extra: string;
  logs: string;
  last_screenshot: string;
  group_id: string;
  device_serial: string;
  dispatched_device: string;
  dispatched_at: string;
  start_at: string;
  end_at: string;
  elapsed_seconds: string;
  items_collected: string;
  retry_count: string;
  record_id: string;
  raw_fields?: any;
};

function buildFilter(fields: Record<string, string>, app: string, scene: string, status: string, datePreset: string) {
  const conds: any[] = [];
  const add = (fieldKey: string, value: string) => {
    const name = (fields[fieldKey] || "").trim();
    const val = value.trim();
    if (name && val) conds.push({ field_name: name, operator: "is", value: [val] });
  };
  add("App", app);
  add("Scene", scene);
  add("Status", status);
  if (datePreset && datePreset !== "Any") add("Date", datePreset);
  if (!conds.length) return null;
  return { conjunction: "and", conditions: conds };
}

function decodeTask(fieldsRaw: Record<string, any>, mapping: Record<string, string>): [Task, boolean] {
  if (!fieldsRaw || !Object.keys(fieldsRaw).length) return [{} as Task, false];
  const taskID = FieldInt(fieldsRaw, mapping["TaskID"]);
  if (taskID === 0) return [{} as Task, false];
  const get = (name: string) => NormalizeBitableValue(fieldsRaw[mapping[name]]).trim();
  const t: Task = {
    task_id: taskID,
    biz_task_id: get("BizTaskID"),
    parent_task_id: get("ParentTaskID"),
    app: get("App"),
    scene: get("Scene"),
    params: get("Params"),
    item_id: get("ItemID"),
    book_id: get("BookID"),
    url: get("URL"),
    user_id: get("UserID"),
    user_name: get("UserName"),
    date: get("Date"),
    status: get("Status"),
    extra: get("Extra"),
    logs: get("Logs"),
    last_screenshot: get("LastScreenShot"),
    group_id: get("GroupID"),
    device_serial: get("DeviceSerial"),
    dispatched_device: get("DispatchedDevice"),
    dispatched_at: get("DispatchedAt"),
    start_at: get("StartAt"),
    end_at: get("EndAt"),
    elapsed_seconds: get("ElapsedSeconds"),
    items_collected: get("ItemsCollected"),
    retry_count: get("RetryCount"),
    record_id: "",
  };
  if (!t.params && !t.item_id && !t.book_id && !t.url && !t.user_id && !t.user_name) return [{} as Task, false];
  return [t, true];
}

async function FetchTasks(opts: any) {
  const taskURL = (opts.task_url || "").trim();
  if (!taskURL) {
    errLogger.error("TASK_BITABLE_URL is required");
    return 2;
  }
  const appID = Env("FEISHU_APP_ID", "");
  const appSecret = Env("FEISHU_APP_SECRET", "");
  if (!appID || !appSecret) {
    errLogger.error("FEISHU_APP_ID/FEISHU_APP_SECRET are required");
    return 2;
  }
  const baseURL = Env("FEISHU_BASE_URL", DefaultBaseURL);
  let ref: BitableRef;
  try {
    ref = ParseBitableURL(taskURL);
  } catch (err: any) {
    errLogger.error("parse bitable URL failed", { err: err?.message || String(err) });
    return 2;
  }
  const fields = LoadTaskFieldsFromEnv();
  const filterObj = buildFilter(fields, opts.app || "", opts.scene || "", opts.status || "", opts.date || "");
  let token: string;
  try {
    token = await GetTenantAccessToken(baseURL, appID, appSecret);
  } catch (err: any) {
    errLogger.error("get tenant access token failed", { err: err?.message || String(err) });
    return 2;
  }
  if (!ref.AppToken) {
    if (!ref.WikiToken) {
      errLogger.error("bitable URL missing app_token and wiki_token");
      return 2;
    }
    try {
      ref.AppToken = await ResolveWikiAppToken(baseURL, token, ref.WikiToken);
    } catch (err: any) {
      errLogger.error("resolve wiki app token failed", { err: err?.message || String(err) });
      return 2;
    }
  }
  let viewID = (opts.view_id || "").trim();
  if (!viewID) viewID = ref.ViewID;

  let pageSize = ClampPageSize(Number(opts.page_size || 0));
  if (opts.limit && Number(opts.limit) > 0 && Number(opts.limit) < pageSize) pageSize = Number(opts.limit);

  const items: any[] = [];
  let pageToken = "";
  let pages = 0;
  const start = Date.now();

  while (true) {
    const q = new URLSearchParams();
    q.set("page_size", String(pageSize));
    if (pageToken) q.set("page_token", pageToken);
    const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/search?${q.toString()}`;
    let body: any = null;
    if ((!opts.ignore_view && viewID) || filterObj) {
      body = {};
      if (!opts.ignore_view && viewID) body.view_id = viewID;
      if (filterObj) body.filter = filterObj;
    }
    let resp: any;
    try {
      resp = await RequestJSON("POST", urlStr, token, body, true);
    } catch (err: any) {
      errLogger.error("search records request failed", { err: err?.message || String(err) });
      return 2;
    }
    if (resp.code !== 0) {
      errLogger.error("search records failed", { code: resp.code, msg: resp.msg });
      return 2;
    }
    items.push(...(resp.data?.items || []));
    pages++;
    pageToken = String(resp.data?.page_token || "").trim();
    if (opts.limit && items.length >= Number(opts.limit)) {
      items.splice(Number(opts.limit));
      break;
    }
    if (opts.max_pages && pages >= Number(opts.max_pages)) break;
    if (!resp.data?.has_more || !pageToken) break;
  }
  const elapsed = Math.floor(((Date.now() - start) / 1000) * 1000) / 1000;

  const tasks: Task[] = [];
  for (const it of items) {
    const recordID = String(it.record_id || "").trim();
    const fieldsRaw = it.fields || {};
    const [t, ok] = decodeTask(fieldsRaw, fields);
    if (!ok) continue;
    t.record_id = recordID;
    if (opts.raw) t.raw_fields = fieldsRaw;
    tasks.push(t);
  }

  if (opts.jsonl) {
    for (const t of tasks) logger.info("task", { task: t });
    return 0;
  }

  logger.info("tasks", {
    data: {
      tasks,
      count: tasks.length,
      elapsed_seconds: elapsed,
      page_info: { has_more: Boolean(pageToken), next_page_token: pageToken, pages },
    },
  });
  return 0;
}

function parseCSVSet(raw: string) {
  const out: Record<string, boolean> = {};
  for (const part of raw.split(",")) {
    const p = part.trim().toLowerCase();
    if (p) out[p] = true;
  }
  return out;
}

function buildIDFilter(fieldName: string, values: string[]) {
  const name = fieldName.trim();
  if (!name) return null;
  const seen: Record<string, boolean> = {};
  const conds: any[] = [];
  for (const v of values) {
    const val = v.trim();
    if (!val || seen[val]) continue;
    seen[val] = true;
    conds.push({ field_name: name, operator: "is", value: [val] });
  }
  if (!conds.length) return null;
  return { conjunction: "or", conditions: conds };
}

async function searchItems(baseURL: string, token: string, ref: BitableRef, filterObj: any, pageSize: number, ignoreView: boolean, viewID: string) {
  const size = ClampPageSize(pageSize);
  const q = new URLSearchParams();
  q.set("page_size", String(size));
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/search?${q.toString()}`;
  let body: any = null;
  if ((!ignoreView && viewID) || filterObj) {
    body = {};
    if (!ignoreView && viewID) body.view_id = viewID;
    if (filterObj) body.filter = filterObj;
  }
  const resp = await RequestJSON("POST", urlStr, token, body, true);
  if (resp.code !== 0) throw new Error(`search records failed: code=${resp.code} msg=${resp.msg}`);
  return resp.data?.items || [];
}

function extractStatusesFromItems(items: any[], statusField: string) {
  const out: Record<string, string> = {};
  for (const item of items) {
    const recordID = BitableValueToString(item.record_id).trim();
    const fieldsRaw = item.fields || {};
    const status = BitableValueToString(fieldsRaw[statusField]).trim();
    if (recordID && status) out[recordID] = status;
  }
  return out;
}

async function resolveRecordIDsByTaskID(baseURL: string, token: string, ref: BitableRef, fieldsMap: Record<string, string>, taskIDs: number[], ignoreView: boolean, viewID: string) {
  const result: Record<number, string> = {};
  const statuses: Record<string, string> = {};
  const values = taskIDs.filter((id) => id > 0).map((id) => String(id));
  if (!values.length) return { result, statuses };
  const taskField = fieldsMap["TaskID"];
  const statusField = fieldsMap["Status"];
  for (const batch of chunkStrings(values, updateMaxFilterValues)) {
    const filterObj = buildIDFilter(taskField, batch);
    if (!filterObj) continue;
    const items = await searchItems(baseURL, token, ref, filterObj, Math.min(MaxPageSize, Math.max(batch.length, 1)), ignoreView, viewID);
    for (const item of items) {
      const recordID = BitableValueToString(item.record_id).trim();
      const fieldsRaw = item.fields || {};
      const taskID = FieldInt(fieldsRaw, taskField);
      if (recordID && taskID > 0 && !(taskID in result)) result[taskID] = recordID;
    }
    Object.assign(statuses, extractStatusesFromItems(items, statusField));
  }
  return { result, statuses };
}

async function resolveRecordIDsByBizTaskID(baseURL: string, token: string, ref: BitableRef, fieldsMap: Record<string, string>, bizIDs: string[], ignoreView: boolean, viewID: string) {
  const result: Record<string, string> = {};
  const statuses: Record<string, string> = {};
  const values = bizIDs.map((id) => id.trim()).filter(Boolean);
  if (!values.length) return { result, statuses };
  const bizField = fieldsMap["BizTaskID"];
  const statusField = fieldsMap["Status"];
  for (const batch of chunkStrings(values, updateMaxFilterValues)) {
    const filterObj = buildIDFilter(bizField, batch);
    if (!filterObj) continue;
    const items = await searchItems(baseURL, token, ref, filterObj, Math.min(MaxPageSize, Math.max(batch.length, 1)), ignoreView, viewID);
    for (const item of items) {
      const recordID = BitableValueToString(item.record_id).trim();
      const fieldsRaw = item.fields || {};
      const bizID = BitableValueToString(fieldsRaw[bizField]).trim();
      if (recordID && bizID && !(bizID in result)) result[bizID] = recordID;
    }
    Object.assign(statuses, extractStatusesFromItems(items, statusField));
  }
  return { result, statuses };
}

async function fetchRecordStatuses(baseURL: string, token: string, ref: BitableRef, recordIDs: string[], statusField: string) {
  const out: Record<string, string> = {};
  for (const rid of recordIDs.map((r) => r.trim()).filter(Boolean)) {
    if (rid in out) continue;
    const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/${encodeURIComponent(rid)}`;
    const resp = await RequestJSON("GET", urlStr, token, null, true);
    if (resp.code !== 0) throw new Error(`get record failed: code=${resp.code} msg=${resp.msg}`);
    const status = BitableValueToString(resp.data?.record?.fields?.[statusField]).trim();
    if (status) out[rid] = status;
  }
  return out;
}

function hasCdnURL(extra: any) {
  const raw = NormalizeExtra(extra);
  if (!raw.trim()) return false;
  try {
    const payload = JSON.parse(raw);
    const v = payload?.cdn_url;
    return typeof v === "string" && v.trim() !== "";
  } catch {
    return false;
  }
}

function buildUpdateFields(fieldsMap: Record<string, string>, upd: Record<string, any>) {
  const out: Record<string, any> = {};
  const status = BitableValueToString(upd.status).trim();
  if (status && fieldsMap["Status"]) out[fieldsMap["Status"]] = status;
  if (fieldsMap["Date"] && upd.date != null) {
    const [payload, ok] = CoerceDatePayload(upd.date);
    if (ok) out[fieldsMap["Date"]] = payload;
  }
  const deviceSerial = BitableValueToString(upd.device_serial).trim();
  if (deviceSerial && fieldsMap["DispatchedDevice"]) out[fieldsMap["DispatchedDevice"]] = deviceSerial;

  let dispatchedMS: number | null = null;
  if (upd.dispatched_at != null && fieldsMap["DispatchedAt"]) {
    const [ms, ok] = CoerceMillis(upd.dispatched_at);
    if (ok) {
      dispatchedMS = ms;
      out[fieldsMap["DispatchedAt"]] = ms;
    }
  }

  let startMS: number | null = null;
  if (upd.start_at != null && fieldsMap["StartAt"]) {
    const [ms, ok] = CoerceMillis(upd.start_at);
    if (ok) {
      startMS = ms;
      out[fieldsMap["StartAt"]] = ms;
    }
  }
  if (startMS == null && dispatchedMS != null && fieldsMap["StartAt"]) {
    out[fieldsMap["StartAt"]] = dispatchedMS;
    startMS = dispatchedMS;
  }

  let endMS: number | null = null;
  if (upd.completed_at != null) {
    const [ms, ok] = CoerceMillis(upd.completed_at);
    if (ok) endMS = ms;
  }
  if (endMS == null && upd.end_at != null) {
    const [ms, ok] = CoerceMillis(upd.end_at);
    if (ok) endMS = ms;
  }
  if (endMS != null && fieldsMap["EndAt"]) out[fieldsMap["EndAt"]] = endMS;

  let [elapsed, hasElapsed] = CoerceInt(upd.elapsed_seconds);
  if (!hasElapsed && startMS != null && endMS != null) {
    let derived = Math.trunc((endMS - startMS) / 1000);
    if (derived < 0) derived = 0;
    elapsed = derived;
    hasElapsed = true;
  }
  if (hasElapsed && fieldsMap["ElapsedSeconds"]) out[fieldsMap["ElapsedSeconds"]] = elapsed;

  const [itemsCollected, okItems] = CoerceInt(upd.items_collected);
  if (okItems && fieldsMap["ItemsCollected"]) out[fieldsMap["ItemsCollected"]] = itemsCollected;

  const logs = BitableValueToString(upd.logs).trim();
  if (logs && fieldsMap["Logs"]) out[fieldsMap["Logs"]] = logs;

  const [retryCount, okRetry] = CoerceInt(upd.retry_count);
  if (okRetry && fieldsMap["RetryCount"]) out[fieldsMap["RetryCount"]] = retryCount;

  const extra = upd.extra;
  const forceExtra = Boolean(upd.force_extra);
  if (fieldsMap["Extra"] && extra != null) {
    if (forceExtra || (status === "success" && hasCdnURL(extra))) {
      const payload = NormalizeExtra(extra);
      if (payload.trim()) out[fieldsMap["Extra"]] = payload;
    }
  }

  if (upd.fields && typeof upd.fields === "object") {
    for (const [k, v] of Object.entries(upd.fields)) {
      if (k.trim() && v != null) out[k] = v;
    }
  }
  return out;
}

function resolveUpdateRecordID(upd: any, resolvedTask: Record<number, string>, resolvedBiz: Record<string, string>) {
  const recordID = BitableValueToString(upd.record_id).trim();
  if (recordID) return recordID;
  const [taskID, ok] = CoerceInt(upd.task_id);
  if (ok && taskID > 0) return (resolvedTask[taskID] || "").trim();
  const bizID = BitableValueToString(upd.biz_task_id).trim();
  if (bizID) return (resolvedBiz[bizID] || "").trim();
  return "";
}

function parseInputItems(inputPath: string) {
  const raw = readAllInput(inputPath);
  const mode = detectInputFormat(inputPath, raw);
  if (mode === "jsonl") return parseJSONLItems(raw);
  return parseJSONItems(raw);
}

async function UpdateTasks(opts: any) {
  const taskURL = (opts.task_url || "").trim();
  if (!taskURL) {
    errLogger.error("TASK_BITABLE_URL is required");
    return 2;
  }
  const appID = Env("FEISHU_APP_ID", "");
  const appSecret = Env("FEISHU_APP_SECRET", "");
  if (!appID || !appSecret) {
    errLogger.error("FEISHU_APP_ID/FEISHU_APP_SECRET are required");
    return 2;
  }
  const baseURL = Env("FEISHU_BASE_URL", DefaultBaseURL);
  const fieldsMap = LoadTaskFieldsFromEnv();

  let updates: any[] = [];
  if ((opts.input || "").trim()) {
    updates = parseInputItems(opts.input.trim());
  } else {
    updates = [
      {
        task_id: opts.task_id,
        biz_task_id: opts.biz_task_id,
        record_id: opts.record_id,
        status: opts.status,
        device_serial: opts.device_serial,
        dispatched_at: opts.dispatched_at,
        start_at: opts.start_at,
        completed_at: opts.completed_at,
        end_at: opts.end_at,
        elapsed_seconds: opts.elapsed_seconds,
        items_collected: opts.items_collected,
        logs: opts.logs,
        retry_count: opts.retry_count,
        extra: opts.extra,
        date: opts.date,
      },
    ];
  }
  if (!updates.length) {
    errLogger.error("no updates provided");
    return 2;
  }

  let ref: BitableRef;
  try {
    ref = ParseBitableURL(taskURL);
  } catch (err: any) {
    errLogger.error("parse bitable URL failed", { err: err?.message || String(err) });
    return 2;
  }

  let token: string;
  try {
    token = await GetTenantAccessToken(baseURL, appID, appSecret);
  } catch (err: any) {
    errLogger.error("get tenant access token failed", { err: err?.message || String(err) });
    return 2;
  }
  if (!ref.AppToken) {
    if (!ref.WikiToken) {
      errLogger.error("bitable URL missing app_token and wiki_token");
      return 2;
    }
    try {
      ref.AppToken = await ResolveWikiAppToken(baseURL, token, ref.WikiToken);
    } catch (err: any) {
      errLogger.error("resolve wiki app token failed", { err: err?.message || String(err) });
      return 2;
    }
  }

  let viewID = (opts.view_id || "").trim();
  if (!viewID) viewID = ref.ViewID;

  const taskIDsToResolve: number[] = [];
  const bizIDsToResolve: string[] = [];
  for (const upd of updates) {
    const recordID = BitableValueToString(upd.record_id).trim();
    const [taskID, okTask] = CoerceInt(upd.task_id);
    const bizID = BitableValueToString(upd.biz_task_id).trim();
    if (!recordID && okTask && taskID > 0) taskIDsToResolve.push(taskID);
    if (!recordID && !okTask && bizID) bizIDsToResolve.push(bizID);
  }

  let resolvedTask: Record<number, string> = {};
  let resolvedBiz: Record<string, string> = {};
  const statusByRecord: Record<string, string> = {};

  if (taskIDsToResolve.length) {
    try {
      const res = await resolveRecordIDsByTaskID(baseURL, token, ref, fieldsMap, taskIDsToResolve, opts.ignore_view, viewID);
      resolvedTask = res.result;
      Object.assign(statusByRecord, res.statuses);
    } catch (err: any) {
      errLogger.error("resolve record IDs by task id failed", { err: err?.message || String(err) });
      return 2;
    }
  }
  if (bizIDsToResolve.length) {
    try {
      const res = await resolveRecordIDsByBizTaskID(baseURL, token, ref, fieldsMap, bizIDsToResolve, opts.ignore_view, viewID);
      resolvedBiz = res.result;
      Object.assign(statusByRecord, res.statuses);
    } catch (err: any) {
      errLogger.error("resolve record IDs by biz task id failed", { err: err?.message || String(err) });
      return 2;
    }
  }

  const skipStatuses = parseCSVSet(opts.skip_status || "");
  if (Object.keys(skipStatuses).length) {
    const recordIDsNeeded: string[] = [];
    for (const upd of updates) {
      const recordID = resolveUpdateRecordID(upd, resolvedTask, resolvedBiz);
      if (recordID && !(recordID in statusByRecord)) recordIDsNeeded.push(recordID);
    }
    if (recordIDsNeeded.length) {
      try {
        const fetched = await fetchRecordStatuses(baseURL, token, ref, recordIDsNeeded, fieldsMap["Status"]);
        Object.assign(statusByRecord, fetched);
      } catch (err: any) {
        errLogger.error("fetch record statuses failed", { err: err?.message || String(err) });
        return 2;
      }
    }
  }

  const records: { record_id: string; fields: any }[] = [];
  const errorsList: string[] = [];
  let skipped = 0;

  for (const upd of updates) {
    const recordID = resolveUpdateRecordID(upd, resolvedTask, resolvedBiz);
    if (!recordID) {
      errorsList.push("missing record_id for update");
      continue;
    }
    if (Object.keys(skipStatuses).length) {
      const cur = (statusByRecord[recordID] || "").trim().toLowerCase();
      if (cur && skipStatuses[cur]) {
        skipped++;
        continue;
      }
    }
    const fields = buildUpdateFields(fieldsMap, upd);
    if (!Object.keys(fields).length) {
      errorsList.push(`record ${recordID}: no fields to update`);
      continue;
    }
    records.push({ record_id: recordID, fields });
  }

  const start = Date.now();
  let updated = 0;
  if (records.length) {
    if (records.length === 1) {
      try {
        await updateRecord(baseURL, token, ref, records[0].record_id, records[0].fields);
        updated = 1;
      } catch (err: any) {
        errorsList.push(err?.message || String(err));
      }
    } else {
      for (let i = 0; i < records.length; i += updateMaxBatchSize) {
        const batch = records.slice(i, i + updateMaxBatchSize).map((r) => ({ record_id: r.record_id, fields: r.fields }));
        try {
          await batchUpdateRecords(baseURL, token, ref, batch);
          updated += batch.length;
        } catch (err: any) {
          errorsList.push(err?.message || String(err));
          break;
        }
      }
    }
  }

  const elapsed = Math.floor(((Date.now() - start) / 1000) * 1000) / 1000;
  logger.info("result", {
    data: {
      updated,
      requested: records.length,
      skipped,
      failed: errorsList.length,
      errors: errorsList,
      elapsed_seconds: elapsed,
    },
  });
  return errorsList.length ? 1 : 0;
}

async function updateRecord(baseURL: string, token: string, ref: BitableRef, recordID: string, fields: any) {
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/${encodeURIComponent(recordID)}`;
  const payload = { fields };
  const resp = await RequestJSON("PUT", urlStr, token, payload, true);
  if (resp.code !== 0) throw new Error(`update record failed: code=${resp.code} msg=${resp.msg}`);
}

async function batchUpdateRecords(baseURL: string, token: string, ref: BitableRef, records: any[]) {
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/batch_update`;
  const payload = { records };
  const resp = await RequestJSON("POST", urlStr, token, payload, true);
  if (resp.code !== 0) throw new Error(`batch update failed: code=${resp.code} msg=${resp.msg}`);
}

function normalizeSkipFields(raw: string) {
  const cleaned = raw.trim();
  if (!cleaned) return [] as string[];
  const parts = cleaned.split(",").map((p) => p.trim()).filter(Boolean);
  const aliases: Record<string, string> = {
    task_id: "TaskID",
    taskid: "TaskID",
    biz_task_id: "BizTaskID",
    biztaskid: "BizTaskID",
    record_id: "RecordID",
    recordid: "RecordID",
    book_id: "BookID",
    bookid: "BookID",
    user_id: "UserID",
    userid: "UserID",
    app: "App",
    scene: "Scene",
  };
  const seen: Record<string, boolean> = {};
  const out: string[] = [];
  for (const p of parts) {
    const key = aliases[p.toLowerCase()] || p;
    if (!key || seen[key]) continue;
    seen[key] = true;
    out.push(key);
  }
  return out;
}

function extractItemValue(item: any, fieldName: string) {
  switch (fieldName) {
    case "TaskID": {
      const [id, ok] = CoerceInt(item.task_id);
      return ok && id > 0 ? String(id) : "";
    }
    case "BizTaskID":
      return BitableValueToString(item.biz_task_id).trim();
    case "RecordID":
      return BitableValueToString(item.record_id).trim();
    case "BookID":
      return BitableValueToString(item.book_id).trim();
    case "UserID":
      return BitableValueToString(item.user_id).trim();
    case "App":
      return BitableValueToString(item.app).trim();
    case "Scene":
      return BitableValueToString(item.scene).trim();
    default:
      return BitableValueToString(item[fieldName]).trim();
  }
}

async function CreateTasks(opts: any) {
  const taskURL = (opts.task_url || "").trim();
  if (!taskURL) {
    errLogger.error("TASK_BITABLE_URL is required");
    return 2;
  }
  const appID = Env("FEISHU_APP_ID", "");
  const appSecret = Env("FEISHU_APP_SECRET", "");
  if (!appID || !appSecret) {
    errLogger.error("FEISHU_APP_ID/FEISHU_APP_SECRET are required");
    return 2;
  }
  const baseURL = Env("FEISHU_BASE_URL", DefaultBaseURL);
  const fieldsMap = LoadTaskFieldsFromEnv();

  let creates: any[] = [];
  if ((opts.input || "").trim()) {
    creates = parseInputItems(opts.input.trim());
  } else {
    creates = [
      {
        biz_task_id: opts.biz_task_id,
        parent_task_id: opts.parent_task_id,
        app: opts.app,
        scene: opts.scene,
        params: opts.params,
        item_id: opts.item_id,
        book_id: opts.book_id,
        url: opts.url,
        user_id: opts.user_id,
        user_name: opts.user_name,
        date: opts.date,
        status: opts.status,
        device_serial: opts.device_serial,
        dispatched_device: opts.dispatched_device,
        dispatched_at: opts.dispatched_at,
        start_at: opts.start_at,
        completed_at: opts.completed_at,
        end_at: opts.end_at,
        elapsed_seconds: opts.elapsed_seconds,
        items_collected: opts.items_collected,
        logs: opts.logs,
        retry_count: opts.retry_count,
        last_screenshot: opts.last_screenshot,
        group_id: opts.group_id,
        extra: opts.extra,
        record_id: "",
      },
    ];
  }
  if (!creates.length) {
    errLogger.error("no tasks provided");
    return 2;
  }

  let ref: BitableRef;
  try {
    ref = ParseBitableURL(taskURL);
  } catch (err: any) {
    errLogger.error("parse bitable URL failed", { err: err?.message || String(err) });
    return 2;
  }
  let token: string;
  try {
    token = await GetTenantAccessToken(baseURL, appID, appSecret);
  } catch (err: any) {
    errLogger.error("get tenant access token failed", { err: err?.message || String(err) });
    return 2;
  }
  if (!ref.AppToken) {
    if (!ref.WikiToken) {
      errLogger.error("bitable URL missing app_token and wiki_token");
      return 2;
    }
    try {
      ref.AppToken = await ResolveWikiAppToken(baseURL, token, ref.WikiToken);
    } catch (err: any) {
      errLogger.error("resolve wiki app token failed", { err: err?.message || String(err) });
      return 2;
    }
  }

  const skipFields = normalizeSkipFields(opts.skip_existing || "");
  const existingByField: Record<string, Record<string, string>> = {};
  const existingRecordIDs: Record<string, boolean> = {};

  if (skipFields.length) {
    const fieldMap: Record<string, string> = {};
    for (const f of skipFields) {
      if (f === "RecordID") continue;
      fieldMap[f] = fieldsMap[f] || f;
    }
    for (const item of creates) {
      for (const f of skipFields) {
        if (f === "RecordID") {
          const rid = BitableValueToString(item.record_id).trim();
          if (rid && !existingRecordIDs[rid]) {
            if (await recordExists(baseURL, token, ref, rid)) existingRecordIDs[rid] = true;
          }
          continue;
        }
        const val = extractItemValue(item, f);
        if (!val) continue;
        if (!existingByField[f]) existingByField[f] = {};
        existingByField[f][val] = "";
      }
    }
    for (const [f, valuesMap] of Object.entries(existingByField)) {
      const values = Object.keys(valuesMap);
      const mappedField = fieldMap[f];
      const resolved = await resolveExistingByField(baseURL, token, ref, mappedField, values);
      existingByField[f] = resolved;
    }
  }

  const records: { fields: any }[] = [];
  const errorsList: string[] = [];
  let skipped = 0;

  for (const item of creates) {
    if (skipFields.length) {
      let allMatch = true;
      for (const f of skipFields) {
        if (f === "RecordID") {
          const rid = BitableValueToString(item.record_id).trim();
          if (!rid || !existingRecordIDs[rid]) {
            allMatch = false;
            break;
          }
          continue;
        }
        const val = extractItemValue(item, f);
        if (!val || !existingByField[f]?.[val]) {
          allMatch = false;
          break;
        }
      }
      if (allMatch) {
        skipped++;
        continue;
      }
    }
    const fields = buildCreateFields(fieldsMap, item);
    if (!Object.keys(fields).length) {
      errorsList.push("task: no fields to create");
      continue;
    }
    records.push({ fields });
  }

  const start = Date.now();
  let created = 0;
  if (records.length) {
    if (records.length === 1) {
      try {
        await createRecord(baseURL, token, ref, records[0].fields);
        created = 1;
      } catch (err: any) {
        errorsList.push(err?.message || String(err));
      }
    } else {
      for (let i = 0; i < records.length; i += createMaxBatchSize) {
        const batch = records.slice(i, i + createMaxBatchSize).map((r) => ({ fields: r.fields }));
        try {
          await batchCreateRecords(baseURL, token, ref, batch);
          created += batch.length;
        } catch (err: any) {
          errorsList.push(err?.message || String(err));
          break;
        }
      }
    }
  }

  const elapsed = Math.floor(((Date.now() - start) / 1000) * 1000) / 1000;
  logger.info("result", {
    data: {
      created,
      requested: records.length,
      skipped,
      failed: errorsList.length,
      errors: errorsList,
      elapsed_seconds: elapsed,
    },
  });
  return errorsList.length ? 1 : 0;
}

function buildCreateFields(fieldsMap: Record<string, string>, item: any) {
  const out: Record<string, any> = {};
  const setStr = (jsonKey: string, colKey: string) => {
    const v = BitableValueToString(item[jsonKey]).trim();
    if (!v) return;
    const col = (fieldsMap[colKey] || "").trim();
    if (!col) return;
    out[col] = v;
  };
  setStr("biz_task_id", "BizTaskID");
  setStr("parent_task_id", "ParentTaskID");

  const appValue = BitableValueToString(item.app).trim();
  const sceneValue = BitableValueToString(item.scene).trim();
  const paramsValue = BitableValueToString(item.params).trim();
  const itemIDValue = BitableValueToString(item.item_id).trim();
  const bookIDValue = BitableValueToString(item.book_id).trim();
  const urlValue = BitableValueToString(item.url).trim();
  const userIDValue = BitableValueToString(item.user_id).trim();
  const userNameValue = BitableValueToString(item.user_name).trim();
  const statusValue = BitableValueToString(item.status).trim();
  const logsValue = BitableValueToString(item.logs).trim();
  const lastScreenshotValue = BitableValueToString(item.last_screenshot).trim();
  const groupIDValue = BitableValueToString(item.group_id).trim();

  const kvs: Array<[string, string]> = [
    ["App", appValue],
    ["Scene", sceneValue],
    ["Params", paramsValue],
    ["ItemID", itemIDValue],
    ["BookID", bookIDValue],
    ["URL", urlValue],
    ["UserID", userIDValue],
    ["UserName", userNameValue],
    ["Status", statusValue],
    ["Logs", logsValue],
    ["LastScreenShot", lastScreenshotValue],
    ["GroupID", groupIDValue],
  ];
  for (const [field, value] of kvs) {
    if (!value) continue;
    const col = (fieldsMap[field] || "").trim();
    if (col) out[col] = value;
  }

  if (!groupIDValue && appValue && bookIDValue && userIDValue && (fieldsMap["GroupID"] || "").trim()) {
    const label = appGroupLabels[appValue] || appValue;
    out[fieldsMap["GroupID"]] = `${label}_${bookIDValue}_${userIDValue}`;
  }

  if (fieldsMap["Date"] && item.date != null) {
    const [payload, ok] = CoerceDatePayload(item.date);
    if (ok) out[fieldsMap["Date"]] = payload;
  }

  const deviceSerial = BitableValueToString(item.device_serial).trim();
  if (deviceSerial && fieldsMap["DeviceSerial"]) out[fieldsMap["DeviceSerial"]] = deviceSerial;

  let dispatchedDevice = BitableValueToString(item.dispatched_device).trim();
  if (!dispatchedDevice) dispatchedDevice = deviceSerial;
  if (dispatchedDevice && fieldsMap["DispatchedDevice"]) out[fieldsMap["DispatchedDevice"]] = dispatchedDevice;

  let dispatchedMS: number | null = null;
  if (item.dispatched_at != null && fieldsMap["DispatchedAt"]) {
    const [ms, ok] = CoerceMillis(item.dispatched_at);
    if (ok) {
      dispatchedMS = ms;
      out[fieldsMap["DispatchedAt"]] = ms;
    }
  }

  let startMS: number | null = null;
  if (item.start_at != null && fieldsMap["StartAt"]) {
    const [ms, ok] = CoerceMillis(item.start_at);
    if (ok) {
      startMS = ms;
      out[fieldsMap["StartAt"]] = ms;
    }
  }
  if (startMS == null && dispatchedMS != null && fieldsMap["StartAt"]) {
    out[fieldsMap["StartAt"]] = dispatchedMS;
    startMS = dispatchedMS;
  }

  let endMS: number | null = null;
  if (item.completed_at != null) {
    const [ms, ok] = CoerceMillis(item.completed_at);
    if (ok) endMS = ms;
  }
  if (endMS == null && item.end_at != null) {
    const [ms, ok] = CoerceMillis(item.end_at);
    if (ok) endMS = ms;
  }
  if (endMS != null && fieldsMap["EndAt"]) out[fieldsMap["EndAt"]] = endMS;

  let [elapsed, hasElapsed] = CoerceInt(item.elapsed_seconds);
  if (!hasElapsed && startMS != null && endMS != null) {
    let derived = Math.trunc((endMS - startMS) / 1000);
    if (derived < 0) derived = 0;
    elapsed = derived;
    hasElapsed = true;
  }
  if (hasElapsed && fieldsMap["ElapsedSeconds"]) out[fieldsMap["ElapsedSeconds"]] = elapsed;

  const [itemsCollected, okItems] = CoerceInt(item.items_collected);
  if (okItems && fieldsMap["ItemsCollected"]) out[fieldsMap["ItemsCollected"]] = itemsCollected;

  const [retryCount, okRetry] = CoerceInt(item.retry_count);
  if (okRetry && fieldsMap["RetryCount"]) out[fieldsMap["RetryCount"]] = retryCount;

  const extra = item.extra;
  const forceExtra = Boolean(item.force_extra);
  if (fieldsMap["Extra"] && extra != null) {
    const payload = NormalizeExtra(extra);
    if (payload.trim() || forceExtra) out[fieldsMap["Extra"]] = payload;
  }

  if (item.fields && typeof item.fields === "object") {
    for (const [k, v] of Object.entries(item.fields)) {
      if (k.trim() && v != null) out[k] = v;
    }
  }
  return out;
}

async function batchCreateRecords(baseURL: string, token: string, ref: BitableRef, records: any[]) {
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/batch_create`;
  const payload = { records };
  const resp = await RequestJSON("POST", urlStr, token, payload, true);
  if (resp.code !== 0) throw new Error(`batch create failed: code=${resp.code} msg=${resp.msg}`);
}

async function createRecord(baseURL: string, token: string, ref: BitableRef, fields: any) {
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records`;
  const payload = { fields };
  const resp = await RequestJSON("POST", urlStr, token, payload, true);
  if (resp.code !== 0) throw new Error(`create record failed: code=${resp.code} msg=${resp.msg}`);
}

async function resolveExistingByField(baseURL: string, token: string, ref: BitableRef, fieldName: string, values: string[]) {
  const out: Record<string, string> = {};
  if (!values.length) return out;
  for (const batch of chunkStrings(values, createMaxFilterValues)) {
    const filterObj = buildIDFilter(fieldName, batch);
    if (!filterObj) continue;
    const items = await fetchRecordsForCreate(baseURL, token, ref, filterObj, Math.min(MaxPageSize, Math.max(batch.length, 1)));
    for (const item of items) {
      const recordID = BitableValueToString(item.record_id).trim();
      const fieldsRaw = item.fields || {};
      const val = BitableValueToString(fieldsRaw[fieldName]).trim();
      if (recordID && val && !(val in out)) out[val] = recordID;
    }
  }
  return out;
}

async function fetchRecordsForCreate(baseURL: string, token: string, ref: BitableRef, filterObj: any, pageSize: number) {
  const size = ClampPageSize(pageSize);
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/search?page_size=${size}`;
  const body = filterObj ? { filter: filterObj } : null;
  const resp = await RequestJSON("POST", urlStr, token, body, true);
  if (resp.code !== 0) throw new Error(`search records failed: code=${resp.code} msg=${resp.msg}`);
  return resp.data?.items || [];
}

async function recordExists(baseURL: string, token: string, ref: BitableRef, recordID: string) {
  const rid = recordID.trim();
  if (!rid) return false;
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/${encodeURIComponent(rid)}`;
  try {
    const resp = await RequestJSON("GET", urlStr, token, null, true);
    return resp.code === 0;
  } catch {
    return false;
  }
}

function chunkStrings(values: string[], size: number) {
  if (size <= 0) return [values];
  const out: string[][] = [];
  for (let i = 0; i < values.length; i += size) out.push(values.slice(i, i + size));
  return out;
}

async function main() {
  const argv = process.argv.slice(2);
  const global = parseGlobalFlags(argv);
  setLoggerJSON(global.logJson);
  const args = global.rest;
  if (args.length === 0 || args[0] === "help" || args[0] === "-h" || args[0] === "--help") {
    printUsage(process.stdout);
    process.exit(0);
  }
  const cmd = args[0];
  const parsed = parseFlags(args.slice(1));
  const flags = parsed.flags as any;
  coerceBoolFlags(flags, ["ignore_view", "use_view", "jsonl", "raw"]);

  if (flags.help) {
    if (cmd === "fetch") {
      process.stdout.write("Usage:\n  bitable-task fetch [flags]\n\n");
      process.stdout.write("Flags:\n");
      process.stdout.write("  --task-url <url>        Bitable task table URL\n");
      process.stdout.write("  --app <value>           App value for filter (required)\n");
      process.stdout.write("  --scene <value>         Scene value for filter (required)\n");
      process.stdout.write("  --status <value>        Task status filter (default: pending)\n");
      process.stdout.write("  --date <value>          Date preset: Today/Yesterday/Any\n");
      process.stdout.write("  --limit <n>             Max tasks to return (0 = no cap)\n");
      process.stdout.write("  --page-size <n>         Page size (max 500)\n");
      process.stdout.write("  --max-pages <n>         Max pages to fetch (0 = no cap)\n");
      process.stdout.write("  --ignore-view           Ignore view_id when searching (default: true)\n");
      process.stdout.write("  --use-view              Use view_id from URL\n");
      process.stdout.write("  --view-id <id>          Override view_id when searching\n");
      process.stdout.write("  --jsonl                 Output JSONL (one task per line)\n");
      process.stdout.write("  --raw                   Include raw fields in output\n");
      process.exit(0);
    }
    if (cmd === "update") {
      process.stdout.write("Usage:\n  bitable-task update [flags]\n\n");
      process.stdout.write("Flags:\n");
      process.stdout.write("  --task-url <url>        Bitable task table URL\n");
      process.stdout.write("  --input <path>          Input JSON/JSONL file (use - for stdin)\n");
      process.stdout.write("  --task-id <id>          Single task id to update\n");
      process.stdout.write("  --biz-task-id <id>      Single biz task id to update\n");
      process.stdout.write("  --record-id <id>        Single record id to update\n");
      process.stdout.write("  --status <value>        Status to set\n");
      process.stdout.write("  --date <value>          Date to set (string or epoch/ISO)\n");
      process.stdout.write("  --device-serial <val>   Dispatched device serial\n");
      process.stdout.write("  --dispatched-at <val>   Dispatch time (ms/seconds/ISO/now)\n");
      process.stdout.write("  --start-at <val>        Start time (ms/seconds/ISO)\n");
      process.stdout.write("  --completed-at <val>    Completion time (ms/seconds/ISO)\n");
      process.stdout.write("  --end-at <val>          End time (ms/seconds/ISO)\n");
      process.stdout.write("  --elapsed-seconds <val> Elapsed seconds (int)\n");
      process.stdout.write("  --items-collected <val> Items collected (int)\n");
      process.stdout.write("  --logs <val>            Logs path or identifier\n");
      process.stdout.write("  --retry-count <val>     Retry count (int)\n");
      process.stdout.write("  --extra <json>          Extra JSON string\n");
      process.stdout.write("  --skip-status <csv>     Skip updates when current status matches\n");
      process.stdout.write("  --ignore-view           Ignore view_id when searching (default: true)\n");
      process.stdout.write("  --use-view              Use view_id from URL\n");
      process.stdout.write("  --view-id <id>          Override view_id when searching\n");
      process.exit(0);
    }
    if (cmd === "create") {
      process.stdout.write("Usage:\n  bitable-task create [flags]\n\n");
      process.stdout.write("Flags:\n");
      process.stdout.write("  --task-url <url>        Bitable task table URL\n");
      process.stdout.write("  --input <path>          Input JSON/JSONL file (use - for stdin)\n");
      process.stdout.write("  --biz-task-id <id>      Biz task id to create\n");
      process.stdout.write("  --parent-task-id <id>   Parent task id\n");
      process.stdout.write("  --app <value>           App value\n");
      process.stdout.write("  --scene <value>         Scene value\n");
      process.stdout.write("  --params <value>        Task params\n");
      process.stdout.write("  --item-id <value>       Item id\n");
      process.stdout.write("  --book-id <value>       Book id\n");
      process.stdout.write("  --url <value>           URL\n");
      process.stdout.write("  --user-id <value>       User id\n");
      process.stdout.write("  --user-name <value>     User name\n");
      process.stdout.write("  --date <value>          Date value (string or epoch/ISO)\n");
      process.stdout.write("  --status <value>        Status\n");
      process.stdout.write("  --device-serial <val>   Dispatched device serial\n");
      process.stdout.write("  --dispatched-device <v> Dispatched device (override device-serial)\n");
      process.stdout.write("  --dispatched-at <val>   Dispatch time (ms/seconds/ISO/now)\n");
      process.stdout.write("  --start-at <val>        Start time (ms/seconds/ISO)\n");
      process.stdout.write("  --completed-at <val>    Completion time (ms/seconds/ISO)\n");
      process.stdout.write("  --end-at <val>          End time (ms/seconds/ISO)\n");
      process.stdout.write("  --elapsed-seconds <val> Elapsed seconds (int)\n");
      process.stdout.write("  --items-collected <val> Items collected (int)\n");
      process.stdout.write("  --logs <val>            Logs path or identifier\n");
      process.stdout.write("  --retry-count <val>     Retry count (int)\n");
      process.stdout.write("  --last-screenshot <val> Last screenshot reference\n");
      process.stdout.write("  --group-id <val>        Group id\n");
      process.stdout.write("  --extra <json>          Extra JSON string\n");
      process.stdout.write("  --skip-existing <csv>   Skip create when existing records match fields\n");
      process.exit(0);
    }
  }

  if (cmd === "fetch") {
    const opts: any = {
      task_url: process.env.TASK_BITABLE_URL || "",
      status: "pending",
      date: "Today",
      page_size: 200,
      ignore_view: true,
    };
    Object.assign(opts, flags);
    if (flags.use_view) opts.ignore_view = false;
    if (!opts.app || !opts.scene) {
      errLogger.error("--app and --scene are required");
      process.exit(2);
    }
    process.exit(await FetchTasks(opts));
  }

  if (cmd === "update") {
    const opts: any = {
      task_url: process.env.TASK_BITABLE_URL || "",
      ignore_view: true,
    };
    Object.assign(opts, flags);
    if (flags.use_view) opts.ignore_view = false;
    process.exit(await UpdateTasks(opts));
  }

  if (cmd === "create") {
    const opts: any = {
      task_url: process.env.TASK_BITABLE_URL || "",
    };
    Object.assign(opts, flags);
    process.exit(await CreateTasks(opts));
  }

  errLogger.error("unknown command", { command: cmd });
  printUsage(process.stdout);
  process.exit(2);
}

main().catch((err) => {
  errLogger.error("unhandled error", { err: err?.message || String(err) });
  process.exit(1);
});
