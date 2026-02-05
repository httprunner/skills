#!/usr/bin/env node
import {
  BitableRef,
  ClampPageSize,
  DefaultBaseURL,
  Env,
  GetTenantAccessToken,
  LoadTaskFieldsFromEnv,
  CoerceDatePayload,
  NormalizeBitableValue,
  NormalizeExtra,
  ParseBitableURL,
  RequestJSON,
  ResolveWikiAppToken,
  readAllInput,
  detectInputFormat,
  parseJSONItems,
  parseJSONLItems,
} from "./bitable_common";
import fs from "node:fs";
import chalk from "chalk";
import { Command } from "commander";

const createMaxBatchSize = 500;

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

function getGlobalOptions(program: Command) {
  const opts = program.opts<{ logJson?: boolean }>();
  setLoggerJSON(Boolean(opts.logJson));
  return { logJson: Boolean(opts.logJson) };
}

type SourceFieldMap = {
  bid: string;
  title: string;
  rightsScene: string;
  actor: string;
  paidTitle: string;
};

type SourceItem = {
  bid: string;
  title: string;
  rightsScene: string;
  actor: string;
  paidTitle: string;
};

function normalizeSourceFieldMap(opts: any): SourceFieldMap {
  return {
    bid: (opts.bid_field || Env("SOURCE_FIELD_BID", "BID")).trim() || "BID",
    title: (opts.title_field || Env("SOURCE_FIELD_TITLE", "短剧名")).trim() || "短剧名",
    rightsScene: (opts.scene_field || Env("SOURCE_FIELD_RIGHTS_SCENE", "维权场景")).trim() || "维权场景",
    actor: (opts.actor_field || Env("SOURCE_FIELD_ACTOR", "主角名")).trim() || "主角名",
    paidTitle: (opts.paid_field || Env("SOURCE_FIELD_PAID_TITLE", "付费剧名")).trim() || "付费剧名",
  };
}

function extractSourceItem(fieldsRaw: Record<string, any>, map: SourceFieldMap): SourceItem {
  const get = (name: string) => NormalizeBitableValue(fieldsRaw[name]).trim();
  return {
    bid: get(map.bid),
    title: get(map.title),
    rightsScene: get(map.rightsScene),
    actor: get(map.actor),
    paidTitle: get(map.paidTitle),
  };
}

function validSourceItem(item: SourceItem) {
  if (!item.bid || item.bid === "暂无") return false;
  if (!item.title) return false;
  if (!item.rightsScene) return false;
  return true;
}

function normalizeActor(actor: string) {
  if (!actor) return "";
  const replaced = actor.replace(/[，,]+/g, " ");
  return replaced.replace(/\s+/g, " ").trim();
}

function normalizeTitleForCompare(value: string) {
  if (!value) return "";
  return value.replace(/[\p{P}\p{S}\s]+/gu, "").trim();
}

function buildParamsListPayload(src: SourceItem) {
  const values: string[] = [];
  const seen = new Set<string>();
  const push = (value: string, compareKey?: string) => {
    const v = value.trim();
    if (!v) return;
    const key = (compareKey || v).trim();
    if (seen.has(key)) return;
    seen.add(key);
    values.push(v);
  };
  const title = src.title.trim();
  const titleKey = normalizeTitleForCompare(title);
  if (title) push(title, titleKey || title);
  const actor = normalizeActor(src.actor);
  if (actor) push(actor, actor);
  const paidTitle = src.paidTitle.trim();
  const paidKey = normalizeTitleForCompare(paidTitle);
  if (paidTitle && paidKey && paidKey !== titleKey) push(paidTitle, paidKey);
  return JSON.stringify(values);
}

async function resolveBitableRef(baseURL: string, token: string, ref: BitableRef) {
  if (ref.AppToken) return ref;
  if (!ref.WikiToken) throw new Error("bitable URL missing app_token and wiki_token");
  ref.AppToken = await ResolveWikiAppToken(baseURL, token, ref.WikiToken);
  return ref;
}

async function fetchAllRecords(baseURL: string, token: string, ref: BitableRef, pageSize: number, viewID: string, useView: boolean, filterObj?: any, limit?: number) {
  const items: any[] = [];
  let pageToken = "";
  const size = ClampPageSize(pageSize);
  while (true) {
    const q = new URLSearchParams();
    q.set("page_size", String(size));
    if (pageToken) q.set("page_token", pageToken);
    const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/search?${q.toString()}`;
    const body: any = {};
    if (useView && viewID) body.view_id = viewID;
    if (filterObj) body.filter = filterObj;
    const resp = await RequestJSON("POST", urlStr, token, Object.keys(body).length ? body : null, true);
    if (resp.code !== 0) throw new Error(`search records failed: code=${resp.code} msg=${resp.msg}`);
    const batch = resp.data?.items || [];
    items.push(...batch);
    if (limit && items.length >= limit) return items.slice(0, limit);
    if (!resp.data?.has_more) break;
    pageToken = resp.data?.page_token || "";
    if (!pageToken) break;
  }
  return items;
}

function resolveFilterDate(preset: string) {
  const trimmed = (preset || "").trim();
  if (!trimmed) return "";
  if (trimmed === "Today") return "Today";
  if (trimmed === "Yesterday") return "Yesterday";
  if (trimmed === "Any") return "";
  return trimmed;
}

function resolveFilterDayForCompare(preset: string) {
  const trimmed = (preset || "").trim();
  if (!trimmed) return "";
  if (trimmed === "Today") return todayDateString();
  if (trimmed === "Yesterday") {
    const now = new Date();
    const d = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1, 0, 0, 0, 0);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }
  if (trimmed === "Any") return "";
  return trimmed;
}

function resolveExactDateMs(day: string) {
  const trimmed = (day || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return "";
  const ms = Date.parse(`${trimmed}T00:00:00`);
  if (Number.isNaN(ms)) return "";
  return String(ms);
}

function buildTaskFilter(fieldsMap: Record<string, string>, app: string, scene: string, datePreset: string) {
  const conds: any[] = [];
  const add = (fieldKey: string, value: string) => {
    const name = (fieldsMap[fieldKey] || "").trim();
    const val = value.trim();
    if (name && val) conds.push({ field_name: name, operator: "is", value: [val] });
  };
  add("App", app);
  add("Scene", scene);
  const dateField = (fieldsMap["Date"] || "").trim();
  if (dateField) {
    const preset = resolveFilterDate(datePreset);
    if (preset) {
      if (/^\d{4}-\d{2}-\d{2}$/.test(preset)) {
        const ms = resolveExactDateMs(preset);
        if (ms) {
          conds.push({ field_name: dateField, operator: "is", value: ["ExactDate", ms] });
        } else {
          conds.push({ field_name: dateField, operator: "is", value: [preset] });
        }
      } else {
        conds.push({ field_name: dateField, operator: "is", value: [preset] });
      }
    }
  }
  if (!conds.length) return null;
  return { conjunction: "and", conditions: conds };
}

function parseInputItems(pathStr: string) {
  const raw = readAllInput(pathStr);
  const format = detectInputFormat(pathStr, raw);
  if (format === "jsonl") return parseJSONLItems(raw);
  return parseJSONItems(raw);
}

function simplifyValue(value: any): any {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) {
    if (value.length === 0) return "";
    if (value.every((it) => it && typeof it === "object" && "text" in it)) {
      const texts = value
        .map((it) => (typeof (it as any).text === "string" ? (it as any).text.trim() : ""))
        .filter((s) => s);
      return texts.join(",");
    }
    if (value.every((it) => it && typeof it === "object" && "link" in it)) {
      const links = value
        .map((it) => (typeof (it as any).link === "string" ? (it as any).link.trim() : ""))
        .filter((s) => s);
      if (links.length) return links.join(",");
    }
    const parts = value.map((it) => simplifyValue(it)).filter((s) => s !== "");
    if (parts.length === 0) return "";
    if (parts.every((it) => typeof it === "string")) return (parts as string[]).join(",");
    return parts;
  }
  if (typeof value === "object") {
    if ("text" in value && typeof (value as any).text === "string") return (value as any).text.trim();
    if ("link" in value && typeof (value as any).link === "string") return (value as any).link.trim();
    if ("value" in value) return simplifyValue((value as any).value);
  }
  return NormalizeBitableValue(value);
}

function simplifyFields(fieldsRaw: Record<string, any>) {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(fieldsRaw)) {
    out[k] = simplifyValue(v);
  }
  return out;
}

function todayDateString() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function normalizeDateToYMD(value: any): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") {
    const ms = value < 100000000000 ? value * 1000 : value;
    const dt = new Date(ms);
    if (Number.isNaN(dt.getTime())) return "";
    return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")}`;
  }
  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return "";
    if (/^\d+$/.test(s)) {
      const n = Number(s);
      if (!Number.isFinite(n)) return "";
      const ms = n < 100000000000 ? n * 1000 : n;
      const dt = new Date(ms);
      if (Number.isNaN(dt.getTime())) return "";
      return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")}`;
    }
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
    const dt = new Date(s);
    if (!Number.isNaN(dt.getTime())) {
      return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")}`;
    }
    return "";
  }
  const asStr = NormalizeBitableValue(value).trim();
  if (!asStr) return "";
  return normalizeDateToYMD(asStr);
}

function buildTaskFields(fieldsMap: Record<string, string>, item: { app: string; scene: string; date: string; status: string; extra: string; book_id: string; params: string }) {
  const out: Record<string, any> = {};
  const set = (key: string, value: string) => {
    const col = (fieldsMap[key] || "").trim();
    if (!col || !value) return;
    out[col] = value;
  };
  set("App", item.app);
  set("Scene", item.scene);
  if (fieldsMap["Date"] && item.date != null) {
    const [payload, ok] = CoerceDatePayload(item.date);
    if (ok) out[fieldsMap["Date"]] = payload;
  }
  set("Status", item.status);
  set("BookID", item.book_id);
  set("Params", item.params);
  const extraVal = NormalizeExtra(item.extra);
  if (extraVal && (fieldsMap["Extra"] || "").trim()) out[fieldsMap["Extra"]] = extraVal;
  return out;
}

async function batchCreate(baseURL: string, token: string, ref: BitableRef, records: Array<{ fields: any }>) {
  const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/batch_create`;
  const payload = { records };
  const resp = await RequestJSON("POST", urlStr, token, payload, true);
  if (resp.code !== 0) throw new Error(`batch create failed: code=${resp.code} msg=${resp.msg}`);
}

function deriveTasksFromSource(items: Array<Record<string, any>>, sourceFieldMap: SourceFieldMap, opts: any) {
  const derived: Array<{ app: string; scene: string; date: string; status: string; extra: string; book_id: string; params: string }> = [];
  let filtered = 0;
  const useParamsList = Boolean(opts.paramsList);
  for (const fieldsRaw of items) {
    const src = extractSourceItem(fieldsRaw, sourceFieldMap);
    if (!validSourceItem(src)) continue;
    filtered++;
    if (useParamsList) {
      derived.push({
        app: opts.app,
        scene: opts.scene,
        date: opts.date,
        status: opts.status,
        extra: opts.extra,
        book_id: src.bid,
        params: buildParamsListPayload(src),
      });
      continue;
    }
    derived.push({
      app: opts.app,
      scene: opts.scene,
      date: opts.date,
      status: opts.status,
      extra: opts.extra,
      book_id: src.bid,
      params: src.title,
    });
    const actor = normalizeActor(src.actor);
    if (actor) {
      derived.push({
        app: opts.app,
        scene: opts.scene,
        date: opts.date,
        status: opts.status,
        extra: opts.extra,
        book_id: src.bid,
        params: actor,
      });
    }
    if (src.paidTitle && normalizeTitleForCompare(src.paidTitle) !== normalizeTitleForCompare(src.title)) {
      derived.push({
        app: opts.app,
        scene: opts.scene,
        date: opts.date,
        status: opts.status,
        extra: opts.extra,
        book_id: src.bid,
        params: src.paidTitle,
      });
    }
  }
  return { derived, filtered };
}

async function loadExistingBookIDs(baseURL: string, token: string, taskRef: BitableRef, fieldsMap: Record<string, string>, opts: any, pageSize: number, useView: boolean) {
  const existingBookIDs = new Set<string>();
  const dateCol = (fieldsMap["Date"] || "").trim();
  const targetDay = resolveFilterDayForCompare(opts.date || "Today");

  const taskFilter = buildTaskFilter(fieldsMap, opts.app, opts.scene, opts.date);
  const taskItems = await fetchAllRecords(baseURL, token, taskRef, pageSize, taskRef.ViewID, useView, taskFilter);

  for (const item of taskItems) {
    const fieldsRaw = item.fields || {};
    const bookID = NormalizeBitableValue(fieldsRaw[fieldsMap["BookID"]]).trim();
    if (!bookID) continue;
    if (!dateCol) {
      existingBookIDs.add(bookID);
      continue;
    }
    const dateValue = fieldsRaw[dateCol];
    const ymd = normalizeDateToYMD(dateValue);
    if (!targetDay || !ymd) continue;
    if (ymd === targetDay) existingBookIDs.add(bookID);
  }
  return existingBookIDs;
}

async function main() {
  const argv = process.argv.slice(2);
  const program = new Command();
  program
    .name("bitable-derive")
    .description("Derive tasks from a source Feishu Bitable")
    .option("--log-json", "Output logs as JSON lines")
    .helpOption(true);

  const sourceFieldOptions = (cmd: Command) =>
    cmd
      .option("--bid-field <name>", "Source field name for BID")
      .option("--title-field <name>", "Source field name for 短剧名")
      .option("--scene-field <name>", "Source field name for 维权场景")
      .option("--actor-field <name>", "Source field name for 主角名")
      .option("--paid-field <name>", "Source field name for 付费剧名");

  program
    .command("fetch")
    .description("Fetch source Bitable records and output JSONL")
    .option("--bitable-url <url>", "Source Bitable URL (original table)")
    .option("--output <path>", "Output path for JSONL (default stdout)")
    .action(async (opts) => {
    setLoggerJSON(Boolean(program.opts()?.logJson));
    const sourceURL = (opts.bitableUrl || "").trim();
    if (!sourceURL) {
      errLogger.error("bitable url is required", { hint: "--bitable-url" });
      process.exit(2);
    }
    const appID = Env("FEISHU_APP_ID", "");
    const appSecret = Env("FEISHU_APP_SECRET", "");
    if (!appID || !appSecret) {
      errLogger.error("FEISHU_APP_ID/FEISHU_APP_SECRET are required");
      process.exit(2);
    }
    const baseURL = Env("FEISHU_BASE_URL", DefaultBaseURL);
    let sourceRef: BitableRef;
    try {
      sourceRef = ParseBitableURL(sourceURL);
    } catch (err: any) {
      errLogger.error("parse bitable URL failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    let token: string;
    try {
      token = await GetTenantAccessToken(baseURL, appID, appSecret);
    } catch (err: any) {
      errLogger.error("get tenant access token failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    try {
      sourceRef = await resolveBitableRef(baseURL, token, sourceRef);
    } catch (err: any) {
      errLogger.error("resolve bitable app token failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    const sourceItems = await fetchAllRecords(baseURL, token, sourceRef, 200, sourceRef.ViewID, Boolean(sourceRef.ViewID), null);
    const out = opts.output ? fs.createWriteStream(opts.output) : process.stdout;
    for (const item of sourceItems) {
      const fieldsRaw = item.fields || {};
      const simplified = simplifyFields(fieldsRaw);
      out.write(`${JSON.stringify(simplified)}\n`);
    }
    logger.info("result", { data: { source_total: sourceItems.length, output: opts.output || "-" } });
  });

  sourceFieldOptions(
    program
      .command("create")
      .description("Create tasks from JSONL generated by fetch")
      .allowExcessArguments(false)
      .option("--input <path>", "Input JSONL path (or - for stdin)")
      .option("--task-url <url>", "Task Bitable URL (default: TASK_BITABLE_URL)")
      .option("--app <app>", "Task app", "com.smile.gifmaker")
      .option("--extra <extra>", "Task extra", "春节档专项")
      .option("--params-list", "Store [短剧名, 主角名, 付费剧名] as a JSON list in Params (one task per source row)")
      .option("--skip-existing", "Skip creating tasks when BookID already exists for Today")
  ).action(async (opts) => {
    setLoggerJSON(Boolean(program.opts()?.logJson));
    const inputPath = (opts.input || "").trim();
    if (!inputPath) {
      errLogger.error("input path is required", { hint: "--input" });
      process.exit(2);
    }
    const taskURL = (opts.taskUrl || Env("TASK_BITABLE_URL", "")).trim();
    if (!taskURL) {
      errLogger.error("task url is required", { hint: "--task-url or TASK_BITABLE_URL" });
      process.exit(2);
    }
    const appID = Env("FEISHU_APP_ID", "");
    const appSecret = Env("FEISHU_APP_SECRET", "");
    if (!appID || !appSecret) {
      errLogger.error("FEISHU_APP_ID/FEISHU_APP_SECRET are required");
      process.exit(2);
    }
    const baseURL = Env("FEISHU_BASE_URL", DefaultBaseURL);
    const fieldsMap = LoadTaskFieldsFromEnv();
    const sourceFieldMap = normalizeSourceFieldMap(opts);
    let taskRef: BitableRef;
    try {
      taskRef = ParseBitableURL(taskURL);
    } catch (err: any) {
      errLogger.error("parse bitable URL failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    let token: string;
    try {
      token = await GetTenantAccessToken(baseURL, appID, appSecret);
    } catch (err: any) {
      errLogger.error("get tenant access token failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    try {
      taskRef = await resolveBitableRef(baseURL, token, taskRef);
    } catch (err: any) {
      errLogger.error("resolve bitable app token failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    const useView = false;
    const pageSize = 200;
    const sourceItems = parseInputItems(inputPath).map((it: any) => (it && typeof it === "object" && (it as any).fields ? (it as any).fields : it));
    const fixedOpts = { ...opts, scene: "综合页搜索", date: "Today", status: "pending" };
    const { derived, filtered } = deriveTasksFromSource(sourceItems, sourceFieldMap, fixedOpts);
    let existingBookIDs = new Set<string>();
    const skipExisting = Boolean(opts.skipExisting);
    if (skipExisting) {
      existingBookIDs = await loadExistingBookIDs(baseURL, token, taskRef, fieldsMap, fixedOpts, pageSize, useView);
    }
    const createDate = todayDateString();
    let skipped = 0;
    const records = derived
      .filter((item) => {
        if (!skipExisting) return true;
        const exists = existingBookIDs.has(item.book_id);
        if (exists) skipped++;
        return !exists;
      })
      .map((item) => buildTaskFields(fieldsMap, { ...item, date: createDate }))
      .filter((fields) => Object.keys(fields).length)
      .map((fields) => ({ fields }));
    if (!records.length) {
      logger.info("result", { data: { derived: derived.length, filtered, created: 0, skipped } });
      process.exit(0);
    }
    let created = 0;
    const start = Date.now();
    const errors: string[] = [];
    for (let i = 0; i < records.length; i += createMaxBatchSize) {
      const batch = records.slice(i, i + createMaxBatchSize);
      try {
        await batchCreate(baseURL, token, taskRef, batch);
        created += batch.length;
      } catch (err: any) {
        errors.push(err?.message || String(err));
        break;
      }
    }
    const elapsed = Math.floor(((Date.now() - start) / 1000) * 1000) / 1000;
    logger.info("result", { data: { derived: derived.length, filtered, created, skipped, failed: errors.length, errors, elapsed_seconds: elapsed } });
    process.exit(errors.length ? 1 : 0);
  });

  sourceFieldOptions(
    program
      .command("sync")
      .description("Fetch source and create tasks in one step")
      .option("--bitable-url <url>", "Source Bitable URL (original table)")
      .option("--task-url <url>", "Task Bitable URL (default: TASK_BITABLE_URL)")
      .option("--app <app>", "Task app", "com.smile.gifmaker")
      .option("--extra <extra>", "Task extra", "春节档专项")
      .option("--params-list", "Store [短剧名, 主角名, 付费剧名] as a JSON list in Params (one task per source row)")
      .option("--skip-existing", "Skip creating tasks when BookID already exists for Today")
  ).action(async (opts) => {
    setLoggerJSON(Boolean(program.opts()?.logJson));
    const sourceURL = (opts.bitableUrl || "").trim();
    if (!sourceURL) {
      errLogger.error("bitable url is required", { hint: "--bitable-url" });
      process.exit(2);
    }
    const taskURL = (opts.taskUrl || "").trim();
    if (!taskURL) {
      errLogger.error("task url is required", { hint: "--task-url" });
      process.exit(2);
    }
    const appID = Env("FEISHU_APP_ID", "");
    const appSecret = Env("FEISHU_APP_SECRET", "");
    if (!appID || !appSecret) {
      errLogger.error("FEISHU_APP_ID/FEISHU_APP_SECRET are required");
      process.exit(2);
    }
    const baseURL = Env("FEISHU_BASE_URL", DefaultBaseURL);
    const fieldsMap = LoadTaskFieldsFromEnv();
    const sourceFieldMap = normalizeSourceFieldMap(opts);
    let sourceRef: BitableRef;
    let taskRef: BitableRef;
    try {
      sourceRef = ParseBitableURL(sourceURL);
      taskRef = ParseBitableURL(taskURL);
    } catch (err: any) {
      errLogger.error("parse bitable URL failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    let token: string;
    try {
      token = await GetTenantAccessToken(baseURL, appID, appSecret);
    } catch (err: any) {
      errLogger.error("get tenant access token failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    try {
      sourceRef = await resolveBitableRef(baseURL, token, sourceRef);
      taskRef = await resolveBitableRef(baseURL, token, taskRef);
    } catch (err: any) {
      errLogger.error("resolve bitable app token failed", { err: err?.message || String(err) });
      process.exit(2);
    }
    const useView = false;
    const pageSize = 200;
    const sourceItems = await fetchAllRecords(baseURL, token, sourceRef, pageSize, sourceRef.ViewID, Boolean(sourceRef.ViewID));
    const sourceFields = sourceItems.map((it: any) => it.fields || {});
    const fixedOpts = { ...opts, scene: "综合页搜索", date: "Today", status: "pending" };
    const { derived, filtered } = deriveTasksFromSource(sourceFields, sourceFieldMap, fixedOpts);
    let existingBookIDs = new Set<string>();
    const skipExisting = Boolean(opts.skipExisting);
    if (skipExisting) {
      existingBookIDs = await loadExistingBookIDs(baseURL, token, taskRef, fieldsMap, fixedOpts, pageSize, useView);
    }
    const createDate = todayDateString();
    let skipped = 0;
    const records = derived
      .filter((item) => {
        if (!skipExisting) return true;
        const exists = existingBookIDs.has(item.book_id);
        if (exists) skipped++;
        return !exists;
      })
      .map((item) => buildTaskFields(fieldsMap, { ...item, date: createDate }))
      .filter((fields) => Object.keys(fields).length)
      .map((fields) => ({ fields }));
    if (!records.length) {
      logger.info("result", { data: { source_total: sourceItems.length, filtered, created: 0, skipped } });
      process.exit(0);
    }
    let created = 0;
    const start = Date.now();
    const errors: string[] = [];
    for (let i = 0; i < records.length; i += createMaxBatchSize) {
      const batch = records.slice(i, i + createMaxBatchSize);
      try {
        await batchCreate(baseURL, token, taskRef, batch);
        created += batch.length;
      } catch (err: any) {
        errors.push(err?.message || String(err));
        break;
      }
    }
    const elapsed = Math.floor(((Date.now() - start) / 1000) * 1000) / 1000;
    logger.info("result", { data: { source_total: sourceItems.length, filtered, derived: derived.length, created, skipped, failed: errors.length, errors, elapsed_seconds: elapsed } });
    process.exit(errors.length ? 1 : 0);
  });

  program.parse(argv, { from: "user" });
  getGlobalOptions(program);
}

main().catch((err) => {
  errLogger.error("fatal", { err: err?.message || String(err) });
  process.exit(1);
});
