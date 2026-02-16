#!/usr/bin/env node
import { Command } from "commander";
import { chunk, readInput as readTextInput, runTaskFetch, todayLocal } from "./shared/lib";
import {
  batchCreate,
  batchUpdate,
  dayStartMs,
  env,
  type FeishuCtx,
  getTenantToken,
  must,
  parseTaskIDs,
  readInput,
  searchRecords,
  toDay,
  webhookFields,
} from "./webhook/lib";

type CLIOptions = {
  source: string;
  input?: string;
  groupId?: string;
  date?: string;
  bizType: string;
  taskId?: string;
  dramaInfo?: string;
  dryRun: boolean;
};

type UpsertItem = {
  group_id: string;
  date: string;
  biz_type?: string;
  app?: string;
  task_ids: number[];
  drama_info?: string;
};

function isSamePositiveIntSet(a: number[], b: number[]): boolean {
  const aa = Array.from(new Set(a.map((x) => Math.trunc(Number(x))).filter((x) => Number.isFinite(x) && x > 0))).sort((x, y) => x - y);
  const bb = Array.from(new Set(b.map((x) => Math.trunc(Number(x))).filter((x) => Number.isFinite(x) && x > 0))).sort((x, y) => x - y);
  if (aa.length !== bb.length) return false;
  for (let i = 0; i < aa.length; i++) {
    if (aa[i] !== bb[i]) return false;
  }
  return true;
}

function isSameText(a: string, b: string): boolean {
  return String(a || "").trim() === String(b || "").trim();
}

function parsePositiveIDs(values: any): number[] {
  const input = Array.isArray(values) ? values : [];
  const ids = input
    .map((x: any) => Math.trunc(Number(x)))
    .filter((x: number) => Number.isFinite(x) && x > 0);
  return Array.from(new Set(ids)).sort((a, b) => a - b);
}

function parseTaskIDsInput(v: any): number[] {
  if (v == null) return [];
  if (Array.isArray(v)) return parsePositiveIDs(v);
  if (typeof v === "number") return parsePositiveIDs([v]);
  if (typeof v === "object") {
    const candidates = [v.task_ids, v.taskIDs, v.taskIds].filter((x) => x != null);
    if (candidates.length) return parseTaskIDsInput(candidates[0]);
    return [];
  }
  const s = String(v).trim();
  if (!s) return [];
  try {
    const j = JSON.parse(s);
    if (Array.isArray(j)) return parsePositiveIDs(j);
  } catch {
    // ignore JSON parse error
  }
  const ids = s
    .split(/[\s,，,；;、|]+/)
    .map((x) => Math.trunc(Number(x)))
    .filter((x) => Number.isFinite(x) && x > 0);
  return Array.from(new Set(ids)).sort((a, b) => a - b);
}

function encodeTaskIDsByStatus(taskIDs: number[]): string {
  const status = String(env("WEBHOOK_TASKIDS_DEFAULT_STATUS", "pending")).trim().toLowerCase() || "pending";
  return JSON.stringify({ [status]: taskIDs });
}

function parseItems(inputText: string): UpsertItem[] {
  const txt = String(inputText || "").trim();
  if (!txt) return [];
  if (txt.startsWith("{") || txt.startsWith("[")) {
    try {
      const j = JSON.parse(txt);
      if (Array.isArray(j)) return j as UpsertItem[];
      return [j as UpsertItem];
    } catch {
      // continue to JSONL parsing below
    }
  }
  const lines = txt
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("#"));
  return lines.map((l) => JSON.parse(l)) as UpsertItem[];
}

function normalizeItem(raw: any, defaultBizType: string): UpsertItem | null {
  const groupID = String(raw?.group_id ?? raw?.groupID ?? "").trim();
  const rawDate = String(raw?.date ?? raw?.day ?? "").trim();
  const date = toDay(rawDate) || rawDate;
  const bizType = String(raw?.biz_type ?? raw?.bizType ?? defaultBizType).trim() || defaultBizType;
  const app = String(raw?.app ?? raw?.App ?? "").trim();
  const taskIDs = parseTaskIDsInput(raw?.task_ids ?? raw?.taskIDs ?? raw?.task_ids_json ?? raw?.taskIDsJSON ?? raw?.taskIds ?? "");
  const dramaInfo = typeof raw?.drama_info === "string" ? raw.drama_info : typeof raw?.dramaInfo === "string" ? raw.dramaInfo : "";
  if (!groupID || !date || !taskIDs.length) return null;
  return { group_id: groupID, date, biz_type: bizType, app: app || undefined, task_ids: taskIDs, drama_info: dramaInfo || undefined };
}

function normalizeDayValue(v: any): string {
  if (v == null) return "";
  if (Array.isArray(v)) {
    for (const it of v) {
      const d = normalizeDayValue(it);
      if (d) return d;
    }
    return "";
  }
  if (typeof v === "object") {
    if ((v as any).value != null) return normalizeDayValue((v as any).value);
    if ((v as any).text != null) return normalizeDayValue((v as any).text);
  }
  const s = String(v).trim();
  if (!s) return "";
  return toDay(s) || "";
}

function normalizeTextValue(v: any): string {
  if (v == null) return "";
  if (Array.isArray(v)) {
    const parts = v.map((x) => normalizeTextValue(x)).filter(Boolean);
    return parts.join(" ").trim();
  }
  if (typeof v === "object") {
    if ((v as any).value != null) return normalizeTextValue((v as any).value);
    if ((v as any).text != null) return normalizeTextValue((v as any).text);
  }
  return String(v).trim();
}

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("upsert_webhook_plan")
    .description("Single webhook upsert entry: from detect output or from plan input")
    .option("--source <type>", "Input source: auto|detect|plan", "auto")
    .option("--input <path>", "Input file path; detect.json or plan JSON/JSONL (use - for stdin)")
    .option("--group-id <id>", "Plan source: single GroupID")
    .option("--date <yyyy-mm-dd>", "Plan source capture day (default: today)", todayLocal())
    .option("--biz-type <name>", "BizType", "piracy_general_search")
    .option("--task-id <csv>", "Plan source TaskID(s), comma-separated (e.g. 111 or 111,222,333)")
    .option("--drama-info <json>", "Plan source DramaInfo JSON string")
    .option("--dry-run", "Compute only, do not write records")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  return program.opts<CLIOptions>();
}

function mapAppLabel(app: string): string {
  const trimmed = String(app || "").trim();
  if (!trimmed) return "";
  const m: Record<string, string> = {
    "com.smile.gifmaker": "快手",
    "com.tencent.mm": "微信视频号",
    "com.eg.android.AlipayGphone": "支付宝",
  };
  return m[trimmed] || trimmed;
}

function looksLikeDetectOutput(v: any): boolean {
  if (!v || typeof v !== "object") return false;
  return Array.isArray(v.groups_by_app_book) && Array.isArray(v.source_tasks);
}

function parseDetectObjectFromText(txt: string): any {
  try {
    const v = JSON.parse(String(txt || "").trim());
    if (!looksLikeDetectOutput(v)) throw new Error("input is not a valid detect output object");
    return v;
  } catch (err) {
    throw new Error(`failed to parse detect input: ${err instanceof Error ? err.message : String(err)}`);
  }
}

function parseGroupTaskIDs(g: any): number[] {
  const fromTaskInfo = Array.isArray(g?.task_info) ? g.task_info.map((x: any) => x?.task_id) : [];
  return parsePositiveIDs(fromTaskInfo);
}

function parseDetectSourceTaskIDs(detect: any): number[] {
  const fromSourceTasks = Array.isArray(detect?.source_tasks) ? detect.source_tasks.map((x: any) => x?.task_id) : [];
  return parsePositiveIDs(fromSourceTasks);
}

function collectTaskIDsByGroup(app: string, day: string, groupIDs: string[]): Map<string, number[]> {
  const out = new Map<string, number[]>();
  const appValue = String(app || "").trim();
  if (!appValue || !day || !groupIDs.length) return out;

  const targetScenes = ["综合页搜索", "个人页搜索"];
  const uniqueGroupIDs = Array.from(new Set(groupIDs.map((x) => String(x || "").trim()).filter(Boolean)));
  for (const scene of targetScenes) {
    for (const batch of chunk(uniqueGroupIDs, 40)) {
      const queryIDs = batch.length === 1 ? [batch[0], batch[0]] : batch;
      const rows = runTaskFetch([
        "--group-id",
        queryIDs.join(","),
        "--app",
        appValue,
        "--scene",
        scene,
        "--status",
        "Any",
        "--date",
        day,
      ]);
      for (const row of rows) {
        const gid = String(row?.group_id || "").trim();
        if (!gid) continue;
        const rowDay = toDay(String(row?.date || "").trim());
        if (rowDay !== day) continue;
        const tid = Math.trunc(Number(row?.task_id));
        if (!Number.isFinite(tid) || tid <= 0) continue;
        const current = out.get(gid) || [];
        current.push(tid);
        out.set(gid, current);
      }
    }
  }

  for (const [gid, ids] of out.entries()) {
    out.set(gid, Array.from(new Set(ids)).sort((a, b) => a - b));
  }
  return out;
}

function buildUpsertItemsFromDetect(detect: any, bizType: string): UpsertItem[] {
  const day = String(detect?.capture_day || detect?.day || "").trim();
  const dayMs = Math.trunc(Number(detect?.capture_day_ms ?? detect?.day_ms ?? 0));
  const topSourceTaskIDs = parseDetectSourceTaskIDs(detect);
  if (!day) throw new Error("invalid detect input: missing capture day");
  if (!Array.isArray(detect?.groups_by_app_book)) throw new Error("invalid detect input: missing groups_by_app_book");

  const out: UpsertItem[] = [];
  for (const appBook of detect.groups_by_app_book) {
    const app = String(appBook?.app || "").trim();
    const bookID = String(appBook?.book_id || "").trim();
    const drama = appBook?.drama || {};
    const groups = Array.isArray(appBook?.groups) ? appBook.groups : [];
    const groupTaskIDMap = collectTaskIDsByGroup(
      app,
      day,
      groups.map((x: any) => String(x?.group_id || "").trim()),
    );

    for (const g of groups) {
      const groupID = String(g?.group_id || "").trim();
      if (!groupID) continue;
      const groupTaskIDs = parseGroupTaskIDs(g);
      const fetchedTaskIDs = groupTaskIDMap.get(groupID) || [];
      const tids = Array.from(new Set([...groupTaskIDs, ...topSourceTaskIDs, ...fetchedTaskIDs])).sort((a, b) => a - b);
      if (!tids.length) continue;

      const dramaInfoObj = {
        CaptureDate: String(dayMs || ""),
        DramaID: bookID,
        DramaName: String(drama?.name || g?.task_info?.[0]?.params || "").trim(),
        EpisodeCount: String(drama?.episode_count || "").trim(),
        Priority: String(drama?.priority || "").trim(),
        RightsProtectionScenario: String(drama?.rights_protection_scenario || "").trim(),
        TotalDuration: String(drama?.total_duration_sec ?? ""),
        CaptureDuration: String(g?.capture_duration_sec ?? ""),
        GeneralSearchRatio: `${(Number(g?.ratio || 0) * 100).toFixed(2)}%`,
      };

      out.push({
        group_id: groupID,
        date: day,
        biz_type: bizType,
        app: mapAppLabel(app),
        task_ids: tids,
        drama_info: JSON.stringify(dramaInfoObj),
      });
    }
  }
  return out;
}

function resolveSourceMode(args: CLIOptions, parsedInput?: any): "detect" | "plan" {
  const source = String(args.source || "auto").trim().toLowerCase();
  if (source === "detect" || source === "plan") return source;
  if (source !== "auto") throw new Error(`invalid --source: ${args.source}; expected auto|detect|plan`);

  if (String(args.groupId || "").trim()) return "plan";
  if (String(args.input || "").trim()) {
    return looksLikeDetectOutput(parsedInput) ? "detect" : "plan";
  }
  throw new Error("cannot resolve source mode; use --source or provide --group-id/--input");
}

function buildPlanItems(args: CLIOptions, bizTypeDefault: string, inputText?: string): UpsertItem[] {
  if (inputText) return parseItems(inputText);
  if (!args.groupId) return [];
  return [
    {
      group_id: String(args.groupId).trim(),
      date: String(args.date || "").trim(),
      biz_type: bizTypeDefault,
      task_ids: parseTaskIDsInput(args.taskId),
      drama_info: String(args.dramaInfo || "").trim() || undefined,
    },
  ];
}

function buildDetectItems(args: CLIOptions, bizTypeDefault: string, inputText?: string, parsedInput?: any): UpsertItem[] {
  if (!args.input && !inputText) throw new Error("detect source requires --input");
  const detectObj = looksLikeDetectOutput(parsedInput) ? parsedInput : parseDetectObjectFromText(inputText || readTextInput(String(args.input)));
  return buildUpsertItemsFromDetect(detectObj, bizTypeDefault);
}

async function main() {
  const args = parseCLI(process.argv);
  const dryRun = Boolean(args.dryRun);
  const bizTypeDefault = String(args.bizType || "piracy_general_search").trim();
  const inputPath = String(args.input || "").trim();
  const inputText = inputPath ? readInput(inputPath) : "";
  let parsedInput: any = undefined;
  if (inputText) {
    try {
      parsedInput = JSON.parse(String(inputText || "").trim());
    } catch {
      parsedInput = undefined;
    }
  }
  const sourceMode = resolveSourceMode(args, parsedInput);

  const rawItems =
    sourceMode === "detect"
      ? buildDetectItems(args, bizTypeDefault, inputText, parsedInput)
      : buildPlanItems(args, bizTypeDefault, inputText);
  const normalized = rawItems.map((it) => normalizeItem(it, bizTypeDefault)).filter(Boolean) as UpsertItem[];
  if (!normalized.length) {
    throw new Error(sourceMode === "detect" ? "no valid upsert items generated from detect input" : "no upsert items provided");
  }

  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const webhookURL = must("WEBHOOK_BITABLE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const wf = webhookFields();

  const scanLimit = Math.trunc(Number(env("WEBHOOK_UPSERT_SCAN_LIMIT", "10000"))) || 10000;
  const targetKeys = new Set<string>();
  for (const it of normalized) {
    const day = normalizeDayValue(it.date);
    const dayMs = dayStartMs(day);
    if (!dayMs) throw new Error(`invalid date: ${it.date}`);
    const bizType = it.biz_type || bizTypeDefault;
    targetKeys.add(`${bizType}@@${day}@@${it.group_id}`);
  }

  const allRows = await searchRecords(ctx, webhookURL, null, 200, scanLimit);
  const existingByKey = new Map<
    string,
    { recordID: string; taskIDs: number[]; app: string; dramaInfo: string; taskIDsByStatusRaw: string; status: string }
  >();
  for (const r of allRows) {
    const recordID = String(r.record_id || "").trim();
    const bizType = normalizeTextValue(r?.fields?.[wf.BizType]);
    const groupID = normalizeTextValue(r?.fields?.[wf.GroupID]);
    const day = normalizeDayValue(r?.fields?.[wf.Date]);
    if (!recordID || !bizType || !groupID || !day) continue;
    const key = `${bizType}@@${day}@@${groupID}`;
    if (targetKeys.has(key) && !existingByKey.has(key)) {
      existingByKey.set(key, {
        recordID,
        taskIDs: parseTaskIDs(r?.fields?.[wf.TaskIDs]),
        app: normalizeTextValue(r?.fields?.[wf.App]),
        dramaInfo: normalizeTextValue(r?.fields?.[wf.DramaInfo]),
        taskIDsByStatusRaw: normalizeTextValue(r?.fields?.[wf.TaskIDsByStatus]),
        status: normalizeTextValue(r?.fields?.[wf.Status]),
      });
    }
  }

  const createRows: Array<{ fields: Record<string, any> }> = [];
  const updateRows: Array<{ record_id: string; fields: Record<string, any> }> = [];
  const errors: Array<{ group_id: string; date: string; biz_type: string; err: string }> = [];

  for (const it of normalized) {
    const day = normalizeDayValue(it.date);
    const dayMs = dayStartMs(day);
    if (!dayMs) throw new Error(`invalid date: ${it.date}`);
    const bizType = it.biz_type || bizTypeDefault;
    const k = `${bizType}@@${day}@@${it.group_id}`;
    const taskIDsPayload = encodeTaskIDsByStatus(it.task_ids);
    const taskIDsByStatusPayload = taskIDsPayload;
    const dramaInfo = it.drama_info || "";
    const app = String(it.app || "").trim();
    const taskIDsByStatusField =
      typeof wf.TaskIDsByStatus === "string" && wf.TaskIDsByStatus.trim() ? wf.TaskIDsByStatus.trim() : "";
    const updateAtField = typeof wf.UpdateAt === "string" && wf.UpdateAt.trim() ? wf.UpdateAt.trim() : "";

    const exist = existingByKey.get(k);
    if (exist?.recordID) {
      const taskIDsChanged = !isSamePositiveIntSet(exist.taskIDs, it.task_ids);
      const appChanged = Boolean(app && wf.App && !isSameText(app, exist.app));
      const dramaChanged = Boolean(dramaInfo && wf.DramaInfo && !isSameText(dramaInfo, exist.dramaInfo));
      const byStatusChanged = Boolean(taskIDsByStatusField && !isSameText(taskIDsByStatusPayload, exist.taskIDsByStatusRaw));

      const fields: Record<string, any> = {};
      if (taskIDsChanged) fields[wf.TaskIDs] = taskIDsPayload;
      if (taskIDsByStatusField && byStatusChanged) fields[taskIDsByStatusField] = taskIDsByStatusPayload;
      if (appChanged && wf.App) fields[wf.App] = app;
      if (dramaChanged && wf.DramaInfo) fields[wf.DramaInfo] = dramaInfo;
      if (taskIDsChanged) {
        fields[wf.Status] = "pending";
        fields[wf.RetryCount] = 0;
        fields[wf.LastError] = "";
      }
      if (Object.keys(fields).length > 0) {
        if (updateAtField) fields[updateAtField] = Date.now();
        updateRows.push({
          record_id: exist.recordID,
          fields,
        });
      }
      continue;
    }

    createRows.push({
      fields: {
        ...(app && wf.App ? { [wf.App]: app } : {}),
        [wf.BizType]: bizType,
        [wf.GroupID]: it.group_id,
        [wf.Status]: "pending",
        [wf.TaskIDs]: taskIDsPayload,
        ...(taskIDsByStatusField ? { [taskIDsByStatusField]: taskIDsByStatusPayload } : {}),
        ...(dramaInfo ? { [wf.DramaInfo]: dramaInfo } : {}),
        [wf.Date]: dayMs,
        [wf.RetryCount]: 0,
      },
    });
  }

  if (!dryRun) {
    if (createRows.length) {
      try {
        await batchCreate(ctx, webhookURL, createRows);
      } catch (err: any) {
        errors.push({ group_id: "-", date: "-", biz_type: bizTypeDefault, err: err?.message || String(err) });
      }
    }
    if (updateRows.length) {
      try {
        await batchUpdate(ctx, webhookURL, updateRows);
      } catch (err: any) {
        errors.push({ group_id: "-", date: "-", biz_type: bizTypeDefault, err: err?.message || String(err) });
      }
    }
  }

  const summary = {
    source: sourceMode,
    dry_run: dryRun,
    input_items: normalized.length,
    created: createRows.length,
    updated: updateRows.length,
    errors,
  };
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
