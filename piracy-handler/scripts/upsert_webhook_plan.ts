#!/usr/bin/env node
import { Command } from "commander";
import { chunk, defaultDetectPath, parsePositiveInt, readInput as readTextInput, runTaskFetch } from "./shared/lib";
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
  parentTaskId?: string;
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
  const taskIDs = parseTaskIDs(raw?.task_ids ?? raw?.taskIDs ?? raw?.task_ids_json ?? raw?.taskIDsJSON ?? raw?.taskIds ?? "");
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
    .option("--parent-task-id <id>", "Detect source parent TaskID; read ~/.eval/<TaskID>/detect.json when --input omitted")
    .option("--group-id <id>", "Plan source: single GroupID")
    .option("--date <yyyy-mm-dd>", "Plan source capture day (default: today)", new Date().toISOString().slice(0, 10))
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
  return Boolean(v && typeof v === "object" && Number(v.parent_task_id) > 0 && Array.isArray(v.selected_groups));
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

function buildUpsertItemsFromDetect(detect: any, bizType: string): UpsertItem[] {
  const parentTaskID = Math.trunc(Number(detect?.parent_task_id));
  const day = String(detect?.day || "").trim();
  const dayMs = Math.trunc(Number(detect?.day_ms || 0));
  const selected = Array.isArray(detect?.selected_groups) ? detect.selected_groups : [];

  if (!parentTaskID || !day) throw new Error("invalid detect input: missing parent_task_id/day");

  const groupsByApp = new Map<string, any[]>();
  for (const g of selected) {
    const app = String(g?.app || "").trim();
    const groupID = String(g?.group_id || "").trim();
    if (!app || !groupID) continue;
    const arr = groupsByApp.get(app) || [];
    arr.push(g);
    groupsByApp.set(app, arr);
  }

  const taskIDsByGroup = new Map<string, number[]>();
  for (const [app, groups] of groupsByApp.entries()) {
    const ids = Array.from(new Set(groups.map((g) => String(g?.group_id || "").trim()).filter(Boolean)));
    for (const batch of chunk(ids, 40)) {
      const tasks = runTaskFetch([
        "--group-id",
        batch.join(","),
        "--app",
        app,
        "--scene",
        "Any",
        "--status",
        "Any",
        "--date",
        day,
      ]);
      for (const t of tasks) {
        const gid = String((t as any)?.group_id || "").trim();
        const tid = Math.trunc(Number((t as any)?.task_id));
        if (!gid || !Number.isFinite(tid) || tid <= 0) continue;
        const arr = taskIDsByGroup.get(gid) || [];
        arr.push(tid);
        taskIDsByGroup.set(gid, arr);
      }
    }
  }

  const out: UpsertItem[] = [];
  for (const g of selected) {
    const groupID = String(g?.group_id || "").trim();
    if (!groupID) continue;
    const tids = Array.from(new Set([parentTaskID, ...(taskIDsByGroup.get(groupID) || [])])).sort((a, b) => a - b);
    if (!tids.length) continue;

    const drama = g?.drama || {};
    const dramaInfoObj = {
      CaptureDate: String(dayMs || ""),
      DramaID: String(g?.book_id || "").trim(),
      DramaName: String(drama?.name || g?.params || "").trim(),
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
      app: mapAppLabel(String(g?.app || "").trim()),
      task_ids: tids,
      drama_info: JSON.stringify(dramaInfoObj),
    });
  }

  return out;
}

function resolveSourceMode(args: CLIOptions): "detect" | "plan" {
  const source = String(args.source || "auto").trim().toLowerCase();
  if (source === "detect" || source === "plan") return source;
  if (source !== "auto") throw new Error(`invalid --source: ${args.source}; expected auto|detect|plan`);

  if (String(args.parentTaskId || "").trim()) return "detect";
  if (String(args.groupId || "").trim()) return "plan";
  if (String(args.input || "").trim()) {
    const txt = readInput(String(args.input));
    try {
      const parsed = JSON.parse(String(txt || "").trim());
      return looksLikeDetectOutput(parsed) ? "detect" : "plan";
    } catch {
      return "plan";
    }
  }
  throw new Error("cannot resolve source mode; use --source or provide --parent-task-id/--group-id/--input");
}

function buildPlanItems(args: CLIOptions, bizTypeDefault: string): UpsertItem[] {
  if (args.input) return parseItems(readInput(args.input));
  if (!args.groupId) return [];
  return [
    {
      group_id: String(args.groupId).trim(),
      date: String(args.date || "").trim(),
      biz_type: bizTypeDefault,
      task_ids: parseTaskIDs(args.taskId),
      drama_info: String(args.dramaInfo || "").trim() || undefined,
    },
  ];
}

function buildDetectItems(args: CLIOptions, bizTypeDefault: string): UpsertItem[] {
  let detectObj: any;
  if (args.input) {
    detectObj = parseDetectObjectFromText(readTextInput(String(args.input)));
  } else {
    const tidRaw = String(args.parentTaskId || "").trim();
    if (!tidRaw) throw new Error("detect source requires --input or --parent-task-id");
    const tid = parsePositiveInt(tidRaw, "--parent-task-id");
    detectObj = parseDetectObjectFromText(readTextInput(defaultDetectPath(tid)));
  }
  return buildUpsertItemsFromDetect(detectObj, bizTypeDefault);
}

async function main() {
  const args = parseCLI(process.argv);
  const dryRun = Boolean(args.dryRun);
  const bizTypeDefault = String(args.bizType || "piracy_general_search").trim();
  const sourceMode = resolveSourceMode(args);

  const rawItems = sourceMode === "detect" ? buildDetectItems(args, bizTypeDefault) : buildPlanItems(args, bizTypeDefault);
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
  const existingByKey = new Map<string, { recordID: string }>();
  for (const r of allRows) {
    const recordID = String(r.record_id || "").trim();
    const bizType = normalizeTextValue(r?.fields?.[wf.BizType]);
    const groupID = normalizeTextValue(r?.fields?.[wf.GroupID]);
    const day = normalizeDayValue(r?.fields?.[wf.Date]);
    if (!recordID || !bizType || !groupID || !day) continue;
    const key = `${bizType}@@${day}@@${groupID}`;
    if (targetKeys.has(key) && !existingByKey.has(key)) existingByKey.set(key, { recordID });
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
    const dramaInfo = it.drama_info || "";
    const app = String(it.app || "").trim();

    const exist = existingByKey.get(k);
    if (exist?.recordID) {
      updateRows.push({
        record_id: exist.recordID,
        fields: {
          [wf.TaskIDs]: taskIDsPayload,
          ...(app && wf.App ? { [wf.App]: app } : {}),
          ...(dramaInfo ? { [wf.DramaInfo]: dramaInfo } : {}),
        },
      });
      continue;
    }

    createRows.push({
      fields: {
        ...(app && wf.App ? { [wf.App]: app } : {}),
        [wf.BizType]: bizType,
        [wf.GroupID]: it.group_id,
        [wf.Status]: "pending",
        [wf.TaskIDs]: taskIDsPayload,
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
