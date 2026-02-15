#!/usr/bin/env node
import { Command } from "commander";
import {
  batchCreate,
  batchUpdate,
  dayStartMs,
  env,
  expandHome,
  type FeishuCtx,
  getTenantToken,
  must,
  parseTaskIDs,
  readInput,
  searchRecords,
  toDay,
  webhookFields,
} from "./webhook_lib";

type CLIOptions = {
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
  date: string; // yyyy-mm-dd
  biz_type?: string;
  app?: string;
  task_ids: number[];
  drama_info?: string; // JSON string
};

function encodeTaskIDsByStatus(taskIDs: number[]): string {
  const status = String(env("WEBHOOK_TASKIDS_DEFAULT_STATUS", "pending")).trim().toLowerCase() || "pending";
  return JSON.stringify({ [status]: taskIDs });
}

function parseItems(inputText: string): UpsertItem[] {
  const txt = String(inputText || "").trim();
  if (!txt) return [];
  if (txt.startsWith("{") || txt.startsWith("[")) {
    // Accept both JSON and JSONL:
    // - JSON object / array: parse directly
    // - JSONL: fallback to line-by-line parsing when direct parse fails
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
    .description("Upsert webhook plans in WEBHOOK_BITABLE_URL by (BizType, GroupID, Date)")
    .option("--input <path>", "Input JSON/JSONL file (use - for stdin)")
    .option("--group-id <id>", "Single GroupID")
    .option("--date <yyyy-mm-dd>", "Capture day (default: today)", new Date().toISOString().slice(0, 10))
    .option("--biz-type <name>", "BizType", "piracy_general_search")
    .option("--task-id <csv>", "TaskID(s), comma-separated (e.g. 111 or 111,222,333)")
    .option("--drama-info <json>", "DramaInfo JSON string")
    .option("--dry-run", "Compute only, do not write records")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  return program.opts<CLIOptions>();
}

async function main() {
  const args = parseCLI(process.argv);
  const dryRun = Boolean(args.dryRun);
  const bizTypeDefault = String(args.bizType || "piracy_general_search").trim();

  let items: UpsertItem[] = [];
  if (args.input) {
    items = parseItems(readInput(args.input));
  } else if (args.groupId) {
    items = [
      {
        group_id: String(args.groupId).trim(),
        date: String(args.date || "").trim(),
        biz_type: bizTypeDefault,
        task_ids: parseTaskIDs(args.taskId),
        drama_info: String(args.dramaInfo || "").trim() || undefined,
      },
    ];
  }

  const normalized = items.map((it) => normalizeItem(it, bizTypeDefault)).filter(Boolean) as UpsertItem[];
  if (!normalized.length) throw new Error("no upsert items provided (use --input or --group-id + --task-id)");

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
    if (targetKeys.has(key) && !existingByKey.has(key)) {
      existingByKey.set(key, { recordID });
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

  const summary = { dry_run: dryRun, input_items: normalized.length, created: createRows.length, updated: updateRows.length, errors };
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
