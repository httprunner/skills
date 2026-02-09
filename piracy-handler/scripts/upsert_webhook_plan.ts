#!/usr/bin/env node
import { Command } from "commander";
import {
  andFilter,
  batchCreate,
  batchUpdate,
  condition,
  dayStartMs,
  env,
  expandHome,
  type FeishuCtx,
  getTenantToken,
  must,
  orFilter,
  parseTaskIDs,
  readInput,
  searchRecords,
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
  task_ids: number[];
  drama_info?: string; // JSON string
};

function parseItems(inputText: string): UpsertItem[] {
  const txt = String(inputText || "").trim();
  if (!txt) return [];
  if (txt.startsWith("{") || txt.startsWith("[")) {
    const j = JSON.parse(txt);
    if (Array.isArray(j)) return j as UpsertItem[];
    return [j as UpsertItem];
  }
  const lines = txt
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("#"));
  return lines.map((l) => JSON.parse(l)) as UpsertItem[];
}

function normalizeItem(raw: any, defaultBizType: string): UpsertItem | null {
  const groupID = String(raw?.group_id ?? raw?.groupID ?? "").trim();
  const date = String(raw?.date ?? raw?.day ?? "").trim();
  const bizType = String(raw?.biz_type ?? raw?.bizType ?? defaultBizType).trim() || defaultBizType;
  const taskIDs = parseTaskIDs(raw?.task_ids ?? raw?.taskIDs ?? raw?.task_ids_json ?? raw?.taskIDsJSON ?? raw?.taskIds ?? "");
  const dramaInfo = typeof raw?.drama_info === "string" ? raw.drama_info : typeof raw?.dramaInfo === "string" ? raw.dramaInfo : "";
  if (!groupID || !date || !taskIDs.length) return null;
  return { group_id: groupID, date, biz_type: bizType, task_ids: taskIDs, drama_info: dramaInfo || undefined };
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

  const buckets = new Map<string, { bizType: string; day: string; dayMs: number; groupIDs: string[] }>();
  for (const it of normalized) {
    const day = it.date;
    const dayMs = dayStartMs(day);
    if (!dayMs) throw new Error(`invalid date: ${day}`);
    const key = `${it.biz_type || bizTypeDefault}@@${dayMs}`;
    const b = buckets.get(key) || { bizType: it.biz_type || bizTypeDefault, day, dayMs, groupIDs: [] as string[] };
    b.groupIDs.push(it.group_id);
    buckets.set(key, b);
  }

  const existingByKey = new Map<string, { recordID: string }>();
  for (const b of buckets.values()) {
    const uniq = Array.from(new Set(b.groupIDs)).filter(Boolean);
    for (let i = 0; i < uniq.length; i += 40) {
      const chunk = uniq.slice(i, i + 40);
      const rows = await searchRecords(
        ctx,
        webhookURL,
        andFilter(
          [condition(wf.BizType, "is", b.bizType), condition(wf.Date, "is", "ExactDate", String(b.dayMs))],
          [orFilter(chunk.map((gid) => condition(wf.GroupID, "is", gid)))],
        ),
        200,
        200,
      );
      for (const r of rows) {
        const recordID = String(r.record_id || "").trim();
        const groupID = String(r?.fields?.[wf.GroupID] ?? "").trim();
        if (!recordID || !groupID) continue;
        existingByKey.set(`${b.bizType}@@${b.dayMs}@@${groupID}`, { recordID });
      }
    }
  }

  const createRows: Array<{ fields: Record<string, any> }> = [];
  const updateRows: Array<{ record_id: string; fields: Record<string, any> }> = [];
  const errors: Array<{ group_id: string; date: string; biz_type: string; err: string }> = [];

  for (const it of normalized) {
    const dayMs = dayStartMs(it.date);
    const bizType = it.biz_type || bizTypeDefault;
    const k = `${bizType}@@${dayMs}@@${it.group_id}`;
    const taskIDsPayload = JSON.stringify(it.task_ids);
    const dramaInfo = it.drama_info || "";

    const exist = existingByKey.get(k);
    if (exist?.recordID) {
      updateRows.push({
        record_id: exist.recordID,
        fields: {
          [wf.TaskIDs]: taskIDsPayload,
          ...(dramaInfo ? { [wf.DramaInfo]: dramaInfo } : {}),
        },
      });
      continue;
    }

    createRows.push({
      fields: {
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
