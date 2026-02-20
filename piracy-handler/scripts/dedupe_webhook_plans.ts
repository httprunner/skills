#!/usr/bin/env node
import { Command } from "commander";
import {
  batchDelete,
  env,
  getTenantToken,
  must,
  searchRecords,
  webhookFields,
  type FeishuCtx,
} from "./webhook/lib";
import { firstText, todayLocal, toNumber, yesterdayLocal, toDay } from "./shared/lib";

type CLIOptions = {
  bizType?: string;
  date?: string;
  limit: string;
  sample: string;
  dryRun: boolean;
};

type PlanRow = {
  recordID: string;
  bizType: string;
  day: string;
  groupID: string;
  updateAtMs: number;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("dedupe_webhook_plans")
    .description("One-shot dedupe for webhook plan records by BizType+Date+GroupID")
    .option("--biz-type <csv>", "Optional BizType filter, comma-separated")
    .option("--date <csv>", "Optional day filter, supports yyyy-mm-dd,Today,Yesterday")
    .option("--limit <n>", "Max rows to scan", "50000")
    .option("--sample <n>", "Sample duplicated keys in summary", "20")
    .option("--dry-run", "Preview only; default behavior is applying deletion")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  return program.opts<CLIOptions>();
}

function normalizeText(v: any): string {
  return String(firstText(v) || "").trim();
}

function normalizeDay(v: any): string {
  const s = normalizeText(v);
  return s ? toDay(s) : "";
}

function parseMs(v: any): number {
  const n = Math.trunc(Number(normalizeText(v)));
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function parseCSV(raw: string): string[] {
  return Array.from(
    new Set(
      String(raw || "")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean),
    ),
  );
}

function parseDayFilters(raw?: string): string[] {
  const out: string[] = [];
  for (const token of parseCSV(String(raw || ""))) {
    const lower = token.toLowerCase();
    if (lower === "today") {
      out.push(todayLocal());
      continue;
    }
    if (lower === "yesterday") {
      out.push(yesterdayLocal());
      continue;
    }
    const day = toDay(token);
    if (day) out.push(day);
  }
  return Array.from(new Set(out));
}

function makeKey(row: Pick<PlanRow, "bizType" | "day" | "groupID">): string {
  return `${row.bizType}@@${row.day}@@${row.groupID}`;
}

function comparePlanRow(a: PlanRow, b: PlanRow): number {
  if (a.updateAtMs !== b.updateAtMs) return a.updateAtMs - b.updateAtMs;
  return a.recordID.localeCompare(b.recordID);
}

async function main() {
  const args = parseCLI(process.argv);
  const dryRun = Boolean(args.dryRun);
  const scanLimit = Math.max(1, Math.trunc(toNumber(args.limit, 50000)));
  const sampleSize = Math.max(1, Math.trunc(toNumber(args.sample, 20)));
  const bizTypeFilters = parseCSV(String(args.bizType || ""));
  const dayFilters = parseDayFilters(args.date);

  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const webhookURL = must("WEBHOOK_BITABLE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const wf = webhookFields();
  const useView = String(env("WEBHOOK_USE_VIEW", "false")).trim().toLowerCase();
  if (useView === "1" || useView === "true" || useView === "yes" || useView === "on") {
    throw new Error("WEBHOOK_USE_VIEW=true is not supported for dedupe_webhook_plans; disable it to ensure full-table visibility");
  }

  const rows = await searchRecords(ctx, webhookURL, null, 200, scanLimit);
  const parsedRows: PlanRow[] = [];
  for (const r of rows) {
    const fields = r?.fields || {};
    const recordID = String(r?.record_id || "").trim();
    const bizType = normalizeText(fields[wf.BizType]);
    const groupID = normalizeText(fields[wf.GroupID]);
    const day = normalizeDay(fields[wf.Date]);
    if (!recordID || !bizType || !groupID || !day) continue;
    if (bizTypeFilters.length > 0 && !bizTypeFilters.includes(bizType)) continue;
    if (dayFilters.length > 0 && !dayFilters.includes(day)) continue;
    parsedRows.push({
      recordID,
      bizType,
      day,
      groupID,
      updateAtMs: parseMs(fields[wf.UpdateAt]),
    });
  }

  const grouped = new Map<string, PlanRow[]>();
  for (const row of parsedRows) {
    const key = makeKey(row);
    const arr = grouped.get(key) || [];
    arr.push(row);
    grouped.set(key, arr);
  }

  const duplicateGroups: Array<{
    key: string;
    canonical: PlanRow;
    duplicates: PlanRow[];
  }> = [];
  for (const [key, arr] of grouped.entries()) {
    if (arr.length <= 1) continue;
    const sorted = arr.slice().sort(comparePlanRow);
    const canonical = sorted[sorted.length - 1];
    const duplicates = sorted.slice(0, -1);
    duplicateGroups.push({ key, canonical, duplicates });
  }
  duplicateGroups.sort((a, b) => b.duplicates.length - a.duplicates.length);

  const recordIDsToDelete: string[] = [];
  for (const group of duplicateGroups) {
    for (const row of group.duplicates) {
      recordIDsToDelete.push(row.recordID);
    }
  }

  if (!dryRun && recordIDsToDelete.length > 0) {
    await batchDelete(ctx, webhookURL, recordIDsToDelete);
  }

  const sample = duplicateGroups.slice(0, sampleSize).map((g) => ({
    key: g.key,
    total: g.duplicates.length + 1,
    canonical_record_id: g.canonical.recordID,
    duplicate_record_ids: g.duplicates.map((x) => x.recordID),
  }));
  const summary = {
    dry_run: dryRun,
    input_filters: {
      biz_types: bizTypeFilters,
      days: dayFilters,
    },
    scanned_rows: rows.length,
    valid_rows: parsedRows.length,
    scan_limit: scanLimit,
    grouped_keys: grouped.size,
    duplicate_keys: duplicateGroups.length,
    duplicate_rows: duplicateGroups.reduce((acc, g) => acc + g.duplicates.length, 0),
    rows_to_delete: recordIDsToDelete.length,
    deleted: dryRun ? 0 : recordIDsToDelete.length,
    sample,
    warning: rows.length >= scanLimit ? "scan limit reached; results may be partial" : "",
  };
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
