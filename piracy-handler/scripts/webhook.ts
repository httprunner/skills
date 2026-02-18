#!/usr/bin/env node
import { Command } from "commander";
import { parseLimit, parseOptionalPositiveInt } from "./shared/cli";
import { buildResultSourceOptionsFromCLI } from "./data/result_source_cli";
import { toDay, todayLocal, yesterdayLocal } from "./shared/lib";
import { listPendingOrFailedRows, processOneGroup, resolveGroupFromTaskID } from "./webhook/lib";

type CLIOptions = {
  mode: string;
  taskId?: string;
  groupId?: string;
  date?: string;
  bizType: string;
  limit: string;
  dryRun: boolean;
  dataSource: string;
  sqlitePath?: string;
  table?: string;
  pageSize?: string;
  timeoutMs?: string;
  maxRetries?: string;
  logLevel?: string;
};

type WebhookMode = "single" | "reconcile";

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("webhook")
    .description("Webhook dispatch/reconcile entry")
    .option("--mode <type>", "Mode: auto|single|reconcile", "auto")
    .option("--task-id <id>", "TaskID used to resolve group/day (single mode)")
    .option("--group-id <id>", "GroupID to dispatch (single mode)")
    .option("--date <date>", "Capture day; supports CSV and Today/Yesterday presets (default: today)")
    .option("--biz-type <name>", "BizType", "piracy_general_search")
    .option("--limit <num>", "Max rows to reconcile (reconcile mode)", "50")
    .option("--dry-run", "Compute only, do not call webhook or write back")
    .option("--data-source <type>", "Result source: sqlite|supabase", "sqlite")
    .option("--sqlite-path <path>", "SQLite path override")
    .option("--table <name>", "Supabase table name")
    .option("--page-size <n>", "Supabase page size", "1000")
    .option("--timeout-ms <n>", "Supabase timeout in milliseconds", "30000")
    .option("--max-retries <num>", "Max retries override")
    .option("--log-level <level>", "Log level: trace|debug|info|warn|error", "info")
    .showHelpAfterError()
    .showSuggestionAfterError();

  program.parse(argv);
  return program.opts<CLIOptions>();
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

function resolveDateToken(token: string): string {
  const raw = String(token || "").trim();
  if (!raw) return "";
  const lower = raw.toLowerCase();
  if (lower === "today") return todayLocal();
  if (lower === "yesterday") return yesterdayLocal();
  return toDay(raw) || raw;
}

function parseDateFilters(raw: string | undefined): string[] {
  const tokens = parseCSV(raw || "");
  if (!tokens.length) return [todayLocal()];
  const out: string[] = [];
  for (const token of tokens) {
    const day = resolveDateToken(token);
    if (!day) continue;
    if (!out.includes(day)) out.push(day);
  }
  if (!out.length) throw new Error(`invalid --date: ${raw}`);
  return out;
}

function resolveMode(args: CLIOptions): WebhookMode {
  const modeRaw = String(args.mode || "auto").trim().toLowerCase();
  const hasSingleHint = Boolean(String(args.taskId || "").trim() || String(args.groupId || "").trim());
  if (modeRaw === "auto") return hasSingleHint ? "single" : "reconcile";
  if (modeRaw === "single" || modeRaw === "reconcile") return modeRaw;
  throw new Error(`invalid --mode: ${args.mode}; expected auto|single|reconcile`);
}

async function runSingle(args: CLIOptions) {
  const taskIDRaw = String(args.taskId || "").trim();
  const groupIDRaw = String(args.groupId || "").trim();
  const dryRun = Boolean(args.dryRun);
  const source = buildResultSourceOptionsFromCLI(args);
  const maxRetries = parseOptionalPositiveInt(args.maxRetries, "--max-retries");

  let groupID = groupIDRaw;
  let dateFilter = String(args.date || "").trim();
  let bizType = String(args.bizType).trim();

  if (!groupID && taskIDRaw) {
    const tid = parseOptionalPositiveInt(taskIDRaw, "--task-id");
    if (tid == null) throw new Error(`invalid --task-id: ${taskIDRaw}`);
    const resolved = await resolveGroupFromTaskID(tid);
    if (!resolved.groupID) throw new Error(`group not found for --task-id: ${tid}`);
    groupID = resolved.groupID;
    if (!dateFilter) dateFilter = resolved.day;
    if (!bizType) bizType = resolved.bizType;
  }

  if (!groupID) throw new Error("single mode requires --group-id or --task-id");
  const days = parseDateFilters(dateFilter);

  const results = [] as any[];
  for (const d of days) {
    const result = await processOneGroup({
      groupID,
      day: d,
      bizType,
      dryRun,
      dataSource: source.dataSource,
      dbPath: source.dbPath,
      table: source.supabaseTable,
      pageSize: source.pageSize,
      timeoutMs: source.timeoutMs,
      maxRetries,
    });
    results.push(result);
  }

  if (results.length === 1) {
    console.log(JSON.stringify(results[0], null, 2));
    return;
  }

  console.log(
    JSON.stringify(
      {
        mode: "single",
        group_id: groupID,
        day: days.length === 1 ? days[0] : "",
        days,
        biz_type: bizType,
        dry_run: dryRun,
        processed: results.length,
        results,
      },
      null,
      2,
    ),
  );
}

async function runReconcile(args: CLIOptions) {
  const days = parseDateFilters(args.date);
  const bizType = String(args.bizType || "piracy_general_search").trim();
  const limit = parseLimit(String(args.limit), "--limit");
  const dryRun = Boolean(args.dryRun);
  const source = buildResultSourceOptionsFromCLI(args);
  const maxRetries = parseOptionalPositiveInt(args.maxRetries, "--max-retries");

  const results = [] as any[];
  let successCount = 0;
  let failedCount = 0;
  let errorCount = 0;
  let pendingCount = 0;
  let scanned = 0;

  for (const day of days) {
    console.error(`[reconcile] date=${day} bizType=${bizType} limit=${limit} dryRun=${dryRun} source=${source.dataSource}`);
    const rows = await listPendingOrFailedRows(day, bizType, limit);
    scanned += rows.length;
    console.error(`[reconcile] found ${rows.length} pending/failed rows to process for date=${day}`);

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (!row.groupID) continue;

      const idx = i + 1;
      console.error(`[reconcile] [${idx}/${rows.length}] processing group: ${row.groupID}`);

      const result = await processOneGroup({
        groupID: row.groupID,
        day: row.day || day,
        bizType: row.bizType || bizType,
        dryRun,
        dataSource: source.dataSource,
        dbPath: source.dbPath,
        table: source.supabaseTable,
        pageSize: source.pageSize,
        timeoutMs: source.timeoutMs,
        maxRetries,
      });

      if (result.status === "success") successCount++;
      else if (result.status === "failed") failedCount++;
      else if (result.status === "error") errorCount++;
      else pendingCount++;

      const statusIcon = result.status === "success" ? "✓" : result.status === "failed" ? "✗" : result.status === "error" ? "!" : "?";
      console.error(
        `[reconcile] [${idx}/${rows.length}] ${statusIcon} ${row.groupID} => ${result.status} (ready=${result.ready}, pushed=${result.pushed})${result.reason ? ` reason=${result.reason}` : ""}`,
      );

      results.push(result);
    }
  }

  console.error(`[reconcile] done: success=${successCount} failed=${failedCount} error=${errorCount} pending=${pendingCount}`);

  const summary = {
    mode: "reconcile",
    day: days.length === 1 ? days[0] : "",
    days,
    biz_type: bizType,
    scanned,
    processed: results.length,
    success: successCount,
    failed: failedCount,
    error: errorCount,
    pending: pendingCount,
    dry_run: dryRun,
    results,
  };
  console.log(JSON.stringify(summary, null, 2));
}

async function main() {
  const args = parseCLI(process.argv);
  const logLevel = String(args.logLevel || "info").trim().toLowerCase();
  process.env.LOG_LEVEL = logLevel;
  const mode = resolveMode(args);
  if (mode === "single") return runSingle(args);
  return runReconcile(args);
}

main().catch((err) => {
  console.error(`[piracy-handler] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
