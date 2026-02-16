#!/usr/bin/env node
import { Command } from "commander";
import { parseLimit, parseOptionalPositiveInt } from "./shared/cli";
import { buildResultSourceOptionsFromCLI } from "./data/result_source_cli";
import { todayLocal } from "./shared/lib";
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
    .option("--date <yyyy-mm-dd>", "Capture day (default: today)")
    .option("--biz-type <name>", "BizType", "piracy_general_search")
    .option("--limit <num>", "Max rows to reconcile (reconcile mode)", "50")
    .option("--dry-run", "Compute only, do not call webhook or write back")
    .option("--data-source <type>", "Result source: sqlite|supabase", "sqlite")
    .option("--sqlite-path <path>", "SQLite path override")
    .option("--table <name>", "Supabase table name")
    .option("--page-size <n>", "Supabase page size", "1000")
    .option("--timeout-ms <n>", "Supabase timeout in milliseconds", "30000")
    .option("--max-retries <num>", "Max retries override")
    .showHelpAfterError()
    .showSuggestionAfterError();

  program.parse(argv);
  return program.opts<CLIOptions>();
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
  let day = String(args.date || "").trim();
  let bizType = String(args.bizType).trim();

  if (!groupID && taskIDRaw) {
    const tid = parseOptionalPositiveInt(taskIDRaw, "--task-id");
    if (tid == null) throw new Error(`invalid --task-id: ${taskIDRaw}`);
    const resolved = await resolveGroupFromTaskID(tid);
    if (!resolved.groupID) throw new Error(`group not found for --task-id: ${tid}`);
    groupID = resolved.groupID;
    if (!day) day = resolved.day;
    if (!bizType) bizType = resolved.bizType;
  }

  if (!groupID) throw new Error("single mode requires --group-id or --task-id");
  if (!day) day = todayLocal();

  const result = await processOneGroup({
    groupID,
    day,
    bizType,
    dryRun,
    dataSource: source.dataSource,
    dbPath: source.dbPath,
    table: source.supabaseTable,
    pageSize: source.pageSize,
    timeoutMs: source.timeoutMs,
    maxRetries,
  });
  console.log(JSON.stringify(result, null, 2));
}

async function runReconcile(args: CLIOptions) {
  const day = String(args.date || "").trim() || todayLocal();
  const bizType = String(args.bizType || "piracy_general_search").trim();
  const limit = parseLimit(String(args.limit), "--limit");
  const dryRun = Boolean(args.dryRun);
  const source = buildResultSourceOptionsFromCLI(args);
  const maxRetries = parseOptionalPositiveInt(args.maxRetries, "--max-retries");

  console.error(`[reconcile] date=${day} bizType=${bizType} limit=${limit} dryRun=${dryRun} source=${source.dataSource}`);

  const rows = await listPendingOrFailedRows(day, bizType, limit);
  console.error(`[reconcile] found ${rows.length} pending/failed rows to process`);

  const results = [] as any[];
  let successCount = 0;
  let failedCount = 0;
  let errorCount = 0;
  let pendingCount = 0;

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
    console.error(`[reconcile] [${idx}/${rows.length}] ${statusIcon} ${row.groupID} => ${result.status} (ready=${result.ready}, pushed=${result.pushed})${result.reason ? ` reason=${result.reason}` : ""}`);

    results.push(result);
  }

  console.error(`[reconcile] done: success=${successCount} failed=${failedCount} error=${errorCount} pending=${pendingCount}`);

  const summary = {
    mode: "reconcile",
    day,
    biz_type: bizType,
    scanned: rows.length,
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
  const mode = resolveMode(args);
  if (mode === "single") return runSingle(args);
  return runReconcile(args);
}

main().catch((err) => {
  console.error(`[piracy-handler] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
