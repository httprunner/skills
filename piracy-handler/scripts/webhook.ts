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

  const rows = await listPendingOrFailedRows(day, bizType, limit);
  const results = [] as any[];

  for (const row of rows) {
    if (!row.groupID) continue;
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
    results.push(result);
  }

  const summary = {
    mode: "reconcile",
    day,
    biz_type: bizType,
    scanned: rows.length,
    processed: results.length,
    success: results.filter((r) => r.status === "success").length,
    failed: results.filter((r) => r.status === "failed").length,
    error: results.filter((r) => r.status === "error").length,
    pending: results.filter((r) => r.status === "pending").length,
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
