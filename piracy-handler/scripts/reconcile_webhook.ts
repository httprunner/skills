#!/usr/bin/env node
import { Command } from "commander";
import { parseLimit, parseOptionalPositiveInt } from "./shared/cli";
import { listPendingOrFailedRows, processOneGroup } from "./webhook/lib";
import { buildResultSourceOptionsFromCLI } from "./data/result_source_cli";

type CLIOptions = {
  date: string;
  bizType: string;
  limit: string;
  dryRun: boolean;
  dataSource: string;
  dbPath?: string;
  table?: string;
  pageSize?: string;
  timeoutMs?: string;
  maxRetries?: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("reconcile_webhook")
    .description("Run one-shot reconcile for pending/failed group webhooks")
    .option("--date <yyyy-mm-dd>", "Capture day (default: today)", new Date().toISOString().slice(0, 10))
    .option("--biz-type <name>", "BizType", "piracy_general_search")
    .option("--limit <num>", "Max rows to reconcile", "50")
    .option("--dry-run", "Compute only, do not call webhook or write back")
    .option("--data-source <type>", "Result source: sqlite|supabase", "sqlite")
    .option("--db-path <path>", "SQLite path override")
    .option("--table <name>", "Supabase table name")
    .option("--page-size <n>", "Supabase page size", "1000")
    .option("--timeout-ms <n>", "Supabase timeout in milliseconds", "30000")
    .option("--max-retries <num>", "Max retries override")
    .showHelpAfterError()
    .showSuggestionAfterError();

  program.parse(argv);
  return program.opts<CLIOptions>();
}

async function main() {
  const args = parseCLI(process.argv);
  const day = String(args.date).trim();
  const bizType = String(args.bizType || "piracy_general_search").trim();
  const limit = parseLimit(String(args.limit), "--limit");
  const dryRun = Boolean(args.dryRun);
  const source = buildResultSourceOptionsFromCLI(args);

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
      maxRetries: parseOptionalPositiveInt(args.maxRetries, "--max-retries"),
    });
    results.push(result);
  }

  const summary = {
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

main().catch((err) => {
  console.error(`[piracy-handler] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
