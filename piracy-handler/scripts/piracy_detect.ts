#!/usr/bin/env node
import { Command } from "commander";
import { parsePositiveInt, toNumber } from "./shared/lib";
import { resolveDetectTaskUnits } from "./detect/task_units";
import { runDetectForUnits } from "./detect/runner";
import { buildResultSourceOptionsFromCLI } from "./data/result_source_cli";

type CLIOptions = {
  taskIds?: string;
  fromFeishu?: boolean;
  taskApp?: string;
  taskScene: string;
  taskStatus: string;
  taskDate: string;
  taskLimit: string;
  parentTaskId?: string;

  dataSource: string;
  dbPath: string;
  table: string;
  pageSize: string;
  timeoutMs: string;

  threshold: string;
  output?: string;
  app?: string;
  bookId?: string;
  date?: string;

  taskId?: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("piracy_detect")
    .description("Piracy clustering and threshold detection (sqlite/supabase)")
    .option("--task-ids <csv>", "Comma-separated TaskID list (single or multiple)")
    .option("--from-feishu", "Select parent tasks from task-status Bitable, then merge by BookID")
    .option("--task-app <app>", "Feishu task filter: App")
    .option("--task-scene <scene>", "Feishu task filter: Scene", "综合页搜索")
    .option("--task-status <status>", "Feishu task filter: Status", "success")
    .option("--task-date <date>", "Feishu task filter: Date preset/value (Today/Yesterday/Any/YYYY-MM-DD)", "Today")
    .option("--task-limit <n>", "Feishu task fetch limit (0 = no cap)", "0")
    .option("--parent-task-id <id>", "Parent task TaskID used in detect output (default: first task id)")
    .option("--task-id <id>", "[deprecated] single TaskID, equal to --task-ids <id>")

    .option("--data-source <type>", "Data source: sqlite|supabase", "sqlite")
    .option("--db-path <path>", "SQLite path", "~/.eval/records.sqlite")
    .option("--table <name>", "Supabase table", "capture_results")
    .option("--page-size <n>", "Supabase page size", "1000")
    .option("--timeout-ms <n>", "Supabase timeout (ms)", "30000")

    .option("--threshold <num>", "Threshold ratio", "0.5")
    .option("--output <path>", "Write JSON to file; use - for stdout (single unit only)")
    .option("--app <app>", "Override parent task app")
    .option("--book-id <id>", "Override parent task book ID")
    .option("--date <yyyy-mm-dd>", "Override capture day")
    .showHelpAfterError()
    .showSuggestionAfterError();

  program.parse(argv);
  return program.opts<CLIOptions>();
}

function parseOptionalLegacyTaskIDs(args: CLIOptions): string | undefined {
  const taskIDs = String(args.taskIds || "").trim();
  if (taskIDs) return taskIDs;
  const taskID = String(args.taskId || "").trim();
  if (!taskID) return undefined;
  const n = parsePositiveInt(taskID, "task id");
  process.stderr.write(`[piracy-handler] --task-id is deprecated, use --task-ids ${n}\n`);
  return String(n);
}

async function main() {
  const args = parseCLI(process.argv);
  const threshold = toNumber(args.threshold, 0.5);
  const resultSource = buildResultSourceOptionsFromCLI(args);
  const taskIds = parseOptionalLegacyTaskIDs(args);

  const units = resolveDetectTaskUnits({
    taskIds,
    fromFeishu: Boolean(args.fromFeishu),
    taskApp: args.taskApp,
    taskScene: args.taskScene,
    taskStatus: args.taskStatus,
    taskDate: args.taskDate,
    taskLimit: args.taskLimit,
    parentTaskId: args.parentTaskId,
    app: args.app,
    bookId: args.bookId,
    date: args.date,
  });

  const output = String(args.output || "").trim();
  if (output === "-" && units.length !== 1) {
    throw new Error("--output - is only supported when exactly one detect unit is produced");
  }

  const results = await runDetectForUnits({
    units,
    threshold,
    output,
    resultSource,
  });

  if (output === "-") {
    process.stdout.write(`${JSON.stringify(results[0].detect, null, 2)}\n`);
    return;
  }

  for (const r of results) {
    const summary = (r.detect.summary || {}) as Record<string, any>;
    console.log("----------------------------------------");
    console.log(`PIRACY DETECT SUMMARY (ParentTaskID: ${r.unit.parentTaskID})`);
    console.log("----------------------------------------");
    console.log(`TaskIDs:             ${r.unit.taskIDs.join(",")}`);
    console.log(`Data Source:         ${resultSource.dataSource}`);
    console.log(`Rows Read:           ${r.rowCount}`);
    console.log(`Threshold:           ${threshold}`);
    console.log(`Capture Day:         ${r.unit.day}`);
    console.log(`Groups Selected:     ${summary.groups_above_threshold ?? 0}`);
    if (Array.isArray(summary.unresolved_task_ids) && summary.unresolved_task_ids.length > 0) {
      console.log(`Unresolved Tasks:    ${summary.unresolved_task_ids.length}`);
    }
    if (Array.isArray(summary.missing_drama_meta_book_ids) && summary.missing_drama_meta_book_ids.length > 0) {
      console.log(`Missing Meta:        ${summary.missing_drama_meta_book_ids.length} books`);
    }
    if (Array.isArray(summary.invalid_drama_duration_book_ids) && summary.invalid_drama_duration_book_ids.length > 0) {
      console.log(`Invalid Duration:    ${summary.invalid_drama_duration_book_ids.length} books`);
    }
    console.log(`Output File:         ${r.outputPath}`);
  }

  if (results.length > 1) {
    const totalRows = results.reduce((acc, x) => acc + x.rowCount, 0);
    const totalSelected = results.reduce((acc, x) => acc + Number((x.detect.summary as any)?.groups_above_threshold || 0), 0);
    console.log("========================================");
    console.log("PIRACY DETECT BATCH SUMMARY");
    console.log("========================================");
    console.log(`Units:               ${results.length}`);
    console.log(`Rows Read:           ${totalRows}`);
    console.log(`Groups Selected:     ${totalSelected}`);
  }
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
