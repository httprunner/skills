#!/usr/bin/env node
import { Command } from "commander";
import path from "path";
import fs from "fs";
import {
  dayStartMs,
  defaultDetectPath,
  ensureDir,
  expandHome,
  must,
  parsePositiveInt,
  runTaskFetch,
  sqliteJSON,
  toDay,
  toNumber,
} from "./lib";
import { buildDetectOutput } from "./piracy_detect_core";

type CLIOptions = {
  taskId: string;
  dbPath: string;
  threshold: string;
  logLevel: string;
  output?: string;
  app?: string;
  bookId?: string;
  date?: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("piracy_detect")
    .description("SQLite-driven piracy clustering and threshold detection (no writes)")
    .requiredOption("--task-id <id>", "Parent task TaskID (general search task)")
    .option("--db-path <path>", "SQLite path", "~/.eval/records.sqlite")
    .option("--threshold <num>", "Threshold ratio", "0.5")
    .option("--log-level <level>", "Log level: silent|error|info|debug", "info")
    .option("--output <path>", "Write JSON to file; use - for stdout (default: ~/.eval/<TaskID>/detect.json)")
    .option("--app <app>", "Override parent task app")
    .option("--book-id <id>", "Override parent task book ID")
    .option("--date <yyyy-mm-dd>", "Override capture day")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  return program.opts<CLIOptions>();
}

type LogLevel = "silent" | "error" | "info" | "debug";
function parseLogLevel(raw: any): LogLevel {
  const v = String(raw || "info").trim().toLowerCase();
  if (v === "silent" || v === "error" || v === "info" || v === "debug") return v;
  return "info";
}

function createLogger(level: LogLevel, stream: NodeJS.WriteStream) {
  const rank: Record<LogLevel, number> = { silent: 0, error: 1, info: 2, debug: 3 };
  const can = (want: LogLevel) => rank[level] >= rank[want];
  const write = (want: LogLevel, msg: string, extra?: Record<string, unknown>) => {
    if (!can(want)) return;
    const payload: Record<string, unknown> = {
      time: new Date().toISOString(),
      level: want.toUpperCase(),
      mod: "piracy-handler",
      msg,
      ...(extra || {}),
    };
    stream.write(`${JSON.stringify(payload)}\n`);
  };
  return {
    error: (msg: string, extra?: Record<string, unknown>) => write("error", msg, extra),
    info: (msg: string, extra?: Record<string, unknown>) => write("info", msg, extra),
    debug: (msg: string, extra?: Record<string, unknown>) => write("debug", msg, extra),
  };
}

function sqliteTableColumns(dbPath: string, table: string): string[] {
  const rows = sqliteJSON(dbPath, `PRAGMA table_info(${table});`);
  return rows
    .map((r) => String(r?.name || "").trim())
    .filter(Boolean);
}

const TASK_ID_CANDIDATE_FIELDS = ["TaskID", "task_id"] as const;

async function main() {
  const args = parseCLI(process.argv);
  const taskID = parsePositiveInt(args.taskId, "task id");
  const threshold = toNumber(args.threshold, 0.5);
  const logLevel = parseLogLevel(args.logLevel);
  const logger = createLogger(logLevel, process.stderr);
  const dbPath = expandHome(String(args.dbPath || "~/.eval/records.sqlite"));

  must("FEISHU_APP_ID");
  must("FEISHU_APP_SECRET");
  const dramaURL = must("DRAMA_BITABLE_URL");

  const parentTasks = runTaskFetch(["--task-id", String(taskID), "--status", "Any", "--date", "Any"]);
  if (!parentTasks.length) throw new Error(`parent task not found: ${taskID}`);
  const parentTask = parentTasks[0];
  const parentApp = String(args.app || parentTask.app || "").trim();
  const parentBookID = String(args.bookId || parentTask.book_id || "").trim();
  const parentParams = String(parentTask.params || "").trim();
  const day = String(args.date || toDay(parentTask.date) || new Date().toISOString().slice(0, 10));
  const dayMs = dayStartMs(day);
  if (!dayMs) throw new Error(`invalid day: ${day}`);

  logger.info("detect started", { task_id: taskID, threshold, day, db_path: dbPath });

  const captureCols = new Set(sqliteTableColumns(dbPath, "capture_results"));
  const taskIDCols = TASK_ID_CANDIDATE_FIELDS.filter((name) => captureCols.has(name));
  if (!taskIDCols.length) throw new Error("capture_results missing task id column: expected TaskID or task_id");
  const taskIDExpr =
    taskIDCols.length === 1
      ? `CAST(COALESCE(${taskIDCols[0]}, 0) AS INTEGER)`
      : `CAST(COALESCE(${taskIDCols.join(", ")}, 0) AS INTEGER)`;
  const rawRows = sqliteJSON(dbPath, `SELECT * FROM capture_results WHERE ${taskIDExpr} = ${taskID};`);

  const output = buildDetectOutput({
    parentTaskID: taskID,
    threshold,
    day,
    dayMs,
    parent: { app: parentApp, book_id: parentBookID, params: parentParams },
    rawRows,
    dramaURL,
    sourcePath: dbPath,
    logger,
  });

  const payload = JSON.stringify(output, null, 2);
  const outArg = String(args.output || "").trim();
  if (outArg === "-") {
    process.stdout.write(payload + "\n");
    return;
  }
  const outPath = expandHome(outArg || defaultDetectPath(taskID));
  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, payload);

  const summary = (output.summary || {}) as Record<string, any>;
  console.log("----------------------------------------");
  console.log(`PIRACY DETECT SUMMARY (TaskID: ${taskID})`);
  console.log("----------------------------------------");
  console.log(`DB Path:             ${dbPath}`);
  console.log(`Threshold:           ${threshold}`);
  console.log(`Capture Day:         ${day}`);
  console.log(`SQLite Rows:         ${rawRows.length}`);
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
  console.log("----------------------------------------");
  console.log(`Output File:         ${outPath}`);
  console.log("----------------------------------------");
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
