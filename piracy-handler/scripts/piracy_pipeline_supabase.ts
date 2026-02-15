#!/usr/bin/env node
import { Command } from "commander";
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import {
  defaultDetectPath,
  ensureDir,
  env,
  expandHome,
  must,
  parsePositiveInt,
  runTaskFetch,
  toDay,
  toNumber,
} from "./lib";
import { buildDetectOutput } from "./piracy_detect_core";

type CLIOptions = {
  taskIds: string;
  parentTaskId?: string;
  threshold: string;
  output?: string;
  table: string;
  pageSize: string;
  timeoutMs: string;
  dryRun: boolean;
  skipCreateSubtasks: boolean;
  skipUpsertWebhookPlans: boolean;
  bizType: string;
  app?: string;
  bookId?: string;
  date?: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("piracy_pipeline_supabase")
    .description("Read capture_results from Supabase for multiple task IDs, run piracy detect, create subtasks and webhook plans")
    .requiredOption("--task-ids <csv>", "Comma-separated TaskID list, e.g. 69111,69112,69113")
    .option("--parent-task-id <id>", "Parent task TaskID used in detect output (default: first task id)")
    .option("--threshold <num>", "Threshold ratio", "0.5")
    .option("--output <path>", "Detect output path (default: ~/.eval/<ParentTaskID>/detect.json)")
    .option("--table <name>", "Supabase table name", env("SUPABASE_RESULT_TABLE", "capture_results"))
    .option("--page-size <n>", "Supabase page size", "1000")
    .option("--timeout-ms <n>", "HTTP timeout in milliseconds", "30000")
    .option("--biz-type <name>", "Webhook BizType", "piracy_general_search")
    .option("--app <app>", "Override parent task app")
    .option("--book-id <id>", "Override parent task book id")
    .option("--date <yyyy-mm-dd>", "Override capture day")
    .option("--dry-run", "Compute and print, skip writes in create/upsert")
    .option("--skip-create-subtasks", "Skip creating child tasks")
    .option("--skip-upsert-webhook-plans", "Skip upserting webhook plans")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  return program.opts<CLIOptions>();
}

function parseTaskIDs(csv: string): number[] {
  const ids = String(csv || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean)
    .map((x) => parsePositiveInt(x, "task id"));
  const dedup = Array.from(new Set(ids));
  if (!dedup.length) throw new Error("--task-ids is empty");
  return dedup;
}

function getSupabaseEnv() {
  const baseURL = must("SUPABASE_URL").replace(/\/+$/, "");
  const serviceRoleKey = must("SUPABASE_SERVICE_ROLE_KEY");
  return { baseURL, serviceRoleKey };
}

async function fetchSupabaseRowsByTaskIDs(
  baseURL: string,
  serviceRoleKey: string,
  table: string,
  taskIDs: number[],
  pageSize: number,
  timeoutMs: number,
): Promise<Array<Record<string, unknown>>> {
  const all: Array<Record<string, unknown>> = [];
  let offset = 0;
  const taskFilter = taskIDs.join(",");

  while (true) {
    const qs = new URLSearchParams();
    qs.set("select", "*");
    qs.set("task_id", `in.(${taskFilter})`);
    qs.set("order", "id.asc");
    qs.set("limit", String(pageSize));
    qs.set("offset", String(offset));

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const url = `${baseURL}/rest/v1/${encodeURIComponent(table)}?${qs.toString()}`;

    const resp = await fetch(url, {
      method: "GET",
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
      },
      signal: controller.signal,
    }).finally(() => clearTimeout(timer));

    if (!resp.ok) {
      const body = await resp.text();
      throw new Error(`supabase query failed: status=${resp.status} body=${body}`);
    }

    const rows = (await resp.json()) as Array<Record<string, unknown>>;
    all.push(...rows);

    if (rows.length < pageSize) break;
    offset += pageSize;
  }

  return all;
}

function runLocalScript(scriptName: string, args: string[]) {
  const run = spawnSync("npx", ["tsx", `scripts/${scriptName}`, ...args], {
    cwd: path.resolve(__dirname, ".."),
    env: process.env,
    encoding: "utf-8",
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) {
    throw new Error(`${scriptName} failed: ${run.stderr || run.stdout || "unknown error"}`);
  }
  if (run.stdout) process.stdout.write(run.stdout);
  if (run.stderr) process.stderr.write(run.stderr);
}

async function main() {
  const args = parseCLI(process.argv);
  const taskIDs = parseTaskIDs(args.taskIds);
  const parentTaskID = args.parentTaskId ? parsePositiveInt(args.parentTaskId, "parent task id") : taskIDs[0];

  const threshold = toNumber(args.threshold, 0.5);
  const pageSize = parsePositiveInt(args.pageSize, "page size");
  const timeoutMs = parsePositiveInt(args.timeoutMs, "timeout ms");

  const dramaURL = must("DRAMA_BITABLE_URL");
  must("FEISHU_APP_ID");
  must("FEISHU_APP_SECRET");

  const parentTasks = runTaskFetch(["--task-id", String(parentTaskID), "--status", "Any", "--date", "Any"]);
  if (!parentTasks.length) throw new Error(`parent task not found: ${parentTaskID}`);

  const parentTask = parentTasks[0];
  const day = String(args.date || toDay(parentTask.date) || new Date().toISOString().slice(0, 10));
  const dayStart = new Date(`${day}T00:00:00`);
  if (Number.isNaN(dayStart.getTime())) throw new Error(`invalid day: ${day}`);
  const dayMs = dayStart.getTime();

  const parent = {
    app: String(args.app || parentTask.app || "").trim(),
    book_id: String(args.bookId || parentTask.book_id || "").trim(),
    params: String(parentTask.params || "").trim(),
  };

  const { baseURL, serviceRoleKey } = getSupabaseEnv();
  const table = String(args.table || "capture_results").trim() || "capture_results";

  const rows = await fetchSupabaseRowsByTaskIDs(baseURL, serviceRoleKey, table, taskIDs, pageSize, timeoutMs);

  const detect = buildDetectOutput({
    parentTaskID,
    threshold,
    day,
    dayMs,
    parent,
    rawRows: rows,
    dramaURL,
    sourcePath: `supabase:${table}`,
  });

  const outArg = String(args.output || "").trim();
  const outPath = expandHome(outArg || defaultDetectPath(parentTaskID));
  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(detect, null, 2));

  const summary = detect.summary || {};
  console.log("----------------------------------------");
  console.log(`PIRACY PIPELINE SUMMARY (ParentTaskID: ${parentTaskID})`);
  console.log("----------------------------------------");
  console.log(`TaskIDs:             ${taskIDs.join(",")}`);
  console.log(`Rows from Supabase:  ${rows.length}`);
  console.log(`Groups Selected:     ${summary.groups_above_threshold ?? 0}`);
  console.log(`Output File:         ${outPath}`);
  console.log("----------------------------------------");

  if (!args.skipCreateSubtasks) {
    const createArgs = ["--input", outPath];
    if (args.dryRun) createArgs.push("--dry-run");
    runLocalScript("piracy_create_subtasks.ts", createArgs);
  }

  if (!args.skipUpsertWebhookPlans) {
    const upsertArgs = ["--input", outPath, "--biz-type", String(args.bizType || "piracy_general_search")];
    if (args.dryRun) upsertArgs.push("--dry-run");
    runLocalScript("piracy_upsert_webhook_plans.ts", upsertArgs);
  }
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
