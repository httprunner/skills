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
  taskIds?: string;
  fromFeishu?: boolean;
  taskApp?: string;
  taskScene: string;
  taskStatus: string;
  taskDate: string;
  taskLimit: string;
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
    .option("--task-ids <csv>", "Comma-separated TaskID list, e.g. 69111,69112,69113")
    .option("--from-feishu", "Select parent tasks from task-status Bitable, then merge by BookID")
    .option("--task-app <app>", "Feishu task filter: App")
    .option("--task-scene <scene>", "Feishu task filter: Scene", "综合页搜索")
    .option("--task-status <status>", "Feishu task filter: Status", "success")
    .option("--task-date <date>", "Feishu task filter: Date preset/value (e.g. Today/Yesterday/2026-02-15)", "Today")
    .option("--task-limit <n>", "Feishu task fetch limit (0 = no cap)", "0")
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

function parseNonNegativeInt(raw: string, flag: string): number {
  const n = Math.trunc(Number(String(raw ?? "").trim()));
  if (!Number.isFinite(n) || n < 0) throw new Error(`invalid ${flag}: ${raw}`);
  return n;
}

type PipelineUnit = {
  parentTaskID: number;
  taskIDs: number[];
  day: string;
  parent: {
    app: string;
    book_id: string;
    params: string;
  };
};

function uniqueNumbers(values: number[]): number[] {
  return Array.from(new Set(values));
}

function normalizeText(v: string): string {
  return String(v || "").trim().toLowerCase();
}

function resolveDateFilter(v: string): string {
  const trimmed = String(v || "").trim();
  if (!trimmed) return "today";
  const lower = trimmed.toLowerCase();
  if (lower === "any") return "any";
  if (lower === "today") return new Date().toISOString().slice(0, 10);
  if (lower === "yesterday") {
    const now = new Date();
    const y = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    return y.toISOString().slice(0, 10);
  }
  return toDay(trimmed) || trimmed;
}

function resolveTaskIDGroups(args: CLIOptions): PipelineUnit[] {
  const hasTaskIDs = String(args.taskIds || "").trim() !== "";
  const useFeishu = Boolean(args.fromFeishu) || !hasTaskIDs;

  if (!useFeishu) {
    const taskIDs = parseTaskIDs(String(args.taskIds || ""));
    const parentTaskID = args.parentTaskId ? parsePositiveInt(args.parentTaskId, "parent task id") : taskIDs[0];
    const parentTasks = runTaskFetch(["--task-id", String(parentTaskID), "--status", "Any", "--date", "Any"]);
    if (!parentTasks.length) throw new Error(`parent task not found: ${parentTaskID}`);
    const parentTask = parentTasks[0];
    const day = String(args.date || toDay(parentTask.date) || new Date().toISOString().slice(0, 10));
    return [
      {
        parentTaskID,
        taskIDs,
        day,
        parent: {
          app: String(args.app || parentTask.app || "").trim(),
          book_id: String(args.bookId || parentTask.book_id || "").trim(),
          params: String(parentTask.params || "").trim(),
        },
      },
    ];
  }

  const taskApp = String(args.taskApp || "").trim();
  const taskScene = String(args.taskScene || "综合页搜索").trim() || "综合页搜索";
  const taskStatus = String(args.taskStatus || "success").trim() || "success";
  const taskDate = String(args.taskDate || "Today").trim() || "Today";
  const taskLimit = parseNonNegativeInt(String(args.taskLimit || "0"), "task limit");
  if (!taskApp) throw new Error("--task-app is required when using --from-feishu (or when --task-ids is absent)");

  // Fetch with remote filters first to keep dataset small, then filter locally as a safety net.
  const fetchArgs = [
    "--app",
    taskApp,
    "--scene",
    taskScene,
    "--status",
    taskStatus,
    "--date",
    taskDate,
  ];
  if (taskLimit > 0) {
    fetchArgs.push("--limit", String(taskLimit));
  }
  const fetchedTasks = runTaskFetch(fetchArgs);
  const expectDate = resolveDateFilter(taskDate);
  const expectStatus = normalizeText(taskStatus);
  const expectScene = normalizeText(taskScene);
  const filteredTasks = fetchedTasks.filter((task) => {
    const appOK = normalizeText(String(task.app || "")) === normalizeText(taskApp);
    if (!appOK) return false;

    if (expectScene !== "any" && normalizeText(String(task.scene || "")) !== expectScene) return false;
    if (expectStatus !== "any" && normalizeText(String(task.status || "")) !== expectStatus) return false;

    if (expectDate !== "any") {
      const actualDay = toDay(String(task.date || ""));
      if (!actualDay || actualDay !== expectDate) return false;
    }
    return true;
  });
  const tasks = taskLimit > 0 ? filteredTasks.slice(0, taskLimit) : filteredTasks;
  if (!tasks.length) {
    throw new Error(
      `no tasks matched from feishu after local filter: app=${taskApp}, scene=${taskScene}, status=${taskStatus}, date=${taskDate}, limit=${taskLimit}, fetched=${fetchedTasks.length}`,
    );
  }

  const byBookID = new Map<string, ReturnType<typeof runTaskFetch>>();
  for (const task of tasks) {
    const bookID = String(task.book_id || "").trim();
    if (!bookID) continue;
    const bucket = byBookID.get(bookID) || [];
    bucket.push(task);
    byBookID.set(bookID, bucket);
  }
  if (!byBookID.size) {
    throw new Error("none of fetched tasks has non-empty BookID; cannot group for piracy detection");
  }

  const units: PipelineUnit[] = [];
  for (const [bookID, bucket] of byBookID.entries()) {
    const ids = uniqueNumbers(bucket.map((x) => parsePositiveInt(String(x.task_id), "task id")));
    if (!ids.length) continue;
    const sortedIDs = [...ids].sort((a, b) => a - b);
    const chosenParent = sortedIDs[0];
    const parentTask = bucket.find((x) => Number(x.task_id) === chosenParent) || bucket[0];
    const day = String(args.date || toDay(parentTask.date) || new Date().toISOString().slice(0, 10));
    units.push({
      parentTaskID: chosenParent,
      taskIDs: sortedIDs,
      day,
      parent: {
        app: String(args.app || parentTask.app || taskApp || "").trim(),
        book_id: String(args.bookId || bookID || "").trim(),
        params: String(parentTask.params || "").trim(),
      },
    });
  }
  units.sort((a, b) => a.parentTaskID - b.parentTaskID);
  return units;
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

function sanitizeForFilename(v: string): string {
  return String(v || "")
    .trim()
    .replace(/[^\w.-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 64);
}

function resolveOutputPath(baseOutputArg: string | undefined, unit: PipelineUnit, multi: boolean): string {
  const outArg = String(baseOutputArg || "").trim();
  if (!outArg) return expandHome(defaultDetectPath(unit.parentTaskID));

  const expanded = expandHome(outArg);
  if (!multi) return expanded;

  const parsed = path.parse(expanded);
  const bookSuffix = sanitizeForFilename(unit.parent.book_id) || "book";
  if (parsed.ext.toLowerCase() === ".json") {
    return path.join(parsed.dir, `${parsed.name}_${unit.parentTaskID}_${bookSuffix}.json`);
  }
  return path.join(expanded, `${unit.parentTaskID}_${bookSuffix}.json`);
}

async function main() {
  const args = parseCLI(process.argv);
  const threshold = toNumber(args.threshold, 0.5);
  const pageSize = parsePositiveInt(args.pageSize, "page size");
  const timeoutMs = parsePositiveInt(args.timeoutMs, "timeout ms");

  const dramaURL = must("DRAMA_BITABLE_URL");
  must("FEISHU_APP_ID");
  must("FEISHU_APP_SECRET");

  const { baseURL, serviceRoleKey } = getSupabaseEnv();
  const table = String(args.table || "capture_results").trim() || "capture_results";
  const units = resolveTaskIDGroups(args);
  const multi = units.length > 1;

  let totalSupabaseRows = 0;
  let totalGroupsSelected = 0;
  let processedGroups = 0;

  for (const unit of units) {
    const day = String(unit.day || "").trim();
    const dayStart = new Date(`${day}T00:00:00`);
    if (Number.isNaN(dayStart.getTime())) throw new Error(`invalid day: ${day}`);
    const dayMs = dayStart.getTime();

    const rows = await fetchSupabaseRowsByTaskIDs(baseURL, serviceRoleKey, table, unit.taskIDs, pageSize, timeoutMs);
    totalSupabaseRows += rows.length;

    const detect = buildDetectOutput({
      parentTaskID: unit.parentTaskID,
      threshold,
      day,
      dayMs,
      parent: unit.parent,
      rawRows: rows,
      dramaURL,
      sourcePath: `supabase:${table}`,
    });

    const outPath = resolveOutputPath(args.output, unit, multi);
    ensureDir(path.dirname(outPath));
    fs.writeFileSync(outPath, JSON.stringify(detect, null, 2));

    const summary = detect.summary || {};
    const selected = Number(summary.groups_above_threshold ?? 0);
    totalGroupsSelected += selected;
    processedGroups++;

    console.log("----------------------------------------");
    console.log(`PIRACY PIPELINE SUMMARY (ParentTaskID: ${unit.parentTaskID})`);
    console.log("----------------------------------------");
    console.log(`BookID:              ${unit.parent.book_id || "-"}`);
    console.log(`TaskIDs:             ${unit.taskIDs.join(",")}`);
    console.log(`Rows from Supabase:  ${rows.length}`);
    console.log(`Groups Selected:     ${selected}`);
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

  if (multi) {
    console.log("========================================");
    console.log("PIRACY PIPELINE BATCH SUMMARY");
    console.log("========================================");
    console.log(`BookID Groups:        ${processedGroups}`);
    console.log(`Rows from Supabase:   ${totalSupabaseRows}`);
    console.log(`Groups Selected Sum:  ${totalGroupsSelected}`);
    console.log("========================================");
  }
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
