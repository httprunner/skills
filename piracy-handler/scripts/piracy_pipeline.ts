#!/usr/bin/env node
import { Command } from "commander";
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import { env, runTaskFetch, toNumber } from "./shared/lib";
import { precheckUnitsByItemsCollected } from "./detect/precheck";
import { resolveDetectTaskUnitsDetailed, type DetectSkippedUnit, type DetectTaskUnit } from "./detect/task_units";
import { resolveDetectOutputPath, runDetectForUnits } from "./detect/runner";

type CLIOptions = {
  taskIds?: string;
  taskApp?: string;
  taskDate?: string;
  taskLimit: string;
  threshold: string;
  output?: string;
  table: string;
  pageSize: string;
  timeoutMs: string;
  dryRun: boolean;
  skipCreateSubtasks: boolean;
  skipUpsertWebhookPlans: boolean;
  skipProcessed: boolean;
  bizType: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("piracy_pipeline")
    .description("Compatibility shell: run detect/create/upsert pipeline")
    .option("--task-ids <csv>", "Comma-separated TaskID list, e.g. 69111,69112,69113")
    .option("--task-app <app>", "Feishu task filter: App; supports comma-separated values; default com.tencent.mm when task filters are used")
    .option("--task-date <date>", "Feishu task filter: Date preset/value; supports comma-separated values; default Today when task filters are used")
    .option("--task-limit <n>", "Feishu task fetch limit (0 = no cap)", "0")
    .option("--threshold <num>", "Threshold ratio", "0.5")
    .option("--output <path>", "Detect output path")
    .option("--table <name>", "Supabase table name", env("SUPABASE_RESULT_TABLE", "capture_results"))
    .option("--page-size <n>", "Supabase page size", "1000")
    .option("--timeout-ms <n>", "HTTP timeout in milliseconds", "30000")
    .option("--biz-type <name>", "Webhook BizType", "piracy_general_search")
    .option("--dry-run", "Compute and print, skip writes in create/upsert")
    .option("--skip-create-subtasks", "Skip creating child tasks")
    .option("--skip-upsert-webhook-plans", "Skip upserting webhook plan records")
    .option("--no-skip-processed", "Always rerun detect/downstream even if existing detect output is already fully processed")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  const opts = program.opts<CLIOptions>();
  const hasTaskIDs = String(opts.taskIds || "").trim() !== "";
  const hasTaskApp = String(opts.taskApp || "").trim() !== "";
  const hasTaskDate = String(opts.taskDate || "").trim() !== "";
  if (hasTaskIDs && (hasTaskApp || hasTaskDate)) {
    throw new Error("--task-ids is mutually exclusive with --task-app/--task-date");
  }
  return opts;
}

function parseTaskIDsCSV(raw: string): number[] {
  const out = String(raw || "")
    .split(",")
    .map((x) => Math.trunc(Number(x.trim())))
    .filter((x) => Number.isFinite(x) && x > 0);
  return Array.from(new Set(out));
}

function normalizeTaskIDsArg(taskIDs: number[]): string {
  return taskIDs
    .slice()
    .sort((a, b) => a - b)
    .join(",");
}

function filterGeneralSearchTaskIDs(taskIDsRaw: string): { kept: number[]; skipped: Array<{ task_id: number; scene: string }> } {
  const taskIDs = parseTaskIDsCSV(taskIDsRaw);
  if (!taskIDs.length) return { kept: [], skipped: [] };

  const rows = runTaskFetch(["--task-id", taskIDs.join(","), "--status", "Any", "--date", "Any"]);
  const byID = new Map<number, string>();
  for (const row of rows) {
    const tid = Math.trunc(Number(row?.task_id));
    if (!Number.isFinite(tid) || tid <= 0) continue;
    if (!byID.has(tid)) byID.set(tid, String(row?.scene || "").trim());
  }

  const kept: number[] = [];
  const skipped: Array<{ task_id: number; scene: string }> = [];
  for (const tid of taskIDs) {
    const scene = String(byID.get(tid) || "").trim();
    if (scene === "综合页搜索") {
      kept.push(tid);
      continue;
    }
    skipped.push({ task_id: tid, scene: scene || "missing" });
  }
  return { kept: Array.from(new Set(kept)).sort((a, b) => a - b), skipped };
}

function runLocalScriptCapture(scriptName: string, args: string[]) {
  const run = spawnSync("npx", ["tsx", `scripts/${scriptName}`, ...args], {
    cwd: path.resolve(__dirname, ".."),
    env: process.env,
    encoding: "utf-8",
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) {
    throw new Error(`${scriptName} failed: ${run.stderr || run.stdout || "unknown error"}`);
  }
  return {
    stdout: String(run.stdout || ""),
    stderr: String(run.stderr || ""),
  };
}

function runLocalScript(scriptName: string, args: string[]) {
  const out = runLocalScriptCapture(scriptName, args);
  if (out.stdout) process.stdout.write(out.stdout);
  if (out.stderr) process.stderr.write(out.stderr);
}

function parseJSONObjects(text: string): any[] {
  const out: any[] = [];
  const s = String(text || "");
  let depth = 0;
  let start = -1;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === "{") {
      if (depth === 0) start = i;
      depth += 1;
      continue;
    }
    if (ch === "}") {
      if (depth > 0) depth -= 1;
      if (depth === 0 && start >= 0) {
        const raw = s.slice(start, i + 1);
        try {
          out.push(JSON.parse(raw));
        } catch {
          // ignore invalid chunk
        }
        start = -1;
      }
    }
  }
  return out;
}

function runLocalScriptLastJSON(scriptName: string, args: string[]): Record<string, unknown> {
  const out = runLocalScriptCapture(scriptName, args);
  const parsed = parseJSONObjects(out.stdout);
  const last = parsed[parsed.length - 1];
  if (!last || typeof last !== "object" || Array.isArray(last)) {
    throw new Error(`${scriptName} returned no JSON summary`);
  }
  return last as Record<string, unknown>;
}

function parseDetectSourceTaskIDs(detect: Record<string, unknown>): number[] {
  const rows = Array.isArray(detect.source_tasks) ? detect.source_tasks : [];
  const ids = rows
    .map((x) => Math.trunc(Number((x as any)?.task_id)))
    .filter((x) => Number.isFinite(x) && x > 0);
  return Array.from(new Set(ids)).sort((a, b) => a - b);
}

function isSamePositiveIntSet(a: number[], b: number[]): boolean {
  const aa = Array.from(new Set(a.map((x) => Math.trunc(Number(x))).filter((x) => Number.isFinite(x) && x > 0))).sort((x, y) => x - y);
  const bb = Array.from(new Set(b.map((x) => Math.trunc(Number(x))).filter((x) => Number.isFinite(x) && x > 0))).sort((x, y) => x - y);
  if (aa.length !== bb.length) return false;
  for (let i = 0; i < aa.length; i++) {
    if (aa[i] !== bb[i]) return false;
  }
  return true;
}

function hasDetectOutputForUnit(unit: DetectTaskUnit, detectPath: string): boolean {
  if (!detectPath || detectPath === "-" || !fs.existsSync(detectPath)) return false;
  let parsed: any;
  try {
    parsed = JSON.parse(fs.readFileSync(detectPath, "utf-8"));
  } catch {
    return false;
  }
  if (!parsed || typeof parsed !== "object") return false;
  const day = String(parsed.capture_day || parsed.day || "").trim();
  if (day !== unit.day) return false;
  const taskIDs = parseDetectSourceTaskIDs(parsed as Record<string, unknown>);
  return isSamePositiveIntSet(taskIDs, unit.taskIDs);
}

function parseNumber(v: unknown): number {
  const n = Math.trunc(Number(v));
  return Number.isFinite(n) ? n : 0;
}

function parseArrayLength(v: unknown): number {
  return Array.isArray(v) ? v.length : 0;
}

function countDetectSelectedGroups(detect: Record<string, unknown>): number {
  const appBooks = Array.isArray(detect.groups_by_app_book) ? detect.groups_by_app_book : [];
  let total = 0;
  for (const it of appBooks) {
    const groups = Array.isArray((it as any)?.groups) ? (it as any).groups : [];
    total += groups.length;
  }
  return total;
}

function canSkipByExistingDetect(
  unit: DetectTaskUnit,
  detectPath: string,
  args: CLIOptions,
): { skip: boolean; reason: string } {
  if (!hasDetectOutputForUnit(unit, detectPath)) return { skip: false, reason: "no_reusable_detect_output" };
  let parsedDetect: Record<string, unknown> | null = null;
  try {
    parsedDetect = JSON.parse(fs.readFileSync(detectPath, "utf-8")) as Record<string, unknown>;
  } catch {
    parsedDetect = null;
  }
  if (parsedDetect && countDetectSelectedGroups(parsedDetect) <= 0) {
    return { skip: true, reason: "existing_detect_no_hits" };
  }

  let createReady = true;
  let webhookReady = true;
  if (!args.skipCreateSubtasks) {
    const createSummary = runLocalScriptLastJSON("piracy_create_subtasks.ts", ["--input", detectPath, "--dry-run"]);
    createReady = parseNumber(createSummary.tasks_to_create) === 0;
  }
  if (!args.skipUpsertWebhookPlans) {
    try {
      const upsertSummary = runLocalScriptLastJSON("upsert_webhook_plan.ts", [
        "--source",
        "detect",
        "--input",
        detectPath,
        "--biz-type",
        String(args.bizType || "piracy_general_search"),
        "--dry-run",
      ]);
      webhookReady = parseNumber(upsertSummary.created) === 0 && parseArrayLength(upsertSummary.errors) === 0;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("no valid upsert items generated from detect input")) {
        webhookReady = true;
      } else {
        throw err;
      }
    }
  }
  if (createReady && webhookReady) return { skip: true, reason: "existing_detect_and_downstream_ready" };
  return { skip: false, reason: "downstream_not_ready" };
}

function pickStringArrayField(details: Record<string, unknown> | undefined, key: string): string[] {
  const raw = details?.[key];
  if (!Array.isArray(raw)) return [];
  return raw.map((x) => String(x || "").trim()).filter(Boolean);
}

function pickNumberArrayField(details: Record<string, unknown> | undefined, key: string): number[] {
  const raw = details?.[key];
  if (!Array.isArray(raw)) return [];
  return raw
    .map((x) => Math.trunc(Number(x)))
    .filter((x) => Number.isFinite(x) && x > 0);
}

function formatSkippedMark(unit: DetectSkippedUnit): string {
  if (unit.reason !== "status_not_terminal") return `blocked(${unit.reason})`;
  const statuses = pickStringArrayField(unit.details, "non_terminal_statuses");
  const nonTerminalTaskIDs = pickNumberArrayField(unit.details, "non_terminal_task_ids");
  if (!statuses.length) return "blocked";
  if (!nonTerminalTaskIDs.length) return `blocked(${statuses.join("|")})`;
  return `blocked(${statuses.join("|")};tasks=${nonTerminalTaskIDs.join(",")})`;
}

async function main() {
  const args = parseCLI(process.argv);
  const hasTaskIDs = String(args.taskIds || "").trim() !== "";
  if (hasTaskIDs) {
    const filtered = filterGeneralSearchTaskIDs(String(args.taskIds || ""));
    if (filtered.skipped.length > 0) {
      console.log("========================================");
      console.log("PIRACY PIPELINE TASK-ID FILTER");
      console.log("========================================");
      console.log(`Input TaskIDs:        ${String(args.taskIds || "")}`);
      console.log(`Kept(综合页搜索):       ${filtered.kept.length}`);
      console.log(`Skipped(non-综合页):   ${filtered.skipped.length}`);
      for (const it of filtered.skipped) {
        console.log(`- skip TaskID=${it.task_id} scene=${it.scene}`);
      }
      console.log("========================================");
    }
    if (!filtered.kept.length) {
      console.log("No 综合页搜索 TaskID remained after filtering; skip pipeline.");
      return;
    }
    args.taskIds = normalizeTaskIDsArg(filtered.kept);
  }
  process.stderr.write("[piracy-handler] piracy_pipeline is a compatibility shell and internally reuses piracy_detect flow\n");
  const output = String(args.output || "").trim();
  if (output === "-") throw new Error("--output - is not supported in pipeline mode");

  const unitResult = resolveDetectTaskUnitsDetailed({
    taskIds: args.taskIds,
    taskApp: args.taskApp,
    taskDate: args.taskDate,
    taskLimit: args.taskLimit,
  });
  const units = unitResult.readyUnits;
  const skippedBeforePrecheck = unitResult.skippedUnits;

  const totalTasks = units.reduce((acc, u) => acc + u.taskIDs.length, 0);
  console.log("========================================");
  console.log("PIRACY PIPELINE TASK UNITS");
  console.log("========================================");
  console.log(`BookID Groups Total:  ${unitResult.scanSummary.group_count_total}`);
  console.log(`BookID Groups Ready:  ${units.length}`);
  console.log(`BookID Groups Skipped:${skippedBeforePrecheck.length}`);
  console.log(`TaskIDs Total:        ${totalTasks}`);
  console.log(`Tasks Scanned:        ${unitResult.scanSummary.task_count_scanned}`);
  const scanSkipByReason = Object.entries(unitResult.scanSummary.group_count_skipped_by_reason)
    .map(([k, v]) => `${k}:${v}`)
    .join(", ");
  if (scanSkipByReason) {
    console.log(`Skipped Reasons:      ${scanSkipByReason}`);
  }
  for (const [idx, u] of units.entries()) {
    console.log(
      `[${idx + 1}/${units.length}] App=${u.parent.app || "-"} BookID=${u.parent.book_id || "-"} Date=${u.day} TaskCount=${u.taskIDs.length}`,
    );
  }
  if (skippedBeforePrecheck.length > 0) {
    console.log("----------------------------------------");
    console.log("PIRACY PIPELINE SKIPPED GROUPS");
    console.log("----------------------------------------");
    for (const [idx, s] of skippedBeforePrecheck.entries()) {
      const mark = formatSkippedMark(s);
      console.log(
        `[S${idx + 1}/${skippedBeforePrecheck.length}] App=${s.app || "-"} BookID=${s.book_id || "-"} Date=${s.day || "-"} Status=${mark} TaskIDs=${s.task_ids.join(",")}`,
      );
    }
  }
  console.log("========================================");

  let totalSelected = 0;
  let totalRows = 0;
  let failedGroups = 0;
  const threshold = toNumber(args.threshold, 0.5);
  const resultSource = {
    dataSource: "supabase" as const,
    supabaseTable: args.table,
    pageSize: Number(args.pageSize),
    timeoutMs: Number(args.timeoutMs),
  };
  let skippedProcessed = 0;
  const unitsForPrecheck: DetectTaskUnit[] = [];
  const precheckMultiOutput = units.length > 1;
  if (args.skipProcessed && units.length > 0) {
    console.log("========================================");
    console.log("PIRACY PIPELINE PROCESSED-SKIP CHECK");
    console.log("========================================");
  }
  for (const unit of units) {
    const unitOutputPath = resolveDetectOutputPath(output, unit, precheckMultiOutput);
    if (args.skipProcessed) {
      const skipCheck = canSkipByExistingDetect(unit, unitOutputPath, args);
      if (skipCheck.skip) {
        skippedProcessed += 1;
        console.log(
          `[SKIP-CHECK] ${skippedProcessed}/${units.length} skip BookID=${unit.parent.book_id || "-"} reason=${skipCheck.reason}`,
        );
        continue;
      }
      console.log(
        `[SKIP-CHECK] ${skippedProcessed + unitsForPrecheck.length + 1}/${units.length} keep BookID=${unit.parent.book_id || "-"} reason=${skipCheck.reason}`,
      );
    }
    unitsForPrecheck.push(unit);
  }
  if (args.skipProcessed && units.length > 0) {
    console.log(`Skip-check done:      skipped=${skippedProcessed}, to_precheck=${unitsForPrecheck.length}`);
    console.log("========================================");
  }

  const precheckResult = await precheckUnitsByItemsCollected(unitsForPrecheck, resultSource);
  const unitsForDetect = precheckResult.readyUnits;
  const skippedAfterPrecheck = precheckResult.skippedUnits;
  const precheckReasonSummary = Object.entries(precheckResult.summary.skipped_by_reason)
    .map(([k, v]) => `${k}:${v}`)
    .join(", ");
  console.log("========================================");
  console.log("PIRACY PIPELINE PRECHECK");
  console.log("========================================");
  console.log(`Checked Groups:       ${precheckResult.summary.checked_groups}`);
  console.log(`Passed Groups:        ${precheckResult.summary.passed_groups}`);
  console.log(`Skipped Groups:       ${precheckResult.summary.skipped_groups}`);
  console.log(`Skipped Processed:    ${skippedProcessed}`);
  console.log(`Rows Read:            ${precheckResult.summary.rows_read}`);
  if (precheckReasonSummary) {
    console.log(`Skipped Reasons:      ${precheckReasonSummary}`);
  }
  console.log("========================================");

  if (!unitsForDetect.length) {
    console.log("No groups passed precheck; skip detect/create/upsert.");
    console.log("========================================");
    console.log("PIRACY PIPELINE BATCH SUMMARY");
    console.log("========================================");
    console.log(`BookID Groups Total:  ${unitResult.scanSummary.group_count_total}`);
    console.log(`Skipped Before Check: ${skippedBeforePrecheck.length}`);
    console.log(`Skipped In Precheck:  ${skippedAfterPrecheck.length}`);
    console.log(`Skipped Processed:    ${skippedProcessed}`);
    console.log(`Processed Groups:     0`);
    console.log(`Failed Groups:        0`);
    console.log(`Rows from Supabase:   ${precheckResult.summary.rows_read}`);
    console.log(`Groups Selected Sum:  0`);
    console.log("========================================");
    return;
  }

  let processedCount = 0;
  const multiOutput = unitsForDetect.length > 1;

  for (const [idx, unit] of unitsForDetect.entries()) {
    try {
      console.log(
        `>>> [${idx + 1}/${unitsForDetect.length}] Start App=${unit.parent.app || "-"} BookID=${unit.parent.book_id || "-"} Date=${unit.day} TaskIDs=${unit.taskIDs.join(",")}`,
      );
      const unitOutputPath = resolveDetectOutputPath(output, unit, multiOutput);
      const results = await runDetectForUnits({
        units: [unit],
        threshold,
        output: unitOutputPath,
        resultSource,
      });
      const r = results[0];
      const selected = Number((r.detect.summary as any)?.group_count_hit || 0);
      totalSelected += selected;
      totalRows += r.rowCount;
      processedCount += 1;

      console.log("----------------------------------------");
      console.log("PIRACY PIPELINE SUMMARY");
      console.log("----------------------------------------");
      console.log(`BookID:              ${r.unit.parent.book_id || "-"}`);
      console.log(`SourceTaskIDs:       ${r.unit.taskIDs.join(",")}`);
      console.log(`Rows from Supabase:  ${r.rowCount}`);
      console.log(`Groups Selected:     ${selected}`);
      console.log(`Output File:         ${r.outputPath}`);
      console.log("----------------------------------------");

      if (selected <= 0) {
        console.log(`Skip downstream:     no selected groups for BookID=${r.unit.parent.book_id || "-"}`);
      } else {
        if (!args.skipCreateSubtasks) {
          const createArgs = ["--input", r.outputPath];
          if (args.dryRun) createArgs.push("--dry-run");
          runLocalScript("piracy_create_subtasks.ts", createArgs);
        }

        if (!args.skipUpsertWebhookPlans) {
          const upsertArgs = ["--source", "detect", "--input", r.outputPath, "--biz-type", String(args.bizType || "piracy_general_search")];
          if (args.dryRun) upsertArgs.push("--dry-run");
          runLocalScript("upsert_webhook_plan.ts", upsertArgs);
        }
      }
      console.log(
        `<<< [${idx + 1}/${unitsForDetect.length}] Done BookID=${r.unit.parent.book_id || "-"} Selected=${selected} Rows=${r.rowCount}`,
      );
    } catch (err) {
      failedGroups += 1;
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`xxx [${idx + 1}/${unitsForDetect.length}] Failed BookID=${unit.parent.book_id || "-"}: ${msg}`);
    }
  }

  console.log("========================================");
  console.log("PIRACY PIPELINE BATCH SUMMARY");
  console.log("========================================");
  console.log(`BookID Groups Total:  ${unitResult.scanSummary.group_count_total}`);
  console.log(`Skipped Before Check: ${skippedBeforePrecheck.length}`);
  console.log(`Skipped In Precheck:  ${skippedAfterPrecheck.length}`);
  console.log(`Ready For Detect:     ${unitsForDetect.length}`);
  console.log(`Skipped Processed:    ${skippedProcessed}`);
  console.log(`Processed Groups:     ${processedCount}`);
  console.log(`Failed Groups:        ${failedGroups}`);
  console.log(`Rows from Supabase:   ${totalRows + precheckResult.summary.rows_read}`);
  console.log(`Groups Selected Sum:  ${totalSelected}`);
  console.log("========================================");
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
