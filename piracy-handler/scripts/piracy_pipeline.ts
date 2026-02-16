#!/usr/bin/env node
import { Command } from "commander";
import { spawnSync } from "child_process";
import path from "path";
import { env, toNumber } from "./shared/lib";
import { precheckUnitsByItemsCollected } from "./detect/precheck";
import { resolveDetectTaskUnitsDetailed, type DetectSkippedUnit } from "./detect/task_units";
import { runDetectForUnits } from "./detect/runner";

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
  const precheckResult = await precheckUnitsByItemsCollected(units, resultSource);
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
    console.log(`Processed Groups:     0`);
    console.log(`Failed Groups:        0`);
    console.log(`Rows from Supabase:   ${precheckResult.summary.rows_read}`);
    console.log(`Groups Selected Sum:  0`);
    console.log("========================================");
    return;
  }

  let processedCount = 0;

  for (const [idx, unit] of unitsForDetect.entries()) {
    try {
      console.log(
        `>>> [${idx + 1}/${unitsForDetect.length}] Start App=${unit.parent.app || "-"} BookID=${unit.parent.book_id || "-"} Date=${unit.day} TaskIDs=${unit.taskIDs.join(",")}`,
      );
      const results = await runDetectForUnits({
        units: [unit],
        threshold,
        output,
        resultSource,
      });
      const r = results[0];
      const selected = Number((r.detect.summary as any)?.group_count_hit || 0);
      totalSelected += selected;
      totalRows += r.rowCount;
      processedCount += 1;

      console.log("----------------------------------------");
      console.log(`PIRACY PIPELINE SUMMARY (ParentTaskID: ${r.unit.parentTaskID})`);
      console.log("----------------------------------------");
      console.log(`BookID:              ${r.unit.parent.book_id || "-"}`);
      console.log(`TaskIDs:             ${r.unit.taskIDs.join(",")}`);
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
