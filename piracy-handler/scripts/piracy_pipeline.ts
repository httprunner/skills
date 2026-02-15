#!/usr/bin/env node
import { Command } from "commander";
import { spawnSync } from "child_process";
import path from "path";
import { env, must, toNumber } from "./shared/lib";
import { resolveDetectTaskUnits } from "./detect/task_units";
import { runDetectForUnits } from "./detect/runner";

type CLIOptions = {
  taskIds?: string;
  taskApp?: string;
  taskScene: string;
  taskStatus: string;
  taskDate: string;
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
  app?: string;
  bookId?: string;
  date?: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("piracy_pipeline")
    .description("Compatibility shell: run detect/create/upsert pipeline")
    .option("--task-ids <csv>", "Comma-separated TaskID list, e.g. 69111,69112,69113")
    .option("--task-app <app>", "Feishu task filter: App")
    .option("--task-scene <scene>", "Feishu task filter: Scene", "综合页搜索")
    .option("--task-status <status>", "Feishu task filter: Status", "success")
    .option("--task-date <date>", "Feishu task filter: Date preset/value (e.g. Today/Yesterday/2026-02-15)", "Today")
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
  return program.opts<CLIOptions>();
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
  process.stderr.write("[piracy-handler] piracy_pipeline is a compatibility shell and internally reuses piracy_detect flow\n");
  const output = String(args.output || "").trim();
  if (output === "-") throw new Error("--output - is not supported in pipeline mode");

  must("SUPABASE_URL");
  must("SUPABASE_SERVICE_ROLE_KEY");
  must("FEISHU_APP_ID");
  must("FEISHU_APP_SECRET");
  must("DRAMA_BITABLE_URL");

  const units = resolveDetectTaskUnits({
    taskIds: args.taskIds,
    taskApp: args.taskApp,
    taskScene: args.taskScene,
    taskStatus: args.taskStatus,
    taskDate: args.taskDate,
    taskLimit: args.taskLimit,
  });

  const results = await runDetectForUnits({
    units,
    threshold: toNumber(args.threshold, 0.5),
    output,
    resultSource: {
      dataSource: "supabase",
      supabaseTable: args.table,
      pageSize: Number(args.pageSize),
      timeoutMs: Number(args.timeoutMs),
    },
  });

  let totalSelected = 0;
  let totalRows = 0;

  for (const r of results) {
    const selected = Number((r.detect.summary as any)?.group_count_hit || 0);
    totalSelected += selected;
    totalRows += r.rowCount;

    console.log("----------------------------------------");
    console.log(`PIRACY PIPELINE SUMMARY (ParentTaskID: ${r.unit.parentTaskID})`);
    console.log("----------------------------------------");
    console.log(`BookID:              ${r.unit.parent.book_id || "-"}`);
    console.log(`TaskIDs:             ${r.unit.taskIDs.join(",")}`);
    console.log(`Rows from Supabase:  ${r.rowCount}`);
    console.log(`Groups Selected:     ${selected}`);
    console.log(`Output File:         ${r.outputPath}`);
    console.log("----------------------------------------");

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

  if (results.length > 1) {
    console.log("========================================");
    console.log("PIRACY PIPELINE BATCH SUMMARY");
    console.log("========================================");
    console.log(`BookID Groups:        ${results.length}`);
    console.log(`Rows from Supabase:   ${totalRows}`);
    console.log(`Groups Selected Sum:  ${totalSelected}`);
    console.log("========================================");
  }
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
