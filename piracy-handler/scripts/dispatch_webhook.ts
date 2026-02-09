#!/usr/bin/env node
import { Command } from "commander";
import { parseOptionalPositiveInt } from "./cli";
import { processOneGroup, resolveGroupFromTaskID } from "./webhook_lib";

type CLIOptions = {
  taskId?: string;
  groupId?: string;
  date?: string;
  bizType: string;
  dryRun: boolean;
  dbPath?: string;
  maxRetries?: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("dispatch_webhook")
    .description("Dispatch group webhook for one task or one group")
    .option("--task-id <id>", "TaskID used to resolve group/day")
    .option("--group-id <id>", "GroupID to dispatch")
    .option("--date <yyyy-mm-dd>", "Capture day (default: today)")
    .option("--biz-type <name>", "BizType", "piracy_general_search")
    .option("--dry-run", "Compute only, do not call webhook or write back")
    .option("--db-path <path>", "SQLite path override")
    .option("--max-retries <num>", "Max retries override")
    .showHelpAfterError()
    .showSuggestionAfterError();

  program.parse(argv);
  return program.opts<CLIOptions>();
}

async function main() {
  const args = parseCLI(process.argv);
  const taskIDRaw = String(args.taskId || "").trim();
  const groupIDRaw = String(args.groupId || "").trim();
  const dryRun = Boolean(args.dryRun);

  let groupID = groupIDRaw;
  let day = String(args.date || "").trim();
  let bizType = String(args.bizType).trim();

  if (!groupID && taskIDRaw) {
    const tid = parseOptionalPositiveInt(taskIDRaw, "--task-id");
    if (tid == null) throw new Error(`invalid --task-id: ${taskIDRaw}`);
    const resolved = await resolveGroupFromTaskID(tid);
    if (!resolved.groupID) {
      throw new Error(`group not found for --task-id: ${tid}`);
    }
    groupID = resolved.groupID;
    if (!day) day = resolved.day;
    if (!bizType) bizType = resolved.bizType;
  }

  if (!groupID) {
    throw new Error("either --group-id or --task-id is required");
  }
  if (!day) {
    day = new Date().toISOString().slice(0, 10);
  }

  const result = await processOneGroup({
    groupID,
    day,
    bizType,
    dryRun,
    dbPath: String(args.dbPath || "").trim() || undefined,
    maxRetries: parseOptionalPositiveInt(args.maxRetries, "--max-retries"),
  });

  console.log(JSON.stringify(result, null, 2));
}

main().catch((err) => {
  console.error(`[piracy-handler] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
