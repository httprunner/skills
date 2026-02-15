#!/usr/bin/env node
import { Command } from "commander";
import { chunk, defaultDetectPath, parsePositiveInt, readInput, runTaskFetch, runWebhookPlanUpsert } from "./shared/lib";

type CLIOptions = {
  input?: string;
  taskId?: string;
  dryRun: boolean;
  bizType: string;
};

function mapAppLabel(app: string): string {
  const trimmed = String(app || "").trim();
  if (!trimmed) return "";
  const m: Record<string, string> = {
    "com.smile.gifmaker": "快手",
    "com.tencent.mm": "微信视频号",
    "com.eg.android.AlipayGphone": "支付宝",
  };
  return m[trimmed] || trimmed;
}

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("piracy_upsert_webhook_plans")
    .description("Upsert webhook plans for detected piracy groups")
    .option("--input <path>", "Detect output JSON file (use - for stdin)")
    .option("--task-id <id>", "Parent task TaskID; use default ~/.eval/<TaskID>/detect.json when --input omitted")
    .option("--biz-type <name>", "BizType", "piracy_general_search")
    .option("--dry-run", "Compute only, do not write records")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  return program.opts<CLIOptions>();
}

async function main() {
  const args = parseCLI(process.argv);
  const dryRun = Boolean(args.dryRun);
  const bizType = String(args.bizType || "piracy_general_search").trim() || "piracy_general_search";
  const inputArg = String(args.input || "").trim();
  const taskIDArg = String(args.taskId || "").trim();
  let detectText = "";
  if (inputArg) {
    detectText = readInput(inputArg);
  } else if (taskIDArg) {
    const tid = parsePositiveInt(taskIDArg, "--task-id");
    detectText = readInput(defaultDetectPath(tid));
  } else {
    throw new Error("either --input or --task-id is required");
  }
  const detect = JSON.parse(detectText);

  const parentTaskID = Math.trunc(Number(detect?.parent_task_id));
  const day = String(detect?.day || "").trim();
  const dayMs = Math.trunc(Number(detect?.day_ms || 0));
  const selected = Array.isArray(detect?.selected_groups) ? detect.selected_groups : [];

  if (!parentTaskID || !day) throw new Error("invalid detect input: missing parent_task_id/day");
  if (!Array.isArray(selected)) throw new Error("invalid detect input: selected_groups must be array");

  const groupsByApp = new Map<string, any[]>();
  for (const g of selected) {
    const app = String(g?.app || "").trim();
    const groupID = String(g?.group_id || "").trim();
    if (!app || !groupID) continue;
    const arr = groupsByApp.get(app) || [];
    arr.push(g);
    groupsByApp.set(app, arr);
  }

  const taskIDsByGroup = new Map<string, number[]>();
  for (const [app, groups] of groupsByApp.entries()) {
    const ids = Array.from(new Set(groups.map((g) => String(g?.group_id || "").trim()).filter(Boolean)));
    for (const batch of chunk(ids, 40)) {
      const tasks = runTaskFetch([
        "--group-id",
        batch.join(","),
        "--app",
        app,
        "--scene",
        "Any",
        "--status",
        "Any",
        "--date",
        day,
      ]);
      for (const t of tasks) {
        const gid = String((t as any)?.group_id || "").trim();
        const tid = Math.trunc(Number((t as any)?.task_id));
        if (!gid || !Number.isFinite(tid) || tid <= 0) continue;
        const arr = taskIDsByGroup.get(gid) || [];
        arr.push(tid);
        taskIDsByGroup.set(gid, arr);
      }
    }
  }

  const upsertItems: any[] = [];
  for (const g of selected) {
    const groupID = String(g?.group_id || "").trim();
    if (!groupID) continue;
    const tids = Array.from(new Set([parentTaskID, ...(taskIDsByGroup.get(groupID) || [])])).sort((a, b) => a - b);
    if (!tids.length) continue;

    const drama = g?.drama || {};
    const dramaInfoObj = {
      CaptureDate: String(dayMs || ""),
      DramaID: String(g?.book_id || "").trim(),
      DramaName: String(drama?.name || g?.params || "").trim(),
      EpisodeCount: String(drama?.episode_count || "").trim(),
      Priority: String(drama?.priority || "").trim(),
      RightsProtectionScenario: String(drama?.rights_protection_scenario || "").trim(),
      TotalDuration: String(drama?.total_duration_sec ?? ""),
      CaptureDuration: String(g?.capture_duration_sec ?? ""),
      GeneralSearchRatio: `${(Number(g?.ratio || 0) * 100).toFixed(2)}%`,
    };

    upsertItems.push({
      group_id: groupID,
      date: day,
      biz_type: bizType,
      app: mapAppLabel(String(g?.app || "").trim()),
      task_ids: tids,
      drama_info: JSON.stringify(dramaInfoObj),
    });
  }

  const summary = {
    dry_run: dryRun,
    parent_task_id: parentTaskID,
    day,
    selected_groups: selected.length,
    upsert_items: upsertItems.length,
  };

  if (!upsertItems.length) {
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
    return;
  }

  const jsonl = upsertItems.map((x) => JSON.stringify(x)).join("\n") + "\n";
  const stdout = runWebhookPlanUpsert(jsonl, dryRun, bizType);
  process.stdout.write(stdout || "");
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
