#!/usr/bin/env node
import { Command } from "commander";
import { chunk, defaultDetectPath, parsePositiveInt, readInput, runTaskCreate, runTaskFetch, toDay } from "./shared/lib";

type CLIOptions = {
  input?: string;
  taskId?: string;
  dryRun: boolean;
};

type NormalizedGroup = {
  group_id: string;
  app: string;
  book_id: string;
  user_id: string;
  user_name: string;
  params: string;
  task_ids: number[];
};

function parsePositiveIDs(values: any): number[] {
  const input = Array.isArray(values) ? values : [];
  const ids = input
    .map((x: any) => Math.trunc(Number(x)))
    .filter((x: number) => Number.isFinite(x) && x > 0);
  return Array.from(new Set(ids)).sort((a, b) => a - b);
}

function parseTaskIDsFromGroup(g: any): number[] {
  const fromTaskInfo = Array.isArray(g?.task_info) ? g.task_info.map((x: any) => x?.task_id) : [];
  return parsePositiveIDs(fromTaskInfo);
}

function parseSourceTaskIDs(detect: any): number[] {
  const fromSourceTasks = Array.isArray(detect?.source_tasks) ? detect.source_tasks.map((x: any) => x?.task_id) : [];
  return parsePositiveIDs(fromSourceTasks);
}

function normalizeDetectPayload(detect: any): {
  day: string;
  dayMs: number;
  sourceTaskIDs: number[];
  groups: NormalizedGroup[];
} {
  const day = String(detect?.capture_day || detect?.day || "").trim();
  const dayMs = Math.trunc(Number(detect?.capture_day_ms ?? detect?.day_ms ?? 0));
  const sourceTaskIDs = parseSourceTaskIDs(detect);
  if (!Array.isArray(detect?.groups_by_app_book)) {
    throw new Error("invalid detect input: missing groups_by_app_book");
  }

  const out: NormalizedGroup[] = [];
  for (const appBook of detect.groups_by_app_book) {
    const app = String(appBook?.app || "").trim();
    const bookID = String(appBook?.book_id || "").trim();
    const groups = Array.isArray(appBook?.groups) ? appBook.groups : [];
    for (const g of groups) {
      const groupID = String(g?.group_id || "").trim();
      if (!groupID || !app || !bookID) continue;
      const tids = parseTaskIDsFromGroup(g);
      out.push({
        group_id: groupID,
        app,
        book_id: bookID,
        user_id: String(g?.user_id || "").trim(),
        user_name: String(g?.user_name || "").trim(),
        params: String(g?.task_info?.[0]?.params || "").trim(),
        task_ids: tids,
      });
    }
  }
  return { day, dayMs, sourceTaskIDs, groups: out };
}

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("piracy_create_subtasks")
    .description("Create child tasks for detected piracy groups (writes TASK_BITABLE_URL via feishu-bitable-task-manager)")
    .option("--input <path>", "Detect output JSON file (use - for stdin)")
    .option("--task-id <id>", "Parent task TaskID; use default ~/.eval/<TaskID>/detect.json when --input omitted")
    .option("--dry-run", "Compute only, do not write records")
    .showHelpAfterError()
    .showSuggestionAfterError();
  program.parse(argv);
  return program.opts<CLIOptions>();
}

async function main() {
  const args = parseCLI(process.argv);
  const dryRun = Boolean(args.dryRun);
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
  const normalized = normalizeDetectPayload(detect);
  const day = normalized.day;
  const dayMs = normalized.dayMs;
  const selected = normalized.groups;
  const sourceTaskIDs = normalized.sourceTaskIDs;
  if (!day) throw new Error("invalid detect input: missing capture day");
  if (!Array.isArray(selected)) throw new Error("invalid detect input: groups must be array");
  const defaultParentTaskID = sourceTaskIDs[0] || 0;
  if (!defaultParentTaskID) throw new Error("invalid detect input: source tasks are empty");

  const groupsByApp = new Map<string, any[]>();
  for (const g of selected) {
    const app = String(g?.app || "").trim();
    const groupID = String(g?.group_id || "").trim();
    if (!app || !groupID) continue;
    const arr = groupsByApp.get(app) || [];
    arr.push(g);
    groupsByApp.set(app, arr);
  }

  const existingGroupIDs = new Set<string>();
  for (const [app, groups] of groupsByApp.entries()) {
    const ids = Array.from(new Set(groups.map((g) => String(g?.group_id || "").trim()).filter(Boolean)));
    for (const batch of chunk(ids, 40)) {
      const queryIDs = batch.length === 1 ? [batch[0], batch[0]] : batch;
      const tasks = runTaskFetch([
        "--group-id",
        queryIDs.join(","),
        "--app",
        app,
        "--scene",
        "个人页搜索",
        "--status",
        "Any",
        "--date",
        day,
      ]);
      for (const t of tasks) {
        const taskDay = toDay(String((t as any)?.date || "").trim());
        if (taskDay !== day) continue;
        const gid = String((t as any)?.group_id || "").trim();
        if (gid) existingGroupIDs.add(gid);
      }
    }
  }

  const creates: any[] = [];
  const parentTaskField = process.env.TASK_FIELD_PARENT_TASK_ID || "ParentTaskID";
  const dateField = process.env.TASK_FIELD_DATE || "Date";
  for (const g of selected) {
    const groupID = String(g?.group_id || "").trim();
    if (!groupID || existingGroupIDs.has(groupID)) continue;

    const app = String(g?.app || "").trim();
    const bookID = String(g?.book_id || "").trim();
    const userID = String(g?.user_id || "").trim();
    const userName = String(g?.user_name || "").trim();
    const params = String(g?.params || "").trim();
    const parentTaskID = Array.isArray(g?.task_ids) && g.task_ids.length > 0 ? Number(g.task_ids[0]) : defaultParentTaskID;
    const base = {
      app,
      group_id: groupID,
      book_id: bookID,
      user_id: userID,
      user_name: userName,
      parent_task_id: parentTaskID,
      date: dayMs || day,
      params,
      status: "pending",
      // Force numeric payload for number/date columns to avoid Feishu NumberFieldConvFail.
      fields: {
        [parentTaskField]: parentTaskID,
        ...(dayMs > 0 ? { [dateField]: dayMs } : {}),
      },
    };

    creates.push({ ...base, scene: "个人页搜索" });
  }

  const summary = {
    dry_run: dryRun,
    source_tasks: sourceTaskIDs,
    day,
    selected_groups: selected.length,
    existing_groups: existingGroupIDs.size,
    tasks_to_create: creates.length,
  };

  if (dryRun || !creates.length) {
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
    return;
  }

  const payload = JSON.stringify(creates);
  const stdout = runTaskCreate(payload);
  process.stdout.write(stdout || "");
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
