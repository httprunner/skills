#!/usr/bin/env node
import { Command } from "commander";
import fs from "fs";
import os from "os";
import path from "path";
import { spawnSync } from "child_process";

type CLIOptions = {
  input?: string;
  taskId?: string;
  dryRun: boolean;
};

function expandHome(p: string) {
  if (!p.startsWith("~")) return p;
  return p.replace(/^~(?=$|\/)/, os.homedir());
}

function defaultDetectPath(taskID: number) {
  return path.join(os.homedir(), ".eval", String(taskID), "detect.json");
}

function readInput(pathArg: string): string {
  const p = String(pathArg || "").trim();
  if (!p || p === "-") return fs.readFileSync(0, "utf-8");
  return fs.readFileSync(expandHome(p), "utf-8");
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

function taskManagerDir() {
  return path.resolve(__dirname, "../../feishu-bitable-task-manager");
}

type TaskRow = { task_id: number; group_id: string };

function parseTaskManagerFetchOutput(stdout: string): TaskRow[] {
  const out: TaskRow[] = [];
  for (const line of String(stdout || "").split("\n")) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (obj?.msg === "task" && obj?.task) out.push(obj.task as TaskRow);
    } catch {
      // ignore
    }
  }
  return out;
}

function runTaskFetch(args: string[]) {
  const run = spawnSync("npx", ["tsx", "scripts/bitable_task.ts", "fetch", "--log-json", "--jsonl", ...args], {
    cwd: taskManagerDir(),
    encoding: "utf-8",
    env: process.env,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`bitable-task fetch failed: ${run.stderr || run.stdout}`);
  return parseTaskManagerFetchOutput(String(run.stdout || ""));
}

function runTaskCreate(jsonl: string) {
  const run = spawnSync("npx", ["tsx", "scripts/bitable_task.ts", "create", "--input", "-"], {
    cwd: taskManagerDir(),
    encoding: "utf-8",
    env: process.env,
    input: jsonl,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`bitable-task create failed: ${run.stderr || run.stdout}`);
  return String(run.stdout || "");
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
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
    const tid = Math.trunc(Number(taskIDArg));
    if (!Number.isFinite(tid) || tid <= 0) throw new Error(`invalid --task-id: ${taskIDArg}`);
    detectText = readInput(defaultDetectPath(tid));
  } else {
    throw new Error("either --input or --task-id is required");
  }
  const detect = JSON.parse(detectText);

  const parentTaskID = Math.trunc(Number(detect?.parent_task_id));
  const day = String(detect?.day || "").trim();
  const dayMs = Math.trunc(Number(detect?.day_ms || 0));
  const parent = detect?.parent || {};
  const selected = Array.isArray(detect?.selected_groups) ? detect.selected_groups : [];

  if (!parentTaskID || !day) throw new Error("invalid detect input: missing parent_task_id/day");
  if (!Array.isArray(selected)) throw new Error("invalid detect input: selected_groups must be array");

  const groupsByApp = new Map<string, any[]>();
  for (const g of selected) {
    const app = String(g?.app || parent?.app || "").trim();
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
      const tasks = runTaskFetch([
        "--group-ids",
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
        if (gid) existingGroupIDs.add(gid);
      }
    }
  }

  const creates: any[] = [];
  for (const g of selected) {
    const groupID = String(g?.group_id || "").trim();
    if (!groupID || existingGroupIDs.has(groupID)) continue;

    const app = String(g?.app || parent?.app || "").trim();
    const bookID = String(g?.book_id || "").trim();
    const userID = String(g?.user_id || "").trim();
    const userName = String(g?.user_name || "").trim();
    const params = String(g?.params || parent?.params || "").trim();
    const collectionItemID = String(g?.collection_item_id || "").trim();
    const anchorLinks = Array.isArray(g?.anchor_links) ? g.anchor_links.map((x: any) => String(x).trim()).filter(Boolean) : [];

    const base = {
      app,
      group_id: groupID,
      book_id: bookID,
      user_id: userID,
      user_name: userName,
      parent_task_id: String(parentTaskID),
      date: String(dayMs || day),
      params,
      status: "pending",
    };

    creates.push({ ...base, scene: "个人页搜索" });

    if (collectionItemID && !collectionItemID.startsWith("__row_")) {
      creates.push({ ...base, scene: "合集视频采集", item_id: collectionItemID });
    }

    for (const link of anchorLinks) {
      creates.push({ ...base, scene: "视频锚点采集", extra: link });
    }
  }

  const summary = {
    dry_run: dryRun,
    parent_task_id: parentTaskID,
    day,
    selected_groups: selected.length,
    existing_groups: existingGroupIDs.size,
    tasks_to_create: creates.length,
  };

  if (dryRun || !creates.length) {
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
    return;
  }

  const jsonl = creates.map((x) => JSON.stringify(x)).join("\n") + "\n";
  const stdout = runTaskCreate(jsonl);
  process.stdout.write(stdout || "");
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
