#!/usr/bin/env node
import { spawnSync } from "child_process";
import { Command } from "commander";
import os from "os";
import path from "path";
import fs from "fs";

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

type TaskRow = {
  task_id: number;
  app: string;
  params: string;
  book_id: string;
  user_id: string;
  user_name: string;
  date: string;
  group_id: string;
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

function env(name: string, def = "") {
  const v = (process.env[name] || "").trim();
  return v || def;
}

function must(name: string) {
  const v = env(name, "");
  if (!v) throw new Error(`${name} is required`);
  return v;
}

function expandHome(p: string) {
  if (!p.startsWith("~")) return p;
  return p.replace(/^~(?=$|\/)/, os.homedir());
}

function ensureDir(dirPath: string) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function defaultDetectPath(taskID: number) {
  return path.join(os.homedir(), ".eval", String(taskID), "detect.json");
}

function toNumber(v: any, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function parseTaskID(raw: any) {
  const n = Math.trunc(Number(raw));
  if (!Number.isFinite(n) || n <= 0) throw new Error(`invalid task id: ${raw}`);
  return n;
}

function dayStartMs(day: string) {
  const m = day.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return 0;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 0, 0, 0, 0);
  return d.getTime();
}

function toDay(v: any) {
  const s = String(v ?? "").trim();
  if (!s) return "";
  if (/^\d{13}$/.test(s)) {
    const d = new Date(Number(s));
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }
  if (/^\d{10}$/.test(s)) {
    const d = new Date(Number(s) * 1000);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  return m ? `${m[1]}-${m[2]}-${m[3]}` : "";
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

// ---------- sqlite ----------
function sqliteJSON(dbPath: string, sql: string): any[] {
  const run = spawnSync("sqlite3", ["-json", dbPath, sql], { encoding: "utf-8" });
  if (run.status !== 0) throw new Error(`sqlite query failed: ${run.stderr || run.stdout}`);
  const out = (run.stdout || "").trim();
  if (!out) return [];
  const data = JSON.parse(out);
  return Array.isArray(data) ? data : [];
}

function sqliteTableColumns(dbPath: string, table: string): string[] {
  const rows = sqliteJSON(dbPath, `PRAGMA table_info(${table});`);
  return rows
    .map((r) => String(r?.name || "").trim())
    .filter(Boolean);
}

function pickField(row: Record<string, any>, names: string[]) {
  for (const n of names) {
    if (row[n] !== undefined && row[n] !== null && String(row[n]).trim() !== "") return row[n];
  }
  return "";
}

function firstText(v: any): string {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number") return String(v);
  if (Array.isArray(v)) return v.map((x) => firstText(x)).filter(Boolean).join(" ").trim();
  if (typeof v === "object") {
    if (typeof v.text === "string") return v.text.trim();
    if (v.value != null) return firstText(v.value);
    return "";
  }
  return String(v).trim();
}

function normalizeDurationSec(v: any): number {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return 0;
  if (n > 100000) return Math.round(n / 1000);
  return Math.round(n);
}

function mapAppValue(app: string) {
  const m: Record<string, string> = {
    "com.smile.gifmaker": "快手",
    "com.tencent.mm": "视频号",
    "com.eg.android.AlipayGphone": "支付宝",
  };
  return m[app] || app;
}

const TASK_ID_CANDIDATE_FIELDS = ["TaskID", "task_id"] as const;
const USER_ALIAS_FIELDS = ["UserAlias", "user_alias"] as const;
const USER_ID_FIELDS = ["UserID", "user_id"] as const;
const USER_NAME_FIELDS = ["UserName", "user_name"] as const;
const PARAMS_FIELDS = ["Params", "params", "query"] as const;
const ITEM_ID_FIELDS = ["ItemID", "item_id"] as const;
const TAGS_FIELDS = ["Tags", "tags"] as const;
const ANCHOR_FIELDS = ["AnchorPoint", "anchor_point", "Extra", "extra"] as const;
const DURATION_FIELDS = [
  "DurationSec",
  "duration_sec",
  "Duration",
  "duration",
  "ItemDuration",
  "item_duration",
  "itemDuration",
] as const;

// ---------- feishu-bitable-task-manager (task table) ----------
function taskManagerDir() {
  return path.resolve(__dirname, "../../feishu-bitable-task-manager");
}

function runTaskFetch(args: string[]) {
  const run = spawnSync("npx", ["tsx", "scripts/bitable_task.ts", "fetch", "--log-json", "--jsonl", ...args], {
    cwd: taskManagerDir(),
    encoding: "utf-8",
    env: process.env,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`bitable-task fetch failed: ${run.stderr || run.stdout}`);
  const out = String(run.stdout || "");
  const tasks: TaskRow[] = [];
  for (const line of out.split("\n")) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (obj?.msg === "task" && obj?.task) tasks.push(obj.task as TaskRow);
    } catch {
      // ignore non-json noise
    }
  }
  return tasks;
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function runBitableLookup(args: string[]) {
  const run = spawnSync("npx", ["tsx", "scripts/bitable_lookup.ts", "fetch", ...args], {
    cwd: taskManagerDir(),
    encoding: "utf-8",
    env: process.env,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`bitable-lookup fetch failed: ${run.stderr || run.stdout}`);
  const out = String(run.stdout || "");
  const rows: Record<string, string>[] = [];
  for (const line of out.split("\n")) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (obj && typeof obj === "object") rows.push(obj as any);
    } catch {
      // ignore
    }
  }
  return rows;
}

async function main() {
  const args = parseCLI(process.argv);
  const taskID = parseTaskID(args.taskId);
  const threshold = toNumber(args.threshold, 0.5);
  const logLevel = parseLogLevel(args.logLevel);
  const logger = createLogger(logLevel, process.stderr);
  const dbPath = expandHome(String(args.dbPath || "~/.eval/records.sqlite"));

  must("FEISHU_APP_ID");
  must("FEISHU_APP_SECRET");
  const dramaURL = must("DRAMA_BITABLE_URL");

  // parent task (task table via feishu-bitable-task-manager)
  const parentTasks = runTaskFetch(["--task-ids", String(taskID), "--status", "Any", "--date", "Any"]);
  if (!parentTasks.length) throw new Error(`parent task not found: ${taskID}`);
  const parentTask = parentTasks[0];
  const parentApp = String(args.app || parentTask.app || "").trim();
  const parentBookID = String(args.bookId || parentTask.book_id || "").trim();
  const parentParams = String(parentTask.params || "").trim();
  const day = String(args.date || toDay(parentTask.date) || new Date().toISOString().slice(0, 10));
  const dayMs = dayStartMs(day);
  if (!dayMs) throw new Error(`invalid day: ${day}`);

  logger.info("detect started", { task_id: taskID, threshold, day, db_path: dbPath });

  // sqlite rows for current general-search task
  const captureCols = new Set(sqliteTableColumns(dbPath, "capture_results"));
  const taskIDCols = TASK_ID_CANDIDATE_FIELDS.filter((name) => captureCols.has(name));
  if (!taskIDCols.length) throw new Error("capture_results missing task id column: expected TaskID or task_id");
  const taskIDExpr =
    taskIDCols.length === 1
      ? `CAST(COALESCE(${taskIDCols[0]}, 0) AS INTEGER)`
      : `CAST(COALESCE(${taskIDCols.join(", ")}, 0) AS INTEGER)`;
  const rawRows = sqliteJSON(dbPath, `SELECT * FROM capture_results WHERE ${taskIDExpr} = ${taskID};`);

  const summary: Record<string, any> = {
    parent_task_id: taskID,
    parent: { app: parentApp, book_id: parentBookID, params: parentParams },
    day,
    day_ms: dayMs,
    db_path: dbPath,
    threshold,
    sqlite_rows: rawRows.length,
    resolved_task_count: 0,
    unresolved_task_ids: [] as number[],
    missing_drama_meta_book_ids: [] as string[],
    invalid_drama_duration_book_ids: [] as string[],
    groups_above_threshold: 0,
  };

  if (!rawRows.length) {
    const out = { ...summary, selected_groups: [] as any[], summary };
    const payload = JSON.stringify(out, null, 2);
    if (args.output) {
      const fs = await import("fs");
      fs.writeFileSync(expandHome(args.output), payload);
    } else {
      process.stdout.write(payload + "\n");
    }
    return;
  }

  const taskIDSet = new Set<number>();
  for (const row of rawRows) {
    const rid = toNumber(pickField(row, [...TASK_ID_CANDIDATE_FIELDS]), 0);
    if (rid > 0) taskIDSet.add(Math.trunc(rid));
  }
  taskIDSet.add(taskID);
  const taskIDs = Array.from(taskIDSet).sort((a, b) => a - b);

  // resolve tasks by ids via task-manager
  const taskMap = new Map<number, TaskRow>();
  for (const batch of chunk(taskIDs, 50)) {
    const tasks = runTaskFetch(["--task-ids", batch.join(","), "--status", "Any", "--date", "Any"]);
    for (const t of tasks) {
      if (typeof t?.task_id === "number" && t.task_id > 0 && !taskMap.has(t.task_id)) taskMap.set(t.task_id, t);
    }
  }
  summary.resolved_task_count = taskMap.size;

  // group aggregate
  type G = {
    group_id: string;
    app: string;
    book_id: string;
    user_id: string;
    user_name: string;
    params: string;
    capture_duration_sec: number;
    item_ids: Set<string>;
    collection_item_id: string;
    anchor_links: Set<string>;
  };

  const groups = new Map<string, G>();
  const unresolvedTaskIDs = new Set<number>();

  let rowsWithDuration = 0;
  let rowsWithoutDuration = 0;
  for (let i = 0; i < rawRows.length; i++) {
    const row = rawRows[i] || {};
    const rowTaskID = Math.trunc(toNumber(pickField(row, [...TASK_ID_CANDIDATE_FIELDS]), 0));
    if (rowTaskID <= 0) continue;
    const t = taskMap.get(rowTaskID);

    const app = String((t?.app || "") || parentApp).trim();
    const bookID = String((t?.book_id || "") || parentBookID).trim();
    if (!bookID) {
      unresolvedTaskIDs.add(rowTaskID);
      continue;
    }

    const userAlias = String(pickField(row, [...USER_ALIAS_FIELDS]) || "").trim();
    const userID = String(pickField(row, [...USER_ID_FIELDS]) || t?.user_id || "").trim();
    const userName = String(pickField(row, [...USER_NAME_FIELDS]) || t?.user_name || "").trim();
    const userKey = (userAlias || userID || userName).trim();
    if (!userKey) continue;

    const groupID = `${mapAppValue(app)}_${bookID}_${userKey}`;
    const params = String(pickField(row, [...PARAMS_FIELDS]) || t?.params || parentParams || "").trim();

    const itemID = String(pickField(row, [...ITEM_ID_FIELDS]) || "").trim() || `__row_${i}`;
    const durationSec = normalizeDurationSec(pickField(row, [...DURATION_FIELDS]));
    if (durationSec > 0) rowsWithDuration++;
    else rowsWithoutDuration++;

    let g = groups.get(groupID);
    if (!g) {
      g = {
        group_id: groupID,
        app,
        book_id: bookID,
        user_id: userID,
        user_name: userName,
        params,
        capture_duration_sec: 0,
        item_ids: new Set<string>(),
        collection_item_id: "",
        anchor_links: new Set<string>(),
      };
      groups.set(groupID, g);
    }

    if (!g.item_ids.has(itemID)) {
      g.item_ids.add(itemID);
      g.capture_duration_sec += durationSec;
    }

    const tags = String(pickField(row, [...TAGS_FIELDS]) || "").trim();
    if (!g.collection_item_id && itemID && /合集|短剧/.test(tags)) {
      g.collection_item_id = itemID;
    }

    const anchor = String(pickField(row, [...ANCHOR_FIELDS]) || "").trim();
    if (anchor) {
      const m = anchor.match(/(kwai:\/\/[^\s"']+|weixin:\/\/[^\s"']+|alipays?:\/\/[^\s"']+|https?:\/\/[^\s"']+)/g);
      if (m) for (const link of m) g.anchor_links.add(link);
    }
  }
  summary.unresolved_task_ids = Array.from(unresolvedTaskIDs).sort((a, b) => a - b);
  logger.debug("group aggregation finished", {
    total_groups: groups.size,
    rows_with_duration: rowsWithDuration,
    rows_without_duration: rowsWithoutDuration,
    unresolved_task_ids: summary.unresolved_task_ids,
  });

  // fetch drama meta by book id (via feishu-bitable-task-manager)
  const dramaFields = {
    bookID: env("DRAMA_FIELD_BOOKID", "短剧id"),
    name: env("DRAMA_FIELD_NAME", "短剧名"),
    durationMin: env("DRAMA_FIELD_DURATION_MIN", "短剧总时长（分钟）"),
    episodeCount: env("DRAMA_FIELD_EPISODE_COUNT", "集数"),
    rightsProtectionScenario: env("DRAMA_FIELD_RIGHTS_PROTECTION_SCENARIO", "维权场景"),
    priority: env("DRAMA_FIELD_PRIORITY", "优先级"),
  };

  const bookIDs = Array.from(new Set(Array.from(groups.values()).map((g) => g.book_id))).filter(Boolean);
  const dramaMap = new Map<
    string,
    {
      name: string;
      total_duration_sec: number;
      episode_count: string;
      rights_protection_scenario: string;
      priority: string;
    }
  >();

  for (const batch of chunk(bookIDs, 50)) {
    const rows = runBitableLookup([
      "--bitable-url",
      dramaURL,
      "--book-ids",
      batch.join(","),
    ]);
    for (const row of rows) {
      const id = String(row?.book_id || "").trim();
      if (!id) continue;
      const durationMin = Number(String(row?.duration_min || "").trim());
      const totalDurationSec = Number.isFinite(durationMin) ? Math.round(durationMin * 60) : 0;
      dramaMap.set(id, {
        name: String(row?.name || "").trim(),
        total_duration_sec: totalDurationSec,
        episode_count: String(row?.episode_count || "").trim(),
        rights_protection_scenario: String(row?.rights_protection_scenario || "").trim(),
        priority: String(row?.priority || "").trim(),
      });
    }
  }

  const selected_groups: any[] = [];
  const missingMeta = new Set<string>();
  const invalidDuration = new Set<string>();

  for (const g of groups.values()) {
    const drama = dramaMap.get(g.book_id);
    if (!drama) {
      missingMeta.add(g.book_id);
      continue;
    }
    if (!Number.isFinite(drama.total_duration_sec) || drama.total_duration_sec <= 0) {
      invalidDuration.add(g.book_id);
      continue;
    }
    const ratio = g.capture_duration_sec / drama.total_duration_sec;
    if (ratio < threshold) continue;
    selected_groups.push({
      group_id: g.group_id,
      app: g.app,
      book_id: g.book_id,
      user_id: g.user_id,
      user_name: g.user_name,
      params: g.params,
      capture_duration_sec: g.capture_duration_sec,
      collection_item_id: g.collection_item_id,
      anchor_links: Array.from(g.anchor_links),
      ratio: Number(ratio.toFixed(6)),
      drama: {
        name: drama.name,
        episode_count: drama.episode_count,
        rights_protection_scenario: drama.rights_protection_scenario,
        priority: drama.priority,
        total_duration_sec: drama.total_duration_sec,
      },
    });
  }

  summary.missing_drama_meta_book_ids = Array.from(missingMeta).sort();
  summary.invalid_drama_duration_book_ids = Array.from(invalidDuration).sort();
  summary.groups_above_threshold = selected_groups.length;

  const output = {
    parent_task_id: taskID,
    day,
    day_ms: dayMs,
    threshold,
    db_path: dbPath,
    parent: { app: parentApp, book_id: parentBookID, params: parentParams },
    selected_groups,
    summary,
  };

  const payload = JSON.stringify(output, null, 2);
  const outArg = String(args.output || "").trim();
  if (outArg === "-") {
    process.stdout.write(payload + "\n");
    return;
  }
  const outPath = expandHome(outArg || defaultDetectPath(taskID));
  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, payload);
  process.stdout.write(`${JSON.stringify({ output: outPath })}\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[piracy-handler] ${msg}\n`);
  process.exit(1);
});
