import { spawnSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";

// ---------- env / path ----------

export function env(name: string, def = "") {
  const v = (process.env[name] || "").trim();
  return v || def;
}

export function must(name: string) {
  const v = env(name, "");
  if (!v) throw new Error(`${name} is required`);
  return v;
}

export function expandHome(p: string) {
  if (!p.startsWith("~")) return p;
  return p.replace(/^~(?=$|\/)/, os.homedir());
}

export function ensureDir(dirPath: string) {
  fs.mkdirSync(dirPath, { recursive: true });
}

export function defaultDetectPath(taskID: number) {
  return path.join(os.homedir(), ".eval", String(taskID), "detect.json");
}

export function readInput(pathArg: string): string {
  const p = String(pathArg || "").trim();
  if (!p || p === "-") return fs.readFileSync(0, "utf-8");
  return fs.readFileSync(expandHome(p), "utf-8");
}

// ---------- parsing ----------

export function parsePositiveInt(raw: any, flag: string): number {
  const v = String(raw ?? "").trim();
  const n = Math.trunc(Number(v));
  if (!Number.isFinite(n) || n <= 0) throw new Error(`invalid ${flag}: ${raw}`);
  return n;
}

export function toNumber(v: any, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

// ---------- date ----------

export function dayStartMs(day: string) {
  const m = String(day || "").trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return 0;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 0, 0, 0, 0);
  return d.getTime();
}

export function toDay(v: any) {
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

// ---------- field helpers ----------

export function firstText(v: any): string {
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

export function pickField(row: Record<string, any>, names: string[]) {
  for (const n of names) {
    if (row[n] !== undefined && row[n] !== null && String(row[n]).trim() !== "") return row[n];
  }
  return "";
}

// ---------- sqlite ----------

export function sqliteJSON(dbPath: string, sql: string): any[] {
  const run = spawnSync("sqlite3", ["-json", dbPath, sql], {
    encoding: "utf-8",
    maxBuffer: 100 * 1024 * 1024,
  });
  if (run.error) throw run.error;
  if (run.status !== 0) {
    let msg = String(run.stderr || run.stdout || "unknown error");
    if (msg.length > 2000) msg = msg.slice(0, 2000) + "...(truncated)";
    throw new Error(`sqlite query failed: ${msg}`);
  }
  const out = (run.stdout || "").trim();
  if (!out) return [];
  try {
    return Array.isArray(JSON.parse(out)) ? JSON.parse(out) : [];
  } catch {
    return [];
  }
}

// ---------- collections ----------

export function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// ---------- feishu-bitable-task-manager subprocess ----------

export type TaskRow = {
  task_id: number;
  group_id: string;
  app?: string;
  scene?: string;
  status?: string;
  params?: string;
  book_id?: string;
  user_id?: string;
  user_name?: string;
  date?: string;
};

export function taskManagerDir() {
  return path.resolve(__dirname, "../../feishu-bitable-task-manager");
}

export function parseTaskManagerFetchOutput(stdout: string): TaskRow[] {
  const out: TaskRow[] = [];
  for (const line of String(stdout || "").split("\n")) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (obj?.msg === "task" && obj?.task) out.push(obj.task as TaskRow);
    } catch {
      // ignore non-json noise
    }
  }
  return out;
}

export function runTaskFetch(args: string[]): TaskRow[] {
  const run = spawnSync("npx", ["tsx", "scripts/bitable_task.ts", "fetch", "--log-json", "--jsonl", ...args], {
    cwd: taskManagerDir(),
    encoding: "utf-8",
    env: process.env,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`bitable-task fetch failed: ${run.stderr || run.stdout}`);
  return parseTaskManagerFetchOutput(String(run.stdout || ""));
}

export function runTaskCreate(jsonl: string) {
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

export function runWebhookPlanUpsert(jsonl: string, dryRun: boolean, bizType: string) {
  const args = ["tsx", "scripts/upsert_webhook_plan.ts", "--input", "-", "--biz-type", bizType];
  if (dryRun) args.push("--dry-run");
  const run = spawnSync("npx", args, {
    cwd: path.resolve(__dirname, ".."),
    encoding: "utf-8",
    env: process.env,
    input: jsonl,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`upsert_webhook_plan failed: ${run.stderr || run.stdout}`);
  return String(run.stdout || "");
}

export function runDramaFetchMeta(args: string[]): Record<string, string>[] {
  const run = spawnSync("npx", ["tsx", "scripts/drama_fetch.ts", "--format", "meta", ...args], {
    cwd: taskManagerDir(),
    encoding: "utf-8",
    env: process.env,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`drama-fetch meta failed: ${run.stderr || run.stdout}`);
  const rows: Record<string, string>[] = [];
  for (const line of String(run.stdout || "").split("\n")) {
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
