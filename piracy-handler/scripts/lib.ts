import { spawnSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";

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

export function parsePositiveInt(raw: any, flag: string): number {
  const v = String(raw ?? "").trim();
  const n = Math.trunc(Number(v));
  if (!Number.isFinite(n) || n <= 0) throw new Error(`invalid ${flag}: ${raw}`);
  return n;
}

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

export function taskManagerDir() {
  return path.resolve(__dirname, "../../feishu-bitable-task-manager");
}

export function webhookDispatchDir() {
  return path.resolve(__dirname, "../../group-webhook-dispatch");
}

export type TaskRow = { task_id: number; group_id: string };

export function parseTaskManagerFetchOutput(stdout: string): TaskRow[] {
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

export function runTaskFetch(args: string[]) {
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
    cwd: webhookDispatchDir(),
    encoding: "utf-8",
    env: process.env,
    input: jsonl,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`upsert_webhook_plan failed: ${run.stderr || run.stdout}`);
  return String(run.stdout || "");
}

export function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

