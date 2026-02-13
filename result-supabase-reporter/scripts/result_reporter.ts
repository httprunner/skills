#!/usr/bin/env -S npx tsx

import { execFileSync, spawn } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { Command } from "commander";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

type CaptureRow = {
  id: number;
  Datetime: number | null;
  DeviceSerial: string | null;
  App: string | null;
  Scene: string | null;
  Params: string | null;
  ItemID: string | null;
  ItemCaption: string | null;
  ItemCDNURL: string | null;
  ItemURL: string | null;
  ItemDuration: number | null;
  UserName: string | null;
  UserID: string | null;
  UserAlias: string | null;
  UserAuthEntity: string | null;
  Tags: string | null;
  TaskID: number | null;
  Extra: string | null;
  LikeCount: number | null;
  ViewCount: number | null;
  AnchorPoint: string | null;
  CommentCount: number | null;
  CollectCount: number | null;
  ForwardCount: number | null;
  ShareCount: number | null;
  PayMode: string | null;
  Collection: string | null;
  Episode: string | null;
  PublishTime: string | null;
};

type FilterOptions = {
  dbPath: string;
  table: string;
  limit: number;
  status: number[];
  taskID?: number;
  app?: string;
  scene?: string;
  paramsLike?: string;
  itemID?: string;
  dateFrom?: string;
  dateTo?: string;
  where?: string;
  whereArgs: string[];
};

type ReportOptions = FilterOptions & {
  supabaseTable: string;
  batchSize: number;
  dryRun: boolean;
  maxRows?: number;
};

type RetryResetOptions = {
  dbPath: string;
  table: string;
  app?: string;
  scene?: string;
};

type CollectOptions = {
  dbPath?: string;
  table?: string;
  taskId?: string;
};

type CollectStopOptions = {
  serial?: string;
};

type IntString = number | string | null | undefined;

type CollectState = {
  serial: string;
  bundleID: string;
  taskID: string;
  pid: number;
  dbPath: string;
  table: string;
  countBefore: number;
  maxIDBefore: number;
  startedAt: number;
  artifactDir: string;
};

type SupabaseCaptureRecord = {
  datetime?: number | null;
  device_serial?: string | null;
  app?: string | null;
  scene?: string | null;
  params?: string | null;
  item_id?: string | null;
  item_caption?: string | null;
  item_cdn_url?: string | null;
  item_url?: string | null;
  item_duration?: number | null;
  user_name?: string | null;
  user_id?: string | null;
  user_alias?: string | null;
  user_auth_entity?: string | null;
  tags?: string | null;
  task_id?: number | null;
  extra?: string | null;
  like_count?: number | null;
  view_count?: number | null;
  anchor_point?: string | null;
  comment_count?: number | null;
  collect_count?: number | null;
  forward_count?: number | null;
  share_count?: number | null;
  pay_mode?: string | null;
  collection?: string | null;
  episode?: string | null;
  publish_time?: string | null;
};

const DEFAULT_DB_PATH = join(homedir(), ".eval", "records.sqlite");
const DEFAULT_TABLE = "capture_results";
const DEFAULT_SUPABASE_TABLE = "capture_results";
const COLLECT_STATE_DIR = join(homedir(), ".eval", "collectors");
const REPORT_PAGE_SIZE = 200;

const program = new Command();
program
  .name("result_reporter")
  .description("Filter capture results from SQLite and report to Supabase")
  .showHelpAfterError();

function collect(value: string, previous: string[]): string[] {
  previous.push(value);
  return previous;
}

function parseStatusCSV(raw: string): number[] {
  const values = raw
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => Number(item));
  if (values.length === 0 || values.some((v) => Number.isNaN(v))) {
    throw new Error(`invalid --status value: ${raw}`);
  }
  return values;
}

function parsePositiveInt(value: string, flagName: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`invalid ${flagName}: ${value}; expected positive integer`);
  }
  return parsed;
}

function parseTaskIDNumber(raw: string): number {
  const value = raw.trim();
  if (!/^\d+$/.test(value)) {
    throw new Error(`invalid --task-id value: ${raw}; expected digits only`);
  }
  const taskID = Number(value);
  if (!Number.isSafeInteger(taskID) || taskID < 0) {
    throw new Error(`invalid --task-id value: ${raw}; out of safe integer range`);
  }
  return taskID;
}

function parseCount(value: IntString): number {
  const parsed = typeof value === "number" ? value : Number(value ?? 0);
  return Number.isFinite(parsed) ? Math.max(0, Math.trunc(parsed)) : 0;
}

function expandHome(path: string): string {
  if (path === "~") {
    return homedir();
  }
  if (path.startsWith("~/")) {
    return join(homedir(), path.slice(2));
  }
  return path;
}

function resolveDBPath(flagValue?: string): string {
  if (flagValue && flagValue.trim() !== "") {
    return expandHome(flagValue.trim());
  }
  const fromEnv = process.env.TRACKING_STORAGE_DB_PATH?.trim();
  return fromEnv && fromEnv !== "" ? expandHome(fromEnv) : DEFAULT_DB_PATH;
}

function resolveTable(flagValue?: string): string {
  if (flagValue && flagValue.trim() !== "") {
    return flagValue.trim();
  }
  const fromEnv = process.env.RESULT_SQLITE_TABLE?.trim();
  return fromEnv && fromEnv !== "" ? fromEnv : DEFAULT_TABLE;
}

function resolveSupabaseTable(flagValue?: string): string {
  if (flagValue && flagValue.trim() !== "") {
    return flagValue.trim();
  }
  const fromEnv = process.env.SUPABASE_RESULT_TABLE?.trim();
  return fromEnv && fromEnv !== "" ? fromEnv : DEFAULT_SUPABASE_TABLE;
}

function quoteIdent(identifier: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    throw new Error(`invalid SQL identifier: ${identifier}`);
  }
  return `"${identifier}"`;
}

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function sqlLiteral(value: string | number | null): string {
  if (value === null) {
    return "NULL";
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "NULL";
  }
  return sqlString(value);
}

function parseDateToUnixMillis(input: string, endOfDay: boolean): number {
  const raw = input.trim();
  if (!raw) {
    throw new Error("date input is empty");
  }
  const dateOnly = /^\d{4}-\d{2}-\d{2}$/.test(raw);
  let d: Date;
  if (dateOnly) {
    d = new Date(`${raw}T00:00:00`);
    if (endOfDay) {
      d = new Date(`${raw}T23:59:59.999`);
    }
  } else {
    d = new Date(raw);
  }
  const ts = d.getTime();
  if (Number.isNaN(ts)) {
    throw new Error(`invalid date/datetime: ${input}`);
  }
  return ts;
}

function applyWhereArgs(whereExpr: string, args: string[]): string {
  let out = whereExpr;
  for (const arg of args) {
    const idx = out.indexOf("?");
    if (idx < 0) {
      throw new Error("--where-arg provided but --where has fewer placeholders");
    }
    out = `${out.slice(0, idx)}${sqlLiteral(arg)}${out.slice(idx + 1)}`;
  }
  if (out.includes("?")) {
    throw new Error("--where still contains placeholders; provide matching --where-arg values");
  }
  return out;
}

function buildWhereClause(opts: FilterOptions, extraClauses: string[] = []): string {
  const clauses: string[] = [];

  if (opts.status.length > 0) {
    const vals = opts.status.map((s) => String(s)).join(",");
    clauses.push(`reported IN (${vals})`);
  }
  if (opts.taskID !== undefined) {
    clauses.push(`TaskID = ${opts.taskID}`);
  }
  if (opts.app) {
    clauses.push(`App = ${sqlLiteral(opts.app)}`);
  }
  if (opts.scene) {
    clauses.push(`Scene = ${sqlLiteral(opts.scene)}`);
  }
  if (opts.paramsLike) {
    clauses.push(`Params LIKE ${sqlLiteral(`%${opts.paramsLike}%`)}`);
  }
  if (opts.itemID) {
    clauses.push(`ItemID = ${sqlLiteral(opts.itemID)}`);
  }
  if (opts.dateFrom) {
    clauses.push(`Datetime >= ${parseDateToUnixMillis(opts.dateFrom, false)}`);
  }
  if (opts.dateTo) {
    clauses.push(`Datetime <= ${parseDateToUnixMillis(opts.dateTo, true)}`);
  }
  if (opts.where) {
    clauses.push(`(${applyWhereArgs(opts.where, opts.whereArgs)})`);
  }
  clauses.push(...extraClauses);

  return clauses.length === 0 ? "" : `WHERE ${clauses.join(" AND ")}`;
}

function runSQLiteRaw(dbPath: string, sql: string, asJSON: boolean): string {
  const args = [asJSON ? "-json" : "-batch", dbPath, sql];
  return execFileSync("sqlite3", args, { encoding: "utf8" });
}

function runSQLite(dbPath: string, sql: string): void {
  runSQLiteRaw(dbPath, sql, false);
}

function runSQLiteJSON<T>(dbPath: string, sql: string): T {
  const out = runSQLiteRaw(dbPath, sql, true).trim();
  if (!out) {
    return [] as unknown as T;
  }
  return JSON.parse(out) as T;
}

function fetchRows(opts: FilterOptions): CaptureRow[] {
  return fetchRowsPage(opts, opts.limit, 0);
}

function fetchRowsPage(opts: FilterOptions, limit: number, afterID: number): CaptureRow[] {
  const table = quoteIdent(opts.table);
  const extraClauses = afterID > 0 ? [`id > ${afterID}`] : [];
  const whereSQL = buildWhereClause(opts, extraClauses);
  const sql = `
SELECT id, Datetime, DeviceSerial, App, Scene, Params, ItemID, ItemCaption,
       ItemCDNURL, ItemURL, ItemDuration, UserName, UserID, UserAlias,
       UserAuthEntity, Tags, TaskID, Extra, LikeCount, ViewCount, AnchorPoint,
       CommentCount, CollectCount, ForwardCount, ShareCount, PayMode, Collection,
       Episode, PublishTime
FROM ${table}
${whereSQL}
ORDER BY id ASC
LIMIT ${limit};`;
  return runSQLiteJSON<CaptureRow[]>(opts.dbPath, sql);
}

function countRows(dbPath: string, table: string): number {
  const qTable = quoteIdent(table);
  const rows = runSQLiteJSON<Array<{ count: number | string }>>(dbPath, `SELECT COUNT(*) AS count FROM ${qTable};`);
  return parseCount(rows[0]?.count);
}

function maxRowID(dbPath: string, table: string): number {
  const qTable = quoteIdent(table);
  const rows = runSQLiteJSON<Array<{ max_id: number | string | null }>>(
    dbPath,
    `SELECT COALESCE(MAX(id), 0) AS max_id FROM ${qTable};`,
  );
  return parseCount(rows[0]?.max_id);
}

function countRowsByTaskAfterID(dbPath: string, table: string, taskID: string, afterID: number): number {
  const qTable = quoteIdent(table);
  const sql = `SELECT COUNT(*) AS count FROM ${qTable} WHERE id > ${afterID} AND TaskID = ${sqlLiteral(taskID)};`;
  const rows = runSQLiteJSON<Array<{ count: number | string }>>(dbPath, sql);
  return parseCount(rows[0]?.count);
}

function validateTaskID(taskID: string): string {
  const trimmed = taskID.trim();
  if (trimmed === "") {
    throw new Error("--task-id is required");
  }
  if (!/^\d+$/.test(trimmed)) {
    throw new Error(`invalid --task-id: ${taskID}; expected digits only`);
  }
  return trimmed;
}

function taskArtifactDir(taskID: string): string {
  return join(homedir(), ".eval", taskID);
}

function collectStatePath(serial: string): string {
  const safeSerial = serial.replace(/[^A-Za-z0-9._-]/g, "_");
  return join(COLLECT_STATE_DIR, `${safeSerial}.json`);
}

function readCollectState(serial: string): CollectState | null {
  const file = collectStatePath(serial);
  if (!existsSync(file)) {
    return null;
  }
  try {
    const raw = readFileSync(file, "utf8");
    const data = JSON.parse(raw) as CollectState;
    if (!data || typeof data !== "object") {
      return null;
    }
    if (!Number.isInteger(data.pid) || data.pid <= 0) {
      return null;
    }
    return data;
  } catch {
    return null;
  }
}

function writeCollectState(state: CollectState): void {
  mkdirSync(COLLECT_STATE_DIR, { recursive: true });
  writeFileSync(collectStatePath(state.serial), `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function clearCollectState(serial: string): void {
  const file = collectStatePath(serial);
  if (existsSync(file)) {
    unlinkSync(file);
  }
}

function isPidRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function killProcessGroup(pid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(-pid, signal);
  } catch {
    try {
      process.kill(pid, signal);
    } catch {
      // noop
    }
  }
}

function listEvalpkgsRunProcesses(): Array<{ pid: number; command: string }> {
  const out = execFileSync("ps", ["-axo", "pid=,command="], { encoding: "utf8" });
  const lines = out
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const rows: Array<{ pid: number; command: string }> = [];
  for (const line of lines) {
    const m = line.match(/^(\d+)\s+(.+)$/);
    if (!m) {
      continue;
    }
    const pid = Number(m[1]);
    const command = m[2];
    if (!Number.isInteger(pid) || pid <= 0) {
      continue;
    }
    if (!/\bevalpkgs\b/.test(command) || !/\brun\b/.test(command)) {
      continue;
    }
    rows.push({ pid, command });
  }
  return rows;
}

function findCollectorPidsBySerial(serial: string): number[] {
  const escaped = serial.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const patterns = [
    new RegExp(`(?:^|\\s)--did(?:=|\\s+)${escaped}(?:\\s|$)`),
    new RegExp(`(?:^|\\s)-d(?:\\s+)${escaped}(?:\\s|$)`),
  ];
  const pids = new Set<number>();
  for (const proc of listEvalpkgsRunProcesses()) {
    if (patterns.some((re) => re.test(proc.command))) {
      pids.add(proc.pid);
    }
  }
  return [...pids];
}

async function stopCollectorProcess(pid: number): Promise<void> {
  if (!isPidRunning(pid)) {
    return;
  }
  killProcessGroup(pid, "SIGINT");
  for (let i = 0; i < 240; i += 1) {
    await sleep(250);
    if (!isPidRunning(pid)) {
      return;
    }
  }
  try {
    process.kill(pid, "SIGTERM");
  } catch {
    return;
  }
  for (let i = 0; i < 20; i += 1) {
    await sleep(250);
    if (!isPidRunning(pid)) {
      return;
    }
  }
  killProcessGroup(pid, "SIGKILL");
  await sleep(100);
}

async function stopCollectorsForSerial(serial: string, statePID?: number): Promise<void> {
  const pids = new Set<number>();
  if (statePID && statePID > 0) {
    pids.add(statePID);
  }
  for (const pid of findCollectorPidsBySerial(serial)) {
    pids.add(pid);
  }
  for (const pid of pids) {
    process.stderr.write(`[collect-start] stopping existing collector for SerialNumber=${serial}, pid=${pid}\n`);
    await stopCollectorProcess(pid);
  }
  clearCollectState(serial);
}

async function waitForCountStable(
  dbPath: string,
  table: string,
  maxWaitMs = 8000,
  stableMs = 1500,
): Promise<number> {
  const intervalMs = 250;
  const maxTicks = Math.max(1, Math.ceil(maxWaitMs / intervalMs));
  const needStableTicks = Math.max(1, Math.ceil(stableMs / intervalMs));
  let last = countRows(dbPath, table);
  let stableTicks = 0;
  for (let i = 0; i < maxTicks; i += 1) {
    await sleep(intervalMs);
    const cur = countRows(dbPath, table);
    if (cur === last) {
      stableTicks += 1;
      if (stableTicks >= needStableTicks) {
        return cur;
      }
      continue;
    }
    last = cur;
    stableTicks = 0;
  }
  return last;
}

function countJSONLLinesSince(dir: string, sinceMs: number, suffix?: string): number {
  if (!existsSync(dir)) {
    return 0;
  }
  let total = 0;
  const stack = [dir];
  while (stack.length > 0) {
    const current = stack.pop() as string;
    const entries = readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile() || !entry.name.endsWith(".jsonl")) {
        continue;
      }
      if (suffix && !entry.name.endsWith(suffix)) {
        continue;
      }
      const st = statSync(fullPath);
      if (st.mtimeMs < sinceMs) {
        continue;
      }
      const text = readFileSync(fullPath, "utf8");
      const lines = text.split(/\r?\n/).filter((line) => line.trim() !== "").length;
      total += lines;
    }
  }
  return total;
}

function truncateError(err: unknown): string {
  const text = err instanceof Error ? err.message : String(err);
  return text.length <= 512 ? text : text.slice(0, 512);
}

function markSuccessBatch(dbPath: string, table: string, ids: number[]): void {
  if (ids.length === 0) {
    return;
  }
  const qTable = quoteIdent(table);
  const list = ids.join(",");
  const now = Date.now();
  const sql = `UPDATE ${qTable} SET reported=1, reported_at=${now}, report_error=NULL WHERE id IN (${list});`;
  runSQLite(dbPath, sql);
}

function markFailureBatch(dbPath: string, table: string, ids: number[], error: string): void {
  if (ids.length === 0) {
    return;
  }
  const qTable = quoteIdent(table);
  const list = ids.join(",");
  const now = Date.now();
  const sql = `UPDATE ${qTable} SET reported=-1, reported_at=${now}, report_error=${sqlLiteral(error)} WHERE id IN (${list});`;
  runSQLite(dbPath, sql);
}

function normalizeText(raw: string | null): string | null {
  if (raw === null) {
    return null;
  }
  const t = raw.trim();
  return t === "" ? null : t;
}

function stableItemID(row: CaptureRow): string | null {
  const itemID = normalizeText(row.ItemID);
  if (itemID) {
    return itemID;
  }
  const itemURL = normalizeText(row.ItemURL);
  return itemURL;
}

function rowToSupabaseRecord(row: CaptureRow): SupabaseCaptureRecord {
  return {
    datetime: row.Datetime,
    device_serial: normalizeText(row.DeviceSerial),
    app: normalizeText(row.App),
    scene: normalizeText(row.Scene),
    params: normalizeText(row.Params),
    item_id: stableItemID(row),
    item_caption: normalizeText(row.ItemCaption),
    item_cdn_url: normalizeText(row.ItemCDNURL),
    item_url: normalizeText(row.ItemURL),
    item_duration: row.ItemDuration,
    user_name: normalizeText(row.UserName),
    user_id: normalizeText(row.UserID),
    user_alias: normalizeText(row.UserAlias),
    user_auth_entity: normalizeText(row.UserAuthEntity),
    tags: normalizeText(row.Tags),
    task_id: row.TaskID,
    extra: normalizeText(row.Extra),
    like_count: row.LikeCount,
    view_count: row.ViewCount,
    anchor_point: normalizeText(row.AnchorPoint),
    comment_count: row.CommentCount,
    collect_count: row.CollectCount,
    forward_count: row.ForwardCount,
    share_count: row.ShareCount,
    pay_mode: normalizeText(row.PayMode),
    collection: normalizeText(row.Collection),
    episode: normalizeText(row.Episode),
    publish_time: normalizeText(row.PublishTime),
  };
}

function createSupabaseClientFromEnv(): SupabaseClient {
  const url = process.env.SUPABASE_URL?.trim();
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY?.trim();
  if (!url || !key) {
    throw new Error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required");
  }
  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

async function upsertSupabaseBatch(
  client: SupabaseClient,
  supabaseTable: string,
  records: SupabaseCaptureRecord[],
): Promise<void> {
  if (records.length === 0) {
    return;
  }
  const { error } = await client.from(supabaseTable).upsert(records, {
    onConflict: "task_id,item_id",
    ignoreDuplicates: false,
  });
  if (error) {
    throw new Error(`supabase upsert failed: ${error.message}`);
  }
}

function buildFilterOptions(cmd: {
  dbPath?: string;
  table?: string;
  limit?: string;
  status?: string;
  taskId?: string;
  app?: string;
  scene?: string;
  paramsLike?: string;
  itemId?: string;
  dateFrom?: string;
  dateTo?: string;
  where?: string;
  whereArg?: string[];
}): FilterOptions {
  const limit = parsePositiveInt(cmd.limit ?? "30", "--limit");
  const status = parseStatusCSV(cmd.status ?? "0,-1");
  const taskID = cmd.taskId?.trim() ? parseTaskIDNumber(cmd.taskId) : undefined;
  return {
    dbPath: resolveDBPath(cmd.dbPath),
    table: resolveTable(cmd.table),
    limit,
    status,
    taskID,
    app: cmd.app?.trim() || undefined,
    scene: cmd.scene?.trim() || undefined,
    paramsLike: cmd.paramsLike?.trim() || undefined,
    itemID: cmd.itemId?.trim() || undefined,
    dateFrom: cmd.dateFrom?.trim() || undefined,
    dateTo: cmd.dateTo?.trim() || undefined,
    where: cmd.where?.trim() || undefined,
    whereArgs: (cmd.whereArg ?? []).map((x) => x.trim()),
  };
}

program
  .command("collect-start")
  .description("Start evalpkgs real-time collection in background")
  .requiredOption("--task-id <value>", "Task identifier used by evalpkgs artifact output (digits only)")
  .option("--db-path <path>", "SQLite db path (default from TRACKING_STORAGE_DB_PATH or ~/.eval/records.sqlite)")
  .option("--table <name>", "Result table name (default from RESULT_SQLITE_TABLE or capture_results)")
  .action(async (cmd: CollectOptions) => {
    const taskID = validateTaskID(String(cmd.taskId || ""));
    const dbPath = resolveDBPath(cmd.dbPath);
    const table = resolveTable(cmd.table);

    const bundleID = process.env.BUNDLE_ID?.trim();
    const serial = process.env.SerialNumber?.trim();
    if (!bundleID) {
      throw new Error("BUNDLE_ID is required in environment");
    }
    if (!serial) {
      throw new Error("SerialNumber is required in environment");
    }

    const artifactDir = taskArtifactDir(taskID);
    mkdirSync(artifactDir, { recursive: true });
    const countBefore = countRows(dbPath, table);
    const maxIDBefore = maxRowID(dbPath, table);

    const existing = readCollectState(serial);
    await stopCollectorsForSerial(serial, existing?.pid);

    process.stderr.write(
      `[collect-start] start evalpkgs in background with TaskID=${taskID} BUNDLE_ID=${bundleID} SerialNumber=${serial}\n`,
    );
    process.stderr.write(`[collect-start] sqlite db=${dbPath} table=${table} before_count=${countBefore}\n`);

    const child = spawn("evalpkgs", ["run", "--log-level", "debug", "--bundleID", bundleID, "--did", serial], {
      detached: true,
      stdio: "ignore",
      env: {
        ...process.env,
        TaskID: taskID,
        BUNDLE_ID: bundleID,
        SerialNumber: serial,
        TRACKING_STORAGE_DB_PATH: dbPath,
        RESULT_SQLITE_TABLE: table,
      },
    });
    child.unref();
    const childPid = child.pid;
    if (!childPid || !Number.isInteger(childPid) || childPid <= 0) {
      throw new Error("failed to obtain evalpkgs pid");
    }

    await sleep(150);
    if (!isPidRunning(childPid)) {
      throw new Error("evalpkgs failed to stay running in background");
    }

    writeCollectState({
      serial,
      bundleID,
      taskID,
      pid: childPid,
      dbPath,
      table,
      countBefore,
      maxIDBefore,
      startedAt: Date.now(),
      artifactDir,
    });

    process.stderr.write(
      `[collect-start] started pid=${childPid}; stop with: SerialNumber=${serial} npx tsx scripts/result_reporter.ts collect-stop\n`,
    );
  });

program
  .command("collect-stop")
  .description("Stop background evalpkgs collector for one device and print collection delta")
  .option("--serial <value>", "Device serial (default from SerialNumber env)")
  .action(async (cmd: CollectStopOptions) => {
    const serial = cmd.serial?.trim() || process.env.SerialNumber?.trim();
    if (!serial) {
      throw new Error("--serial is required (or set SerialNumber env)");
    }
    const state = readCollectState(serial);
    if (!state) {
      process.stderr.write(`[collect-stop] no active collector state for SerialNumber=${serial}\n`);
      return;
    }

    let stopped = false;
    if (isPidRunning(state.pid)) {
      await stopCollectorProcess(state.pid);
      stopped = true;
    }
    const countAfter = await waitForCountStable(state.dbPath, state.table);
    const delta = countAfter - state.countBefore;
    const taskDelta = countRowsByTaskAfterID(state.dbPath, state.table, state.taskID, state.maxIDBefore);
    const trackingEvents = countJSONLLinesSince(state.artifactDir, state.startedAt, "_track.jsonl");
    const recordsJSONL = countJSONLLinesSince(state.artifactDir, state.startedAt, "_records.jsonl");
    const runtimeSec = Math.max(0, Math.round((Date.now() - state.startedAt) / 1000));
    process.stderr.write(
      `[collect-stop] sqlite db=${state.dbPath} table=${state.table} before_count=${state.countBefore} after_count=${countAfter} delta=${delta} task_delta=${taskDelta} records_jsonl=${recordsJSONL} tracking_events=${trackingEvents} runtime_sec=${runtimeSec}\n`,
    );

    if (trackingEvents === 0 && recordsJSONL === 0) {
      process.stderr.write(
        `[collect-stop] warning: no new *_track.jsonl or *_records.jsonl lines under ${state.artifactDir} since start time\n`,
      );
    }

    clearCollectState(serial);
    process.stderr.write(
      `[collect-stop] completed serial=${serial} task_id=${state.taskID} pid=${state.pid} stopped=${stopped}\n`,
    );
  });

program
  .command("stat")
  .description("Print total row count for one TaskID in capture_results")
  .option("--db-path <path>", "SQLite db path (default from TRACKING_STORAGE_DB_PATH or ~/.eval/records.sqlite)")
  .option("--table <name>", "Result table name (default from RESULT_SQLITE_TABLE or capture_results)")
  .requiredOption("--task-id <value>", "Filter by exact TaskID (digits)")
  .action((cmd) => {
    const dbPath = resolveDBPath(cmd.dbPath);
    const table = resolveTable(cmd.table);
    const taskID = parseTaskIDNumber(String(cmd.taskId ?? ""));
    const qTable = quoteIdent(table);
    const rows = runSQLiteJSON<Array<{ count: number | string }>>(
      dbPath,
      `SELECT COUNT(*) AS count FROM ${qTable} WHERE TaskID = ${sqlLiteral(String(taskID))};`,
    );
    process.stdout.write(`${parseCount(rows[0]?.count)}\n`);
  });

program
  .command("filter")
  .description("Print selected rows as JSONL")
  .option("--db-path <path>", "SQLite db path (default from TRACKING_STORAGE_DB_PATH or ~/.eval/records.sqlite)")
  .option("--table <name>", "Result table name (default from RESULT_SQLITE_TABLE or capture_results)")
  .option("--limit <n>", "Maximum rows to fetch", "30")
  .option("--status <csv>", "Reported status list, comma-separated", "0,-1")
  .option("--task-id <value>", "Filter by exact TaskID (digits)")
  .option("--app <value>", "Exact App filter")
  .option("--scene <value>", "Exact Scene filter")
  .option("--params-like <value>", "Params LIKE filter")
  .option("--item-id <value>", "Exact ItemID filter")
  .option("--date-from <value>", "Datetime lower bound (ISO date/datetime)")
  .option("--date-to <value>", "Datetime upper bound (ISO date/datetime)")
  .option("--where <sql>", "Extra SQL predicate appended with AND")
  .option("--where-arg <value>", "Bound value for --where (repeatable)", collect, [])
  .action((cmd) => {
    const opts = buildFilterOptions(cmd);
    const rows = fetchRows(opts);
    for (const row of rows) {
      process.stdout.write(`${JSON.stringify(row)}\n`);
    }
    process.stderr.write(`[filter] db=${opts.dbPath} table=${opts.table} selected=${rows.length} limit=${opts.limit}\n`);
  });

program
  .command("report")
  .description("Report selected rows to Supabase and write back sqlite status")
  .option("--db-path <path>", "SQLite db path (default from TRACKING_STORAGE_DB_PATH or ~/.eval/records.sqlite)")
  .option("--table <name>", "Result table name (default from RESULT_SQLITE_TABLE or capture_results)")
  .option("--supabase-table <name>", "Supabase table (default capture_results or SUPABASE_RESULT_TABLE)")
  .option("--status <csv>", "Reported status list, comma-separated", "0,-1")
  .option("--task-id <value>", "Filter by exact TaskID (digits)")
  .option("--max-rows <n>", "Maximum total rows to process in this report run")
  .option("--batch-size <n>", "Batch upsert size (max 1000)", "100")
  .option("--dry-run", "Print selected rows only, skip Supabase and writeback", false)
  .option("--app <value>", "Exact App filter")
  .option("--scene <value>", "Exact Scene filter")
  .option("--params-like <value>", "Params LIKE filter")
  .option("--item-id <value>", "Exact ItemID filter")
  .option("--date-from <value>", "Datetime lower bound (ISO date/datetime)")
  .option("--date-to <value>", "Datetime upper bound (ISO date/datetime)")
  .option("--where <sql>", "Extra SQL predicate appended with AND")
  .option("--where-arg <value>", "Bound value for --where (repeatable)", collect, [])
  .action(async (cmd) => {
    const filterOpts = buildFilterOptions(cmd);
    const batchSize = parsePositiveInt(cmd.batchSize ?? "100", "--batch-size");
    if (batchSize > 1000) {
      throw new Error(`invalid --batch-size: ${cmd.batchSize}; expected 1..1000`);
    }

    const maxRows = cmd.maxRows === undefined ? undefined : parsePositiveInt(cmd.maxRows, "--max-rows");
    const reportOpts: ReportOptions = {
      ...filterOpts,
      supabaseTable: resolveSupabaseTable(cmd.supabaseTable),
      batchSize,
      dryRun: Boolean(cmd.dryRun),
      maxRows,
    };

    const supabaseClient = reportOpts.dryRun ? undefined : createSupabaseClientFromEnv();

    const pageSize = Math.max(reportOpts.batchSize, REPORT_PAGE_SIZE);
    let lastID = 0;
    let selectedCount = 0;
    let successCount = 0;
    let failedCount = 0;

    while (true) {
      if (reportOpts.maxRows !== undefined && selectedCount >= reportOpts.maxRows) {
        break;
      }
      const currentLimit =
        reportOpts.maxRows === undefined ? pageSize : Math.min(pageSize, reportOpts.maxRows - selectedCount);
      if (currentLimit <= 0) {
        break;
      }
      const rows = fetchRowsPage(reportOpts, currentLimit, lastID);
      if (rows.length === 0) {
        break;
      }
      selectedCount += rows.length;
      lastID = rows[rows.length - 1]?.id ?? lastID;

      if (reportOpts.dryRun) {
        for (const row of rows) {
          process.stdout.write(`${JSON.stringify(row)}\n`);
        }
        continue;
      }
      if (!supabaseClient) {
        throw new Error("internal error: supabase client is missing");
      }

      for (let i = 0; i < rows.length; i += reportOpts.batchSize) {
        const chunk = rows.slice(i, i + reportOpts.batchSize);
        const ids = chunk.map((row) => row.id);
        const validRows: CaptureRow[] = [];
        const invalidIDs: number[] = [];
        for (const row of chunk) {
          if (stableItemID(row)) {
            validRows.push(row);
          } else {
            invalidIDs.push(row.id);
          }
        }
        if (invalidIDs.length > 0) {
          const msg = "missing stable item id: both ItemID and ItemURL are empty";
          markFailureBatch(reportOpts.dbPath, reportOpts.table, invalidIDs, msg);
          failedCount += invalidIDs.length;
          process.stderr.write(`[report] skipped rows without stable id count=${invalidIDs.length}\n`);
        }
        if (validRows.length === 0) {
          continue;
        }
        const records = validRows.map(rowToSupabaseRecord);
        const validIDs = validRows.map((row) => row.id);
        try {
          await upsertSupabaseBatch(supabaseClient, reportOpts.supabaseTable, records);
          markSuccessBatch(reportOpts.dbPath, reportOpts.table, validIDs);
          successCount += validIDs.length;
        } catch (err) {
          const msg = truncateError(err);
          process.stderr.write(`[report] batch failed size=${validIDs.length} error=${msg}; fallback to single rows\n`);
          for (const row of validRows) {
            try {
              await upsertSupabaseBatch(supabaseClient, reportOpts.supabaseTable, [rowToSupabaseRecord(row)]);
              markSuccessBatch(reportOpts.dbPath, reportOpts.table, [row.id]);
              successCount += 1;
            } catch (singleErr) {
              const singleMsg = truncateError(singleErr);
              markFailureBatch(reportOpts.dbPath, reportOpts.table, [row.id], singleMsg);
              failedCount += 1;
              process.stderr.write(`[report] row failed id=${row.id} error=${singleMsg}\n`);
            }
          }
        }
      }
    }

    process.stderr.write(
      `[report] db=${reportOpts.dbPath} table=${reportOpts.table} selected=${selectedCount} page_size=${pageSize} max_rows=${reportOpts.maxRows ?? "all"} supabase_table=${reportOpts.supabaseTable}\n`,
    );

    if (selectedCount === 0) {
      process.stderr.write("[report] no rows selected, exit\n");
      return;
    }

    if (reportOpts.dryRun) {
      process.stderr.write("[report] dry-run enabled, skipped Supabase upload and sqlite writeback\n");
      return;
    }

    process.stderr.write(`[report] completed success=${successCount} failed=${failedCount}\n`);
  });

program
  .command("retry-reset")
  .description("Reset failed rows (reported=-1) back to pending (reported=0)")
  .option("--db-path <path>", "SQLite db path (default from TRACKING_STORAGE_DB_PATH or ~/.eval/records.sqlite)")
  .option("--table <name>", "Result table name (default from RESULT_SQLITE_TABLE or capture_results)")
  .option("--app <value>", "Optional App filter")
  .option("--scene <value>", "Optional Scene filter")
  .action((cmd: RetryResetOptions) => {
    const dbPath = resolveDBPath(cmd.dbPath);
    const table = resolveTable(cmd.table);
    const qTable = quoteIdent(table);

    const clauses = ["reported = -1"];
    if (cmd.app && cmd.app.trim() !== "") {
      clauses.push(`App = ${sqlLiteral(cmd.app.trim())}`);
    }
    if (cmd.scene && cmd.scene.trim() !== "") {
      clauses.push(`Scene = ${sqlLiteral(cmd.scene.trim())}`);
    }

    const sql = `
BEGIN;
UPDATE ${qTable} SET reported=0, reported_at=NULL, report_error=NULL WHERE ${clauses.join(" AND ")};
SELECT changes() AS changed;
COMMIT;`;
    const ret = runSQLiteJSON<Array<{ changed: number }>>(dbPath, sql);
    const changed = ret[0]?.changed ?? 0;
    process.stderr.write(`[retry-reset] db=${dbPath} table=${table} updated=${changed}\n`);
  });

program.parseAsync(process.argv).catch((err: unknown) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`[error] ${msg}\n`);
  process.exit(1);
});
