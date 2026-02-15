import { env, expandHome, must, sqliteJSON } from "../shared/lib";

export type ResultDataSource = "sqlite" | "supabase";

export type ResultSourceOptions = {
  dataSource: ResultDataSource;
  dbPath?: string;
  supabaseTable?: string;
  pageSize?: number;
  timeoutMs?: number;
};

export interface ResultSource {
  fetchByTaskIDs(taskIDs: number[]): Promise<Array<Record<string, unknown>>>;
  describe(): string;
}

const SQLITE_TASK_ID_CANDIDATES = ["task_id", "TaskID"];
const SUPABASE_TASK_ID_CANDIDATES = ["task_id", "TaskID"];

function parsePositiveList(taskIDs: number[]): number[] {
  return Array.from(
    new Set(
      taskIDs
        .map((x) => Math.trunc(Number(x)))
        .filter((x) => Number.isFinite(x) && x > 0),
    ),
  ).sort((a, b) => a - b);
}

function dedupRows(rows: Array<Record<string, unknown>>): Array<Record<string, unknown>> {
  const byKey = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    const id = String((row as any)?.id ?? "").trim();
    if (id) {
      if (!byKey.has(`id:${id}`)) byKey.set(`id:${id}`, row);
      continue;
    }
    const taskID = String((row as any)?.task_id ?? (row as any)?.TaskID ?? "").trim();
    const itemID = String((row as any)?.item_id ?? (row as any)?.ItemID ?? "").trim();
    const dt = String((row as any)?.datetime ?? (row as any)?.Datetime ?? "").trim();
    const k = `row:${taskID}:${itemID}:${dt}`;
    if (!byKey.has(k)) byKey.set(k, row);
  }
  return Array.from(byKey.values());
}

function isMissingColumnError(msg: string): boolean {
  const s = String(msg || "").toLowerCase();
  return s.includes("column") && (s.includes("does not exist") || s.includes("unknown"));
}

function readSQLiteRows(dbPath: string, taskIDs: number[]): Array<Record<string, unknown>> {
  const ids = parsePositiveList(taskIDs);
  if (!ids.length) return [];

  const cols = new Set(
    sqliteJSON(dbPath, "PRAGMA table_info(capture_results);")
      .map((r) => String((r as any)?.name || "").trim())
      .filter(Boolean),
  );
  const present = SQLITE_TASK_ID_CANDIDATES.filter((x) => cols.has(x));
  if (!present.length) throw new Error("capture_results missing task id column: expected task_id or TaskID");

  const expr = present.length === 1 ? `CAST(COALESCE(${present[0]}, 0) AS INTEGER)` : `CAST(COALESCE(${present.join(", ")}, 0) AS INTEGER)`;
  const sql = `SELECT * FROM capture_results WHERE ${expr} IN (${ids.join(",")});`;
  return sqliteJSON(dbPath, sql) as Array<Record<string, unknown>>;
}

async function querySupabaseByTaskField(
  baseURL: string,
  serviceRoleKey: string,
  table: string,
  taskField: string,
  taskIDs: number[],
  pageSize: number,
  timeoutMs: number,
): Promise<Array<Record<string, unknown>>> {
  const out: Array<Record<string, unknown>> = [];
  let offset = 0;
  const taskFilter = taskIDs.join(",");

  while (true) {
    const qs = new URLSearchParams();
    qs.set("select", "*");
    qs.set(taskField, `in.(${taskFilter})`);
    qs.set("limit", String(pageSize));
    qs.set("offset", String(offset));

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const url = `${baseURL}/rest/v1/${encodeURIComponent(table)}?${qs.toString()}`;

    const resp = await fetch(url, {
      method: "GET",
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
      },
      signal: controller.signal,
    }).finally(() => clearTimeout(timer));

    const body = await resp.text();
    if (!resp.ok) {
      throw new Error(`supabase query failed: status=${resp.status} body=${body}`);
    }

    let rows: Array<Record<string, unknown>> = [];
    try {
      rows = JSON.parse(body);
    } catch {
      rows = [];
    }

    out.push(...rows);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }

  return out;
}

function resolveSupabaseConfig(opts: ResultSourceOptions) {
  const baseURL = must("SUPABASE_URL").replace(/\/+$/, "");
  const serviceRoleKey = must("SUPABASE_SERVICE_ROLE_KEY");
  const table = String(opts.supabaseTable || env("SUPABASE_RESULT_TABLE", "capture_results")).trim() || "capture_results";
  const pageSize = Math.max(1, Math.trunc(Number(opts.pageSize || 1000)) || 1000);
  const timeoutMs = Math.max(1000, Math.trunc(Number(opts.timeoutMs || 30000)) || 30000);
  return { baseURL, serviceRoleKey, table, pageSize, timeoutMs };
}

export function createResultSource(opts: ResultSourceOptions): ResultSource {
  const dataSource = opts.dataSource;

  if (dataSource === "sqlite") {
    const dbPath = expandHome(String(opts.dbPath || env("TRACKING_STORAGE_DB_PATH", "~/.eval/records.sqlite")).trim());
    return {
      async fetchByTaskIDs(taskIDs: number[]) {
        return readSQLiteRows(dbPath, taskIDs);
      },
      describe() {
        return dbPath;
      },
    };
  }

  const conf = resolveSupabaseConfig(opts);
  return {
    async fetchByTaskIDs(taskIDs: number[]) {
      const ids = parsePositiveList(taskIDs);
      if (!ids.length) return [];
      const merged: Array<Record<string, unknown>> = [];
      let success = 0;

      for (const taskField of SUPABASE_TASK_ID_CANDIDATES) {
        try {
          const rows = await querySupabaseByTaskField(
            conf.baseURL,
            conf.serviceRoleKey,
            conf.table,
            taskField,
            ids,
            conf.pageSize,
            conf.timeoutMs,
          );
          success += 1;
          merged.push(...rows);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          if (!isMissingColumnError(msg)) {
            throw err;
          }
        }
      }

      if (success === 0) {
        throw new Error("supabase table missing task id column: expected task_id or TaskID");
      }
      return dedupRows(merged);
    },
    describe() {
      return `supabase:${conf.table}`;
    },
  };
}
