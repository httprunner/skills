import { spawnSync } from "child_process";
import os from "os";

export type DispatchOptions = {
  groupID: string;
  day: string;
  bizType: string;
  dryRun?: boolean;
  dbPath?: string;
  maxRetries?: number;
};

export type DispatchResult = {
  group_id: string;
  day: string;
  biz_type: string;
  ready: boolean;
  pushed: boolean;
  status: string;
  retry_count: number;
  task_ids: number[];
  task_ids_by_status: Record<string, number[]>;
  reason?: string;
};

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

export function dayStartMs(day: string) {
  const m = day.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return 0;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 0, 0, 0, 0);
  return d.getTime();
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

function sqliteJSON(dbPath: string, sql: string): any[] {
  const run = spawnSync("sqlite3", ["-json", dbPath, sql], { encoding: "utf-8" });
  if (run.status !== 0) throw new Error(`sqlite query failed: ${run.stderr || run.stdout}`);
  const out = (run.stdout || "").trim();
  if (!out) return [];
  const data = JSON.parse(out);
  return Array.isArray(data) ? data : [];
}

export function parseTaskIDs(v: any): number[] {
  if (Array.isArray(v)) {
    return Array.from(new Set(v.map((x) => Math.trunc(Number(x))).filter((n) => Number.isFinite(n) && n > 0)));
  }
  const s = String(v ?? "").trim();
  if (!s) return [];
  try {
    const j = JSON.parse(s);
    if (Array.isArray(j)) return parseTaskIDs(j);
  } catch {
    // ignore
  }
  return Array.from(
    new Set(
      s
        .split(/[\s,，]+/)
        .map((x) => Math.trunc(Number(x)))
        .filter((n) => Number.isFinite(n) && n > 0),
    ),
  );
}

export type BitableRef = { appToken: string; tableID: string; viewID: string; wikiToken: string };

export function parseBitableURL(raw: string): BitableRef {
  const u = new URL(raw);
  const seg = u.pathname.split("/").filter(Boolean);
  let appToken = "";
  let wikiToken = "";
  for (let i = 0; i < seg.length - 1; i++) {
    if (seg[i] === "base") appToken = seg[i + 1];
    if (seg[i] === "wiki") wikiToken = seg[i + 1];
  }
  const tableID = u.searchParams.get("table") || u.searchParams.get("table_id") || "";
  const viewID = u.searchParams.get("view") || u.searchParams.get("view_id") || "";
  if (!tableID) throw new Error(`missing table id in url: ${raw}`);
  return { appToken, tableID, viewID, wikiToken };
}

export async function requestJSON(method: string, url: string, token: string, body: any) {
  const res = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body == null ? undefined : JSON.stringify(body),
  });
  const txt = await res.text();
  if (!res.ok) throw new Error(`http ${res.status}: ${txt}`);
  const data = txt ? JSON.parse(txt) : {};
  if (data.code !== 0) throw new Error(`api error code=${data.code} msg=${data.msg || ""}`);
  return data;
}

export async function getTenantToken(baseURL: string, appID: string, appSecret: string) {
  const url = `${baseURL}/open-apis/auth/v3/tenant_access_token/internal`;
  const data = await requestJSON("POST", url, "", { app_id: appID, app_secret: appSecret });
  const token = String(data.tenant_access_token || "").trim();
  if (!token) throw new Error("tenant_access_token missing");
  return token;
}

export async function resolveWikiToken(baseURL: string, token: string, wikiToken: string) {
  const url = `${baseURL}/open-apis/wiki/v2/spaces/get_node?token=${encodeURIComponent(wikiToken)}`;
  const data = await requestJSON("GET", url, token, null);
  const objToken = String(data?.data?.node?.obj_token || "").trim();
  if (!objToken) throw new Error("wiki obj_token missing");
  return objToken;
}

export function condition(field: string, op: string, ...vals: any[]) {
  return { field_name: field, operator: op, value: vals };
}
export function andFilter(conditions: any[], children: any[] = []) {
  return { conjunction: "and", conditions, children };
}
export function orFilter(conditions: any[]) {
  return { conjunction: "or", conditions };
}

export type FeishuCtx = { baseURL: string; token: string };

export async function searchRecords(ctx: FeishuCtx, bitableURL: string, filter: any, pageSize = 200, limit = 200) {
  const ref = parseBitableURL(bitableURL);
  if (!ref.appToken && ref.wikiToken) ref.appToken = await resolveWikiToken(ctx.baseURL, ctx.token, ref.wikiToken);
  if (!ref.appToken) throw new Error("bitable app token missing");

  const out: any[] = [];
  let pageToken = "";
  while (true) {
    const q = new URLSearchParams();
    q.set("page_size", String(pageSize));
    if (pageToken) q.set("page_token", pageToken);
    const url = `${ctx.baseURL}/open-apis/bitable/v1/apps/${ref.appToken}/tables/${ref.tableID}/records/search?${q.toString()}`;
    const body: any = {};
    if (filter) body.filter = filter;
    if (ref.viewID) body.view_id = ref.viewID;
    const data = await requestJSON("POST", url, ctx.token, body);
    const items = Array.isArray(data?.data?.items) ? data.data.items : [];
    out.push(...items);
    if (out.length >= limit) return out.slice(0, limit);
    if (!data?.data?.has_more || !data?.data?.page_token) break;
    pageToken = String(data.data.page_token);
  }
  return out;
}

export async function batchCreate(ctx: FeishuCtx, bitableURL: string, records: Array<{ fields: Record<string, any> }>) {
  if (!records.length) return { recordIDs: [] as string[] };
  const ref = parseBitableURL(bitableURL);
  if (!ref.appToken && ref.wikiToken) ref.appToken = await resolveWikiToken(ctx.baseURL, ctx.token, ref.wikiToken);
  if (!ref.appToken) throw new Error("bitable app token missing");
  const url = `${ctx.baseURL}/open-apis/bitable/v1/apps/${ref.appToken}/tables/${ref.tableID}/records/batch_create`;
  const recordIDs: string[] = [];
  for (let i = 0; i < records.length; i += 500) {
    const data = await requestJSON("POST", url, ctx.token, { records: records.slice(i, i + 500) });
    const created = Array.isArray(data?.data?.records) ? data.data.records : [];
    for (const row of created) {
      const id = String(row?.record_id || "").trim();
      if (id) recordIDs.push(id);
    }
  }
  return { recordIDs };
}

export async function batchUpdate(ctx: FeishuCtx, bitableURL: string, records: Array<{ record_id: string; fields: Record<string, any> }>) {
  if (!records.length) return;
  const ref = parseBitableURL(bitableURL);
  if (!ref.appToken && ref.wikiToken) ref.appToken = await resolveWikiToken(ctx.baseURL, ctx.token, ref.wikiToken);
  if (!ref.appToken) throw new Error("bitable app token missing");
  const url = `${ctx.baseURL}/open-apis/bitable/v1/apps/${ref.appToken}/tables/${ref.tableID}/records/batch_update`;
  for (let i = 0; i < records.length; i += 500) {
    await requestJSON("POST", url, ctx.token, { records: records.slice(i, i + 500) });
  }
}

function taskFields() {
  return {
    TaskID: env("TASK_FIELD_TASKID", "TaskID"),
    App: env("TASK_FIELD_APP", "App"),
    Scene: env("TASK_FIELD_SCENE", "Scene"),
    Date: env("TASK_FIELD_DATE", "Date"),
    Status: env("TASK_FIELD_STATUS", "Status"),
    GroupID: env("TASK_FIELD_GROUPID", "GroupID"),
    UserID: env("TASK_FIELD_USERID", "UserID"),
    UserName: env("TASK_FIELD_USERNAME", "UserName"),
  };
}

export function webhookFields() {
  return {
    BizType: env("WEBHOOK_FIELD_BIZTYPE", "BizType"),
    GroupID: env("WEBHOOK_FIELD_GROUPID", "GroupID"),
    Status: env("WEBHOOK_FIELD_STATUS", "Status"),
    TaskIDs: env("WEBHOOK_FIELD_TASKIDS", "TaskIDs"),
    TaskIDsByStatus: env("WEBHOOK_FIELD_TASK_IDS_BY_STATUS", "TaskIDsByStatus"),
    DramaInfo: env("WEBHOOK_FIELD_DRAMAINFO", "DramaInfo"),
    Date: env("WEBHOOK_FIELD_DATE", "Date"),
    RetryCount: env("WEBHOOK_FIELD_RETRYCOUNT", "RetryCount"),
    LastError: env("WEBHOOK_FIELD_LAST_ERROR", "LastError"),
    Records: env("WEBHOOK_FIELD_RECORDS", "Records"),
    UserInfo: env("WEBHOOK_FIELD_USERINFO", "UserInfo"),
    StartAt: env("WEBHOOK_FIELD_STARTAT", "StartAt"),
    EndAt: env("WEBHOOK_FIELD_ENDAT", "EndAt"),
  };
}

function classifyStatuses(rows: any[], statusField: string, taskIDField: string) {
  const by: Record<string, number[]> = {};
  for (const r of rows) {
    const f = r.fields || {};
    const status = firstText(f[statusField]).toLowerCase() || "unknown";
    const id = Math.trunc(Number(firstText(f[taskIDField])));
    if (!Number.isFinite(id) || id <= 0) continue;
    if (!by[status]) by[status] = [];
    by[status].push(id);
  }
  for (const k of Object.keys(by)) by[k] = Array.from(new Set(by[k])).sort((a, b) => a - b);
  return by;
}

function isTerminalStatus(s: string) {
  const x = s.toLowerCase();
  return x === "success" || x === "failed" || x === "error";
}

function allTerminal(taskRows: any[], statusField: string) {
  return taskRows.every((r) => isTerminalStatus(firstText((r.fields || {})[statusField])));
}

function uniqInts(vals: number[]) {
  return Array.from(new Set(vals.filter((n) => Number.isFinite(n) && n > 0))).sort((a, b) => a - b);
}

function parseDramaInfo(raw: any) {
  const s = firstText(raw);
  if (!s) return {};
  try {
    const j = JSON.parse(s);
    return typeof j === "object" && j ? j : {};
  } catch {
    return {};
  }
}

function pickField(row: Record<string, any>, names: string[]) {
  for (const n of names) {
    if (row[n] !== undefined && row[n] !== null && String(row[n]).trim() !== "") return row[n];
  }
  return "";
}

function collectPayloadFromSQLite(dbPath: string, taskIDs: number[]) {
  const inList = taskIDs.join(",");
  if (!inList) return { records: [], userInfo: {} };
  const rows = sqliteJSON(dbPath, `SELECT * FROM capture_results WHERE CAST(COALESCE(TaskID, task_id, 0) AS INTEGER) IN (${inList});`);
  const userInfo = {
    UserID: String(pickField(rows[0] || {}, ["UserID", "user_id"]) || "").trim(),
    UserName: String(pickField(rows[0] || {}, ["UserName", "user_name"]) || "").trim(),
    UserAlias: String(pickField(rows[0] || {}, ["UserAlias", "user_alias"]) || "").trim(),
    UserAuthEntity: String(pickField(rows[0] || {}, ["UserAuthEntity", "user_auth_entity"]) || "").trim(),
  };
  return { records: rows, userInfo };
}

async function postWebhook(baseURL: string, payload: any) {
  const url = `${baseURL.replace(/\/+$/, "")}/drama/webhook/grab-finished`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json; charset=utf-8" },
    body: JSON.stringify(payload),
  });
  const txt = await res.text();
  if (!res.ok) throw new Error(`webhook http ${res.status}: ${txt}`);
}

export async function resolveGroupFromTaskID(taskID: number) {
  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const taskURL = must("TASK_BITABLE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const tf = taskFields();

  const rows = await searchRecords(ctx, taskURL, andFilter([condition(tf.TaskID, "is", String(taskID))]), 10, 10);
  if (!rows.length) return { groupID: "", day: "", bizType: "piracy_general_search" };
  const f = rows[0].fields || {};
  const groupID = firstText(f[tf.GroupID]);
  const day = toDay(firstText(f[tf.Date]));
  return { groupID, day, bizType: "piracy_general_search" };
}

export async function processOneGroup(opts: DispatchOptions): Promise<DispatchResult> {
  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const taskURL = must("TASK_BITABLE_URL");
  const webhookURL = must("WEBHOOK_BITABLE_URL");
  const crawlerBaseURL = must("CRAWLER_SERVICE_BASE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const dbPath = expandHome(opts.dbPath || env("TRACKING_STORAGE_DB_PATH", "~/.eval/records.sqlite"));
  const maxRetries = opts.maxRetries ?? 3;

  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const tf = taskFields();
  const wf = webhookFields();

  const day = opts.day || new Date().toISOString().slice(0, 10);
  const dayMs = dayStartMs(day);

  const planRows = await searchRecords(
    ctx,
    webhookURL,
    andFilter([
      condition(wf.BizType, "is", opts.bizType),
      condition(wf.GroupID, "is", opts.groupID),
      condition(wf.Date, "is", "ExactDate", String(dayMs)),
    ]),
    20,
    20,
  );

  if (!planRows.length) {
    return {
      group_id: opts.groupID,
      day,
      biz_type: opts.bizType,
      ready: false,
      pushed: false,
      status: "missing_plan",
      retry_count: 0,
      task_ids: [],
      task_ids_by_status: {},
      reason: "webhook plan not found",
    };
  }

  const plan = planRows[0];
  const planFields = plan.fields || {};
  const recordID = String(plan.record_id || "").trim();
  const currentStatus = firstText(planFields[wf.Status]).toLowerCase() || "pending";
  const retryCount = Math.trunc(Number(firstText(planFields[wf.RetryCount]) || "0")) || 0;
  const taskIDs = uniqInts(parseTaskIDs(planFields[wf.TaskIDs]));

  if (!taskIDs.length) {
    return {
      group_id: opts.groupID,
      day,
      biz_type: opts.bizType,
      ready: false,
      pushed: false,
      status: "invalid_plan",
      retry_count: retryCount,
      task_ids: [],
      task_ids_by_status: {},
      reason: "empty TaskIDs",
    };
  }

  const groupTaskRows = await searchRecords(
    ctx,
    taskURL,
    andFilter([
      condition(tf.GroupID, "is", opts.groupID),
      condition(tf.Date, "is", "ExactDate", String(dayMs)),
    ]),
    500,
    500,
  );
  const allTaskRows = groupTaskRows.filter((r) => taskIDs.includes(Math.trunc(Number(firstText((r.fields || {})[tf.TaskID])))));

  const byStatus = classifyStatuses(allTaskRows, tf.Status, tf.TaskID);
  const ready = allTaskRows.length > 0 && allTerminal(allTaskRows, tf.Status);

  const updateBase: Record<string, any> = {
    [wf.TaskIDsByStatus]: JSON.stringify(byStatus),
  };

  if (!ready) {
    if (!opts.dryRun) {
      await batchUpdate(ctx, webhookURL, [{ record_id: recordID, fields: updateBase }]);
    }
    return {
      group_id: opts.groupID,
      day,
      biz_type: opts.bizType,
      ready: false,
      pushed: false,
      status: currentStatus,
      retry_count: retryCount,
      task_ids: taskIDs,
      task_ids_by_status: byStatus,
      reason: "tasks_not_ready",
    };
  }

  const dramaInfo = parseDramaInfo(planFields[wf.DramaInfo]);
  const { records, userInfo } = collectPayloadFromSQLite(dbPath, taskIDs);
  const payload = {
    ...dramaInfo,
    records,
    UserInfo: userInfo,
  };

  const nowMs = Date.now();

  if (opts.dryRun) {
    return {
      group_id: opts.groupID,
      day,
      biz_type: opts.bizType,
      ready: true,
      pushed: false,
      status: currentStatus,
      retry_count: retryCount,
      task_ids: taskIDs,
      task_ids_by_status: byStatus,
      reason: "dry_run",
    };
  }

  try {
    await batchUpdate(ctx, webhookURL, [{ record_id: recordID, fields: { ...updateBase, [wf.StartAt]: nowMs } }]);
    await postWebhook(crawlerBaseURL, payload);
    await batchUpdate(ctx, webhookURL, [
      {
        record_id: recordID,
        fields: {
          ...updateBase,
          [wf.Status]: "success",
          [wf.RetryCount]: 0,
          [wf.LastError]: "",
          [wf.EndAt]: nowMs,
          [wf.Records]: JSON.stringify(records),
          [wf.UserInfo]: JSON.stringify(userInfo),
        },
      },
    ]);
    return {
      group_id: opts.groupID,
      day,
      biz_type: opts.bizType,
      ready: true,
      pushed: true,
      status: "success",
      retry_count: 0,
      task_ids: taskIDs,
      task_ids_by_status: byStatus,
    };
  } catch (err) {
    const next = retryCount + 1;
    const failStatus = next >= maxRetries ? "error" : "failed";
    await batchUpdate(ctx, webhookURL, [
      {
        record_id: recordID,
        fields: {
          ...updateBase,
          [wf.Status]: failStatus,
          [wf.RetryCount]: next,
          [wf.LastError]: String(err instanceof Error ? err.message : err),
          [wf.EndAt]: Date.now(),
          [wf.Records]: JSON.stringify(records),
          [wf.UserInfo]: JSON.stringify(userInfo),
        },
      },
    ]);
    return {
      group_id: opts.groupID,
      day,
      biz_type: opts.bizType,
      ready: true,
      pushed: false,
      status: failStatus,
      retry_count: next,
      task_ids: taskIDs,
      task_ids_by_status: byStatus,
      reason: String(err instanceof Error ? err.message : err),
    };
  }
}

export async function listPendingOrFailedRows(day: string, bizType: string, limit: number) {
  const appID = must("FEISHU_APP_ID");
  const appSecret = must("FEISHU_APP_SECRET");
  const webhookURL = must("WEBHOOK_BITABLE_URL");
  const baseURL = env("FEISHU_BASE_URL", "https://open.feishu.cn").replace(/\/+$/, "");
  const token = await getTenantToken(baseURL, appID, appSecret);
  const ctx: FeishuCtx = { baseURL, token };
  const wf = webhookFields();

  const statusOr = {
    conjunction: "or",
    conditions: [condition(wf.Status, "is", "pending"), condition(wf.Status, "is", "failed")],
  };
  const conditions = [condition(wf.BizType, "is", bizType)];
  if (day) conditions.push(condition(wf.Date, "is", "ExactDate", String(dayStartMs(day))));
  const filter = andFilter(conditions, [statusOr]);

  const rows = await searchRecords(ctx, webhookURL, filter, 200, limit);
  return rows.map((r) => {
    const f = r.fields || {};
    return {
      recordID: String(r.record_id || "").trim(),
      groupID: firstText(f[wf.GroupID]),
      day: day || toDay(firstText(f[wf.Date])),
      bizType: firstText(f[wf.BizType]) || bizType,
    };
  });
}
