import {
  chunk,
  pickField,
  runDramaFetchMeta,
  runTaskFetch,
  toDay,
  toNumber,
  type TaskRow,
} from "../shared/lib";

const TASK_ID_CANDIDATE_FIELDS = ["TaskID", "task_id"] as const;
const USER_ALIAS_FIELDS = ["UserAlias", "user_alias"] as const;
const USER_ID_FIELDS = ["UserID", "user_id"] as const;
const USER_NAME_FIELDS = ["UserName", "user_name"] as const;
const ITEM_ID_FIELDS = ["ItemID", "item_id"] as const;
const ITEM_CDN_URL_FIELDS = ["ItemCdnURL", "item_cdn_url", "itemCdnUrl", "item_url", "itemUrl"] as const;
const ITEM_CAPTION_FIELDS = ["ItemCaption", "item_caption", "caption", "Caption", "title", "Title"] as const;
const DURATION_FIELDS = [
  "DurationSec",
  "duration_sec",
  "Duration",
  "duration",
  "ItemDuration",
  "item_duration",
  "itemDuration",
] as const;

type DetectLogger = {
  debug: (msg: string, extra?: Record<string, unknown>) => void;
};

export type BuildDetectOutputInput = {
  sourceTaskIDs: number[];
  threshold: number;
  day: string;
  dayMs: number;
  rawRows: Array<Record<string, unknown>>;
  dramaURL: string;
  sourcePath: string;
  sourceType: string;
  logger?: DetectLogger;
};

export type DetectOutput = {
  schema_version: "2.0";
  generated_at: string;
  threshold: number;
  capture_day: string;
  capture_day_ms: number;
  data_source: {
    type: string;
    table?: string;
    sqlite_path?: string;
  };
  source_tasks: Array<Record<string, unknown>>;
  groups_by_app_book: Array<Record<string, unknown>>;
  summary: Record<string, unknown>;
};

type DramaMeta = {
  name: string;
  total_duration_sec: number;
  episode_count: string;
  rights_protection_scenario: string;
  priority: string;
};

type GroupAgg = {
  group_id: string;
  app: string;
  book_id: string;
  user_id: string;
  user_name: string;
  user_key: string;
  capture_duration_sec: number;
  items: Map<string, { item_id: string; item_cdn_url: string; item_duration: number; item_caption: string }>;
  task_ids: Set<number>;
};

function mapAppValue(app: string) {
  const m: Record<string, string> = {
    "com.smile.gifmaker": "快手",
    "com.tencent.mm": "微信视频号",
    "com.eg.android.AlipayGphone": "支付宝",
  };
  return m[app] || app;
}

function normalizeDurationSec(v: unknown): number {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return 0;
  if (n > 100000) return Math.round(n / 1000);
  return Math.round(n);
}

function decodeCommonEscapes(input: string): string {
  if (!input) return "";
  return input
    .replace(/\\u([0-9a-fA-F]{4})/g, (_, hex: string) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex: string) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/&#([0-9]+);/g, (_, dec: string) => String.fromCharCode(parseInt(dec, 10)))
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function normalizeURL(link: string): string {
  const decoded = decodeCommonEscapes(String(link || "").trim());
  return decoded.trim();
}

function normalizeTaskRecord(t: TaskRow): Record<string, unknown> {
  return {
    task_id: Number(t.task_id),
    app: String(t.app || "").trim(),
    scene: String(t.scene || "").trim(),
    status: String(t.status || "").trim(),
    book_id: String(t.book_id || "").trim(),
    params: String(t.params || "").trim(),
    date: toDay(String(t.date || "").trim()) || String(t.date || "").trim(),
    group_id: String(t.group_id || "").trim(),
    user_id: String(t.user_id || "").trim(),
    user_name: String(t.user_name || "").trim(),
  };
}

export function buildDetectOutput(input: BuildDetectOutputInput): DetectOutput {
  const {
    sourceTaskIDs,
    threshold,
    day,
    dayMs,
    rawRows,
    dramaURL,
    sourcePath,
    sourceType,
    logger,
  } = input;

  const normalizedSourceTaskIDs = Array.from(
    new Set((sourceTaskIDs || []).map((x) => Math.trunc(Number(x))).filter((x) => Number.isFinite(x) && x > 0)),
  ).sort((a, b) => a - b);

  const taskIDSet = new Set<number>();
  for (const id of normalizedSourceTaskIDs) taskIDSet.add(id);
  for (const row of rawRows) {
    const rid = toNumber(pickField(row, [...TASK_ID_CANDIDATE_FIELDS]), 0);
    if (rid > 0) taskIDSet.add(Math.trunc(rid));
  }
  const queryTaskIDs = Array.from(taskIDSet).sort((a, b) => a - b);

  const taskMap = new Map<number, TaskRow>();
  const successTaskIDs = new Set<number>();
  const nonSuccessTaskIDs = new Set<number>();
  for (const batch of chunk(queryTaskIDs, 50)) {
    const tasks = runTaskFetch(["--task-id", batch.join(","), "--status", "Any", "--date", "Any"]);
    for (const t of tasks) {
      if (typeof t?.task_id !== "number" || t.task_id <= 0) continue;
      if (!taskMap.has(t.task_id)) taskMap.set(t.task_id, t);
      const status = String(t?.status || "").trim().toLowerCase();
      if (status === "success") successTaskIDs.add(t.task_id);
      else nonSuccessTaskIDs.add(t.task_id);
    }
  }

  const unresolvedTaskIDs = new Set<number>();
  let rowsWithDuration = 0;
  let rowsWithoutDuration = 0;
  let skippedRowsNonSuccessTasks = 0;
  const groups = new Map<string, GroupAgg>();

  for (let i = 0; i < rawRows.length; i++) {
    const row = rawRows[i] || {};
    const rowTaskID = Math.trunc(toNumber(pickField(row, [...TASK_ID_CANDIDATE_FIELDS]), 0));
    if (rowTaskID <= 0) continue;
    if (!successTaskIDs.has(rowTaskID)) {
      skippedRowsNonSuccessTasks++;
      continue;
    }

    const t = taskMap.get(rowTaskID);
    const app = String(t?.app || "").trim();
    const bookID = String(t?.book_id || "").trim();
    if (!app || !bookID) {
      unresolvedTaskIDs.add(rowTaskID);
      continue;
    }

    const userAlias = String(pickField(row, [...USER_ALIAS_FIELDS]) || "").trim();
    const userID = String(pickField(row, [...USER_ID_FIELDS]) || t?.user_id || "").trim();
    const userName = String(pickField(row, [...USER_NAME_FIELDS]) || t?.user_name || "").trim();
    const userKey = (userAlias || userID || userName).trim();
    if (!userKey) continue;

    const groupID = `${mapAppValue(app)}_${bookID}_${userKey}`;
    const itemID = String(pickField(row, [...ITEM_ID_FIELDS]) || "").trim() || `__row_${i}`;
    const itemCdnURL = normalizeURL(String(pickField(row, [...ITEM_CDN_URL_FIELDS]) || "").trim());
    const itemCaption = String(pickField(row, [...ITEM_CAPTION_FIELDS]) || "").trim();
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
        user_key: userKey,
        capture_duration_sec: 0,
        items: new Map<string, { item_id: string; item_cdn_url: string; item_duration: number; item_caption: string }>(),
        task_ids: new Set<number>(),
      };
      groups.set(groupID, g);
    }

    g.task_ids.add(rowTaskID);

    const existingItem = g.items.get(itemID);
    if (!existingItem) {
      g.items.set(itemID, {
        item_id: itemID,
        item_cdn_url: itemCdnURL,
        item_duration: durationSec,
        item_caption: itemCaption,
      });
      g.capture_duration_sec += durationSec;
    } else {
      if (!existingItem.item_cdn_url && itemCdnURL) existingItem.item_cdn_url = itemCdnURL;
      if (!existingItem.item_caption && itemCaption) existingItem.item_caption = itemCaption;
      if (existingItem.item_duration <= 0 && durationSec > 0) {
        existingItem.item_duration = durationSec;
        g.capture_duration_sec += durationSec;
      }
    }
  }

  logger?.debug("group aggregation finished", {
    total_groups: groups.size,
    rows_with_duration: rowsWithDuration,
    rows_without_duration: rowsWithoutDuration,
    unresolved_task_ids: Array.from(unresolvedTaskIDs).sort((a, b) => a - b),
  });

  const bookIDs = Array.from(new Set(Array.from(groups.values()).map((g) => g.book_id))).filter(Boolean);
  const dramaMap = new Map<string, DramaMeta>();

  for (const batch of chunk(bookIDs, 50)) {
    const rows = runDramaFetchMeta(["--bitable-url", dramaURL, "--book-id", batch.join(",")]);
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

  const appBookMap = new Map<
    string,
    {
      app: string;
      app_label: string;
      book_id: string;
      drama: DramaMeta;
      groups: any[];
      group_count_total: number;
    }
  >();

  const missingMeta = new Set<string>();
  const invalidDuration = new Set<string>();
  let groupCountHit = 0;

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

    const appBookKey = `${g.app}@@${g.book_id}`;
    const existing = appBookMap.get(appBookKey) || {
      app: g.app,
      app_label: mapAppValue(g.app),
      book_id: g.book_id,
      drama,
      groups: [] as any[],
      group_count_total: 0,
    };
    existing.group_count_total += 1;

    const ratio = g.capture_duration_sec / drama.total_duration_sec;
    const hit = ratio >= threshold;
    if (hit) {
      groupCountHit += 1;
      const taskIDs = Array.from(g.task_ids).sort((a, b) => a - b);
      const taskInfo = taskIDs
        .map((id) => taskMap.get(id))
        .filter(Boolean)
        .map((t) => ({
          task_id: Number(t!.task_id),
          status: String(t!.status || "").trim(),
          scene: String(t!.scene || "").trim(),
          params: String(t!.params || "").trim(),
          date: toDay(String(t!.date || "").trim()) || String(t!.date || "").trim(),
        }));

      existing.groups.push({
        group_id: g.group_id,
        user_key: g.user_key,
        user_id: g.user_id,
        user_name: g.user_name,
        task_info: taskInfo,
        capture_duration_sec: g.capture_duration_sec,
        drama_total_duration_sec: drama.total_duration_sec,
        ratio: Number(ratio.toFixed(6)),
        hit_threshold: true,
        items: Array.from(g.items.values()),
      });
    }

    appBookMap.set(appBookKey, existing);
  }

  const sourceTasks = normalizedSourceTaskIDs
    .map((id) => taskMap.get(id))
    .filter(Boolean)
    .map((t) => normalizeTaskRecord(t as TaskRow));

  const groupsByAppBook = Array.from(appBookMap.values())
    .filter((x) => x.groups.length > 0)
    .map((x) => ({
      app: x.app,
      app_label: x.app_label,
      book_id: x.book_id,
      drama: {
        name: x.drama.name,
        total_duration_sec: x.drama.total_duration_sec,
        episode_count: x.drama.episode_count,
        rights_protection_scenario: x.drama.rights_protection_scenario,
        priority: x.drama.priority,
      },
      groups: x.groups,
      summary: {
        group_count_total: x.group_count_total,
        group_count_hit: x.groups.length,
      },
    }))
    .sort((a, b) => {
      const ka = `${a.app}@@${a.book_id}`;
      const kb = `${b.app}@@${b.book_id}`;
      return ka.localeCompare(kb);
    });

  const summary: Record<string, unknown> = {
    source_task_count: normalizedSourceTaskIDs.length,
    resolved_task_count: taskMap.size,
    success_task_count: successTaskIDs.size,
    non_success_task_ids: Array.from(nonSuccessTaskIDs).sort((a, b) => a - b),
    rows_read: rawRows.length,
    rows_with_duration: rowsWithDuration,
    rows_without_duration: rowsWithoutDuration,
    rows_skipped_non_success_tasks: skippedRowsNonSuccessTasks,
    unresolved_task_ids: Array.from(unresolvedTaskIDs).sort((a, b) => a - b),
    missing_drama_meta_book_ids: Array.from(missingMeta).sort(),
    invalid_drama_duration_book_ids: Array.from(invalidDuration).sort(),
    app_book_group_count: groupsByAppBook.length,
    group_count_total: groups.size,
    group_count_hit: groupCountHit,
  };

  return {
    schema_version: "2.0",
    generated_at: new Date().toISOString(),
    threshold,
    capture_day: day,
    capture_day_ms: dayMs,
    data_source: {
      type: sourceType,
      ...(sourceType === "supabase"
        ? { table: String(sourcePath || "").replace(/^supabase:/, "") }
        : { sqlite_path: sourcePath }),
    },
    source_tasks: sourceTasks,
    groups_by_app_book: groupsByAppBook,
    summary,
  };
}
