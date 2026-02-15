import {
  chunk,
  pickField,
  runDramaFetchMeta,
  runTaskFetch,
  toNumber,
  type TaskRow,
} from "./lib";

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

type DetectLogger = {
  debug: (msg: string, extra?: Record<string, unknown>) => void;
};

export type DetectParent = {
  app: string;
  book_id: string;
  params: string;
};

export type BuildDetectOutputInput = {
  parentTaskID: number;
  threshold: number;
  day: string;
  dayMs: number;
  parent: DetectParent;
  rawRows: Array<Record<string, unknown>>;
  dramaURL: string;
  sourcePath: string;
  logger?: DetectLogger;
};

export type DetectOutput = {
  parent_task_id: number;
  day: string;
  day_ms: number;
  threshold: number;
  db_path: string;
  parent: DetectParent;
  selected_groups: Array<Record<string, unknown>>;
  summary: Record<string, unknown>;
};

function mapAppValue(app: string) {
  const m: Record<string, string> = {
    "com.smile.gifmaker": "快手",
    "com.tencent.mm": "视频号",
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

export function buildDetectOutput(input: BuildDetectOutputInput): DetectOutput {
  const {
    parentTaskID,
    threshold,
    day,
    dayMs,
    parent,
    rawRows,
    dramaURL,
    sourcePath,
    logger,
  } = input;

  const summary: Record<string, any> = {
    parent_task_id: parentTaskID,
    parent,
    day,
    day_ms: dayMs,
    db_path: sourcePath,
    threshold,
    sqlite_rows: rawRows.length,
    resolved_task_count: 0,
    unresolved_task_ids: [] as number[],
    missing_drama_meta_book_ids: [] as string[],
    invalid_drama_duration_book_ids: [] as string[],
    groups_above_threshold: 0,
  };

  const taskIDSet = new Set<number>();
  for (const row of rawRows) {
    const rid = toNumber(pickField(row, [...TASK_ID_CANDIDATE_FIELDS]), 0);
    if (rid > 0) taskIDSet.add(Math.trunc(rid));
  }
  taskIDSet.add(parentTaskID);
  const taskIDs = Array.from(taskIDSet).sort((a, b) => a - b);

  const taskMap = new Map<number, TaskRow>();
  for (const batch of chunk(taskIDs, 50)) {
    const tasks = runTaskFetch(["--task-id", batch.join(","), "--status", "Any", "--date", "Any"]);
    for (const t of tasks) {
      if (typeof t?.task_id === "number" && t.task_id > 0 && !taskMap.has(t.task_id)) taskMap.set(t.task_id, t);
    }
  }
  summary.resolved_task_count = taskMap.size;

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

    const app = String((t?.app || "") || parent.app).trim();
    const bookID = String((t?.book_id || "") || parent.book_id).trim();
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
    const params = String(pickField(row, [...PARAMS_FIELDS]) || t?.params || parent.params || "").trim();

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
  logger?.debug("group aggregation finished", {
    total_groups: groups.size,
    rows_with_duration: rowsWithDuration,
    rows_without_duration: rowsWithoutDuration,
    unresolved_task_ids: summary.unresolved_task_ids,
  });

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
    const rows = runDramaFetchMeta([
      "--bitable-url",
      dramaURL,
      "--book-id",
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

  return {
    parent_task_id: parentTaskID,
    day,
    day_ms: dayMs,
    threshold,
    db_path: sourcePath,
    parent,
    selected_groups,
    summary,
  };
}
