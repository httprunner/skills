import { parsePositiveInt, runTaskFetch, toDay, todayLocal, yesterdayLocal } from "../shared/lib";

export type DetectTaskUnit = {
  anchorTaskID: number;
  taskIDs: number[];
  day: string;
  sumItemsCollected: number;
  taskItemsCollected: Record<number, number>;
  parent: {
    app: string;
    book_id: string;
    params: string;
  };
};

export type DetectSkippedUnit = {
  app: string;
  book_id: string;
  day: string;
  task_ids: number[];
  reason:
    | "missing_book_id"
    | "status_not_terminal"
    | "items_collected_missing_or_invalid"
    | "items_count_mismatch";
  details?: Record<string, unknown>;
};

export type ResolveDetectTaskUnitsDetailedResult = {
  readyUnits: DetectTaskUnit[];
  skippedUnits: DetectSkippedUnit[];
  scanSummary: {
    task_count_scanned: number;
    group_count_total: number;
    group_count_ready: number;
    group_count_skipped: number;
    group_count_skipped_by_reason: Record<string, number>;
  };
};

export type ResolveDetectTaskUnitsOptions = {
  taskIds?: string;
  taskApp?: string;
  taskDate?: string;
  taskLimit?: string;
};

type UnitOrderPriority = {
  day: Map<string, number>;
  app: Map<string, number>;
};

function parseTaskIDs(csv: string): number[] {
  const ids = String(csv || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean)
    .map((x) => parsePositiveInt(x, "task id"));
  const dedup = Array.from(new Set(ids));
  if (!dedup.length) throw new Error("--task-ids is empty");
  return dedup;
}

function parseNonNegativeInt(raw: string | undefined, flag: string): number {
  const n = Math.trunc(Number(String(raw ?? "").trim()));
  if (!Number.isFinite(n) || n < 0) throw new Error(`invalid ${flag}: ${raw}`);
  return n;
}

function normalizeText(v: string): string {
  return String(v || "").trim().toLowerCase();
}

function parseCSVList(raw: string): string[] {
  return Array.from(
    new Set(
      String(raw || "")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean),
    ),
  );
}

function parseCSVListWithDefault(raw: string, fallback: string): string[] {
  const values = parseCSVList(raw);
  return values.length > 0 ? values : [fallback];
}

function parseTaskApps(raw: string): string[] {
  return parseCSVListWithDefault(raw, "com.tencent.mm");
}

function parseTaskDates(raw: string): string[] {
  return parseCSVListWithDefault(raw, "Today");
}

function parseItemsCollected(raw: unknown): { ok: boolean; value: number } {
  const trimmed = String(raw ?? "").trim();
  if (!trimmed) return { ok: false, value: 0 };
  const n = Math.trunc(Number(trimmed));
  if (!Number.isFinite(n) || n < 0) return { ok: false, value: 0 };
  return { ok: true, value: n };
}

function resolveDateFilter(v: string): string {
  const trimmed = String(v || "").trim();
  if (!trimmed) return todayLocal();
  const lower = trimmed.toLowerCase();
  if (lower === "any") return "any";
  if (lower === "today") return todayLocal();
  if (lower === "yesterday") return yesterdayLocal();
  return toDay(trimmed) || trimmed;
}

function buildUnitOrderPriority(taskApps: string[], taskDates: string[]): UnitOrderPriority {
  const appPriority = new Map<string, number>();
  for (const [idx, app] of taskApps.entries()) {
    const key = normalizeText(app);
    if (!key || appPriority.has(key)) continue;
    appPriority.set(key, idx);
  }

  const dayPriority = new Map<string, number>();
  for (const [idx, token] of taskDates.entries()) {
    const resolved = resolveDateFilter(token);
    const key = normalizeText(resolved);
    if (!key || key === "any" || dayPriority.has(key)) continue;
    dayPriority.set(key, idx);
  }
  return { day: dayPriority, app: appPriority };
}

function markReason(reasonCounter: Record<string, number>, reason: string) {
  reasonCounter[reason] = (reasonCounter[reason] || 0) + 1;
}

type TaskGroupItem = {
  task_id: number;
  app: string;
  scene: string;
  status: string;
  book_id: string;
  params: string;
  date: string;
  day: string;
  items_collected_raw: string;
};

function buildUnitsFromTasks(tasks: TaskGroupItem[]): ResolveDetectTaskUnitsDetailedResult {
  const readyUnits: DetectTaskUnit[] = [];
  const skippedUnits: DetectSkippedUnit[] = [];
  const reasonCounter: Record<string, number> = {};

  const byGroup = new Map<string, TaskGroupItem[]>();
  for (const task of tasks) {
    const app = String(task.app || "").trim();
    const day = String(task.day || "").trim();
    const bookID = String(task.book_id || "").trim();
    if (!bookID) {
      skippedUnits.push({
        app,
        book_id: "",
        day,
        task_ids: [task.task_id],
        reason: "missing_book_id",
      });
      markReason(reasonCounter, "missing_book_id");
      continue;
    }
    const key = `${app}@@${bookID}@@${day}`;
    const arr = byGroup.get(key) || [];
    arr.push(task);
    byGroup.set(key, arr);
  }

  for (const bucket of byGroup.values()) {
    if (!bucket.length) continue;
    const app = String(bucket[0].app || "").trim();
    const day = String(bucket[0].day || "").trim();
    const bookID = String(bucket[0].book_id || "").trim();
    const taskIDs = Array.from(new Set(bucket.map((x) => x.task_id))).sort((a, b) => a - b);

    const nonTerminal = bucket
      .map((x) => ({ taskID: x.task_id, status: normalizeText(x.status) }))
      .filter((x) => x.status !== "success" && x.status !== "error");
    if (nonTerminal.length > 0) {
      skippedUnits.push({
        app,
        book_id: bookID,
        day,
        task_ids: taskIDs,
        reason: "status_not_terminal",
        details: {
          non_terminal_statuses: Array.from(new Set(nonTerminal.map((x) => x.status).filter(Boolean))).sort(),
          non_terminal_task_ids: Array.from(new Set(nonTerminal.map((x) => x.taskID))).sort((a, b) => a - b),
        },
      });
      markReason(reasonCounter, "status_not_terminal");
      continue;
    }

    let sumItemsCollected = 0;
    const taskItemsCollected: Record<number, number> = {};
    let hasInvalidItemsCollected = false;
    const invalidTaskIDs: number[] = [];
    for (const t of bucket) {
      const parsedItems = parseItemsCollected(t.items_collected_raw);
      if (!parsedItems.ok) {
        hasInvalidItemsCollected = true;
        invalidTaskIDs.push(t.task_id);
        continue;
      }
      sumItemsCollected += parsedItems.value;
      taskItemsCollected[t.task_id] = parsedItems.value;
    }

    if (hasInvalidItemsCollected) {
      skippedUnits.push({
        app,
        book_id: bookID,
        day,
        task_ids: taskIDs,
        reason: "items_collected_missing_or_invalid",
        details: { invalid_task_ids: Array.from(new Set(invalidTaskIDs)).sort((a, b) => a - b) },
      });
      markReason(reasonCounter, "items_collected_missing_or_invalid");
      continue;
    }

    const anchorTaskID = taskIDs[0];
    const parentTask = bucket.find((x) => x.task_id === anchorTaskID) || bucket[0];
    readyUnits.push({
      anchorTaskID,
      taskIDs,
      day,
      sumItemsCollected,
      taskItemsCollected,
      parent: {
        app: app || String(parentTask.app || "").trim(),
        book_id: bookID,
        params: String(parentTask.params || "").trim(),
      },
    });
  }

  readyUnits.sort((a, b) => a.anchorTaskID - b.anchorTaskID);
  skippedUnits.sort((a, b) => {
    if (a.day !== b.day) return a.day.localeCompare(b.day);
    if (a.app !== b.app) return a.app.localeCompare(b.app);
    if (a.book_id !== b.book_id) return a.book_id.localeCompare(b.book_id);
    return (a.task_ids[0] || 0) - (b.task_ids[0] || 0);
  });

  return {
    readyUnits,
    skippedUnits,
    scanSummary: {
      task_count_scanned: tasks.length,
      group_count_total: byGroup.size,
      group_count_ready: readyUnits.length,
      group_count_skipped: skippedUnits.length,
      group_count_skipped_by_reason: reasonCounter,
    },
  };
}

export function resolveDetectTaskUnitsDetailed(args: ResolveDetectTaskUnitsOptions): ResolveDetectTaskUnitsDetailedResult {
  const hasTaskIDs = String(args.taskIds || "").trim() !== "";
  const hasTaskApp = String(args.taskApp || "").trim() !== "";
  const hasTaskDate = String(args.taskDate || "").trim() !== "";
  const taskScene = "综合页搜索";
  const seenTaskIDs = new Set<number>();
  const scannedTasks: TaskGroupItem[] = [];

  if (hasTaskIDs && (hasTaskApp || hasTaskDate)) {
    throw new Error("--task-ids is mutually exclusive with --task-app/--task-date");
  }

  if (hasTaskIDs) {
    const taskIDs = parseTaskIDs(String(args.taskIds || ""));
    const fetched = runTaskFetch(["--task-id", taskIDs.join(","), "--status", "Any", "--date", "Any"]);
    for (const task of fetched) {
      const tid = Number(task.task_id);
      if (!Number.isFinite(tid) || tid <= 0 || seenTaskIDs.has(tid)) continue;
      const day = String(toDay(task.date) || "").trim();
      if (!day) continue;
      seenTaskIDs.add(tid);
      scannedTasks.push({
        task_id: tid,
        app: String(task.app || "").trim(),
        scene: String(task.scene || "").trim(),
        status: String(task.status || "").trim(),
        book_id: String(task.book_id || "").trim(),
        params: String(task.params || "").trim(),
        date: String(task.date || "").trim(),
        day,
        items_collected_raw: String((task as any).items_collected || "").trim(),
      });
    }
    if (!scannedTasks.length) {
      throw new Error(`no tasks resolved by --task-ids: ${taskIDs.join(",")}`);
    }
    return buildUnitsFromTasks(scannedTasks);
  }

  const taskApps = parseTaskApps(String(args.taskApp || ""));
  const taskDates = parseTaskDates(String(args.taskDate || ""));
  const taskLimit = parseNonNegativeInt(String(args.taskLimit || "0"), "task limit");
  const unitOrderPriority = buildUnitOrderPriority(taskApps, taskDates);

  for (const dateToken of taskDates) {
    for (const app of taskApps) {
      const fetchArgs = ["--app", app, "--scene", taskScene, "--status", "Any", "--date", dateToken];
      if (taskLimit > 0) fetchArgs.push("--limit", String(taskLimit));

      const fetchedTasks = runTaskFetch(fetchArgs);
      const expectDate = resolveDateFilter(dateToken);
      const expectScene = normalizeText(taskScene);

      for (const task of fetchedTasks) {
        const tid = Number(task.task_id);
        if (!Number.isFinite(tid) || tid <= 0 || seenTaskIDs.has(tid)) continue;
        const taskApp = String(task.app || "").trim();
        const taskSceneValue = String(task.scene || "").trim();
        const actualDay = toDay(String(task.date || ""));

        if (normalizeText(taskApp) !== normalizeText(app)) continue;
        if (expectScene !== "any" && normalizeText(taskSceneValue) !== expectScene) continue;
        if (expectDate !== "any") {
          if (!actualDay || actualDay !== expectDate) continue;
        } else if (!actualDay) {
          continue;
        }

        seenTaskIDs.add(tid);
        scannedTasks.push({
          task_id: tid,
          app: taskApp,
          scene: taskSceneValue,
          status: String(task.status || "").trim(),
          book_id: String(task.book_id || "").trim(),
          params: String(task.params || "").trim(),
          date: String(task.date || "").trim(),
          day: actualDay,
          items_collected_raw: String((task as any).items_collected || "").trim(),
        });
      }
    }
  }

  if (!scannedTasks.length) {
    throw new Error(
      `no tasks matched from feishu after local filter: app=${taskApps.join(",")}, scene=${taskScene}, date=${taskDates.join(",")}, limit=${taskLimit}`,
    );
  }
  const built = buildUnitsFromTasks(scannedTasks);
  built.readyUnits.sort((a, b) => {
    const dayA = unitOrderPriority.day.get(normalizeText(a.day));
    const dayB = unitOrderPriority.day.get(normalizeText(b.day));
    if (dayA !== undefined || dayB !== undefined) {
      const va = dayA ?? Number.MAX_SAFE_INTEGER;
      const vb = dayB ?? Number.MAX_SAFE_INTEGER;
      if (va !== vb) return va - vb;
    }

    const appA = unitOrderPriority.app.get(normalizeText(a.parent.app));
    const appB = unitOrderPriority.app.get(normalizeText(b.parent.app));
    if (appA !== undefined || appB !== undefined) {
      const va = appA ?? Number.MAX_SAFE_INTEGER;
      const vb = appB ?? Number.MAX_SAFE_INTEGER;
      if (va !== vb) return va - vb;
    }
    return a.anchorTaskID - b.anchorTaskID;
  });
  return built;
}

export function resolveDetectTaskUnits(args: ResolveDetectTaskUnitsOptions): DetectTaskUnit[] {
  return resolveDetectTaskUnitsDetailed(args).readyUnits;
}
