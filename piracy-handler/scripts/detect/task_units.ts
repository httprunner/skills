import { parsePositiveInt, runTaskFetch, toDay, todayLocal, yesterdayLocal } from "../shared/lib";

export type DetectTaskUnit = {
  parentTaskID: number;
  taskIDs: number[];
  day: string;
  parent: {
    app: string;
    book_id: string;
    params: string;
  };
};

export type ResolveDetectTaskUnitsOptions = {
  taskIds?: string;
  taskApp?: string;
  taskStatus?: string;
  taskDate?: string;
  taskLimit?: string;
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

function resolveDateFilter(v: string): string {
  const trimmed = String(v || "").trim();
  if (!trimmed) return "today";
  const lower = trimmed.toLowerCase();
  if (lower === "any") return "any";
  if (lower === "today") return todayLocal();
  if (lower === "yesterday") return yesterdayLocal();
  return toDay(trimmed) || trimmed;
}

export function resolveDetectTaskUnits(args: ResolveDetectTaskUnitsOptions): DetectTaskUnit[] {
  const hasTaskIDs = String(args.taskIds || "").trim() !== "";

  if (hasTaskIDs) {
    const taskIDs = parseTaskIDs(String(args.taskIds || ""));
    const parentTaskID = taskIDs[0];
    const parentTasks = runTaskFetch(["--task-id", String(parentTaskID), "--status", "Any", "--date", "Any"]);
    if (!parentTasks.length) throw new Error(`parent task not found: ${parentTaskID}`);
    const parentTask = parentTasks[0];
    const day = String(toDay(parentTask.date) || todayLocal());
    return [
      {
        parentTaskID,
        taskIDs,
        day,
        parent: {
          app: String(parentTask.app || "").trim(),
          book_id: String(parentTask.book_id || "").trim(),
          params: String(parentTask.params || "").trim(),
        },
      },
    ];
  }

  const taskApp = String(args.taskApp || "").trim();
  const taskScene = "综合页搜索";
  const taskStatus = String(args.taskStatus || "success").trim() || "success";
  const taskDate = String(args.taskDate || "Today").trim() || "Today";
  const taskLimit = parseNonNegativeInt(String(args.taskLimit || "0"), "task limit");
  if (!taskApp) throw new Error("--task-app is required when --task-ids is absent");

  const fetchArgs = ["--app", taskApp, "--scene", taskScene, "--status", taskStatus, "--date", taskDate];
  if (taskLimit > 0) fetchArgs.push("--limit", String(taskLimit));

  const fetchedTasks = runTaskFetch(fetchArgs);
  const expectDate = resolveDateFilter(taskDate);
  const expectStatus = normalizeText(taskStatus);
  const expectScene = normalizeText(taskScene);
  const filteredTasks = fetchedTasks.filter((task) => {
    if (normalizeText(String(task.app || "")) !== normalizeText(taskApp)) return false;
    if (expectScene !== "any" && normalizeText(String(task.scene || "")) !== expectScene) return false;
    if (expectStatus !== "any" && normalizeText(String(task.status || "")) !== expectStatus) return false;
    if (expectDate !== "any") {
      const actualDay = toDay(String(task.date || ""));
      if (!actualDay || actualDay !== expectDate) return false;
    }
    return true;
  });

  const tasks = taskLimit > 0 ? filteredTasks.slice(0, taskLimit) : filteredTasks;
  if (!tasks.length) {
    throw new Error(
      `no tasks matched from feishu after local filter: app=${taskApp}, scene=${taskScene}, status=${taskStatus}, date=${taskDate}, limit=${taskLimit}, fetched=${fetchedTasks.length}`,
    );
  }

  const byBookID = new Map<string, typeof tasks>();
  for (const task of tasks) {
    const bookID = String(task.book_id || "").trim();
    if (!bookID) continue;
    const arr = byBookID.get(bookID) || [];
    arr.push(task);
    byBookID.set(bookID, arr);
  }
  if (!byBookID.size) {
    throw new Error("none of fetched tasks has non-empty BookID; cannot group for piracy detection");
  }

  const units: DetectTaskUnit[] = [];
  for (const [bookID, bucket] of byBookID.entries()) {
    const ids = Array.from(new Set(bucket.map((x) => parsePositiveInt(String(x.task_id), "task id")))).sort((a, b) => a - b);
    if (!ids.length) continue;
    const parentTaskID = ids[0];
    const parentTask = bucket.find((x) => Number(x.task_id) === parentTaskID) || bucket[0];
    const day = String(toDay(parentTask.date) || todayLocal());

    units.push({
      parentTaskID,
      taskIDs: ids,
      day,
      parent: {
        app: String(parentTask.app || taskApp || "").trim(),
        book_id: String(bookID || "").trim(),
        params: String(parentTask.params || "").trim(),
      },
    });
  }

  units.sort((a, b) => a.parentTaskID - b.parentTaskID);
  return units;
}
