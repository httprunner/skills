import { createResultSource, type ResultSourceOptions } from "../data/result_source";
import type { DetectSkippedUnit, DetectTaskUnit } from "./task_units";

type PrecheckSummary = {
  checked_groups: number;
  passed_groups: number;
  skipped_groups: number;
  skipped_by_reason: Record<string, number>;
  rows_read: number;
};

export type PrecheckResult = {
  readyUnits: DetectTaskUnit[];
  skippedUnits: DetectSkippedUnit[];
  summary: PrecheckSummary;
};

function markReason(counter: Record<string, number>, reason: string) {
  counter[reason] = (counter[reason] || 0) + 1;
}

function pickItemID(row: Record<string, unknown>): string {
  const candidates = ["item_id", "ItemID"] as const;
  for (const key of candidates) {
    const v = String((row as any)?.[key] ?? "").trim();
    if (v) return v;
  }
  return "";
}

function pickTaskID(row: Record<string, unknown>): number {
  const candidates = ["task_id", "TaskID"] as const;
  for (const key of candidates) {
    const n = Math.trunc(Number((row as any)?.[key]));
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}

export async function precheckUnitsByItemsCollected(
  units: DetectTaskUnit[],
  sourceOpts: ResultSourceOptions,
): Promise<PrecheckResult> {
  if (!units.length) {
    return {
      readyUnits: [],
      skippedUnits: [],
      summary: {
        checked_groups: 0,
        passed_groups: 0,
        skipped_groups: 0,
        skipped_by_reason: {},
        rows_read: 0,
      },
    };
  }

  const source = createResultSource(sourceOpts);
  const readyUnits: DetectTaskUnit[] = [];
  const skippedUnits: DetectSkippedUnit[] = [];
  const reasonCounter: Record<string, number> = {};
  let rowsRead = 0;

  for (const unit of units) {
    const rows = await source.fetchByTaskIDs(unit.taskIDs);
    rowsRead += rows.length;
    const observedByTask = new Map<number, Set<string>>();

    for (let idx = 0; idx < rows.length; idx++) {
      const row = rows[idx];
      const taskID = pickTaskID(row);
      if (!taskID) continue;
      const itemID = pickItemID(row) || `__row_${idx}`;
      const seen = observedByTask.get(taskID) || new Set<string>();
      seen.add(itemID);
      observedByTask.set(taskID, seen);
    }

    const underflowTasks = unit.taskIDs
      .map((taskID) => {
        const expected = Number(unit.taskItemsCollected[taskID] || 0);
        const observed = observedByTask.get(taskID)?.size || 0;
        return {
          task_id: taskID,
          expected_items_collected: expected,
          observed_distinct_item_count: observed,
          delta: observed - expected,
        };
      })
      .filter((x) => x.observed_distinct_item_count < x.expected_items_collected);

    // Precheck is for detecting lag/missing rows before detect runs.
    // observed > expected is tolerated because upstream items_collected can be stale,
    // while extra rows are still usable for downstream detect.
    if (underflowTasks.length > 0) {
      skippedUnits.push({
        app: unit.parent.app,
        book_id: unit.parent.book_id,
        day: unit.day,
        task_ids: unit.taskIDs,
        reason: "items_count_mismatch",
        details: {
          mismatch_tasks: underflowTasks,
        },
      });
      markReason(reasonCounter, "items_count_mismatch");
      continue;
    }

    readyUnits.push(unit);
  }

  return {
    readyUnits,
    skippedUnits,
    summary: {
      checked_groups: units.length,
      passed_groups: readyUnits.length,
      skipped_groups: skippedUnits.length,
      skipped_by_reason: reasonCounter,
      rows_read: rowsRead,
    },
  };
}
