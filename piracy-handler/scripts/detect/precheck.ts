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
    const seenItemIDs = new Set<string>();
    for (const row of rows) {
      const itemID = pickItemID(row);
      if (!itemID) continue;
      seenItemIDs.add(itemID);
    }

    const distinctItemCount = seenItemIDs.size;
    if (distinctItemCount !== unit.sumItemsCollected) {
      skippedUnits.push({
        app: unit.parent.app,
        book_id: unit.parent.book_id,
        day: unit.day,
        task_ids: unit.taskIDs,
        reason: "items_count_mismatch",
        details: {
          sum_items_collected: unit.sumItemsCollected,
          distinct_item_count: distinctItemCount,
          delta: distinctItemCount - unit.sumItemsCollected,
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
