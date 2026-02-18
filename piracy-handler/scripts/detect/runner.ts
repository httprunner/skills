import fs from "fs";
import os from "os";
import path from "path";
import { dayStartMs, defaultDetectPath, ensureDir, expandHome, must, toNumber } from "../shared/lib";
import { buildDetectOutput, type DetectOutput } from "./core";
import { createResultSource, type ResultSourceOptions } from "../data/result_source";
import type { DetectTaskUnit } from "./task_units";

export type DetectRunnerInput = {
  units: DetectTaskUnit[];
  threshold: number;
  output?: string;
  resultSource: ResultSourceOptions;
};

export type DetectRunnerOutput = {
  unit: DetectTaskUnit;
  detect: DetectOutput;
  outputPath: string;
  rowCount: number;
};

function sanitizeForFilename(v: string): string {
  return String(v || "")
    .trim()
    .replace(/[^\w.-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 64);
}

function defaultDetectPathByTaskIDs(taskIDs: number[]): string {
  const ids = Array.from(new Set((taskIDs || []).map((x) => Math.trunc(Number(x))).filter((x) => x > 0))).sort((a, b) => a - b);
  if (!ids.length) throw new Error("cannot build default detect path: empty task ids");
  if (ids.length === 1) return defaultDetectPath(ids[0]);
  const joined = ids.join("_");
  return path.join(os.homedir(), ".eval", "detect", `${joined}.json`);
}

export function resolveDetectOutputPath(baseOutputArg: string | undefined, unit: DetectTaskUnit, multi: boolean): string {
  const outArg = String(baseOutputArg || "").trim();
  if (!outArg) return expandHome(defaultDetectPathByTaskIDs(unit.taskIDs));
  if (outArg === "-") return "-";

  const expanded = expandHome(outArg);
  if (!multi) return expanded;

  const parsed = path.parse(expanded);
  const bookSuffix = sanitizeForFilename(unit.parent.book_id) || "book";
  if (parsed.ext.toLowerCase() === ".json") {
    return path.join(parsed.dir, `${parsed.name}_${unit.anchorTaskID}_${bookSuffix}.json`);
  }
  return path.join(expanded, `${unit.anchorTaskID}_${bookSuffix}.json`);
}

export async function runDetectForUnits(input: DetectRunnerInput): Promise<DetectRunnerOutput[]> {
  must("FEISHU_APP_ID");
  must("FEISHU_APP_SECRET");
  const dramaURL = must("DRAMA_BITABLE_URL");

  const threshold = toNumber(input.threshold, 0.5);
  const units = input.units || [];
  if (!units.length) throw new Error("no task units to detect");

  const source = createResultSource(input.resultSource);
  const multi = units.length > 1;
  const out: DetectRunnerOutput[] = [];

  for (const unit of units) {
    const day = String(unit.day || "").trim();
    const dayMs = dayStartMs(day);
    if (!dayMs) throw new Error(`invalid day: ${day}`);

    const rawRows = await source.fetchByTaskIDs(unit.taskIDs);
    const detect = buildDetectOutput({
      sourceTaskIDs: unit.taskIDs,
      threshold,
      day,
      dayMs,
      rawRows,
      dramaURL,
      sourcePath: source.describe(),
      sourceType: input.resultSource.dataSource,
    });

    const outputPath = resolveDetectOutputPath(input.output, unit, multi);
    if (outputPath !== "-") {
      ensureDir(path.dirname(outputPath));
      fs.writeFileSync(outputPath, JSON.stringify(detect, null, 2));
    }

    out.push({
      unit,
      detect,
      outputPath,
      rowCount: rawRows.length,
    });
  }

  return out;
}
