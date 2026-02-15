import { parseOptionalPositiveInt } from "../shared/cli";
import type { ResultDataSource, ResultSourceOptions } from "./result_source";

export type ResultSourceCLIArgs = {
  dataSource?: string;
  sqlitePath?: string;
  dbPath?: string;
  table?: string;
  pageSize?: string;
  timeoutMs?: string;
};

export function parseResultDataSource(raw: string | undefined): ResultDataSource {
  const v = String(raw || "sqlite").trim().toLowerCase();
  if (v === "sqlite" || v === "supabase") return v;
  throw new Error(`invalid --data-source: ${raw}`);
}

export function buildResultSourceOptionsFromCLI(args: ResultSourceCLIArgs): ResultSourceOptions {
  return {
    dataSource: parseResultDataSource(args.dataSource),
    dbPath: String(args.sqlitePath || args.dbPath || "").trim() || undefined,
    supabaseTable: String(args.table || "").trim() || undefined,
    pageSize: parseOptionalPositiveInt(args.pageSize, "--page-size"),
    timeoutMs: parseOptionalPositiveInt(args.timeoutMs, "--timeout-ms"),
  };
}
