export function parseOptionalPositiveInt(raw: string | undefined, flag: string): number | undefined {
  if (raw == null || String(raw).trim() === "") return undefined;
  const n = Math.trunc(Number(raw));
  if (!Number.isFinite(n) || n <= 0) throw new Error(`invalid ${flag}: ${raw}`);
  return n;
}

export function parseLimit(raw: string, flag = "--limit"): number {
  const n = Math.trunc(Number(String(raw || "").trim()));
  if (!Number.isFinite(n) || n <= 0) throw new Error(`invalid ${flag}: ${raw}`);
  return n;
}
