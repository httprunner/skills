#!/usr/bin/env node
import {
  BitableRef,
  ClampPageSize,
  DefaultBaseURL,
  Env,
  GetTenantAccessToken,
  NormalizeBitableValue,
  ParseBitableURL,
  RequestJSON,
  ResolveWikiAppToken,
} from "./bitable_common";
import fs from "node:fs";
import { Command } from "commander";

type DramaFieldMap = {
  bookID: string;
  name: string;
  durationMin: string;
  episodeCount: string;
  rightsProtectionScenario: string;
  priority: string;
};

function dramaFieldsFromEnv(): DramaFieldMap {
  return {
    bookID: Env("DRAMA_FIELD_BOOKID", "短剧id").trim() || "短剧id",
    name: Env("DRAMA_FIELD_NAME", "短剧名").trim() || "短剧名",
    durationMin: Env("DRAMA_FIELD_DURATION_MIN", "短剧总时长（分钟）").trim() || "短剧总时长（分钟）",
    episodeCount: Env("DRAMA_FIELD_EPISODE_COUNT", "集数").trim() || "集数",
    rightsProtectionScenario: Env("DRAMA_FIELD_RIGHTS_PROTECTION_SCENARIO", "维权场景").trim() || "维权场景",
    priority: Env("DRAMA_FIELD_PRIORITY", "优先级").trim() || "优先级",
  };
}

function parseCSVArg(raw: any, flag: string): string[] {
  const s = String(raw ?? "").trim();
  if (!s) return [];
  if (s.startsWith("[") || s.startsWith("{")) {
    throw new Error(`${flag} must be a comma-separated string, e.g. 111,222,333`);
  }
  return s
    .split(/[\s,，]+/g)
    .map((x) => x.trim())
    .filter(Boolean);
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function resolveBitableRef(baseURL: string, token: string, ref: BitableRef) {
  if (ref.AppToken) return ref;
  if (!ref.WikiToken) throw new Error("bitable URL missing app_token and wiki_token");
  ref.AppToken = await ResolveWikiAppToken(baseURL, token, ref.WikiToken);
  return ref;
}

async function searchAll(baseURL: string, token: string, ref: BitableRef, filterObj: any, pageSize: number, ignoreView: boolean, viewID: string, limit: number) {
  const items: any[] = [];
  let pageToken = "";
  const size = ClampPageSize(pageSize);
  while (true) {
    const q = new URLSearchParams();
    q.set("page_size", String(size));
    if (pageToken) q.set("page_token", pageToken);
    const urlStr = `${baseURL.replace(/\/+$/, "")}/open-apis/bitable/v1/apps/${ref.AppToken}/tables/${ref.TableID}/records/search?${q.toString()}`;
    const body: any = {};
    if (!ignoreView && viewID) body.view_id = viewID;
    if (filterObj) body.filter = filterObj;
    const resp = (await RequestJSON("POST", urlStr, token, Object.keys(body).length ? body : null, true)) as any;
    if (resp.code !== 0) throw new Error(`search records failed: code=${resp.code} msg=${resp.msg}`);
    const batch = resp.data?.items || [];
    items.push(...batch);
    if (limit > 0 && items.length >= limit) return items.slice(0, limit);
    if (!resp.data?.has_more) break;
    pageToken = resp.data?.page_token || "";
    if (!pageToken) break;
  }
  return items;
}

async function main() {
  const program = new Command();
  program.name("bitable-lookup").description("Lookup drama meta records by 短剧id (BookID) and output JSONL");

  program
    .command("fetch")
    .description("Fetch drama meta by 短剧id list")
    .requiredOption("--bitable-url <url>", "Drama Bitable URL (DRAMA_BITABLE_URL)")
    .requiredOption("--book-id <csv>", "BookID(s), comma-separated (e.g. 111 or 111,222,333)")
    .option("--page-size <n>", "Page size (max 500)", "200")
    .option("--limit <n>", "Max records to return (0 = no cap)", "0")
    .option("--output <path>", "Output path for JSONL (default stdout)")
    .action(async (opts) => {
      const bitableURL = String(opts.bitableUrl || "").trim();
      let bookIDs: string[] = [];
      try {
        bookIDs = parseCSVArg(opts.bookId, "--book-id");
      } catch (err: any) {
        process.stderr.write(`[feishu-bitable-task-manager] ${err?.message || String(err)}\n`);
        process.exit(2);
      }
      const pageSize = Math.trunc(Number(String(opts.pageSize || "200").trim())) || 200;
      const limit = Math.trunc(Number(String(opts.limit || "0").trim())) || 0;

      if (!bitableURL) {
        process.stderr.write("[feishu-bitable-task-manager] --bitable-url is required\n");
        process.exit(2);
      }
      if (!bookIDs.length) {
        process.stderr.write("[feishu-bitable-task-manager] --book-id is empty\n");
        process.exit(2);
      }

      const appID = Env("FEISHU_APP_ID", "");
      const appSecret = Env("FEISHU_APP_SECRET", "");
      if (!appID || !appSecret) {
        process.stderr.write("[feishu-bitable-task-manager] FEISHU_APP_ID/FEISHU_APP_SECRET are required\n");
        process.exit(2);
      }
      const baseURL = Env("FEISHU_BASE_URL", DefaultBaseURL);
      let ref: BitableRef;
      try {
        ref = ParseBitableURL(bitableURL);
      } catch (err: any) {
        process.stderr.write(`[feishu-bitable-task-manager] parse bitable URL failed: ${err?.message || String(err)}\n`);
        process.exit(2);
      }
      let token = "";
      try {
        token = await GetTenantAccessToken(baseURL, appID, appSecret);
      } catch (err: any) {
        process.stderr.write(`[feishu-bitable-task-manager] get tenant access token failed: ${err?.message || String(err)}\n`);
        process.exit(2);
      }
      try {
        ref = await resolveBitableRef(baseURL, token, ref);
      } catch (err: any) {
        process.stderr.write(`[feishu-bitable-task-manager] resolve bitable app token failed: ${err?.message || String(err)}\n`);
        process.exit(2);
      }

      const viewID = String(ref.ViewID || "").trim();
      const ignoreView = true;
      const df = dramaFieldsFromEnv();

      const outStream = opts.output ? fs.createWriteStream(String(opts.output)) : process.stdout;
      let emitted = 0;

      for (const batch of chunk(bookIDs, 50)) {
        const filterObj = {
          conjunction: "or",
          conditions: batch.map((v) => ({ field_name: df.bookID, operator: "is", value: [v] })),
        };
        const items = await searchAll(baseURL, token, ref, filterObj, pageSize, ignoreView, viewID, limit > 0 ? Math.max(limit - emitted, 0) : 0);
        for (const item of items) {
          const recordID = NormalizeBitableValue(item?.record_id).trim();
          const fieldsRaw = item?.fields || {};
          const row = {
            record_id: recordID,
            book_id: NormalizeBitableValue(fieldsRaw[df.bookID]).trim(),
            name: NormalizeBitableValue(fieldsRaw[df.name]).trim(),
            duration_min: NormalizeBitableValue(fieldsRaw[df.durationMin]).trim(),
            episode_count: NormalizeBitableValue(fieldsRaw[df.episodeCount]).trim(),
            rights_protection_scenario: NormalizeBitableValue(fieldsRaw[df.rightsProtectionScenario]).trim(),
            priority: NormalizeBitableValue(fieldsRaw[df.priority]).trim(),
          };
          if (row.book_id) outStream.write(`${JSON.stringify(row)}\n`);
          emitted++;
          if (limit > 0 && emitted >= limit) break;
        }
        if (limit > 0 && emitted >= limit) break;
      }
    });

  program.showHelpAfterError().showSuggestionAfterError();
  await program.parseAsync(process.argv);
}

main().catch((err) => {
  process.stderr.write(`[feishu-bitable-task-manager] ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
