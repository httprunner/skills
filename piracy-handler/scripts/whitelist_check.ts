#!/usr/bin/env node
import { Command } from "commander";
import { env, must } from "./webhook/lib";

type CLIOptions = {
  bookId?: string;
  accountId?: string;
  hasShortPlayTag?: string;
  platform?: string;
  viewingMode?: string;
  infringementRatio?: string;
  format: string;
};

function parseCLI(argv: string[]): CLIOptions {
  const program = new Command();
  program
    .name("whitelist_check")
    .description("Check if a book is exempt via GET /drama/exemption")
    .option("--book-id <id>", "BookID (required)")
    .option("--account-id <id>", "AccountID (required)")
    .option("--has-short-play-tag <bool>", "Has short-play tag: true|false (required)")
    .option("--platform <name>", "Platform: 快手|微信视频号 (default: 微信视频号)", "微信视频号")
    .option("--viewing-mode <mode>", "free or need_pay (default: free)", "free")
    .option("--infringement-ratio <num>", "Infringement ratio percent (default: 1)", "1")
    .option("--format <format>", "Output format: json|text", "text")
    .showHelpAfterError()
    .showSuggestionAfterError();

  program.parse(argv);
  return program.opts<CLIOptions>();
}

type ExemptionResult = {
  exempt: boolean;
  bookId: string;
  accountId: string;
};

async function checkExemption(opts: {
  bookId: string;
  accountId: string;
  hasShortPlayTag: boolean;
  platform: string;
  viewingMode: string;
  infringementRatio: number;
}): Promise<ExemptionResult> {
  const baseURL = must("CRAWLER_SERVICE_BASE_URL").replace(/\/+$/, "");
  const params = new URLSearchParams({
    book_id: opts.bookId,
    account_id: opts.accountId,
    has_short_play_tag: String(opts.hasShortPlayTag),
    platform: opts.platform,
    viewing_mode_level_one: opts.viewingMode,
    infringement_ratio_percent: String(opts.infringementRatio),
  });
  const url = `${baseURL}/drama/exemption?${params}`;

  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { "Content-Type": "application/json; charset=utf-8" },
    });

    if (!res.ok) {
      throw new Error(`http ${res.status}: ${await res.text()}`);
    }

    const data = await res.json();
    if (data?.code !== 0) {
      throw new Error(`api error: code=${data?.code}, msg=${data?.msg}`);
    }
    return {
      exempt: Boolean(data.data),
      bookId: opts.bookId,
      accountId: opts.accountId,
    };
  } catch (err) {
    throw new Error(`exemption check failed: ${err instanceof Error ? err.message : String(err)}`);
  }
}

async function main() {
  const args = parseCLI(process.argv);
  const bookId = String(args.bookId || "").trim();
  const accountId = String(args.accountId || "").trim();
  const hasShortPlayTagStr = String(args.hasShortPlayTag || "").trim().toLowerCase();

  if (!bookId) throw new Error("--book-id is required");
  if (!accountId) throw new Error("--account-id is required");
  if (hasShortPlayTagStr !== "true" && hasShortPlayTagStr !== "false") {
    throw new Error("--has-short-play-tag is required (true|false)");
  }
  const VALID_PLATFORMS = ["快手", "微信视频号"];
  const platform = args.platform || "微信视频号";
  if (!VALID_PLATFORMS.includes(platform)) {
    throw new Error(`--platform must be one of: ${VALID_PLATFORMS.join(", ")}`);
  }

  const result = await checkExemption({
    bookId,
    accountId,
    hasShortPlayTag: hasShortPlayTagStr === "true",
    platform,
    viewingMode: args.viewingMode || "free",
    infringementRatio: Number(args.infringementRatio) || 1,
  });

  if (args.format === "json") {
    console.log(JSON.stringify(result, null, 2));
  } else {
    console.log(result.exempt ? "EXEMPT" : "NOT_EXEMPT");
  }
}

main().catch((err) => {
  console.error(`[piracy-handler] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
