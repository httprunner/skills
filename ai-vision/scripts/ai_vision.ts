#!/usr/bin/env node
import fs from "node:fs";
import { Command } from "commander";
import chalk from "chalk";

const envArkBaseURL = "ARK_BASE_URL";
const envArkAPIKey = "ARK_API_KEY";
const envArkModel = "ARK_MODEL_NAME";
const defaultTimeoutMs = 120_000;

type LogLevel = "debug" | "info" | "error";

function createLogger(json: boolean, level: LogLevel, stream: NodeJS.WriteStream, color: boolean) {
  const levels: Record<LogLevel, number> = { debug: 10, info: 20, error: 40 };
  const min = levels[level] ?? 20;
  const useColor = color && !json;
  const levelColor = (lv: LogLevel, value: string) => {
    if (!useColor) return value;
    return lv === "error" ? chalk.red(value) : lv === "debug" ? chalk.cyan(value) : chalk.green(value);
  };
  const keyColor = (value: string) => (useColor ? chalk.blue(value) : value);
  const msgColor = (value: string) => (useColor ? chalk.green(value) : value);
  const valueColor = (value: string) => (useColor ? chalk.dim(value) : value);
  function shouldLog(lv: LogLevel) {
    return levels[lv] >= min;
  }
  function formatValue(value: unknown): string {
    if (typeof value === "string") {
      if (value === "") return '""';
      if (/\s/.test(value) || value.includes("=") || value.includes("\"")) {
        return JSON.stringify(value);
      }
      return value;
    }
    if (value === null || value === undefined) return "null";
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return JSON.stringify(value);
  }
  function write(lv: LogLevel, msg: string, fields?: Record<string, unknown>) {
    if (!shouldLog(lv)) return;
    const time = new Date().toISOString();
    if (json) {
      const payload: Record<string, unknown> = { time, level: lv.toUpperCase(), msg };
      if (fields) {
        for (const [k, v] of Object.entries(fields)) payload[k] = v;
      }
      stream.write(`${JSON.stringify(payload)}\n`);
      return;
    }
    const parts = [
      `${keyColor("time")}=${valueColor(time)}`,
      `${keyColor("level")}=${levelColor(lv, lv.toUpperCase())}`,
      `${keyColor("msg")}=${msgColor(msg)}`,
    ];
    if (fields) {
      for (const [k, v] of Object.entries(fields)) {
        parts.push(`${keyColor(k)}=${valueColor(formatValue(v))}`);
      }
    }
    stream.write(parts.join(" ") + "\n");
  }
  return {
    debug: (msg: string, fields?: Record<string, unknown>) => write("debug", msg, fields),
    info: (msg: string, fields?: Record<string, unknown>) => write("info", msg, fields),
    error: (msg: string, fields?: Record<string, unknown>) => write("error", msg, fields),
  };
}

let logJsonEnabled = false;
let logger = createLogger(false, "info", process.stderr, false);
let errLogger = createLogger(false, "info", process.stderr, false);

function setLoggerConfig(jsonEnabled: boolean, level: LogLevel) {
  logJsonEnabled = jsonEnabled;
  const stdoutColor = Boolean(process.stderr.isTTY) && !jsonEnabled;
  logger = createLogger(jsonEnabled, level, process.stderr, stdoutColor);
  errLogger = createLogger(jsonEnabled, level, process.stderr, stdoutColor);
}

function setLoggerFromProgram(program: Command) {
  const opts = program.opts<{ logJson?: boolean; logLevel?: string }>();
  const level = String(opts.logLevel || "info").toLowerCase() === "debug" ? "debug" : "info";
  setLoggerConfig(Boolean(opts.logJson), level as LogLevel);
}

function getModelConfig(modelName: string, baseURL: string, apiKey: string) {
  let model = modelName || process.env[envArkModel] || "doubao-seed-1-6-vision-250815";
  let base = baseURL || process.env[envArkBaseURL] || "";
  let key = apiKey || process.env[envArkAPIKey] || "";
  if (!base) throw new Error("missing base URL (set ARK_BASE_URL or pass --base-url)");
  if (!key) throw new Error("missing API key (set ARK_API_KEY or pass --api-key)");
  return { BaseURL: base, APIKey: key, Model: model };
}

function readImageSize(buf: Buffer): { width: number; height: number } {
  if (buf.length < 10) throw new Error("invalid image");
  // PNG
  if (buf.slice(0, 8).toString("hex") === "89504e470d0a1a0a" && buf.length >= 24) {
    return { width: buf.readUInt32BE(16), height: buf.readUInt32BE(20) };
  }
  // JPEG
  if (buf[0] === 0xff && buf[1] === 0xd8) {
    let offset = 2;
    while (offset < buf.length) {
      if (buf[offset] !== 0xff) break;
      const marker = buf[offset + 1];
      const size = buf.readUInt16BE(offset + 2);
      if (marker >= 0xc0 && marker <= 0xc3) {
        const height = buf.readUInt16BE(offset + 5);
        const width = buf.readUInt16BE(offset + 7);
        return { width, height };
      }
      offset += 2 + size;
    }
  }
  throw new Error("unsupported image format");
}

function loadImage(filePath: string): { b64: string; size: { width: number; height: number } } {
  const raw = fs.readFileSync(filePath);
  const size = readImageSize(raw);
  return { b64: raw.toString("base64"), size };
}

async function callModel(
  cfg: { BaseURL: string; APIKey: string; Model: string },
  systemPrompt: string,
  userPrompt: string,
  imgB64: string,
) {
  const url = `${cfg.BaseURL.replace(/\/+$/, "")}/responses`;
  const body = {
    model: cfg.Model,
    temperature: 0,
    instructions: systemPrompt,
    input: [
      {
        role: "user",
        content: [
          { type: "input_image", image_url: `data:image/png;base64,${imgB64}` },
          { type: "input_text", text: userPrompt },
        ],
      },
    ],
  };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), defaultTimeoutMs);
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${cfg.APIKey}`,
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!resp.ok) {
      const msg = (await resp.text()).trim();
      throw new Error(`model request failed: ${resp.status} ${resp.statusText}: ${msg}`);
    }
    const raw = await resp.text();
    logger.debug("model raw response", { data: raw });
    const text = extractResponseText(raw);
    const status = extractResponseStatus(raw);
    return { text, status };
  } finally {
    clearTimeout(timer);
  }
}

function extractResponseText(raw: string) {
  const parsed = JSON.parse(raw);
  const output = parsed.output as Array<any> | undefined;
  if (!output) throw new Error("empty model response");
  const parts: string[] = [];
  for (const item of output) {
    if (item.type !== "message") continue;
    if (item.status && item.status !== "completed") {
      logger.debug("model output status", { status: item.status, partial: item.partial });
    }
    const content = item.content as Array<any> | undefined;
    if (!content) continue;
    for (const c of content) {
      if (c.type !== "output_text") continue;
      const t = String(c.text ?? "").trim();
      if (!t) continue;
      parts.push(t);
    }
  }
  if (parts.length === 0) throw new Error("empty model response");
  return parts.join("\n");
}

function extractResponseStatus(raw: string) {
  try {
    const parsed = JSON.parse(raw);
    return typeof parsed.status === "string" ? parsed.status : "";
  } catch {
    return "";
  }
}

function extractJSONFromContent(content: string) {
  if (content.includes("```json")) {
    const start = content.indexOf("```json") + 7;
    const end = content.indexOf("```", start);
    if (end !== -1) return content.slice(start, end).trim();
  }
  if (content.startsWith("```") && content.endsWith("```")) {
    const lines = content.split("\n");
    if (lines.length >= 3) {
      const jsonContent = lines.slice(1, -1).join("\n").trim();
      if (jsonContent.startsWith("{") && jsonContent.endsWith("}")) return jsonContent;
    }
  }
  const start = content.indexOf("{");
  if (start !== -1) {
    let brace = 0;
    let inString = false;
    let escaped = false;
    for (let i = start; i < content.length; i++) {
      const ch = content[i];
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === "\\" && inString) {
        escaped = true;
        continue;
      }
      if (ch === '"') {
        inString = !inString;
        continue;
      }
      if (!inString) {
        if (ch === "{") brace++;
        if (ch === "}") {
          brace--;
          if (brace === 0) return content.slice(start, i + 1).trim();
        }
      }
    }
  }
  return "";
}

function cleanJSONContent(content: string) {
  return content.replace(/,\}/g, "}").replace(/,\]/g, "]");
}

function parseStructuredResponse(content: string) {
  const clean = content.trim();
  let jsonContent = extractJSONFromContent(clean) || clean;
  try {
    return JSON.parse(jsonContent);
  } catch {
    const cleaned = cleanJSONContent(jsonContent);
    return JSON.parse(cleaned);
  }
}

function normalizeQueryResult(result: any, size: { width: number; height: number }, content: string) {
  let raw = extractJSONFromContent(content.trim());
  if (!raw) raw = content.trim();
  let payload: any;
  try {
    payload = JSON.parse(raw);
  } catch {
    if (!result.content) result.content = content.trim();
    return;
  }
  const x = asNumber(payload.x);
  const y = asNumber(payload.y);
  if (x === null || y === null) {
    if (!result.content) result.content = content.trim();
    return;
  }
  const w = asNumber(payload.w);
  const h = asNumber(payload.h);
  const maxVal = Math.max(x, y, w ?? 0, h ?? 0);
  let nx = x;
  let ny = y;
  let nw = w;
  let nh = h;
  if (maxVal > 0 && maxVal <= 1000 && size.width > 0 && size.height > 0) {
    nx = (x / 1000) * size.width;
    ny = (y / 1000) * size.height;
    if (w !== null) nw = (w / 1000) * size.width;
    if (h !== null) nh = (h / 1000) * size.height;
  }
  payload.x = nx;
  payload.y = ny;
  if (nw !== null) payload.w = nw;
  if (nh !== null) payload.h = nh;
  result.content = JSON.stringify(payload);
  result.data = payload;
}

function asNumber(value: any): number | null {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const v = Number(value);
    return Number.isFinite(v) ? v : null;
  }
  return null;
}

function normalizeCoordinatesFormat(text: string) {
  if (text.includes("<point>")) {
    const re = /<point>(\d+)\s+(\d+)(?:\s+(\d+)\s+(\d+))?<\/point>/g;
    text = text.replace(re, (_, a, b, c, d) => {
      if (c && d) return `(${a},${b},${c},${d})`;
      return `(${a},${b})`;
    });
  }
  if (text.includes("<bbox>")) {
    const re = /<bbox>(\d+)\s+(\d+)\s+(\d+)\s+(\d+)<\/bbox>/g;
    text = text.replace(re, (_, a, b, c, d) => `(${a},${b},${c},${d})`);
  }
  if (text.includes("[") && text.includes("]")) {
    const re = /\[(\d+),\s*(\d+),\s*(\d+),\s*(\d+)\]/g;
    text = text.replace(re, (_, a, b, c, d) => `(${a},${b},${c},${d})`);
  }
  return text;
}

function parseBoxString(text: string): number[] {
  const normalized = normalizeCoordinatesFormat(text);
  const re = /-?\d+(?:\.\d+)?/g;
  const nums = normalized.match(re);
  if (!nums || nums.length < 2) throw new Error("invalid box string");
  const values = nums.map((n) => Number(n));
  if (values.length === 2) return values;
  return values.slice(0, 4);
}

function maxFloat(vals: number[]) {
  return vals.reduce((m, v) => (v > m ? v : m), vals[0] ?? 0);
}

function scaleRelativePoint(pt: number[], size: { width: number; height: number }) {
  if (pt.length !== 2) return pt;
  return [pt[0] / 1000 * size.width, pt[1] / 1000 * size.height];
}

function scaleRelativeBox(box: number[], size: { width: number; height: number }) {
  if (box.length !== 4) return box;
  return [
    box[0] / 1000 * size.width,
    box[1] / 1000 * size.height,
    box[2] / 1000 * size.width,
    box[3] / 1000 * size.height,
  ];
}

function boxCenter(box: number[]) {
  if (box.length < 4) return box;
  return [(box[0] + box[2]) / 2, (box[1] + box[3]) / 2];
}

function processArgument(name: string, value: any, size: { width: number; height: number }) {
  if (name === "start_box" || name === "end_box") {
    const handle = (coords: number[]) => {
      let out = coords;
      if (out.length === 4 && maxFloat(out) <= 1000) out = scaleRelativeBox(out, size);
      if (out.length === 2 && maxFloat(out) <= 1000) out = scaleRelativePoint(out, size);
      if (out.length === 2) return out;
      if (out.length === 4) return boxCenter(out);
      return out;
    };
    if (typeof value === "string") return handle(parseBoxString(value));
    if (Array.isArray(value)) {
      const coords: number[] = [];
      for (const v of value) {
        if (typeof v === "number") coords.push(v);
        else if (typeof v === "string") coords.push(...parseBoxString(v));
      }
      return handle(coords);
    }
  }
  return value;
}

function processActionArguments(raw: Record<string, any>, size: { width: number; height: number }) {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(raw || {})) {
    const key = k === "start_point" || k === "point" ? "start_box" : k === "end_point" ? "end_box" : k;
    out[key] = processArgument(key, v, size);
  }
  return out;
}

function convertAction(action: { action_type: string; action_inputs: Record<string, any> }) {
  const out: any = { action: action.action_type, raw: action.action_inputs };
  switch (action.action_type) {
    case "click":
    case "left_double":
    case "right_single":
    case "long_press":
    case "scroll":
      if (Array.isArray(action.action_inputs.start_box) && action.action_inputs.start_box.length === 2) {
        out.x = action.action_inputs.start_box[0];
        out.y = action.action_inputs.start_box[1];
      }
      if (typeof action.action_inputs.direction === "string") out.direction = action.action_inputs.direction;
      break;
    case "drag":
      if (Array.isArray(action.action_inputs.start_box) && action.action_inputs.start_box.length === 2) {
        out.x = action.action_inputs.start_box[0];
        out.y = action.action_inputs.start_box[1];
      }
      if (Array.isArray(action.action_inputs.end_box) && action.action_inputs.end_box.length === 2) {
        out.to_x = action.action_inputs.end_box[0];
        out.to_y = action.action_inputs.end_box[1];
      }
      break;
    case "type":
      if (typeof action.action_inputs.content === "string") out.text = action.action_inputs.content;
      break;
    case "hotkey":
      if (typeof action.action_inputs.key === "string") out.key = action.action_inputs.key;
      break;
  }
  return out;
}

function parseJSONPlanning(content: string, size: { width: number; height: number }) {
  const resp = parseStructuredResponse(content);
  if (resp.error) throw new Error(resp.error);
  const actions: any[] = [];
  for (const act of resp.actions || []) {
    const processed = processActionArguments(act.action_inputs || {}, size);
    actions.push(convertAction({ action_type: act.action_type, action_inputs: processed }));
  }
  return { thought: resp.thought, actions };
}

function printJSON(value: any) {
  if (logJsonEnabled) {
    if (value && typeof value === "object" && "actions" in value && "thought" in value) {
      logger.info("result", { status: value.status, thought: value.thought, actions: value.actions });
      return 0;
    }
    if (value && typeof value === "object" && "result" in value) {
      const status = value.status;
      const result = value.result as any;
      if (result && typeof result === "object") {
        if ("pass" in result) {
          logger.info("result", { status, pass: result.pass, thought: result.thought, content: result.content });
          return 0;
        }
        if ("content" in result || "data" in result) {
          logger.info("result", { status, thought: result.thought, content: result.content, data: result.data });
          return 0;
        }
      }
    }
    logger.info("result", { data: value });
    return 0;
  }
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
  return 0;
}

const defaultQueryPrompt = `You are an AI assistant specialized in analyzing images and extracting information. User will provide a screenshot and a query asking for specific information to be extracted from the image. Please analyze the image carefully and provide the requested information.`;

const defaultAssertionPrompt = `You are a senior testing engineer. User will give an assertion and a screenshot of a page. By carefully viewing the screenshot, please tell whether the assertion is truthy.`;

const doubaoThinkingVisionPrompt = `You are a GUI agent. You are given a task and your action history, with screenshots. You need to perform the next action to complete the task.

Target: User will give you a screenshot, an instruction and some previous logs indicating what have been done. Please tell what the next one action is (or null if no action should be done) to do the tasks the instruction requires.

Restriction:
- Don't give extra actions or plans beyond the instruction. ONLY plan for what the instruction requires. For example, don't try to submit the form if the instruction is only to fill something.
- Don't repeat actions in the previous logs.
- Bbox is the bounding box of the element to be located. It's an array of 4 numbers, representing [x1, y1, x2, y2] coordinates in 1000x1000 relative coordinates system.

Supporting actions:
- click: { action_type: "click", action_inputs: { start_box: [x1, y1, x2, y2] } }
- long_press: { action_type: "long_press", action_inputs: { start_box: [x1, y1, x2, y2] } }
- type: { action_type: "type", action_inputs: { content: string } } // If you want to submit your input, use "\\n" at the end of content.
- scroll: { action_type: "scroll", action_inputs: { start_box: [x1, y1, x2, y2], direction: "down" | "up" | "left" | "right" } }
- drag: { action_type: "drag", action_inputs: { start_box: [x1, y1, x2, y2], end_box: [x3, y3, x4, y4] } }
- press_home: { action_type: "press_home", action_inputs: {} }
- press_back: { action_type: "press_back", action_inputs: {} }
- wait: { action_type: "wait", action_inputs: {} } // Sleep for 5s and take a screenshot to check for any changes.
- finished: { action_type: "finished", action_inputs: { content: string } } // Use escape characters \\\\', \\\" , and \\\\n in content part to ensure we can parse the content in normal python string format.

Field description:
* The start_box and end_box fields represent the bounding box coordinates of the target element in 1000x1000 relative coordinate system.
* Use Chinese in log and thought fields.

Return in JSON format:
{
  "actions": [
    {
      "action_type": "...",
      "action_inputs": { ... }
    }
  ],
  "thought": "string", // Log what the next action you can do according to the screenshot and the instruction. Use Chinese.
  "error": "string" | null, // Error messages about unexpected situations, if any. Use Chinese.
}

## User Instruction
`;

async function runQuery(options: {
  screenshot?: string;
  prompt?: string;
  model?: string;
  baseUrl?: string;
  apiKey?: string;
}) {
  const screenshot = String(options.screenshot || "");
  const prompt = String(options.prompt || "");
  if (!screenshot || !prompt) {
    errLogger.error("query requires --screenshot and --prompt");
    return 2;
  }
  let cfg;
  try {
    cfg = getModelConfig(String(options.model || ""), String(options.baseUrl || ""), String(options.apiKey || ""));
  } catch (err: any) {
    errLogger.error("get model config failed", { err: err?.message || String(err) });
    return 1;
  }
  let img;
  try {
    img = loadImage(screenshot);
  } catch (err: any) {
    errLogger.error("load image failed", { err: err?.message || String(err) });
    return 1;
  }
  let content: string;
  let status: string;
  try {
    const res = await callModel(cfg, defaultQueryPrompt, prompt, img.b64);
    content = res.text;
    status = res.status;
  } catch (err: any) {
    errLogger.error("call model failed", { err: err?.message || String(err) });
    return 1;
  }
  let result: any = { content: "", thought: "" };
  try {
    result = parseStructuredResponse(content);
  } catch {
    result.content = content;
    result.thought = "Failed to parse structured response";
  }
  normalizeQueryResult(result, img.size, content);
  return printJSON({ size: img.size, result, model: cfg.Model, status });
}

async function runAssert(options: {
  screenshot?: string;
  assertion?: string;
  model?: string;
  baseUrl?: string;
  apiKey?: string;
}) {
  const screenshot = String(options.screenshot || "");
  const assertion = String(options.assertion || "");
  if (!screenshot || !assertion) {
    errLogger.error("assert requires --screenshot and --assertion");
    return 2;
  }
  let cfg;
  try {
    cfg = getModelConfig(String(options.model || ""), String(options.baseUrl || ""), String(options.apiKey || ""));
  } catch (err: any) {
    errLogger.error("get model config failed", { err: err?.message || String(err) });
    return 1;
  }
  let img;
  try {
    img = loadImage(screenshot);
  } catch (err: any) {
    errLogger.error("load image failed", { err: err?.message || String(err) });
    return 1;
  }
  let content: string;
  let status: string;
  try {
    const res = await callModel(cfg, defaultAssertionPrompt, assertion, img.b64);
    content = res.text;
    status = res.status;
  } catch (err: any) {
    errLogger.error("call model failed", { err: err?.message || String(err) });
    return 1;
  }
  let result: any = { content: "", thought: "" };
  try {
    result = parseStructuredResponse(content);
  } catch {
    result.content = content;
    result.thought = "Failed to parse structured response";
  }
  return printJSON({ size: img.size, result, model: cfg.Model, status });
}

async function runPlanNext(options: {
  screenshot?: string;
  instruction?: string;
  history?: string;
  model?: string;
  baseUrl?: string;
  apiKey?: string;
}) {
  const screenshot = String(options.screenshot || "");
  const instruction = String(options.instruction || "").trim();
  const history = String(options.history || "").trim();
  if (!screenshot || !instruction) {
    errLogger.error("plan-next requires --screenshot and --instruction");
    return 2;
  }
  let cfg;
  try {
    cfg = getModelConfig(String(options.model || ""), String(options.baseUrl || ""), String(options.apiKey || ""));
  } catch (err: any) {
    errLogger.error("get model config failed", { err: err?.message || String(err) });
    return 1;
  }
  let img;
  try {
    img = loadImage(screenshot);
  } catch (err: any) {
    errLogger.error("load image failed", { err: err?.message || String(err) });
    return 1;
  }
  let userPrompt = instruction;
  if (history) userPrompt = `Instruction:\n${instruction}\n\nHistory:\n${history}`;
  let content: string;
  let status: string;
  try {
    const res = await callModel(cfg, doubaoThinkingVisionPrompt, userPrompt, img.b64);
    content = res.text;
    status = res.status;
  } catch (err: any) {
    errLogger.error("call model failed", { err: err?.message || String(err) });
    return 1;
  }
  let result: any;
  try {
    result = parseJSONPlanning(content, img.size);
  } catch (err: any) {
    errLogger.error("parse JSON planning failed", { err: err?.message || String(err) });
    return 1;
  }
  result.status = status;
  return printJSON(result);
}

async function main() {
  const program = new Command();
  program
    .name("ai_vision")
    .description("Multimodal UI understanding and single-step planning")
    .option("--log-json", "Output logs in JSON")
    .option("--log-level <level>", "Log level: debug or info", "info")
    .showHelpAfterError()
    .showSuggestionAfterError();

  program
    .command("query")
    .description("Extract coordinates or attributes from a screenshot")
    .requiredOption("--screenshot <file>", "screenshot path (png/jpg)")
    .requiredOption("--prompt <text>", "query prompt")
    .option("--model <name>", "model name")
    .option("--base-url <url>", "override base url")
    .option("--api-key <key>", "override api key")
    .action(async (options) => {
      setLoggerFromProgram(program);
      process.exit(await runQuery(options));
    });

  program
    .command("assert")
    .description("Assert a condition against a screenshot")
    .requiredOption("--screenshot <file>", "screenshot path (png/jpg)")
    .requiredOption("--assertion <text>", "assertion text")
    .option("--model <name>", "model name")
    .option("--base-url <url>", "override base url")
    .option("--api-key <key>", "override api key")
    .action(async (options) => {
      setLoggerFromProgram(program);
      process.exit(await runAssert(options));
    });

  program
    .command("plan-next")
    .description("Plan a single next action based on a screenshot")
    .requiredOption("--screenshot <file>", "screenshot path (png/jpg)")
    .requiredOption("--instruction <text>", "instruction text (for action)")
    .option("--history <text>", "optional action history text")
    .option("--model <name>", "model name")
    .option("--base-url <url>", "override base url")
    .option("--api-key <key>", "override api key")
    .action(async (options) => {
      setLoggerFromProgram(program);
      process.exit(await runPlanNext(options));
    });

  await program.parseAsync(process.argv);
}

main().catch((err) => {
  errLogger.error("unhandled error", { err: err?.message || String(err) });
  process.exit(1);
});
