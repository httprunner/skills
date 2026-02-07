#!/usr/bin/env node
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import chalk from "chalk";
import { Command } from "commander";

const defaultTimeoutMs = 10_000;
const defaultHandleVerificationPrompt =
  "处理系统安全验证页面。目标是仅完成当前验证挑战，不执行与验证无关的业务操作。安全验证可能是滑块、点选图片、按钮确认、验证码确认等形式。每次只返回一个最小必要动作（click/drag/long_press/press_back/wait/finished）。如果无法确定可执行动作，优先返回 wait。";
const defaultInstallSmartPrompt =
  "处理安装APK时的系统安全提示。严格遵守：1) 严禁点击“安装新版本”；2) 优先完成当前版本安装，若看到“已知悉该应用存在风险”或类似勾选框，先勾选，再点“继续安装”；3) 若出现安全验证（可能是滑块、点击图片、按钮确认等任意形式），根据界面选择最合适的单步动作（click/drag/long_press/press_back/wait 等）推进验证；4) 每次只返回一个推进安装的动作。";
const defaultInstallSmartMaxUiSteps = 20;
const defaultInstallSmartUiIntervalSec = 2;
const defaultInstallSmartInitialWaitSec = 5;

type LogLevel = "debug" | "info" | "error";

function createLogger(json: boolean, level: LogLevel, stream: NodeJS.WriteStream, color: boolean) {
  const levels: Record<LogLevel, number> = { debug: 10, info: 20, error: 40 };
  const min = levels[level] ?? 10;
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
      if (fields) for (const [k, v] of Object.entries(fields)) payload[k] = v;
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

let logger = createLogger(false, "info", process.stderr, Boolean(process.stderr.isTTY));

function parseLogLevel(raw: string | undefined): LogLevel {
  const lv = String(raw || "info").toLowerCase();
  if (lv === "debug" || lv === "error" || lv === "info") return lv;
  return "info";
}

function setLoggerConfig(enabled: boolean, level: LogLevel) {
  const useColor = Boolean(process.stderr.isTTY) && !enabled;
  logger = createLogger(enabled, level, process.stderr, useColor);
}

function adbPrefix(serial: string) {
  return serial ? ["adb", "-s", serial] : ["adb"];
}

type CmdResult = { stdout: string; stderr: string; exitCode: number };

function runCmd(cmd: string[], capture: boolean, timeoutMs: number, cwd?: string): CmdResult {
  const res = spawnSync(cmd[0], cmd.slice(1), {
    encoding: "utf8",
    timeout: timeoutMs > 0 ? timeoutMs : defaultTimeoutMs,
    ...(cwd ? { cwd } : {}),
    stdio: capture ? "pipe" : "inherit",
  });
  if (res.error) {
    if ((res.error as any).code === "ETIMEDOUT") {
      return { stdout: res.stdout ?? "", stderr: "Command timed out", exitCode: 124 };
    }
    return { stdout: res.stdout ?? "", stderr: res.stderr ?? String(res.error), exitCode: 1 };
  }
  const exitCode = typeof res.status === "number" ? res.status : 0;
  return { stdout: res.stdout ?? "", stderr: res.stderr ?? "", exitCode };
}

function runAdb(serial: string, capture: boolean, args: string[]): CmdResult {
  const cmd = [...adbPrefix(serial), ...args];
  logger.debug("exec adb", { cmd: cmd.join(" ") });
  return runCmd(cmd, capture, defaultTimeoutMs);
}

function writeCapturedOutput(stdout: string, stderr: string) {
  if (stdout.trim()) process.stdout.write(stdout);
  if (stderr.trim()) process.stderr.write(stderr);
}

function sleepMs(ms: number) {
  if (ms <= 0) return;
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function sleepAsync(ms: number) {
  if (ms <= 0) return Promise.resolve();
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function randomDelayMs(minMs: number, maxMs: number) {
  const min = Math.max(0, Math.floor(minMs));
  const max = Math.max(min, Math.floor(maxMs));
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

type LaunchTargetKind = "package" | "uri" | "activity";

type LaunchTarget = {
  kind: LaunchTargetKind;
  raw: string;
  packageName?: string;
  activity?: string;
  uri?: string;
};

function parseLaunchTarget(target: string): LaunchTarget {
  const t = target.trim();
  if (!t) throw new Error("launch target is empty");
  if (t.includes("://")) return { kind: "uri", raw: t, uri: t };
  if (t.includes("/")) {
    const [pkg, actRaw] = t.split("/", 2).map((s) => s.trim());
    if (!pkg || !actRaw) throw new Error(`invalid activity target: ${JSON.stringify(target)}`);
    let act = actRaw;
    if (!act.startsWith(".") && !act.includes(".")) act = `.${act}`;
    return { kind: "activity", raw: t, packageName: pkg, activity: act };
  }
  return { kind: "package", raw: t, packageName: t };
}

function detectAmStartError(output: string) {
  const trimmed = output.trim();
  if (!trimmed) return null;
  if (trimmed.includes("Error:")) return new Error(trimmed);
  return null;
}

function cmdDevices(serial: string) {
  return runAdb(serial, false, ["devices", "-l"]).exitCode;
}

function cmdStartServer() {
  return runCmd(["adb", "start-server"], false, defaultTimeoutMs).exitCode;
}

function cmdKillServer() {
  return runCmd(["adb", "kill-server"], false, defaultTimeoutMs).exitCode;
}

function cmdConnect(args: string[]) {
  if (args.length < 1) {
    logger.error("connect requires <address>");
    return 2;
  }
  return runCmd(["adb", "connect", args[0]], false, defaultTimeoutMs).exitCode;
}

function cmdDisconnect(args: string[]) {
  const cmd = ["adb", "disconnect", ...(args[0] ? [args[0]] : [])];
  return runCmd(cmd, false, defaultTimeoutMs).exitCode;
}

function cmdGetIP(serial: string) {
  let result = runAdb(serial, true, ["shell", "ip", "route"]);
  for (const line of result.stdout.split("\n")) {
    if (line.includes("src")) {
      const parts = line.trim().split(/\s+/);
      for (let i = 0; i < parts.length; i++) {
        if (parts[i] === "src" && i + 1 < parts.length) {
          console.log(parts[i + 1]);
          return 0;
        }
      }
    }
  }
  result = runAdb(serial, true, ["shell", "ip", "addr", "show", "wlan0"]);
  for (const line of result.stdout.split("\n")) {
    if (line.includes("inet ")) {
      const parts = line.trim().split(/\s+/);
      if (parts.length >= 2) {
        console.log(parts[1].split("/")[0]);
        return 0;
      }
    }
  }
  logger.error("IP not found");
  return 1;
}

function cmdEnableTCPIP(serial: string, args: string[]) {
  let port = 5555;
  if (args.length >= 1) {
    const parsed = Number(args[0]);
    if (!Number.isInteger(parsed)) {
      logger.error("invalid port", { value: args[0] });
      return 2;
    }
    port = parsed;
  }
  const result = runAdb(serial, true, ["tcpip", String(port)]);
  const output = (result.stdout + result.stderr).trim();
  if (output) console.log(output);
  if (output.toLowerCase().includes("restarting") || result.exitCode === 0) return 0;
  return 1;
}

function cmdShell(serial: string, args: string[]) {
  return runAdb(serial, false, ["shell", ...args]).exitCode;
}

function cmdTap(serial: string, args: string[]) {
  if (args.length < 2) {
    logger.error("tap requires <x> <y>");
    return 2;
  }
  return runAdb(serial, false, ["shell", "input", "tap", args[0], args[1]]).exitCode;
}

function cmdDoubleTap(serial: string, args: string[]) {
  if (args.length < 2) {
    logger.error("double-tap requires <x> <y>");
    return 2;
  }
  const cmd = ["shell", "input", "tap", args[0], args[1]];
  runAdb(serial, false, cmd);
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
  return runAdb(serial, false, cmd).exitCode;
}

function cmdSwipe(serial: string, args: string[], durationMs: number) {
  if (args.length < 4) {
    logger.error("swipe requires <x1> <y1> <x2> <y2>");
    return 2;
  }
  const cmd = ["shell", "input", "swipe", args[0], args[1], args[2], args[3]];
  if (durationMs >= 0) cmd.push(String(durationMs));
  return runAdb(serial, false, cmd).exitCode;
}

function cmdLongPress(serial: string, args: string[], durationMs: number) {
  if (args.length < 2) {
    logger.error("long-press requires <x> <y>");
    return 2;
  }
  const cmd = ["shell", "input", "swipe", args[0], args[1], args[0], args[1], String(durationMs)];
  return runAdb(serial, false, cmd).exitCode;
}

function cmdKeyEvent(serial: string, args: string[]) {
  if (args.length < 1) {
    logger.error("keyevent requires <keycode>");
    return 2;
  }
  return runAdb(serial, false, ["shell", "input", "keyevent", args[0]]).exitCode;
}

function escapeInputText(text: string) {
  return text
    .replace(/ /g, "%s")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'")
    .replace(/\(/g, "\\(")
    .replace(/\)/g, "\\)");
}

function getCurrentIME(serial: string) {
  const result = runAdb(serial, true, ["shell", "settings", "get", "secure", "default_input_method"]);
  return (result.stdout + result.stderr).trim();
}

function setIME(serial: string, ime: string) {
  runAdb(serial, true, ["shell", "ime", "set", ime]);
}

function cmdText(serial: string, args: string[], useAdbKeyboard: boolean) {
  if (args.length < 1) {
    logger.error("text requires <text>");
    return 2;
  }
  const filtered: string[] = [];
  for (const arg of args) {
    if (arg === "--adb-keyboard") useAdbKeyboard = true;
    else filtered.push(arg);
  }
  if (filtered.length < 1) {
    logger.error("text requires <text>");
    return 2;
  }
  const text = filtered.join(" ");
  let result: CmdResult;
  if (useAdbKeyboard) {
    const original = getCurrentIME(serial);
    if (!original.includes("com.android.adbkeyboard/.AdbIME")) {
      setIME(serial, "com.android.adbkeyboard/.AdbIME");
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1000);
    }
    const encoded = Buffer.from(text).toString("base64");
    result = runAdb(serial, false, ["shell", "am", "broadcast", "-a", "ADB_INPUT_B64", "--es", "msg", encoded]);
  } else {
    const escaped = escapeInputText(text);
    result = runAdb(serial, false, ["shell", "input", "text", escaped]);
  }
  return result.exitCode;
}

function cmdClearText(serial: string) {
  return runAdb(serial, false, ["shell", "am", "broadcast", "-a", "ADB_CLEAR_TEXT"]).exitCode;
}

function cmdScreenshot(serial: string, outPath: string) {
  let target = outPath || "screen.png";
  try {
    target = path.resolve(target);
  } catch {
    // ignore
  }
  try {
    const cmd = [...adbPrefix(serial), "exec-out", "screencap", "-p"];
    const res = spawnSync(cmd[0], cmd.slice(1), {
      timeout: defaultTimeoutMs,
      maxBuffer: 20 * 1024 * 1024,
    });
    if (res.error) {
      if ((res.error as any).code === "ETIMEDOUT") {
        logger.error("adb command timed out");
        return 124;
      }
      logger.error("adb command failed", { err: String(res.error) });
      return 1;
    }
    if (res.status && res.status !== 0) {
      if (res.stderr) logger.error("adb stderr", { stderr: String(res.stderr).trim() });
      return res.status;
    }
    fs.writeFileSync(target, res.stdout as Buffer);
    return 0;
  } catch (err: any) {
    logger.error("create output file failed", { path: target, err: err?.message || String(err) });
    return 1;
  }
}

function cmdLaunch(serial: string, args: string[]) {
  if (args.length < 1) {
    logger.error("launch requires <target>");
    return 2;
  }
  const target = args.join(" ").trim();
  let launch: LaunchTarget;
  try {
    launch = parseLaunchTarget(target);
  } catch (err: any) {
    logger.error("invalid launch target", { err: err?.message || String(err) });
    return 2;
  }
  let result: CmdResult;
  switch (launch.kind) {
    case "uri":
      result = runAdb(serial, true, ["shell", "am", "start", "-W", "-a", "android.intent.action.VIEW", "-d", launch.uri ?? ""]);
      if (result.exitCode !== 0) return result.exitCode;
      const uriError = detectAmStartError(result.stdout + result.stderr);
      if (uriError) {
        logger.error("am start uri failed", { err: uriError.message });
        return 1;
      }
      break;
    case "activity":
      const component = `${launch.packageName}/${launch.activity}`;
      result = runAdb(serial, true, ["shell", "am", "start", "-W", "-n", component]);
      if (result.exitCode !== 0) return result.exitCode;
      const activityError = detectAmStartError(result.stdout + result.stderr);
      if (activityError) {
        logger.error("am start activity failed", { err: activityError.message });
        return 1;
      }
      break;
    case "package":
      result = runAdb(serial, true, ["shell", "monkey", "-p", launch.packageName ?? "", "-c", "android.intent.category.LAUNCHER", "1"]);
      if (result.exitCode !== 0) return result.exitCode;
      if ((result.stdout + result.stderr).includes("monkey aborted")) {
        logger.error("monkey aborted", { output: (result.stdout + result.stderr).trim() });
        return 1;
      }
      break;
    default:
      logger.error("unsupported launch target", { target });
      return 2;
  }
  return 0;
}

function extractCurrentPackage(output: string) {
  const re = /([a-zA-Z0-9_.]+\.[a-zA-Z0-9_.]+)\//;
  for (const line of output.split("\n")) {
    if (line.includes("mCurrentFocus") || line.includes("mFocusedApp")) {
      const match = line.match(re);
      if (match && match[1]) return match[1];
      const parts = line.trim().split(/\s+/);
      for (const part of parts) {
        if (part.includes("/")) return part.split("/")[0];
      }
    }
  }
  return "";
}

function getCurrentPackage(serial: string) {
  const result = runAdb(serial, true, ["shell", "dumpsys", "window"]);
  return extractCurrentPackage(result.stdout);
}

function cmdGetCurrentApp(serial: string) {
  const current = getCurrentPackage(serial);
  if (current) {
    console.log(current);
    return 0;
  }
  logger.error("system home (or unknown)");
  return 1;
}

function parseHomePackages(output: string) {
  const pkgs = new Set<string>();
  for (const line of output.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const match = trimmed.match(/([a-zA-Z0-9_.]+)\/[a-zA-Z0-9_.]+/);
    if (match?.[1]) pkgs.add(match[1]);
    const pkgMatch = trimmed.match(/packageName=([a-zA-Z0-9_.]+)/);
    if (pkgMatch?.[1]) pkgs.add(pkgMatch[1]);
  }
  return pkgs;
}

function getHomePackages(serial: string) {
  let result = runAdb(serial, true, ["shell", "cmd", "package", "resolve-activity", "--brief", "-c", "android.intent.category.HOME"]);
  let pkgs = parseHomePackages(result.stdout + result.stderr);
  if (pkgs.size === 0) {
    result = runAdb(serial, true, ["shell", "pm", "resolve-activity", "-a", "android.intent.action.MAIN", "-c", "android.intent.category.HOME"]);
    pkgs = parseHomePackages(result.stdout + result.stderr);
  }
  return pkgs;
}

function cmdBackHome(serial: string, args: string[]) {
  const maxRoundsRaw = args[0] ? Number(args[0]) : 20;
  const maxRounds = Number.isFinite(maxRoundsRaw) && maxRoundsRaw > 0 ? Math.floor(maxRoundsRaw) : 20;
  const homePkgs = getHomePackages(serial);
  if (homePkgs.size === 0) {
    logger.error("home package not found");
  }
  for (let round = 1; round <= maxRounds; round++) {
    for (let i = 0; i < 2; i++) {
      runAdb(serial, false, ["shell", "input", "keyevent", "4"]);
      const delay = randomDelayMs(0, 1000);
      sleepMs(delay);
    }
    const current = getCurrentPackage(serial);
    const isHome = current ? homePkgs.has(current) : false;
    logger.debug("back-home check", { round, current, isHome });
    if (isHome) {
      logger.info("home reached", { rounds: round, current });
      return 0;
    }
  }
  logger.error("home not reached after max rounds", { maxRounds });
  return 1;
}

function cmdForceStop(serial: string, args: string[]) {
  if (args.length < 1) {
    logger.error("force-stop requires <package>");
    return 2;
  }
  return runAdb(serial, false, ["shell", "am", "force-stop", args[0]]).exitCode;
}

function parseUINodes(xml: string) {
  const nodes: Array<Record<string, string>> = [];
  const re = /<node\s+([^>]+?)(?:\/?>)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const attrs: Record<string, string> = {};
    const attrRe = /([\w:-]+)="([^"]*)"/g;
    let a: RegExpExecArray | null;
    while ((a = attrRe.exec(m[1])) !== null) {
      attrs[a[1]] = a[2];
    }
    nodes.push(attrs);
  }
  return nodes;
}

function parseUIXML(filePath: string) {
  const xml = fs.readFileSync(filePath, "utf8");
  const nodes = parseUINodes(xml);
  const tappable: string[] = [];
  const inputs: string[] = [];
  const texts: string[] = [];
  const boundsRe = /\[(\d+),(\d+)\]\[(\d+),(\d+)\]/;
  for (const attrs of nodes) {
    const bounds = attrs["bounds"] || "";
    const text = attrs["text"] || "";
    const contentDesc = attrs["content-desc"] || "";
    const resourceID = attrs["resource-id"] || "";
    const className = attrs["class"] || "";
    const clickable = attrs["clickable"] === "true";
    let displayName = text || contentDesc || resourceID;
    if (!bounds) continue;
    const match = bounds.match(boundsRe);
    if (!match) continue;
    const x1 = Number(match[1]);
    const y1 = Number(match[2]);
    const x2 = Number(match[3]);
    const y2 = Number(match[4]);
    const centerX = Math.floor((x1 + x2) / 2);
    const centerY = Math.floor((y1 + y2) / 2);
    const coords = `(${centerX}, ${centerY})`;
    if (className === "android.widget.EditText") {
      inputs.push(`  INPUT "${displayName}" @ ${coords}`);
    } else if (clickable) {
      tappable.push(`  TAP "${displayName}" @ ${coords}`);
    } else if (displayName && displayName.length < 50) {
      texts.push(`  TEXT "${displayName}" @ ${coords}`);
    }
  }
  if (tappable.length > 0) {
    console.log("\nTAPPABLE (clickable=true):");
    const limit = Math.min(20, tappable.length);
    for (const line of tappable.slice(0, limit)) console.log(line);
    if (tappable.length > limit) console.log(`  ... (${tappable.length - limit} more)`);
  }
  if (inputs.length > 0) {
    console.log("\nINPUT FIELDS (EditText):");
    for (const line of inputs) console.log(line);
  }
  if (texts.length > 0) {
    console.log("\nTEXT/INFO (readable):");
    const limit = Math.min(20, texts.length);
    for (const line of texts.slice(0, limit)) console.log(line);
    if (texts.length > limit) console.log(`  ... (${texts.length - limit} more)`);
  }
}

function cmdDumpUI(serial: string, outPath: string, parse: boolean) {
  const remotePath = "/sdcard/window_dump.xml";
  let localPath = outPath || "window_dump.xml";
  const dump = runAdb(serial, true, ["shell", "uiautomator", "dump", remotePath]);
  if (dump.exitCode !== 0) {
    logger.error("dump failed", { stderr: dump.stderr.trim() });
    return dump.exitCode;
  }
  const pull = runAdb(serial, true, ["pull", remotePath, localPath]);
  if (pull.exitCode !== 0) {
    logger.error("pull failed", { stderr: pull.stderr.trim() });
    return pull.exitCode;
  }
  console.log(`UI dumped to ${localPath}`);
  if (parse) parseUIXML(localPath);
  return 0;
}

function cmdWmSize(serial: string) {
  return runAdb(serial, false, ["shell", "wm", "size"]).exitCode;
}

function listOnlineDeviceSerials() {
  const result = runCmd(["adb", "devices", "-l"], true, defaultTimeoutMs);
  if (result.exitCode !== 0) {
    logger.error("failed to list adb devices", { stderr: result.stderr.trim() });
    return null;
  }
  const serials: string[] = [];
  for (const rawLine of result.stdout.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("List of devices attached")) continue;
    const parts = line.split(/\s+/);
    if (parts.length < 2) continue;
    if (parts[1] !== "device") continue;
    serials.push(parts[0]);
  }
  return serials;
}

type PlanNextAction =
  | { kind: "click"; x: number; y: number }
  | { kind: "drag"; x1: number; y1: number; x2: number; y2: number }
  | { kind: "long_press"; x: number; y: number }
  | { kind: "press_back" }
  | { kind: "wait" }
  | { kind: "finished" };

function parsePlanNextAction(first: any): PlanNextAction | null {
  if (!first || typeof first !== "object") return null;
  switch (first.action) {
    case "click":
      return Number.isFinite(first.x) && Number.isFinite(first.y)
        ? { kind: "click", x: Math.round(first.x), y: Math.round(first.y) }
        : null;
    case "drag":
      return Number.isFinite(first.x) && Number.isFinite(first.y) && Number.isFinite(first.to_x) && Number.isFinite(first.to_y)
        ? {
            kind: "drag",
            x1: Math.round(first.x),
            y1: Math.round(first.y),
            x2: Math.round(first.to_x),
            y2: Math.round(first.to_y),
          }
        : null;
    case "long_press":
      return Number.isFinite(first.x) && Number.isFinite(first.y)
        ? { kind: "long_press", x: Math.round(first.x), y: Math.round(first.y) }
        : null;
    case "press_back":
      return { kind: "press_back" };
    case "wait":
      return { kind: "wait" };
    case "finished":
      return { kind: "finished" };
    default:
      return null;
  }
}

function parsePlanNextOutput(stdout: string): { action: PlanNextAction | null; thought: string } {
  try {
    const data = JSON.parse(stdout);
    return {
      action: parsePlanNextAction(Array.isArray(data?.actions) ? data.actions[0] : null),
      thought: typeof data?.thought === "string" ? data.thought : "",
    };
  } catch {
    return { action: null, thought: "" };
  }
}

function isLikelyVerificationDone(thought: string) {
  const t = thought || "";
  const doneKeywords = ["完成当前验证", "验证完成", "安装完成", "点击该按钮即可完成", "已完成"];
  return doneKeywords.some((k) => t.includes(k));
}

function shouldStopAfterWait(thought: string, consecutiveWaits: number) {
  if (consecutiveWaits >= 2) return true;
  const t = thought || "";
  const noSecurityKeywords = [
    "未出现系统安全验证",
    "未出现安全验证",
    "无法确定可执行动作",
    "应用商店",
    "搜索界面",
    "非系统安全验证",
    "未显示系统安全验证相关",
  ];
  return noSecurityKeywords.some((k) => t.includes(k));
}

function shouldStopVerification(action: PlanNextAction, thought: string, consecutiveWaits: number) {
  if (action.kind === "finished") return { stop: true, reason: "planner finished" };
  if (action.kind === "wait" && shouldStopAfterWait(thought, consecutiveWaits)) {
    return { stop: true, reason: "wait heuristic" };
  }
  if (action.kind !== "wait" && isLikelyVerificationDone(thought)) {
    return { stop: true, reason: "completion heuristic" };
  }
  return { stop: false, reason: "" };
}

function executePlanNextAction(serial: string, action: PlanNextAction, step: number, uiIntervalMs: number) {
  if (action.kind === "click") {
    logger.info("tap suggested point", { step, x: action.x, y: action.y });
    cmdTap(serial, [String(action.x), String(action.y)]);
    return;
  }
  if (action.kind === "drag") {
    logger.info("drag suggested path", { step, x1: action.x1, y1: action.y1, x2: action.x2, y2: action.y2 });
    logger.debug("execute drag via adb swipe", { step, duration_ms: 900 });
    cmdSwipe(serial, [String(action.x1), String(action.y1), String(action.x2), String(action.y2)], 900);
    return;
  }
  if (action.kind === "long_press") {
    logger.info("long-press suggested point", { step, x: action.x, y: action.y });
    cmdLongPress(serial, [String(action.x), String(action.y)], 1200);
    return;
  }
  if (action.kind === "press_back") {
    logger.info("execute press_back", { step });
    cmdKeyEvent(serial, ["KEYCODE_BACK"]);
    return;
  }
  if (action.kind === "wait") {
    logger.info("execute wait", { step, ms: uiIntervalMs });
    return;
  }
}

function findAiVisionSkillDir() {
  const candidates = [
    path.resolve(process.cwd(), "../ai-vision"),
    path.resolve(process.cwd(), "ai-vision"),
    path.resolve(__dirname, "../../ai-vision"),
  ];
  const dir = candidates.find((candidate) => fs.existsSync(path.join(candidate, "scripts", "ai_vision.ts"))) || "";
  return { dir, candidates };
}

function hasInstallSmartEnv() {
  return Boolean(process.env.ARK_BASE_URL && process.env.ARK_API_KEY);
}

function parseNumberOption(raw: string | undefined, fallback: number, name: string, validator: (value: number) => boolean) {
  const value = Number(raw ?? String(fallback));
  if (!Number.isFinite(value) || !validator(value)) {
    logger.error(`invalid ${name}`, { value: raw });
    return null;
  }
  return value;
}

function resolveDeviceSerial(serial: string, command: string) {
  if (serial) return serial;
  const onlineSerials = listOnlineDeviceSerials();
  if (!onlineSerials) return "";
  if (onlineSerials.length === 1) {
    logger.info("use the only connected device", { serial: onlineSerials[0], command });
    return onlineSerials[0];
  }
  if (onlineSerials.length === 0) {
    logger.error(`${command} found no online adb device; connect one or pass -s/--serial`);
    return "";
  }
  logger.error(`${command} found multiple online devices; pass -s/--serial`, { devices: onlineSerials });
  return "";
}

function getAiVisionDir() {
  const { dir: aiVisionDir, candidates: aiVisionDirCandidates } = findAiVisionSkillDir();
  if (!aiVisionDir) {
    logger.error("ai-vision skill directory not found", { tried: aiVisionDirCandidates });
    return "";
  }
  return aiVisionDir;
}

async function cmdHandleVerification(
  serial: string,
  options: {
    maxUiSteps: number;
    uiIntervalSec: number;
    prompt: string;
    screenshotTag: string;
  },
) {
  const resolvedSerial = resolveDeviceSerial(serial, "handle-verification");
  if (!resolvedSerial) return 2;

  const aiVisionDir = getAiVisionDir();
  if (!aiVisionDir) return 2;
  if (!hasInstallSmartEnv()) {
    logger.error("missing ARK env vars", { required: "ARK_BASE_URL, ARK_API_KEY" });
    return 2;
  }

  const screenshotDir = path.resolve(os.homedir(), ".eval", "screenshots");
  fs.mkdirSync(screenshotDir, { recursive: true });

  const prompt = options.prompt || defaultHandleVerificationPrompt;
  const maxUiSteps = Math.max(1, Math.floor(options.maxUiSteps));
  const uiIntervalMs = Math.max(0, Math.floor(options.uiIntervalSec * 1000));
  const screenshotTag = (options.screenshotTag || "security").replace(/[^a-zA-Z0-9_-]/g, "_");
  let consecutiveWaits = 0;

  for (let i = 1; i <= maxUiSteps; i++) {
    const shot = path.resolve(
      screenshotDir,
      `${screenshotTag}_${resolvedSerial}_${new Date().toISOString().replace(/[:.]/g, "-")}_${i}.png`,
    );
    const shotCode = cmdScreenshot(resolvedSerial, shot);
    if (shotCode !== 0) {
      logger.error("screenshot failed", { step: i });
      await sleepAsync(uiIntervalMs);
      continue;
    }
    const aiVisionCmd = ["npx", "tsx", "scripts/ai_vision.ts", "plan-next", "--screenshot", shot, "--prompt", prompt];
    const plan = runCmd(
      aiVisionCmd,
      true,
      120_000,
      aiVisionDir,
    );
    if (plan.exitCode !== 0) {
      logger.error("ai-vision plan-next failed", { step: i, stderr: plan.stderr.trim() });
      await sleepAsync(uiIntervalMs);
      continue;
    }
    const parsed = parsePlanNextOutput(plan.stdout);
    if (parsed.thought) logger.debug("ai-vision thought", { step: i, thought: parsed.thought });
    const action = parsed.action;
    if (!action) {
      logger.debug("no recognized action from ai-vision", { step: i });
      await sleepAsync(uiIntervalMs);
      continue;
    }
    logger.debug("ai-vision action", { step: i, action: action.kind });
    if (action.kind === "wait") {
      consecutiveWaits += 1;
    } else {
      consecutiveWaits = 0;
    }
    const stop = shouldStopVerification(action, parsed.thought, consecutiveWaits);
    if (stop.stop) {
      logger.info("stop verification", { step: i, reason: stop.reason, consecutive_waits: consecutiveWaits });
      return 0;
    }
    executePlanNextAction(resolvedSerial, action, i, uiIntervalMs);
    await sleepAsync(uiIntervalMs);
  }

  logger.error("security-check not solved after max ui steps", { max_ui_steps: maxUiSteps });
  return 1;
}

async function cmdInstallSmart(
  serial: string,
  apkPath: string,
  options: {
    maxUiSteps: number;
    uiIntervalSec: number;
    initialWaitSec: number;
    prompt: string;
  },
) {
  const resolvedSerial = resolveDeviceSerial(serial, "install-smart");
  if (!resolvedSerial) return 2;
  if (!apkPath) {
    logger.error("install-smart requires <apk>");
    return 2;
  }
  const resolvedApk = path.resolve(apkPath);
  if (!fs.existsSync(resolvedApk)) {
    logger.error("apk not found", { apk: resolvedApk });
    return 2;
  }

  const adbCmd = [...adbPrefix(resolvedSerial), "install", "-r", resolvedApk];
  logger.info("start install", { cmd: adbCmd.join(" ") });
  const proc = spawn(adbCmd[0], adbCmd.slice(1), { stdio: ["ignore", "pipe", "pipe"] });

  let stdout = "";
  let stderr = "";
  proc.stdout.on("data", (chunk) => {
    stdout += String(chunk);
  });
  proc.stderr.on("data", (chunk) => {
    stderr += String(chunk);
  });

  let exited = false;
  let exitCode = 0;
  const exitPromise = new Promise<number>((resolve) => {
    proc.on("close", (code) => {
      exited = true;
      exitCode = typeof code === "number" ? code : 1;
      resolve(exitCode);
    });
  });

  const initialWaitMs = Math.max(0, Math.floor(options.initialWaitSec * 1000));
  await Promise.race([exitPromise, sleepAsync(initialWaitMs)]);
  if (exited) {
    writeCapturedOutput(stdout, stderr);
    return exitCode;
  }

  logger.info("install appears blocked, invoke handle-verification", {
    initial_wait_sec: options.initialWaitSec,
    max_ui_steps: options.maxUiSteps,
  });

  const solveCode = await cmdHandleVerification(resolvedSerial, {
    maxUiSteps: options.maxUiSteps,
    uiIntervalSec: options.uiIntervalSec,
    prompt: options.prompt || defaultInstallSmartPrompt,
    screenshotTag: "install",
  });
  if (solveCode !== 0 && !exited) {
    logger.error("handle-verification returned non-zero while install still running", { code: solveCode });
  }

  if (!exited) {
    logger.error("install still blocked after max ui steps");
    proc.kill("SIGTERM");
    await sleepAsync(500);
    if (!exited) proc.kill("SIGKILL");
  }
  await exitPromise;
  writeCapturedOutput(stdout, stderr);
  return exitCode;
}

function getGlobalOptions(program: Command) {
  const opts = program.opts<{ logJson?: boolean; serial?: string; logLevel?: string }>();
  const level = parseLogLevel(opts.logLevel);
  setLoggerConfig(Boolean(opts.logJson), level);
  return { serial: String(opts.serial || "") };
}

async function main() {
  const argv = process.argv.slice(2);
  const program = new Command();
  program
    .name("adb_helpers")
    .description("Android adb helper CLI")
    .showHelpAfterError()
    .showSuggestionAfterError()
    .option("--log-json", "Output logs in JSON")
    .option("--log-level <level>", "log verbosity: debug|info|error", "info")
    .option("-s, --serial <id>", "device serial");

  const getSerial = () => getGlobalOptions(program).serial;
  const applyCommandLogLevel = (raw: string | undefined) => {
    if (!raw) return;
    const root = program.opts<{ logJson?: boolean }>();
    setLoggerConfig(Boolean(root.logJson), parseLogLevel(raw));
  };

  program
    .command("devices")
    .description("List connected devices")
    .action(() => {
      process.exit(cmdDevices(getSerial()));
    });
  program
    .command("start-server")
    .description("Start adb server")
    .action(() => {
      getGlobalOptions(program);
      process.exit(cmdStartServer());
    });
  program
    .command("kill-server")
    .description("Kill adb server")
    .action(() => {
      getGlobalOptions(program);
      process.exit(cmdKillServer());
    });
  program
    .command("connect")
    .description("Connect to device over TCP/IP")
    .argument("[address]", "device address, e.g. 192.168.0.10:5555")
    .action((address: string | undefined) => {
      getGlobalOptions(program);
      process.exit(cmdConnect(address ? [address] : []));
    });
  program
    .command("disconnect")
    .description("Disconnect from device over TCP/IP")
    .argument("[address]", "device address")
    .action((address: string | undefined) => {
      getGlobalOptions(program);
      process.exit(cmdDisconnect(address ? [address] : []));
    });
  program
    .command("get-ip")
    .description("Get device IP address")
    .action(() => {
      process.exit(cmdGetIP(getSerial()));
    });
  program
    .command("enable-tcpip")
    .description("Enable adb over TCP/IP")
    .argument("[port]", "tcpip port, default 5555")
    .action((port: string | undefined) => {
      process.exit(cmdEnableTCPIP(getSerial(), port ? [port] : []));
    });
  program
    .command("shell")
    .description("Run adb shell command")
    .argument("<cmd...>", "shell command")
    .action((cmd: string[]) => {
      process.exit(cmdShell(getSerial(), cmd));
    });
  program
    .command("tap")
    .description("Tap at coordinates")
    .argument("<x>", "x coordinate")
    .argument("<y>", "y coordinate")
    .action((x: string, y: string) => {
      process.exit(cmdTap(getSerial(), [x, y]));
    });
  program
    .command("double-tap")
    .description("Double tap at coordinates")
    .argument("<x>", "x coordinate")
    .argument("<y>", "y coordinate")
    .action((x: string, y: string) => {
      process.exit(cmdDoubleTap(getSerial(), [x, y]));
    });
  program
    .command("swipe")
    .description("Swipe from start to end coordinates")
    .argument("<x1>", "start x")
    .argument("<y1>", "start y")
    .argument("<x2>", "end x")
    .argument("<y2>", "end y")
    .option("--duration-ms <ms>", "swipe duration in ms")
    .action((x1: string, y1: string, x2: string, y2: string, options: { durationMs?: string }) => {
      const duration = options.durationMs ? Number(options.durationMs) : -1;
      process.exit(cmdSwipe(getSerial(), [x1, y1, x2, y2], Number.isFinite(duration) ? duration : -1));
    });
  program
    .command("long-press")
    .description("Long press at coordinates")
    .argument("<x>", "x coordinate")
    .argument("<y>", "y coordinate")
    .option("--duration-ms <ms>", "press duration in ms", "3000")
    .action((x: string, y: string, options: { durationMs?: string }) => {
      const duration = options.durationMs ? Number(options.durationMs) : 3000;
      process.exit(cmdLongPress(getSerial(), [x, y], Number.isFinite(duration) ? duration : 3000));
    });
  program
    .command("keyevent")
    .description("Send keyevent by keycode")
    .argument("<keycode>", "key code")
    .action((keycode: string) => {
      process.exit(cmdKeyEvent(getSerial(), [keycode]));
    });
  program
    .command("text")
    .description("Input text (optional ADBKeyboard)")
    .argument("<text>", "text to input")
    .option("--adb-keyboard", "use ADB Keyboard broadcast")
    .action((text: string, options: { adbKeyboard?: boolean }) => {
      process.exit(cmdText(getSerial(), [text], Boolean(options.adbKeyboard)));
    });
  program
    .command("clear-text")
    .description("Clear text via ADBKeyboard")
    .action(() => {
      process.exit(cmdClearText(getSerial()));
    });
  program
    .command("screenshot")
    .description("Capture screenshot to file")
    .option("--out <path>", "output path")
    .action((options: { out?: string }) => {
      process.exit(cmdScreenshot(getSerial(), String(options.out || "")));
    });
  program
    .command("launch")
    .description("Launch app by package/activity/uri")
    .argument("<target>", "package, activity or uri")
    .action((target: string) => {
      process.exit(cmdLaunch(getSerial(), [target]));
    });
  program
    .command("get-current-app")
    .description("Print current foreground package")
    .action(() => {
      process.exit(cmdGetCurrentApp(getSerial()));
    });
  program
    .command("force-stop")
    .description("Force-stop package")
    .argument("<package>", "package name")
    .action((pkg: string) => {
      process.exit(cmdForceStop(getSerial(), [pkg]));
    });
  program
    .command("back-home")
    .description("Press BACK in pairs until reaching home")
    .argument("[max-rounds]", "max rounds, default 20")
    .action((maxRounds: string | undefined) => {
      process.exit(cmdBackHome(getSerial(), maxRounds ? [maxRounds] : []));
    });
  program
    .command("dump-ui")
    .description("Dump UI hierarchy (uiautomator)")
    .option("--out <path>", "output path")
    .option("--parse", "parse UI hierarchy")
    .action((options: { out?: string; parse?: boolean }) => {
      process.exit(cmdDumpUI(getSerial(), String(options.out || ""), Boolean(options.parse)));
    });
  program
    .command("handle-verification")
    .description("Use ai-vision plan-next loop to solve security verification and dialogs")
    .option("--max-ui-steps <n>", "max UI assist steps", String(defaultInstallSmartMaxUiSteps))
    .option("--ui-interval-sec <sec>", "seconds between UI iterations", String(defaultInstallSmartUiIntervalSec))
    .option("--log-level <level>", "override log level: debug|info|error")
    .option("--prompt <text>", "custom ai-vision prompt", defaultHandleVerificationPrompt)
    .action(async (options: { maxUiSteps?: string; uiIntervalSec?: string; logLevel?: string; prompt?: string }) => {
      applyCommandLogLevel(options.logLevel);
      const maxUiSteps = parseNumberOption(options.maxUiSteps, defaultInstallSmartMaxUiSteps, "--max-ui-steps", (v) => v > 0);
      if (maxUiSteps === null) {
        process.exit(2);
      }
      const uiIntervalSec = parseNumberOption(options.uiIntervalSec, defaultInstallSmartUiIntervalSec, "--ui-interval-sec", (v) => v >= 0);
      if (uiIntervalSec === null) {
        process.exit(2);
      }
      process.exit(
        await cmdHandleVerification(getSerial(), {
          maxUiSteps,
          uiIntervalSec,
          prompt: options.prompt ?? "",
          screenshotTag: "security",
        }),
      );
    });
  program
    .command("install-smart")
    .description("Install APK with 5s blocking check, then delegate dialogs to handle-verification")
    .argument("<apk>", "apk path")
    .option("--max-ui-steps <n>", "max UI assist steps", String(defaultInstallSmartMaxUiSteps))
    .option("--ui-interval-sec <sec>", "seconds between UI iterations", String(defaultInstallSmartUiIntervalSec))
    .option("--initial-wait-sec <sec>", "initial wait for install completion", String(defaultInstallSmartInitialWaitSec))
    .option("--log-level <level>", "override log level: debug|info|error")
    .option(
      "--prompt <text>",
      "custom ai-vision prompt",
      defaultInstallSmartPrompt,
    )
    .action(async (apk: string, options: { maxUiSteps?: string; uiIntervalSec?: string; initialWaitSec?: string; logLevel?: string; prompt?: string }) => {
      applyCommandLogLevel(options.logLevel);
      const maxUiSteps = parseNumberOption(options.maxUiSteps, defaultInstallSmartMaxUiSteps, "--max-ui-steps", (v) => v > 0);
      if (maxUiSteps === null) {
        process.exit(2);
      }
      const uiIntervalSec = parseNumberOption(options.uiIntervalSec, defaultInstallSmartUiIntervalSec, "--ui-interval-sec", (v) => v >= 0);
      if (uiIntervalSec === null) {
        process.exit(2);
      }
      const initialWaitSec = parseNumberOption(options.initialWaitSec, defaultInstallSmartInitialWaitSec, "--initial-wait-sec", (v) => v >= 0);
      if (initialWaitSec === null) {
        process.exit(2);
      }
      process.exit(
        await cmdInstallSmart(getSerial(), apk, {
          maxUiSteps,
          uiIntervalSec,
          initialWaitSec,
          prompt: options.prompt ?? "",
        }),
      );
    });
  program
    .command("wm-size")
    .description("Print screen size")
    .action(() => {
      process.exit(cmdWmSize(getSerial()));
    });

  if (argv.length === 0) {
    program.outputHelp();
    process.exit(0);
  }
  await program.parseAsync(process.argv);
}

main().catch((err) => {
  logger.error("unhandled error", { err: err?.message || String(err) });
  process.exit(1);
});
