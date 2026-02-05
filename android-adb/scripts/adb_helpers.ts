#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import chalk from "chalk";

const defaultTimeoutMs = 10_000;

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

let logger = createLogger(false, "debug", process.stderr, Boolean(process.stderr.isTTY));

function setLoggerJSON(enabled: boolean) {
  const useColor = Boolean(process.stderr.isTTY) && !enabled;
  logger = createLogger(enabled, "debug", process.stderr, useColor);
}

function adbPrefix(serial: string) {
  return serial ? ["adb", "-s", serial] : ["adb"];
}

type CmdResult = { stdout: string; stderr: string; exitCode: number };

function runCmd(cmd: string[], capture: boolean, timeoutMs: number): CmdResult {
  const res = spawnSync(cmd[0], cmd.slice(1), {
    encoding: "utf8",
    timeout: timeoutMs > 0 ? timeoutMs : defaultTimeoutMs,
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
    const res = spawnSync(cmd[0], cmd.slice(1), { timeout: defaultTimeoutMs });
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
      if (detectAmStartError(result.stdout + result.stderr)) {
        logger.error("am start uri failed", { err: (detectAmStartError(result.stdout + result.stderr) as Error).message });
        return 1;
      }
      break;
    case "activity":
      const component = `${launch.packageName}/${launch.activity}`;
      result = runAdb(serial, true, ["shell", "am", "start", "-W", "-n", component]);
      if (result.exitCode !== 0) return result.exitCode;
      if (detectAmStartError(result.stdout + result.stderr)) {
        logger.error("am start activity failed", { err: (detectAmStartError(result.stdout + result.stderr) as Error).message });
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

function cmdGetCurrentApp(serial: string) {
  const result = runAdb(serial, true, ["shell", "dumpsys", "window"]);
  const output = result.stdout;
  const re = /([a-zA-Z0-9_.]+\.[a-zA-Z0-9_.]+)\//;
  for (const line of output.split("\n")) {
    if (line.includes("mCurrentFocus") || line.includes("mFocusedApp")) {
      const match = line.match(re);
      if (match && match[1]) {
        console.log(match[1]);
        return 0;
      }
      const parts = line.trim().split(/\s+/);
      for (const part of parts) {
        if (part.includes("/")) {
          console.log(part.split("/")[0]);
          return 0;
        }
      }
    }
  }
  logger.error("system home (or unknown)");
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

function hasHelpArg(args: string[]) {
  return args.some((arg) => arg === "-h" || arg === "--help");
}

function printUsage(out: NodeJS.WriteStream) {
  out.write("Usage:\n");
  out.write("  adb_helpers [--log-json] [flags] <command> [args]\n\n");
  out.write("Commands:\n");
  out.write("  devices\n");
  out.write("  start-server\n");
  out.write("  kill-server\n");
  out.write("  connect <address>\n");
  out.write("  disconnect [address]\n");
  out.write("  get-ip\n");
  out.write("  enable-tcpip [port]\n");
  out.write("  shell <cmd...>\n");
  out.write("  tap <x> <y>\n");
  out.write("  double-tap <x> <y>\n");
  out.write("  swipe <x1> <y1> <x2> <y2> [--duration-ms N]\n");
  out.write("  long-press <x> <y> [--duration-ms N]\n");
  out.write("  keyevent <keycode>\n");
  out.write("  text <text> [--adb-keyboard]\n");
  out.write("  clear-text\n");
  out.write("  screenshot [--out path]\n");
  out.write("  launch <package|package/activity|uri>\n");
  out.write("  get-current-app\n");
  out.write("  force-stop <package>\n");
  out.write("  dump-ui [--out path] [--parse]\n");
  out.write("  wm-size\n\n");
  out.write("Global Flags:\n");
  out.write("  -s, --serial <id>    device serial/id\n");
  out.write("  --log-json           Output logs in JSON\n");
}

function parseGlobalFlags(args: string[]) {
  let serial = "";
  let logJson = false;
  const rest: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--log-json" || arg.startsWith("--log-json=")) {
      const raw = arg.includes("=") ? arg.split("=", 2)[1] : "true";
      const lowered = raw.trim().toLowerCase();
      logJson = !(lowered === "false" || lowered === "0" || lowered === "no");
      continue;
    }
    if (arg === "-s" || arg === "--serial") {
      if (i + 1 < args.length) {
        serial = args[i + 1];
        i++;
      }
      continue;
    }
    if (arg.startsWith("--serial=")) {
      serial = arg.split("=", 2)[1] ?? "";
      continue;
    }
    rest.push(arg);
  }
  return { serial, logJson, rest };
}

function parseCommandFlags(args: string[]) {
  const flags: Record<string, string | boolean> = {};
  const rest: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "-h" || arg === "--help") {
      flags.help = true;
      continue;
    }
    if (arg === "--duration-ms") {
      i++;
      if (i >= args.length) return { error: "--duration-ms requires value" } as const;
      flags["duration-ms"] = args[i];
      continue;
    }
    if (arg.startsWith("--duration-ms=")) {
      flags["duration-ms"] = arg.split("=")[1] ?? "";
      continue;
    }
    if (arg === "--out") {
      i++;
      if (i >= args.length) return { error: "--out requires value" } as const;
      flags.out = args[i];
      continue;
    }
    if (arg.startsWith("--out=")) {
      flags.out = arg.split("=")[1] ?? "";
      continue;
    }
    if (arg === "--parse") {
      flags.parse = true;
      continue;
    }
    if (arg === "--adb-keyboard") {
      flags["adb-keyboard"] = true;
      continue;
    }
    if (!arg.startsWith("-") || arg === "-") {
      rest.push(...args.slice(i));
      break;
    }
    return { error: `unknown flag ${arg}` } as const;
  }
  return { flags, rest } as const;
}

async function main() {
  const argv = process.argv.slice(2);
  const global = parseGlobalFlags(argv);
  setLoggerJSON(global.logJson);
  const args = global.rest;
  if (args.length === 0 || args[0] === "help" || args[0] === "-h" || args[0] === "--help") {
    printUsage(process.stdout);
    process.exit(0);
  }
  const cmd = args[0];
  const cmdArgs = args.slice(1);
  switch (cmd) {
    case "devices":
      process.exit(cmdDevices(global.serial));
    case "start-server":
      process.exit(cmdStartServer());
    case "kill-server":
      process.exit(cmdKillServer());
    case "connect":
      process.exit(cmdConnect(cmdArgs));
    case "disconnect":
      process.exit(cmdDisconnect(cmdArgs));
    case "get-ip":
      process.exit(cmdGetIP(global.serial));
    case "enable-tcpip":
      process.exit(cmdEnableTCPIP(global.serial, cmdArgs));
    case "shell":
      process.exit(cmdShell(global.serial, cmdArgs));
    case "tap":
      process.exit(cmdTap(global.serial, cmdArgs));
    case "double-tap":
      process.exit(cmdDoubleTap(global.serial, cmdArgs));
    case "swipe": {
      const parsed = parseCommandFlags(cmdArgs);
      if ("error" in parsed) {
        logger.error(parsed.error as string);
        process.exit(2);
      }
      if (parsed.flags?.help) {
        process.stdout.write("Usage:\n  adb_helpers swipe [flags] <x1> <y1> <x2> <y2>\n\n");
        process.stdout.write("Flags:\n  --duration-ms <ms>    swipe duration in ms\n");
        process.exit(0);
      }
      const duration = parsed.flags?.["duration-ms"] ? Number(parsed.flags["duration-ms"]) : -1;
      process.exit(cmdSwipe(global.serial, parsed.rest ?? [], Number.isFinite(duration) ? duration : -1));
    }
    case "long-press": {
      const parsed = parseCommandFlags(cmdArgs);
      if ("error" in parsed) {
        logger.error(parsed.error as string);
        process.exit(2);
      }
      if (parsed.flags?.help) {
        process.stdout.write("Usage:\n  adb_helpers long-press [flags] <x> <y>\n\n");
        process.stdout.write("Flags:\n  --duration-ms <ms>    press duration in ms\n");
        process.exit(0);
      }
      const duration = parsed.flags?.["duration-ms"] ? Number(parsed.flags["duration-ms"]) : 3000;
      process.exit(cmdLongPress(global.serial, parsed.rest ?? [], Number.isFinite(duration) ? duration : 3000));
    }
    case "keyevent":
      process.exit(cmdKeyEvent(global.serial, cmdArgs));
    case "text": {
      const parsed = parseCommandFlags(cmdArgs);
      if ("error" in parsed) {
        logger.error(parsed.error as string);
        process.exit(2);
      }
      if (parsed.flags?.help) {
        process.stdout.write("Usage:\n  adb_helpers text [flags] <text>\n\n");
        process.stdout.write("Flags:\n  --adb-keyboard    use ADB Keyboard broadcast\n");
        process.exit(0);
      }
      process.exit(cmdText(global.serial, parsed.rest ?? [], Boolean(parsed.flags?.["adb-keyboard"])));
    }
    case "clear-text":
      process.exit(cmdClearText(global.serial));
    case "screenshot": {
      const parsed = parseCommandFlags(cmdArgs);
      if ("error" in parsed) {
        logger.error(parsed.error as string);
        process.exit(2);
      }
      if (parsed.flags?.help) {
        process.stdout.write("Usage:\n  adb_helpers screenshot [flags]\n\n");
        process.stdout.write("Flags:\n  --out <path>    output path\n");
        process.exit(0);
      }
      process.exit(cmdScreenshot(global.serial, String(parsed.flags?.out || "")));
    }
    case "launch":
      process.exit(cmdLaunch(global.serial, cmdArgs));
    case "get-current-app":
      process.exit(cmdGetCurrentApp(global.serial));
    case "force-stop":
      process.exit(cmdForceStop(global.serial, cmdArgs));
    case "dump-ui": {
      const parsed = parseCommandFlags(cmdArgs);
      if ("error" in parsed) {
        logger.error(parsed.error as string);
        process.exit(2);
      }
      if (parsed.flags?.help) {
        process.stdout.write("Usage:\n  adb_helpers dump-ui [flags]\n\n");
        process.stdout.write("Flags:\n  --out <path>    output path\n  --parse         parse UI hierarchy\n");
        process.exit(0);
      }
      process.exit(cmdDumpUI(global.serial, String(parsed.flags?.out || ""), Boolean(parsed.flags?.parse)));
    }
    case "wm-size":
      process.exit(cmdWmSize(global.serial));
    default:
      logger.error("unknown command", { command: cmd });
      printUsage(process.stdout);
      process.exit(2);
  }
}

main().catch((err) => {
  logger.error("unhandled error", { err: err?.message || String(err) });
  process.exit(1);
});
