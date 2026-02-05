#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import chalk from "chalk";

const defaultTimeoutMs = 15_000;

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

function hdcPrefix(serial: string) {
  return serial ? ["hdc", "-t", serial] : ["hdc"];
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

function runHdc(serial: string, capture: boolean, args: string[]) {
  return runCmd([...hdcPrefix(serial), ...args], capture, defaultTimeoutMs);
}

function cmdDevices(serial: string) {
  const result = runHdc(serial, true, ["list", "targets"]);
  if (result.exitCode !== 0) {
    if (result.stderr) process.stderr.write(result.stderr.trim() + "\n");
    return result.exitCode;
  }
  for (const line of result.stdout.split("\n")) {
    if (line.trim()) console.log(line.trim());
  }
  return 0;
}

function cmdConnect(args: string[]) {
  if (args.length < 1) {
    process.stderr.write("connect requires <address>\n");
    return 2;
  }
  let addr = args[0];
  if (!addr.includes(":")) addr += ":5555";
  return runCmd(["hdc", "tconn", addr], false, defaultTimeoutMs).exitCode;
}

function cmdDisconnect(args: string[]) {
  const cmd = ["hdc", "tdisconn", ...(args[0] ? [args[0]] : [])];
  return runCmd(cmd, false, defaultTimeoutMs).exitCode;
}

function cmdGetIP(serial: string) {
  const result = runHdc(serial, true, ["shell", "ifconfig"]);
  for (let line of result.stdout.split("\n")) {
    line = line.trim();
    if (!line) continue;
    if (line.includes("inet addr:")) {
      const parts = line.split(/\s+/);
      for (const part of parts) {
        if (part.startsWith("addr:")) {
          const ip = part.replace("addr:", "");
          if (ip && !ip.startsWith("127.")) {
            console.log(ip);
            return 0;
          }
        }
      }
    }
    if (line.includes("inet ")) {
      const parts = line.split(/\s+/);
      for (let i = 0; i < parts.length; i++) {
        if (parts[i] === "inet" && i + 1 < parts.length) {
          const ip = parts[i + 1].split("/")[0];
          if (ip && !ip.startsWith("127.")) {
            console.log(ip);
            return 0;
          }
        }
      }
    }
  }
  process.stderr.write("IP not found\n");
  return 1;
}

function cmdShell(serial: string, args: string[]) {
  return runHdc(serial, false, ["shell", ...args]).exitCode;
}

function cmdTap(serial: string, args: string[]) {
  if (args.length < 2) {
    process.stderr.write("tap requires <x> <y>\n");
    return 2;
  }
  return runHdc(serial, false, ["shell", "uitest", "uiInput", "click", args[0], args[1]]).exitCode;
}

function cmdDoubleTap(serial: string, args: string[]) {
  if (args.length < 2) {
    process.stderr.write("double-tap requires <x> <y>\n");
    return 2;
  }
  return runHdc(serial, false, ["shell", "uitest", "uiInput", "doubleClick", args[0], args[1]]).exitCode;
}

function cmdSwipe(serial: string, args: string[], durationMs: number) {
  if (args.length < 4) {
    process.stderr.write("swipe requires <x1> <y1> <x2> <y2>\n");
    return 2;
  }
  const cmd = ["shell", "uitest", "uiInput", "swipe", args[0], args[1], args[2], args[3]];
  if (durationMs >= 0) cmd.push(String(durationMs));
  return runHdc(serial, false, cmd).exitCode;
}

function cmdKeyEvent(serial: string, args: string[]) {
  if (args.length < 1) {
    process.stderr.write("keyevent requires <keycode>\n");
    return 2;
  }
  return runHdc(serial, false, ["shell", "uitest", "uiInput", "keyEvent", args[0]]).exitCode;
}

function cmdText(serial: string, args: string[]) {
  if (args.length < 1) {
    process.stderr.write("text requires <text>\n");
    return 2;
  }
  let text = args.join(" ");
  text = text.replace(/"/g, "\\\"").replace(/\$/g, "\\$");
  return runHdc(serial, false, ["shell", "uitest", "uiInput", "text", text]).exitCode;
}

function cmdScreenshot(serial: string, outPath: string) {
  let target = outPath || "screen.png";
  try {
    target = path.resolve(target);
  } catch {
    // ignore
  }
  const remotePath = "/data/local/tmp/tmp_screenshot.jpeg";
  const prefix = hdcPrefix(serial);
  const res = runCmd([...prefix, "shell", "screenshot", remotePath], true, defaultTimeoutMs);
  const resOut = res.stdout.toLowerCase();
  if (resOut.includes("fail") || resOut.includes("error") || resOut.includes("not found")) {
    runCmd([...prefix, "shell", "snapshot_display", "-f", remotePath], true, defaultTimeoutMs);
  }
  const pull = runCmd([...prefix, "file", "recv", remotePath, target], false, defaultTimeoutMs);
  runCmd([...prefix, "shell", "rm", remotePath], false, defaultTimeoutMs);
  return pull.exitCode;
}

function cmdLaunch(serial: string, args: string[]) {
  if (args.length < 1) {
    process.stderr.write("launch requires <bundle> or <bundle/ability>\n");
    return 2;
  }
  let bundle = args[0];
  let ability = "EntryAbility";
  if (bundle.includes("/")) {
    const parts = bundle.split("/", 2);
    bundle = parts[0];
    ability = parts[1];
  }
  return runHdc(serial, false, ["shell", "aa", "start", "-b", bundle, "-a", ability]).exitCode;
}

function cmdForceStop(serial: string, args: string[]) {
  if (args.length < 1) {
    process.stderr.write("force-stop requires <bundle>\n");
    return 2;
  }
  return runHdc(serial, false, ["shell", "aa", "force-stop", args[0]]).exitCode;
}

function cmdGetCurrentApp(serial: string) {
  const result = runHdc(serial, true, ["shell", "aa", "dump", "-l"]);
  const output = result.stdout;
  const lines = output.split("\n");
  let foreground = "";
  let current = "";
  const re = /\[([^\]]+)\]/;
  for (const line of lines) {
    if (line.includes("app name [")) {
      const match = line.match(re);
      if (match && match[1]) current = match[1];
    }
    if (line.includes("state #FOREGROUND") || line.toLowerCase().includes("state #foreground")) {
      if (current) {
        foreground = current;
        break;
      }
    }
    if (line.includes("Mission ID")) current = "";
  }
  if (foreground) {
    console.log(foreground);
    return 0;
  }
  process.stderr.write("System Home\n");
  return 1;
}

function hasHelpArg(args: string[]) {
  return args.some((arg) => arg === "-h" || arg === "--help");
}

function printUsage(out: NodeJS.WriteStream) {
  out.write("Usage:\n");
  out.write("  hdc_helpers [flags] <command> [args]\n\n");
  out.write("Commands:\n");
  out.write("  devices\n");
  out.write("  connect <address>\n");
  out.write("  disconnect [address]\n");
  out.write("  get-ip\n");
  out.write("  shell <cmd...>\n");
  out.write("  tap <x> <y>\n");
  out.write("  double-tap <x> <y>\n");
  out.write("  swipe <x1> <y1> <x2> <y2> [--duration-ms N]\n");
  out.write("  keyevent <keycode>\n");
  out.write("  text <text>\n");
  out.write("  screenshot [--out path]\n");
  out.write("  launch <bundle>[/Ability]\n");
  out.write("  force-stop <bundle>\n");
  out.write("  get-current-app\n\n");
  out.write("Global Flags:\n");
  out.write("  -t, -s <id>    device serial/id\n");
}

function parseGlobalFlags(args: string[]) {
  let serial = "";
  const rest: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "-t" || arg === "-s") {
      if (i + 1 < args.length) {
        serial = args[i + 1];
        i++;
      }
      continue;
    }
    if (arg.startsWith("-t=")) {
      serial = arg.split("=")[1] ?? "";
      continue;
    }
    rest.push(arg);
  }
  return { serial, rest };
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
    case "connect":
      process.exit(cmdConnect(cmdArgs));
    case "disconnect":
      process.exit(cmdDisconnect(cmdArgs));
    case "get-ip":
      process.exit(cmdGetIP(global.serial));
    case "shell":
      process.exit(cmdShell(global.serial, cmdArgs));
    case "tap":
      process.exit(cmdTap(global.serial, cmdArgs));
    case "double-tap":
      process.exit(cmdDoubleTap(global.serial, cmdArgs));
    case "swipe": {
      const parsed = parseCommandFlags(cmdArgs);
      if ("error" in parsed) {
        process.stderr.write(`${parsed.error}\n`);
        process.exit(2);
      }
      if (parsed.flags?.help) {
        process.stdout.write("Usage:\n  hdc_helpers swipe [flags] <x1> <y1> <x2> <y2>\n\n");
        process.stdout.write("Flags:\n  --duration-ms <ms>    swipe duration in ms\n");
        process.exit(0);
      }
      const duration = parsed.flags?.["duration-ms"] ? Number(parsed.flags["duration-ms"]) : -1;
      process.exit(cmdSwipe(global.serial, parsed.rest ?? [], Number.isFinite(duration) ? duration : -1));
    }
    case "keyevent":
      process.exit(cmdKeyEvent(global.serial, cmdArgs));
    case "text":
      process.exit(cmdText(global.serial, cmdArgs));
    case "screenshot": {
      const parsed = parseCommandFlags(cmdArgs);
      if ("error" in parsed) {
        process.stderr.write(`${parsed.error}\n`);
        process.exit(2);
      }
      if (parsed.flags?.help) {
        process.stdout.write("Usage:\n  hdc_helpers screenshot [flags]\n\n");
        process.stdout.write("Flags:\n  --out <path>    output path\n");
        process.exit(0);
      }
      process.exit(cmdScreenshot(global.serial, String(parsed.flags?.out || "")));
    }
    case "launch":
      process.exit(cmdLaunch(global.serial, cmdArgs));
    case "force-stop":
      process.exit(cmdForceStop(global.serial, cmdArgs));
    case "get-current-app":
      process.exit(cmdGetCurrentApp(global.serial));
    default:
      process.stderr.write(`Unknown command: ${cmd}\n`);
      printUsage(process.stdout);
      process.exit(2);
  }
}

main().catch((err) => {
  logger.error("unhandled error", { err: err?.message || String(err) });
  process.exit(1);
});
