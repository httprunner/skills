#!/usr/bin/env python3
"""Small CLI helpers for common adb tasks."""

import argparse
import base64
import os
import shlex
import subprocess
import sys


def _adb_prefix(serial: str | None) -> list[str]:
    if serial:
        return ["adb", "-s", serial]
    return ["adb"]


def _run(cmd: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, capture_output=capture, check=False)


def cmd_devices(args: argparse.Namespace) -> int:
    result = _run(_adb_prefix(args.serial) + ["devices", "-l"], capture=False)
    return result.returncode


def cmd_connect(args: argparse.Namespace) -> int:
    result = _run(["adb", "connect", args.address], capture=False)
    return result.returncode


def cmd_disconnect(args: argparse.Namespace) -> int:
    cmd = ["adb", "disconnect"]
    if args.address:
        cmd.append(args.address)
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_shell(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell"] + args.command
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_tap(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "input", "tap", str(args.x), str(args.y)]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_swipe(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + [
        "shell",
        "input",
        "swipe",
        str(args.x1),
        str(args.y1),
        str(args.x2),
        str(args.y2),
    ]
    if args.duration_ms is not None:
        cmd.append(str(args.duration_ms))
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_long_press(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + [
        "shell",
        "input",
        "swipe",
        str(args.x),
        str(args.y),
        str(args.x),
        str(args.y),
        str(args.duration_ms),
    ]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_keyevent(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "input", "keyevent", str(args.keycode)]
    result = _run(cmd, capture=False)
    return result.returncode


def _escape_input_text(text: str) -> str:
    # adb input text treats spaces as separators; use %s for spaces
    text = text.replace(" ", "%s")
    # escape shell-sensitive characters
    text = text.replace("\\", "\\\\")
    return text


def cmd_text(args: argparse.Namespace) -> int:
    if args.adb_keyboard:
        encoded = base64.b64encode(args.text.encode("utf-8")).decode("utf-8")
        cmd = _adb_prefix(args.serial) + [
            "shell",
            "am",
            "broadcast",
            "-a",
            "ADB_INPUT_B64",
            "--es",
            "msg",
            encoded,
        ]
    else:
        escaped = _escape_input_text(args.text)
        cmd = _adb_prefix(args.serial) + ["shell", "input", "text", escaped]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_clear_text(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "am", "broadcast", "-a", "ADB_CLEAR_TEXT"]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_screenshot(args: argparse.Namespace) -> int:
    out_path = args.out
    if not out_path:
        out_path = os.path.abspath("screen.png")
    cmd = _adb_prefix(args.serial) + ["exec-out", "screencap", "-p"]
    with open(out_path, "wb") as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, check=False)
    if result.returncode != 0:
        sys.stderr.write(result.stderr.decode("utf-8", errors="ignore"))
    return result.returncode


def cmd_launch(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + [
        "shell",
        "monkey",
        "-p",
        args.package,
        "-c",
        "android.intent.category.LAUNCHER",
        "1",
    ]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_force_stop(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "am", "force-stop", args.package]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_wm_size(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "wm", "size"]
    result = _run(cmd, capture=False)
    return result.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ADB helper commands")
    parser.add_argument("-s", "--serial", help="device serial/id")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("devices", help="list devices").set_defaults(func=cmd_devices)

    p_connect = sub.add_parser("connect", help="connect to a device over tcpip")
    p_connect.add_argument("address")
    p_connect.set_defaults(func=cmd_connect)

    p_disconnect = sub.add_parser("disconnect", help="disconnect from tcpip device")
    p_disconnect.add_argument("address", nargs="?")
    p_disconnect.set_defaults(func=cmd_disconnect)

    p_shell = sub.add_parser("shell", help="run adb shell command")
    p_shell.add_argument("command", nargs=argparse.REMAINDER)
    p_shell.set_defaults(func=cmd_shell)

    p_tap = sub.add_parser("tap", help="tap on screen")
    p_tap.add_argument("x", type=int)
    p_tap.add_argument("y", type=int)
    p_tap.set_defaults(func=cmd_tap)

    p_swipe = sub.add_parser("swipe", help="swipe on screen")
    p_swipe.add_argument("x1", type=int)
    p_swipe.add_argument("y1", type=int)
    p_swipe.add_argument("x2", type=int)
    p_swipe.add_argument("y2", type=int)
    p_swipe.add_argument("--duration-ms", type=int)
    p_swipe.set_defaults(func=cmd_swipe)

    p_long = sub.add_parser("long-press", help="long press on screen")
    p_long.add_argument("x", type=int)
    p_long.add_argument("y", type=int)
    p_long.add_argument("--duration-ms", type=int, default=3000)
    p_long.set_defaults(func=cmd_long_press)

    p_key = sub.add_parser("keyevent", help="send keyevent")
    p_key.add_argument("keycode")
    p_key.set_defaults(func=cmd_keyevent)

    p_text = sub.add_parser("text", help="input text")
    p_text.add_argument("text")
    p_text.add_argument("--adb-keyboard", action="store_true", help="use ADB Keyboard broadcast")
    p_text.set_defaults(func=cmd_text)

    p_clear = sub.add_parser("clear-text", help="clear text via ADB Keyboard")
    p_clear.set_defaults(func=cmd_clear_text)

    p_shot = sub.add_parser("screenshot", help="capture screenshot to file")
    p_shot.add_argument("--out", help="output path")
    p_shot.set_defaults(func=cmd_screenshot)

    p_launch = sub.add_parser("launch", help="launch app by package")
    p_launch.add_argument("package")
    p_launch.set_defaults(func=cmd_launch)

    p_stop = sub.add_parser("force-stop", help="force-stop app by package")
    p_stop.add_argument("package")
    p_stop.set_defaults(func=cmd_force_stop)

    p_wm = sub.add_parser("wm-size", help="print screen size")
    p_wm.set_defaults(func=cmd_wm_size)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
