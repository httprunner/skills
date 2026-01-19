#!/usr/bin/env python3
"""Small CLI helpers for common HDC tasks on HarmonyOS."""

import argparse
import base64
import os
import re
import shlex
import subprocess
import sys
import time
from typing import Optional


def _hdc_prefix(serial: str | None) -> list[str]:
    if serial:
        return ["hdc", "-t", serial]
    return ["hdc"]


def _run(cmd: list[str], capture: bool = False, timeout: int = 15) -> subprocess.CompletedProcess:
    try:
        # print(f"DEBUG: running {' '.join(cmd)}")
        return subprocess.run(
            cmd, text=True, capture_output=capture, check=False, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            args=cmd, returncode=124, stdout="", stderr="Command timed out"
        )


def cmd_devices(args: argparse.Namespace) -> int:
    """List connected devices."""
    result = _run(_hdc_prefix(args.serial) + ["list", "targets"], capture=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        return result.returncode

    # HDC list output is just identifiers, sometimes with status
    # We just print it out, but cleanup empty lines
    for line in result.stdout.split("\n"):
        if line.strip():
            print(line.strip())
    return 0


def cmd_connect(args: argparse.Namespace) -> int:
    """Connect to remote device."""
    addr = args.address
    if ":" not in addr:
        addr = f"{addr}:5555"
    result = _run(["hdc", "tconn", addr], capture=False)
    return result.returncode


def cmd_disconnect(args: argparse.Namespace) -> int:
    """Disconnect remote device."""
    cmd = ["hdc", "tdisconn"]
    if args.address:
        cmd.append(args.address)
    # else: hdc doesnt support bulk disconnect easily without scripting
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_get_ip(args: argparse.Namespace) -> int:
    """Get device IP address via ifconfig."""
    cmd = _hdc_prefix(args.serial) + ["shell", "ifconfig"]
    result = _run(cmd, capture=True)

    for line in result.stdout.split("\n"):
        if "inet addr:" in line or "inet " in line:
            parts = line.strip().split()
            for i, part in enumerate(parts):
                if "addr:" in part:
                    ip = part.split(":")[1]
                    if not ip.startswith("127."):
                        print(ip)
                        return 0
                elif part == "inet" and i + 1 < len(parts):
                    ip = parts[i + 1].split("/")[0]
                    if not ip.startswith("127."):
                        print(ip)
                        return 0

    print("IP not found", file=sys.stderr)
    return 1


def cmd_shell(args: argparse.Namespace) -> int:
    """Run raw shell command."""
    cmd = _hdc_prefix(args.serial) + ["shell"] + args.command
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_tap(args: argparse.Namespace) -> int:
    """Tap at coordinates using uitest."""
    cmd = _hdc_prefix(args.serial) + ["shell", "uitest", "uiInput", "click", str(args.x), str(args.y)]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_double_tap(args: argparse.Namespace) -> int:
    """Double tap at coordinates using uitest."""
    cmd = _hdc_prefix(args.serial) + ["shell", "uitest", "uiInput", "doubleClick", str(args.x), str(args.y)]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_swipe(args: argparse.Namespace) -> int:
    """Swipe using uitest."""
    cmd = _hdc_prefix(args.serial) + [
        "shell",
        "uitest",
        "uiInput",
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


def cmd_keyevent(args: argparse.Namespace) -> int:
    """Send key event."""
    # HarmonyOS uses key codes or names (e.g. Back, Home)
    cmd = _hdc_prefix(args.serial) + ["shell", "uitest", "uiInput", "keyEvent", str(args.keycode)]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_text(args: argparse.Namespace) -> int:
    """Input text."""
    # Escape quotes and special chars
    escaped = args.text.replace('"', '\\"').replace("$", "\\$")

    # hdc shell uitest uiInput text "content"
    cmd = _hdc_prefix(args.serial) + ["shell", "uitest", "uiInput", "text", escaped]
    result = _run(cmd, capture=False)

    # Auto-enter if multi-line (simulated by caller logic usually, but here simple version)
    return result.returncode


def cmd_screenshot(args: argparse.Namespace) -> int:
    """Capture screenshot."""
    out_path = args.out or "screen.png"
    remote_path = "/data/local/tmp/tmp_screenshot.jpeg"
    prefix = _hdc_prefix(args.serial)

    # Try 'screenshot' command
    res = _run(prefix + ["shell", "screenshot", remote_path], capture=True)
    if "fail" in res.stdout.lower() or "error" in res.stdout.lower() or "not found" in res.stdout.lower():
         # Fallback to snapshot_display
         _run(prefix + ["shell", "snapshot_display", "-f", remote_path], capture=True)

    # Pull
    res_pull = _run(prefix + ["file", "recv", remote_path, out_path], capture=False)

    # Cleanup
    _run(prefix + ["shell", "rm", remote_path], capture=False)

    return res_pull.returncode


def cmd_launch(args: argparse.Namespace) -> int:
    """Launch app (Ability) by bundle info."""
    # User passes 'bundle' or 'bundle/ability'.
    # If no ability, we assume 'EntryAbility' (common default) or user should provide it.

    bundle = args.package
    ability = "EntryAbility"

    if "/" in bundle:
        bundle, ability = bundle.split("/", 1)

    cmd = _hdc_prefix(args.serial) + [
        "shell",
        "aa",
        "start",
        "-b",
        bundle,
        "-a",
        ability
    ]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_force_stop(args: argparse.Namespace) -> int:
    """Force stop app."""
    cmd = _hdc_prefix(args.serial) + ["shell", "aa", "force-stop", args.package]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_get_current_app(args: argparse.Namespace) -> int:
    """Get current bundle name from aa dump."""
    cmd = _hdc_prefix(args.serial) + ["shell", "aa", "dump", "-l"]
    result = _run(cmd, capture=True)
    output = result.stdout

    lines = output.split("\n")
    foreground_bundle = None
    current_bundle = None

    for line in lines:
        if "app name [" in line:
            match = re.search(r'\[([^\]]+)\]', line)
            if match:
                current_bundle = match.group(1)

        if "state #FOREGROUND" in line or "state #foreground" in line.lower():
            if current_bundle:
                foreground_bundle = current_bundle
                break

        if "Mission ID" in line:
            current_bundle = None

    if foreground_bundle:
        print(foreground_bundle)
        return 0
    else:
        print("System Home", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HDC helper commands for HarmonyOS")
    parser.add_argument("-s", "--serial", "-t", dest="serial", help="device serial/id (matches -t in hdc)")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("devices", help="list devices").set_defaults(func=cmd_devices)

    p_conn = sub.add_parser("connect", help="connect remote device")
    p_conn.add_argument("address")
    p_conn.set_defaults(func=cmd_connect)

    p_disc = sub.add_parser("disconnect", help="disconnect remote device")
    p_disc.add_argument("address", nargs="?")
    p_disc.set_defaults(func=cmd_disconnect)

    sub.add_parser("get-ip", help="get device ip").set_defaults(func=cmd_get_ip)

    p_shell = sub.add_parser("shell", help="run hdc shell command")
    p_shell.add_argument("command", nargs=argparse.REMAINDER)
    p_shell.set_defaults(func=cmd_shell)

    p_tap = sub.add_parser("tap", help="tap x y")
    p_tap.add_argument("x", type=int)
    p_tap.add_argument("y", type=int)
    p_tap.set_defaults(func=cmd_tap)

    p_dtap = sub.add_parser("double-tap", help="double tap x y")
    p_dtap.add_argument("x", type=int)
    p_dtap.add_argument("y", type=int)
    p_dtap.set_defaults(func=cmd_double_tap)

    p_swipe = sub.add_parser("swipe", help="swipe x1 y1 x2 y2")
    p_swipe.add_argument("x1", type=int)
    p_swipe.add_argument("y1", type=int)
    p_swipe.add_argument("x2", type=int)
    p_swipe.add_argument("y2", type=int)
    p_swipe.add_argument("--duration-ms", type=int, help="duration in ms (200-5000)")
    p_swipe.set_defaults(func=cmd_swipe)

    p_key = sub.add_parser("keyevent", help="send key code/name (e.g. Back, Home)")
    p_key.add_argument("keycode")
    p_key.set_defaults(func=cmd_keyevent)

    p_text = sub.add_parser("text", help="input text")
    p_text.add_argument("text")
    p_text.set_defaults(func=cmd_text)

    p_shot = sub.add_parser("screenshot", help="capture screenshot")
    p_shot.add_argument("--out", help="output path")
    p_shot.set_defaults(func=cmd_screenshot)

    p_launch = sub.add_parser("launch", help="launch app (bundle or bundle/ability)")
    p_launch.add_argument("package", help="bundle name or bundle/AbilityName")
    p_launch.set_defaults(func=cmd_launch)

    p_stop = sub.add_parser("force-stop", help="force stop app")
    p_stop.add_argument("package")
    p_stop.set_defaults(func=cmd_force_stop)

    sub.add_parser("get-current-app", help="get foreground bundle").set_defaults(func=cmd_get_current_app)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
