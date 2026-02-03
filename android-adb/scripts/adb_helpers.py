#!/usr/bin/env python3
"""Small CLI helpers for common adb tasks."""

import argparse
import base64
import os
import re
import shlex
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


def _adb_prefix(serial: str | None) -> list[str]:
    if serial:
        return ["adb", "-s", serial]
    return ["adb"]


def _run(cmd: list[str], capture: bool = False, timeout: int = 10) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            cmd, text=True, capture_output=capture, check=False, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        # Create a dummy completed process for timeout handling
        return subprocess.CompletedProcess(
            args=cmd, returncode=124, stdout="", stderr="Command timed out"
        )


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


def cmd_get_ip(args: argparse.Namespace) -> int:
    """Get device IP address via ip route or wlan0 check."""
    cmd = _adb_prefix(args.serial) + ["shell", "ip", "route"]
    result = _run(cmd, capture=True)

    # Method 1: ip route
    for line in result.stdout.split("\n"):
        if "src" in line:
            parts = line.split()
            for i, part in enumerate(parts):
                if part == "src" and i + 1 < len(parts):
                    print(parts[i + 1])
                    return 0

    # Method 2: ip addr show wlan0
    cmd = _adb_prefix(args.serial) + ["shell", "ip", "addr", "show", "wlan0"]
    result = _run(cmd, capture=True)
    for line in result.stdout.split("\n"):
        if "inet " in line:
            parts = line.strip().split()
            if len(parts) >= 2:
                print(parts[1].split("/")[0])
                return 0

    print("IP not found", file=sys.stderr)
    return 1


def cmd_enable_tcpip(args: argparse.Namespace) -> int:
    """Enable TCP/IP debugging on specified port."""
    cmd = _adb_prefix(args.serial) + ["tcpip", str(args.port)]
    result = _run(cmd, capture=True)

    output = result.stdout + result.stderr
    print(output.strip())

    if "restarting" in output.lower() or result.returncode == 0:
        return 0
    return 1


def cmd_shell(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell"] + args.command
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_tap(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "input", "tap", str(args.x), str(args.y)]
    result = _run(cmd, capture=False)
    return result.returncode


def cmd_double_tap(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "input", "tap", str(args.x), str(args.y)]
    # First tap
    _run(cmd, capture=False)
    time.sleep(0.1)  # 100ms interval
    # Second tap
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
    text = text.replace("'", "\\'")
    text = text.replace("(", "\\(")
    text = text.replace(")", "\\)")
    return text


def _get_current_ime(serial: str | None) -> str:
    cmd = _adb_prefix(serial) + ["shell", "settings", "get", "secure", "default_input_method"]
    result = _run(cmd, capture=True)
    return (result.stdout + result.stderr).strip()


def _set_ime(serial: str | None, ime: str) -> None:
    cmd = _adb_prefix(serial) + ["shell", "ime", "set", ime]
    _run(cmd, capture=True)


def cmd_text(args: argparse.Namespace) -> int:
    serial = args.serial

    # Logic for Auto-IME switching logic
    original_ime = None
    if args.auto_ime:
        original_ime = _get_current_ime(serial)
        if "com.android.adbkeyboard/.AdbIME" not in original_ime:
            _set_ime(serial, "com.android.adbkeyboard/.AdbIME")
            # Wait a bit for switch
            time.sleep(1)
        # Use ADB keyboard broadcast method if auto-ime is on, or if explicitly requested
        args.adb_keyboard = True

    try:
        if args.adb_keyboard:
            encoded = base64.b64encode(args.text.encode("utf-8")).decode("utf-8")
            cmd = _adb_prefix(serial) + [
                "shell",
                "am",
                "broadcast",
                "-a",
                "ADB_INPUT_B64",
                "--es",
                "msg",
                encoded,
            ]
            result = _run(cmd, capture=False)
        else:
            escaped = _escape_input_text(args.text)
            cmd = _adb_prefix(serial) + ["shell", "input", "text", escaped]
            result = _run(cmd, capture=False)
    finally:
        # Restore IME if we switched it
        if original_ime and "com.android.adbkeyboard/.AdbIME" not in original_ime:
            _set_ime(serial, original_ime)

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


def cmd_get_current_app(args: argparse.Namespace) -> int:
    """Get the currently focused app package and activity."""
    cmd = _adb_prefix(args.serial) + ["shell", "dumpsys", "window"]
    result = _run(cmd, capture=True)
    output = result.stdout

    # Common known packages mapping (simplified from AutoGLM)
    # If the user needs the full map, they should likely use the full agent.
    # Here we just try to find the focused package.

    found_package = None

    for line in output.split("\n"):
        if "mCurrentFocus" in line or "mFocusedApp" in line:
            # Look for package format com.example.app/
            match = re.search(r'([a-zA-Z0-9_.]+\.[a-zA-Z0-9_.]+)/', line)
            if match:
                found_package = match.group(1)
                break

            # Fallback: sometimes it's like u0 com.example.app
            parts = line.split()
            for part in parts:
                if "/" in part:
                    found_package = part.split("/")[0]
                    break

    if found_package:
        print(found_package)
        return 0
    else:
        print("System Home (or unknown)", file=sys.stderr)
        return 1


def cmd_force_stop(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "am", "force-stop", args.package]
    result = _run(cmd, capture=False)
    return result.returncode


def _parse_ui_node(node, tappable, inputs, texts):
    """Recursively parse UI XML nodes."""
    bounds = node.get("bounds")
    text = node.get("text", "")
    content_desc = node.get("content-desc", "")
    resource_id = node.get("resource-id", "")
    class_name = node.get("class", "")
    clickable = node.get("clickable") == "true"

    # Text content can be in 'text' or 'content-desc' attributes
    display_name = text or content_desc or resource_id

    if bounds:
        # Bounds format: [x1,y1][x2,y2]
        match = re.search(r'\[(\d+),(\d+)\]\[(\d+),(\d+)\]', bounds)
        if match:
            x1, y1, x2, y2 = map(int, match.groups())
            center_x = (x1 + x2) // 2
            center_y = (y1 + y2) // 2
            coords = f"({center_x}, {center_y})"

            if class_name == "android.widget.EditText":
                inputs.append(f"  ⌨️ \"{display_name}\" @ {coords}")
            elif clickable:
                tappable.append(f"  👆 \"{display_name}\" @ {coords}")
            elif display_name and len(display_name) < 50: # Only short text
                texts.append(f"  👁️ \"{display_name}\" @ {coords}")

    for child in node:
        _parse_ui_node(child, tappable, inputs, texts)


def parse_ui_xml(xml_path: str):
    """Parse UI XML and print summary."""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        tappable = []
        inputs = []
        texts = []

        _parse_ui_node(root, tappable, inputs, texts)

        if tappable:
            print("\nTAPPABLE (clickable=true):")
            print("\n".join(tappable[:20])) # Limit output
            if len(tappable) > 20: print(f"  ... ({len(tappable)-20} more)")

        if inputs:
            print("\nINPUT FIELDS (EditText):")
            print("\n".join(inputs))

        if texts:
            print("\nTEXT/INFO (readable):")
            print("\n".join(texts[:20]))

    except Exception as e:
        print(f"Error parsing XML: {e}", file=sys.stderr)


def cmd_dump_ui(args: argparse.Namespace) -> int:
    """Dump UI hierarchy to XML and optionally parse it."""
    remote_path = "/sdcard/window_dump.xml"
    local_path = args.out or "window_dump.xml"

    # 1. Dump on device
    cmd_dump = _adb_prefix(args.serial) + ["shell", "uiautomator", "dump", remote_path]
    result = _run(cmd_dump, capture=True)
    if result.returncode != 0:
        print(f"Dump failed: {result.stderr}", file=sys.stderr)
        return result.returncode

    # 2. Pull to local
    cmd_pull = _adb_prefix(args.serial) + ["pull", remote_path, local_path]
    result = _run(cmd_pull, capture=True)
    if result.returncode != 0:
        print(f"Pull failed: {result.stderr}", file=sys.stderr)
        return result.returncode

    print(f"UI dumped to {local_path}")

    # 3. Parse if requested
    if args.parse:
        parse_ui_xml(local_path)

    return 0


def cmd_wm_size(args: argparse.Namespace) -> int:
    cmd = _adb_prefix(args.serial) + ["shell", "wm", "size"]
    result = _run(cmd, capture=False)
    return result.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ADB helper commands")
    parser.add_argument("-s", "--serial", help="device serial/id")

    sub = parser.add_subparsers(dest="command", required=True)

    # Device Management
    sub.add_parser("devices", help="list devices").set_defaults(func=cmd_devices)

    p_connect = sub.add_parser("connect", help="connect to a device over tcpip")
    p_connect.add_argument("address")
    p_connect.set_defaults(func=cmd_connect)

    p_disconnect = sub.add_parser("disconnect", help="disconnect from tcpip device")
    p_disconnect.add_argument("address", nargs="?")
    p_disconnect.set_defaults(func=cmd_disconnect)

    sub.add_parser("get-ip", help="get device ip address").set_defaults(func=cmd_get_ip)

    p_tcpip = sub.add_parser("enable-tcpip", help="enable tcpip debugging")
    p_tcpip.add_argument("port", type=int, default=5555, nargs="?")
    p_tcpip.set_defaults(func=cmd_enable_tcpip)

    # Shell
    p_shell = sub.add_parser("shell", help="run adb shell command")
    p_shell.add_argument("command", nargs=argparse.REMAINDER)
    p_shell.set_defaults(func=cmd_shell)

    # Input / Touch
    p_tap = sub.add_parser("tap", help="tap on screen")
    p_tap.add_argument("x", type=int)
    p_tap.add_argument("y", type=int)
    p_tap.set_defaults(func=cmd_tap)

    p_dtap = sub.add_parser("double-tap", help="double tap on screen")
    p_dtap.add_argument("x", type=int)
    p_dtap.add_argument("y", type=int)
    p_dtap.set_defaults(func=cmd_double_tap)

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

    # Text
    p_text = sub.add_parser("text", help="input text")
    p_text.add_argument("text")
    p_text.add_argument("--adb-keyboard", action="store_true", help="use ADB Keyboard broadcast")
    p_text.add_argument("--auto-ime", action="store_true", help="auto switch to ADB Keyboard and restore")
    p_text.set_defaults(func=cmd_text)

    p_clear = sub.add_parser("clear-text", help="clear text via ADB Keyboard")
    p_clear.set_defaults(func=cmd_clear_text)

    # Screen / App
    p_shot = sub.add_parser("screenshot", help="capture screenshot to file")
    p_shot.add_argument("--out", help="output path")
    p_shot.set_defaults(func=cmd_screenshot)

    p_launch = sub.add_parser("launch", help="launch app by package")
    p_launch.add_argument("package")
    p_launch.set_defaults(func=cmd_launch)

    sub.add_parser("get-current-app", help="get currently focused app package").set_defaults(func=cmd_get_current_app)

    p_stop = sub.add_parser("force-stop", help="force-stop app by package")
    p_stop.add_argument("package")
    p_stop.set_defaults(func=cmd_force_stop)

    p_dump = sub.add_parser("dump-ui", help="dump UI hierarchy to XML")
    p_dump.add_argument("--out", help="output path (default: window_dump.xml)")
    p_dump.add_argument("--parse", action="store_true", help="print text summary of UI elements")
    p_dump.set_defaults(func=cmd_dump_ui)

    p_wm = sub.add_parser("wm-size", help="print screen size")
    p_wm.set_defaults(func=cmd_wm_size)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
