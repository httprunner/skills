---
name: android-adb
description: Android device control and UI automation via ADB. Use when tasks involve connecting to devices/emulators, running adb shell commands, tapping/swiping, key events, text input, app launch, screenshots, or general device management through adb.
---

# ADB

Use this skill to drive Android devices with `adb` for common device management and UI actions.

## Quick workflow

- Identify device: `adb devices -l`. If multiple devices, always use `-s <device_id>`.
- Verify readiness: device status should be `device`.
- Prefer deterministic actions: tap/swipe/keyevent via `adb shell input`.
- For text: prefer ADB Keyboard broadcast if installed; otherwise escape text for `adb shell input text`.
- For screenshots: use `adb exec-out screencap -p > file.png` when possible.

## When running commands

- Always include `-s <device_id>` if more than one device is connected.
- If the request is ambiguous, ask for device id, package name, or coordinates.
- Use `adb shell wm size` to confirm screen resolution before coordinate-based actions.
- Treat errors from `adb` as actionable: surface stderr and suggest fixes (authorization, cable, tcpip).

## Resources

- Reference guide: `references/adb-reference.md`.
- Helper script: `scripts/adb_helpers.py` (argparse-based CLI wrappers). Use it for repeatable tasks or when multiple steps are needed.

## Script usage

Run:

```bash
python scripts/adb_helpers.py --help
```

Prefer script subcommands for:

- device listing / connect / disconnect
- tap / swipe / keyevent
- text input (with safe escaping)
- screenshots

If the script is insufficient, fall back to raw `adb` commands from the reference.
