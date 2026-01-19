---
name: harmony-hdc
description: HarmonyOS device control and UI automation via HDC. Use when tasks involve connecting to HarmonyOS devices/emulators, running hdc shell commands, inputting taps/text, launching abilities, or general device management.
---

# HDC

Use this skill to drive HarmonyOS devices with `hdc` for common device management and UI actions.

## Quick workflow

- Identify device: `hdc list targets`. If multiple devices, uses `-t <device_id>`.
- Verify readiness: device should appear in list.
- Prefer deterministic actions: tap/swipe/keyevent via `hdc shell uitest uiInput` (wrapped by helpers).
- For text: uses `hdc shell uitest uiInput text` (supports direct text input).
- For screenshots: uses `hdc shell screenshot` or `snapshot_display` and pulls file.

## When running commands

- Always include `-t <device_id>` if more than one device is connected.
- If the request is ambiguous, ask for device id, bundle name, or coordinates.
- Treat errors from `hdc` as actionable.

## Resources

- Helper script: `scripts/hdc_helpers.py` (argparse-based CLI wrappers). Use it for repeatable tasks.

## Script usage

Run:

```bash
python scripts/hdc_helpers.py --help
```

Prefer script subcommands for:

- device listing / connect / disconnect / get-ip
- tap / double-tap / swipe / keyevent
- text input
- screenshots
- app launch (ability start) / stop / get-current-app

If the script is insufficient, fall back to raw `hdc` commands.
