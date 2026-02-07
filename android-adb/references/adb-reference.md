# Android ADB Command Reference

Use this file as the default command lookup table.
Run commands from `android-adb` skill directory.

## Device And Server

- Start ADB server: `npx tsx scripts/adb_helpers.ts start-server`
- Stop ADB server: `npx tsx scripts/adb_helpers.ts kill-server`
- List devices: `npx tsx scripts/adb_helpers.ts devices`
- Target one device: `npx tsx scripts/adb_helpers.ts -s SERIAL <command>`

## Wi-Fi Connection

- Enable tcpip (USB required): `npx tsx scripts/adb_helpers.ts -s SERIAL enable-tcpip [port]`
- Get device IP: `npx tsx scripts/adb_helpers.ts -s SERIAL get-ip`
- Connect over network: `npx tsx scripts/adb_helpers.ts connect <ip>:5555`
- Disconnect one/all: `npx tsx scripts/adb_helpers.ts disconnect [ip]:5555`

## Device State

- Screen size: `npx tsx scripts/adb_helpers.ts -s SERIAL wm-size`
- Current foreground app: `npx tsx scripts/adb_helpers.ts -s SERIAL get-current-app`
- Raw shell command: `npx tsx scripts/adb_helpers.ts -s SERIAL shell <args...>`

## App Lifecycle

- Verify package: `npx tsx scripts/adb_helpers.ts -s SERIAL shell pm list packages | rg -n "<package>"`
- Launch by package: `npx tsx scripts/adb_helpers.ts -s SERIAL launch <package>`
- Launch by activity: `npx tsx scripts/adb_helpers.ts -s SERIAL launch <package>/<activity>`
- Launch by URI/schema: `npx tsx scripts/adb_helpers.ts -s SERIAL launch <scheme://path>`
- Force-stop: `npx tsx scripts/adb_helpers.ts -s SERIAL force-stop <package>`

## Input Actions

- Tap: `npx tsx scripts/adb_helpers.ts -s SERIAL tap X Y`
- Double tap: `npx tsx scripts/adb_helpers.ts -s SERIAL double-tap X Y`
- Long press: `npx tsx scripts/adb_helpers.ts -s SERIAL long-press X Y [--duration-ms N]`
- Swipe: `npx tsx scripts/adb_helpers.ts -s SERIAL swipe X1 Y1 X2 Y2 [--duration-ms N]`
- Key event: `npx tsx scripts/adb_helpers.ts -s SERIAL keyevent KEYCODE_BACK`
- Return to launcher robustly: `npx tsx scripts/adb_helpers.ts -s SERIAL back-home`

## Text Input

- Clear text via ADB Keyboard broadcast: `npx tsx scripts/adb_helpers.ts -s SERIAL clear-text`
- Input with ADB Keyboard: `npx tsx scripts/adb_helpers.ts -s SERIAL text --adb-keyboard "TEXT"`
- Input with plain `input text`: `npx tsx scripts/adb_helpers.ts -s SERIAL text "TEXT"`

## Screenshot And UI Tree

- Screenshot: `npx tsx scripts/adb_helpers.ts -s SERIAL screenshot --out "<path>/shot.png"`
- Dump UI XML: `npx tsx scripts/adb_helpers.ts -s SERIAL dump-ui [--out path]`
- Dump + parse clickable/input nodes: `npx tsx scripts/adb_helpers.ts -s SERIAL dump-ui --parse`

## Installer Automation

- Handle verification/dialogs only: `npx tsx scripts/adb_helpers.ts -s SERIAL handle-verification`
- Handle install dialogs automatically: `npx tsx scripts/adb_helpers.ts -s SERIAL install-smart /path/to/app.apk`
- `handle-verification` detailed options and behavior: see `handle-verification.md`
- `install-smart` detailed options and behavior: see `install-smart.md`

## Reset Workflow

- Device reset steps and alias mapping: see `device-reset.md`
