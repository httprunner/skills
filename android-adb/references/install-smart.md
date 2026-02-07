# install-smart Reference

Use `install-smart` when `adb install -r` may block on runtime dialogs (install confirm, security prompt, permission screen).

## Prerequisites

- Run command in `android-adb` skill directory.
- `ai-vision` skill directory must be available for `plan-next`.
- Export required ai-vision environment variables (for example `ARK_BASE_URL`, `ARK_API_KEY`).
- If only one online device is connected, `-s SERIAL` is optional.
- If multiple devices are online, pass `-s SERIAL`.

## Command

```bash
npx tsx scripts/adb_helpers.ts -s SERIAL install-smart /path/to/app.apk
npx tsx scripts/adb_helpers.ts install-smart /path/to/app.apk
```

## Behavior

1. Start `adb install -r`.
2. Wait `--initial-wait-sec` (default `5`).
3. Return immediately if install exits.
4. If still running, loop:
   1. Capture screenshot.
   2. Call `ai-vision plan-next` (from `ai-vision` skill directory).
   3. Execute click action coordinates.
5. Stop when install exits or loop reaches `--max-ui-steps`.

## Options

- `--initial-wait-sec <sec>`: seconds before first UI intervention, default `5`
- `--max-ui-steps <n>`: maximum planner-action cycles, default `20`
- `--ui-interval-sec <sec>`: delay between UI cycles, default `2`
- `--prompt <text>`: override planner prompt for special installer UIs

## Failure Handling

- If `ai-vision` skill directory is missing, command exits with explicit path checks.
- If APK file does not exist, command exits before starting install.
- If planner returns no click action repeatedly, stop and surface latest screenshot for manual review.
