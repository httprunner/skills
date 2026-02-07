# install-smart Reference

Use `install-smart` when `adb install -r` may block on runtime dialogs (install confirm, security prompt, permission screen).

## Prerequisites

- Run command in `android-adb` skill directory.
- If only one online device is connected, `-s SERIAL` is optional.
- If multiple devices are online, pass `-s SERIAL`.
- For security-dialog handling prerequisites (`ai-vision`, env vars), see `handle-verification.md`.

## Command

```bash
npx tsx scripts/adb_helpers.ts -s SERIAL install-smart /path/to/app.apk
npx tsx scripts/adb_helpers.ts install-smart /path/to/app.apk
```

## Behavior

1. Start `adb install -r`.
2. Wait `--initial-wait-sec` (default `5`).
3. Return immediately if install exits.
4. If still running, delegate to `handle-verification` for dialog/verification handling.
5. Stop when install exits or loop reaches `--max-ui-steps`.

## Options

- `--initial-wait-sec <sec>`: seconds before first UI intervention, default `5`
- `--max-ui-steps <n>`: forwarded to `handle-verification`
- `--ui-interval-sec <sec>`: forwarded to `handle-verification`
- `--prompt <text>`: forwarded to `handle-verification`
- `--log-level <level>`: forwarded to `handle-verification`

## Failure Handling

- If APK file does not exist, command exits before starting install.
- `handle-verification` behavior, safety policy, and failure handling: see `handle-verification.md`.
