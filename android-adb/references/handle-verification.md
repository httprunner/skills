# handle-verification Reference

Use `handle-verification` to handle security verification flows with a unified `ai-vision plan-next` loop.
It can be run independently and is also used by `install-smart` when install is blocked by dialogs/challenges.

## Prerequisites

- Run command in `android-adb` skill directory.
- `ai-vision` skill directory must be available for `plan-next`.
- Export required ai-vision environment variables (for example `ARK_BASE_URL`, `ARK_API_KEY`).
- If only one online device is connected, `-s SERIAL` is optional.
- If multiple devices are online, pass `-s SERIAL`.

## Command

```bash
npx tsx scripts/adb_helpers.ts -s SERIAL handle-verification
npx tsx scripts/adb_helpers.ts handle-verification
```

## Behavior

1. Capture screenshot.
2. Call `ai-vision plan-next` (from `ai-vision` skill directory).
3. Execute the returned action (for example `click`, `drag`, `long_press`, `press_back`, `wait`).
   `drag` is executed with Android's `adb input swipe` primitive (same intent: press-and-drag).
4. Stop when planner returns `finished` or loop reaches `--max-ui-steps`.
5. Early-stop heuristics are applied by default: if planner indicates a non-verification page, command exits immediately (no extra wait loop).

## Safety Policy

- Never click `安装新版本` (enforced via planner prompt and policy).
- Prefer `继续安装` for current version install.
- If checkbox acknowledgement is required, let planner choose it before continuing install.
- If a security verification page appears (slider/image click/button confirm, etc.), planner should return the appropriate next action and executor follows it.

## Options

- `--max-ui-steps <n>`: maximum planner-action cycles, default `20`
- `--ui-interval-sec <sec>`: delay between UI cycles, default `2`
- `--prompt <text>`: override planner prompt for special security UIs
- `--log-level <level>`: `debug|info|error`, default `info`; `debug` prints planner thought/action and detailed execution trace

## Failure Handling

- If `ai-vision` skill directory is missing, command exits with explicit path checks.
- If planner returns no recognized action repeatedly, stop and surface latest screenshot for manual review.
