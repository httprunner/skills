# Vision-First UI Recognition Workflow

Use this flow as the default UI recognition path. Use `dump-ui --parse` only as supplementary structured data.

## Prerequisites

- Run `npx tsx scripts/ai_vision.ts ...` in `ai-vision` skill directory.
- Export model credentials required by ai-vision (for example `ARK_BASE_URL`, `ARK_API_KEY`).
- Keep device serial fixed for the whole loop.
- Run each `npx` command in its own skill directory:
  - `android-adb` commands inside `android-adb/`
  - `ai-vision` commands inside `ai-vision/`

## Procedure

1. Capture screenshot.
2. Query `ai-vision` for target coordinates or UI assertion.
3. Apply returned coordinates using `tap`.
4. Optionally run `dump-ui --parse` to validate target consistency.
5. Re-capture screenshot and verify.
6. Retry with tighter prompt if result is wrong.

## Example

```bash
# Run in android-adb skill directory:
mkdir -p ~/.eval/screenshots
SHOT=~/.eval/screenshots/ui_$(date +"%Y%m%d_%H%M%S").png
npx tsx scripts/adb_helpers.ts -s SERIAL screenshot -out "$SHOT"

# Run in ai-vision skill directory:
npx tsx scripts/ai_vision.ts query \
  --screenshot "$SHOT" \
  --prompt "Find the button labeled Search and return center coordinates."

# Back in android-adb skill directory:
npx tsx scripts/adb_helpers.ts -s SERIAL tap X Y
```

## Prompting Guidance

- Describe exact text, icon shape, and relative area when possible.
- Explicitly request center coordinates in absolute pixels.
- If there are multiple matches, ask for ranked candidates.
