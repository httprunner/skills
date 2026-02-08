---
name: ai-vision-go
description: Multimodal UI understanding and single-step planning via OpenAI-compatible Responses APIs. Use when you need AIQuery/AIAssert and plan-next to extract UI element coordinates, validate UI assertions, summarize screenshots, or decide the next UI action from an image. External agents handle execution via adb/hdc and multi-step loops. Defaults to Doubao models but can be pointed at other multimodal providers via base URL, API key, and model name.
---

# AI Vision

## Overview
This skill provides a standalone CLI to call multimodal models for UI querying, assertion, and **single-step** planning. It does not depend on device type; you supply a screenshot and receive structured output (coordinates, decisions, or next actions). Execution and multi-step loops are handled externally by agents using adb/hdc or other drivers. Prefer storing screenshots in `~/.eval/screenshots/` and add timestamps to avoid overwriting.

## Path Convention

Canonical install and execution directory: `~/.agents/skills/ai-vision-go/`. Run commands from this directory:

```bash
cd ~/.agents/skills/ai-vision-go
```

One-off (safe in scripts/loops from any working directory):

```bash
(cd ~/.agents/skills/ai-vision-go && go run scripts/ai_vision.go --help)
```

## Model Configuration
Default Doubao configuration via environment variables:
- `ARK_BASE_URL` (e.g. `https://ark.cn-beijing.volces.com/api/v3`)
- `ARK_API_KEY`
- `ARK_MODEL_NAME`

For non-Doubao providers, pass explicit flags:
- `--base-url`, `--api-key`, `--model`

Default model if none provided: `doubao-seed-1-6-vision-250815`.

## Script
Path: `scripts/ai_vision.go`

Run with:
```bash
go run scripts/ai_vision.go --help
```

Log level (for troubleshooting raw model response):
```bash
go run scripts/ai_vision.go --log-level debug <command> [flags]
```

### AIQuery
```bash
go run scripts/ai_vision.go query \
  --screenshot ~/.eval/screenshots/ui_YYYYMMDD_HHMMSS.png \
  --prompt "请识别屏幕上的‘搜索’按钮，并返回其坐标"
```

### AIAssert
```bash
go run scripts/ai_vision.go assert \
  --screenshot ~/.eval/screenshots/ui_YYYYMMDD_HHMMSS.png \
  --assertion "当前页面包含搜索框"
```

### plan-next (single-step planning)
```bash
go run scripts/ai_vision.go plan-next \
  --screenshot ~/.eval/screenshots/ui_YYYYMMDD_HHMMSS.png \
  --instruction "点击放大镜图标进入搜索页"
```

## Output Notes
- `plan-next` returns a normalized next action with absolute pixel coordinates.
- If the model outputs relative coordinates (1000x1000), the script scales to screen pixels.
- Combine with adb/hdc actions (e.g., `adb shell input tap X Y`) for device control.
- Use `--log-level debug` to print the raw model response for troubleshooting.

## Default Models (Doubao)
- `doubao-seed-1-8-251228`
- `doubao-seed-1-6-vision-250815`

## References
- `references/doubao-api.md`
