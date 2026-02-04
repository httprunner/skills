---
name: wechat-search-collector
description: 微信视频号搜索与结果遍历的多端自动化流程（Android/iOS/HarmonyOS/HDC）：打开微信，进入“发现-视频号”，在搜索框输入关键词并触发搜索，滚动结果到底。用于同一产品多端界面一致场景下的搜索与采集。
---

# 微信视频号搜索

## 概述
本技能覆盖同一产品在 Android / iOS / HarmonyOS 等多端界面一致场景的搜索与遍历流程。执行层需根据设备类型选择对应驱动：Android 使用 `android-adb-go`，HarmonyOS 使用 `harmony-hdc`，iOS 使用对应自动化工具链（如 XCUITest/Appium）。始终使用 `ai-vision` 从截图中定位 UI 元素，任何步骤都不要使用 `dump-ui` 做元素发现。为避免 Go 模块依赖问题，所有命令需在各自 skill 目录内执行：`android-adb-go` 的命令在其目录运行，`ai-vision` 的命令在其目录运行。截图统一写入 `~/.eval/screenshots/`，文件名带时间戳避免覆盖。本文示例以 Android/ADB 命令为主，其他端按工具链替换执行命令。

## 流程

### 1. 预检
- 确认 `android-adb-go` 与 `ai-vision` 已安装且可用；按各自技能的预检项检查依赖（ADB 命令可用、`ARK_*` 环境变量已配置等）。
- 以下命令均在 `android-adb-go` 目录执行。
- 列出设备并获取 serial：`go run scripts/adb_helpers.go devices`
- 默认所有设备操作命令都带 `-s SERIAL`。若仅连接一台设备且用户未提供 serial，直接使用该设备的 serial 作为 `SERIAL`，无需额外询问。
- 必须确认已提供搜索词 `QUERY`；若用户未提供，直接报错终止流程并请求提供 `QUERY`。
- 必须确认分辨率（用于坐标点击校准）：
  `go run scripts/adb_helpers.go -s SERIAL wm-size`
- 检查微信是否已安装（Android 示例）：
  `go run scripts/adb_helpers.go -s SERIAL shell pm list packages | rg -n "com.tencent.mm"`
- 准备统一截图目录（本机执行一次即可）：
  `mkdir -p ~/.eval/screenshots`

### 2. 启动微信
- 若微信已在其他页面或标签打开，直接进入下一步。
- 若前台应用不是微信，先确认并启动微信再继续。
  - 以下命令均在 `android-adb-go` 目录执行。
  - 查看当前应用：
    `go run scripts/adb_helpers.go -s SERIAL get-current-app`
  - 启动微信：
    `go run scripts/adb_helpers.go -s SERIAL launch com.tencent.mm`

### 3. 进入 发现 -> 视频号
- 使用 ai-vision 的 `plan-next` 从截图中规划下一步并获取点击坐标，禁止使用 `dump-ui`。
- 点击底部标签 `发现`。
- 点击入口 `视频号`。
- 示例：
  在 `android-adb-go` 目录执行，先截图：
  `SCREENSHOT=~/.eval/screenshots/wechat_$(date +"%Y%m%d_%H%M%S").png`
  `go run scripts/adb_helpers.go -s SERIAL screenshot -out "$SCREENSHOT"`
  在 `ai-vision` 目录执行，获得下一步操作的动作及坐标：
  `go run scripts/ai_vision.go plan-next --screenshot "$SCREENSHOT" --instruction "点击底部“发现”标签"`
  在 `android-adb-go` 目录执行：
  `go run scripts/adb_helpers.go -s SERIAL tap X Y`

### 4. 进入搜索界面
- 点击视频号中的放大镜图标或搜索框（用 ai-vision 通过截图定位；禁止 `dump-ui`）。
  在 `android-adb-go` 目录执行，先截图：
  `SCREENSHOT=~/.eval/screenshots/wechat_$(date +"%Y%m%d_%H%M%S").png`
  `go run scripts/adb_helpers.go -s SERIAL screenshot -out "$SCREENSHOT"`
  在 `ai-vision` 目录执行，获得下一步操作的动作及坐标：
  `go run scripts/ai_vision.go plan-next --screenshot "$SCREENSHOT" --instruction "点击搜索框进入搜索页"`
  在 `android-adb-go` 目录执行：
  `go run scripts/adb_helpers.go -s SERIAL tap X Y`
- 若放大镜/搜索框被视频 UI 遮挡，先滑到下一个视频再重新识别。
  在 `android-adb-go` 目录执行：
  `go run scripts/adb_helpers.go -s SERIAL swipe 540 1400 540 600 600`

### 5. 输入关键词并触发搜索
- 以下命令均在 `android-adb-go` 目录执行。
- 使用 ADBKeyboard 清空并输入文本：
  `go run scripts/adb_helpers.go -s SERIAL clear-text`
  `go run scripts/adb_helpers.go -s SERIAL text --adb-keyboard "QUERY"`
- 点击屏幕上的搜索按钮或发送回车键事件触发搜索：
  `go run scripts/adb_helpers.go -s SERIAL keyevent KEYCODE_ENTER`
- 若未出现结果列表（仍为联想/建议），重试 `KEYCODE_ENTER`。

### 6. 结果滚动到底
- 反复滑动直到页面底部。
- 为减少截图识别开销，每滑动 5 次检测一次是否出现底部分割线作为触底判定，滑动间隔随机 1~3 秒。

示例滑动循环（手动执行）：
```
go run scripts/adb_helpers.go -s SERIAL swipe 540 1800 540 400 800
```
- 根据屏幕尺寸（`wm-size`）调整坐标。
- 示例检测（每 5 次滑动后）：
  在 `android-adb-go` 目录执行，先截图：
  `SCREENSHOT=~/.eval/screenshots/wechat_$(date +"%Y%m%d_%H%M%S").png`
  `go run scripts/adb_helpers.go -s SERIAL screenshot -out "$SCREENSHOT"`
  在 `ai-vision` 目录执行：
  `go run scripts/ai_vision.go plan-next --screenshot "$SCREENSHOT" --instruction "判断是否已到结果底部（是否出现底部分割线），若未到底请继续滑动"`
- 必要时用截图 + ai-vision 确认进度。

## 备注与排障
- 点击不准：重新截图，让 ai-vision 提供更精确坐标（不要改用 `dump-ui`）。
- 异常流程（弹窗遮挡或步骤卡住）：先识别弹窗并关闭，再继续原步骤。
  必须使用 ai-vision 从截图中定位关闭按钮（如 “×”、“关闭”、“取消”、“暂不”、“以后再说”、“允许/拒绝” 等），禁止 `dump-ui`。
  处理流程（必要时重复）：
  在 `android-adb-go` 目录执行，先截图：
  `SCREENSHOT=~/.eval/screenshots/wechat_$(date +"%Y%m%d_%H%M%S").png`
  `go run scripts/adb_helpers.go -s SERIAL screenshot -out "$SCREENSHOT"`
  在 `ai-vision` 目录执行：
  `go run scripts/ai_vision.go plan-next --screenshot "$SCREENSHOT" --instruction "识别并关闭当前弹窗（优先关闭或取消），若无弹窗则提示继续原流程"`
  在 `android-adb-go` 目录执行：
  `go run scripts/adb_helpers.go -s SERIAL tap X Y`
