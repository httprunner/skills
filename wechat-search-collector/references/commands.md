# Commands Reference

## 约定
- 所有设备操作命令默认带 `-s SERIAL`。
- 所有 `android-adb-go` 命令在 `android-adb-go` 目录执行。
- 所有 `ai-vision` 命令在 `ai-vision` 目录执行。
- 截图目录统一为 `~/.eval/screenshots/`。

## 预检
- 列出设备并获取 serial：
  `go run scripts/adb_helpers.go devices`
- 查询分辨率：
  `go run scripts/adb_helpers.go -s SERIAL wm-size`
- 检查微信是否安装：
  `go run scripts/adb_helpers.go -s SERIAL shell pm list packages | rg -n "com.tencent.mm"`
- 准备截图目录：
  `mkdir -p ~/.eval/screenshots`

## 启动微信
- 查看当前前台应用：
  `go run scripts/adb_helpers.go -s SERIAL get-current-app`
- 启动微信：
  `go run scripts/adb_helpers.go -s SERIAL launch com.tencent.mm`

## 截图与定位点击
- 截图：
  `SCREENSHOT=~/.eval/screenshots/wechat_$(date +"%Y%m%d_%H%M%S").png`
  `go run scripts/adb_helpers.go -s SERIAL screenshot -out "$SCREENSHOT"`
- 通过 ai-vision 获取下一步点击坐标：
  `go run scripts/ai_vision.go plan-next --screenshot "$SCREENSHOT" --instruction "<你的操作指令>"`
- 点击坐标：
  `go run scripts/adb_helpers.go -s SERIAL tap X Y`

## 进入搜索页前的滑动
- 当搜索框被视频 UI 遮挡时，先滑动一屏：
  `go run scripts/adb_helpers.go -s SERIAL swipe 540 1400 540 600 600`

## 输入并触发搜索
- 清空输入框：
  `go run scripts/adb_helpers.go -s SERIAL clear-text`
- 输入文本（使用 ADBKeyboard）：
  `go run scripts/adb_helpers.go -s SERIAL text --adb-keyboard "QUERY"`
- 触发搜索：
  `go run scripts/adb_helpers.go -s SERIAL keyevent KEYCODE_ENTER`
- 若未进入结果页，重试 `KEYCODE_ENTER`。

## 结果滚动到底
- 滑动一屏：
  `go run scripts/adb_helpers.go -s SERIAL swipe 540 1800 540 400 800`
- 每滑动 5 次后用 ai-vision 判断是否触底：
  `go run scripts/ai_vision.go plan-next --screenshot "$SCREENSHOT" --instruction "判断是否已到结果底部（是否出现底部分割线），若未到底请继续滑动"`

## 弹窗处理
- 截图后让 ai-vision 识别关闭按钮（优先关闭或取消）：
  `go run scripts/ai_vision.go plan-next --screenshot "$SCREENSHOT" --instruction "识别并关闭当前弹窗（优先关闭或取消），若无弹窗则提示继续原流程"`
- 关闭弹窗：
  `go run scripts/adb_helpers.go -s SERIAL tap X Y`
