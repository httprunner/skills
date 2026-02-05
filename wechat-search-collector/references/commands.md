# Commands Reference

## 约定
- 若未指定设备类型，默认为 Android；若未指定设备序列号，在连接的设备中随机挑选一台
- 所有设备操作命令默认带 `-s SERIAL`。
- 所有 `android-adb` 命令在 `android-adb` 目录执行。
- 所有 `ai-vision` 命令在 `ai-vision` 目录执行。
- 截图与相关产物输出目录由 `TASK_ID` 控制：若指定 `TASK_ID` 则写入 `~/.eval/<TASK_ID>/`，未指定则写入 `~/.eval/debug/`。
- `ai-vision` 会返回已转换的绝对像素坐标，直接用于 `adb_helpers.ts` 操作。

## 预检
- 列出设备并获取 serial：
  `npx tsx scripts/adb_helpers.ts devices`
- 查询分辨率：
  `npx tsx scripts/adb_helpers.ts -s SERIAL wm-size`
- 检查微信是否安装：
  `npx tsx scripts/adb_helpers.ts -s SERIAL shell pm list packages | rg -n "com.tencent.mm"`
- 准备输出目录（有 `TASK_ID` 用对应目录，否则用 debug）：
  `mkdir -p ~/.eval/<TASK_ID>`
  `mkdir -p ~/.eval/debug`

## 启动微信
- 查看当前前台应用：
  `npx tsx scripts/adb_helpers.ts -s SERIAL get-current-app`
- 返回手机桌面（内部执行 2 次 BACK + 0~1s 随机间隔，直到到达桌面）：
  `npx tsx scripts/adb_helpers.ts -s SERIAL back-home`
- 启动微信：
  `npx tsx scripts/adb_helpers.ts -s SERIAL launch com.tencent.mm`

## 截图与定位点击
- 截图（有 `TASK_ID` 用对应目录，否则用 debug；文件名前缀无需固定）：
  `SCREENSHOT=~/.eval/<TASK_ID>/$(date +"%Y%m%d_%H%M%S").png`
  `SCREENSHOT=~/.eval/debug/$(date +"%Y%m%d_%H%M%S").png`
  `npx tsx scripts/adb_helpers.ts -s SERIAL screenshot -out "$SCREENSHOT"`
- 通过 ai-vision 获取下一步点击坐标：
  `npx tsx scripts/ai_vision.ts plan-next --screenshot "$SCREENSHOT" --prompt "<你的操作指令>"`
- 点击坐标（ai-vision 输出为 0-1000 相对坐标，adb_helpers 会自动转换为绝对坐标）：
  `npx tsx scripts/adb_helpers.ts -s SERIAL tap X Y`

## 进入搜索页前的滑动
- 当搜索框被视频 UI 遮挡时，先滑动一屏：
  `npx tsx scripts/adb_helpers.ts -s SERIAL swipe 540 1400 540 600 600`

## 输入并触发搜索
- 清空输入框：
  `npx tsx scripts/adb_helpers.ts -s SERIAL clear-text`
- 输入文本（使用 ADBKeyboard）：
  `npx tsx scripts/adb_helpers.ts -s SERIAL text --adb-keyboard "QUERY"`
- 触发搜索：
  `npx tsx scripts/adb_helpers.ts -s SERIAL keyevent KEYCODE_ENTER`
- 若未进入结果页，重试 `KEYCODE_ENTER`。

## 结果滚动到底
- 滑动一屏：
  `npx tsx scripts/adb_helpers.ts -s SERIAL swipe 540 1800 540 400 800`
- 每滑动 5 次后用 ai-vision 判断是否触底：
  `npx tsx scripts/ai_vision.ts plan-next --screenshot "$SCREENSHOT" --prompt "判断是否已到结果底部（是否出现底部分割线），若未到底请继续滑动"`

## Feishu 任务拉取

### 综合页搜索
- 拉取任务：
  `export FEISHU_APP_ID=...`
  `export FEISHU_APP_SECRET=...`
  `export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID"`
  `npx tsx /Users/debugtalk/MyProjects/HttpRunner-dev/skills/feishu-bitable-task-manager/scripts/bitable_task.ts fetch --app com.tencent.mm --scene 综合页搜索 --status pending,failed --date Today --limit 1`
- 映射参数：
  `TaskID -> TASK_ID`
  `Params -> KEYWORDS`（逗号或换行拆分）

### 个人页搜索
- 拉取任务：
  `export FEISHU_APP_ID=...`
  `export FEISHU_APP_SECRET=...`
  `export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID"`
  `npx tsx /Users/debugtalk/MyProjects/HttpRunner-dev/skills/feishu-bitable-task-manager/scripts/bitable_task.ts fetch --app com.tencent.mm --scene 个人页搜索 --status pending,failed --date Today --limit 1`
- 映射参数：
  `TaskID -> TASK_ID`
  `UserName -> ACCOUNT_NAME`
  `Params -> KEYWORDS`（逗号或换行拆分）

## 弹窗处理
- 截图后让 ai-vision 识别关闭按钮（优先关闭或取消）：
  `npx tsx scripts/ai_vision.ts plan-next --screenshot "$SCREENSHOT" --prompt "识别并关闭当前弹窗（优先关闭或取消），若无弹窗则提示继续原流程"`
- 关闭弹窗：
  `npx tsx scripts/adb_helpers.ts -s SERIAL tap X Y`
