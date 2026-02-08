# Commands Reference

## 约定
- 若未指定设备类型，默认为 Android；若未指定设备序列号，在连接的设备中随机挑选一台
- 所有设备操作命令默认带 `-s SERIAL`。
- 所有 skills 的安装与执行目录统一为：`~/.agents/skills/<skill>/`。为避免 `cd` 漂移导致路径问题，推荐在任意目录用下列函数执行命令：

```bash
ANDROID_ADB_DIR=~/.agents/skills/android-adb
AI_VISION_DIR=~/.agents/skills/ai-vision
FEISHU_TASK_DIR=~/.agents/skills/feishu-bitable-task-manager
RESULT_REPORTER_DIR=~/.agents/skills/result-bitable-reporter
PIRACY_DIR=~/.agents/skills/piracy-handler
WEBHOOK_DIR=~/.agents/skills/group-webhook-dispatch

ADB() { (cd "$ANDROID_ADB_DIR" && npx tsx scripts/adb_helpers.ts "$@"); }
VISION() { (cd "$AI_VISION_DIR" && npx tsx scripts/ai_vision.ts "$@"); }
TASK() { (cd "$FEISHU_TASK_DIR" && npx tsx scripts/bitable_task.ts "$@"); }
REPORT() { (cd "$RESULT_REPORTER_DIR" && npx tsx scripts/result_reporter.ts "$@"); }
PIRACY_DETECT() { (cd "$PIRACY_DIR" && npx tsx scripts/piracy_detect.ts "$@"); }
PIRACY_CREATE_SUBTASKS() { (cd "$PIRACY_DIR" && npx tsx scripts/piracy_create_subtasks.ts "$@"); }
PIRACY_UPSERT_WEBHOOK_PLANS() { (cd "$PIRACY_DIR" && npx tsx scripts/piracy_upsert_webhook_plans.ts "$@"); }
WEBHOOK_DISPATCH() { (cd "$WEBHOOK_DIR" && npx tsx scripts/dispatch_webhook.ts "$@"); }
WEBHOOK_RECONCILE() { (cd "$WEBHOOK_DIR" && npx tsx scripts/reconcile_webhook.ts "$@"); }
```

- `TASK_ID` 必须为数字（与 sqlite `TaskID` 列一致）。
- 截图与相关产物输出目录由 `TASK_ID` 控制：若指定 `TASK_ID` 则写入 `~/.eval/<TASK_ID>/`，未指定则写入 `~/.eval/debug/`。
- `ai-vision` 会返回已转换的绝对像素坐标，直接用于 `adb_helpers.ts` 操作。

## 预检
- 列出设备并获取 serial：
  `ADB devices`
- 查询分辨率：
  `ADB -s SERIAL wm-size`
- 检查微信是否安装：
  `ADB -s SERIAL shell pm list packages | rg -n "com.tencent.mm"`
- 准备输出目录（有 `TASK_ID` 用对应目录，否则用 debug）：
  `mkdir -p ~/.eval/<TASK_ID>`
  `mkdir -p ~/.eval/debug`

## 结果采集（result-bitable-reporter）
- 启动后台采集（前置步骤，开始微信搜索前执行）：
  `export BUNDLE_ID=com.tencent.mm`
  `export SerialNumber=SERIAL`
  `REPORT collect-start --task-id TASK_ID --db-path ~/.eval/records.sqlite --table capture_results`
- 停止采集（收尾步骤，任务结束/异常中断都执行）：
  `SerialNumber=SERIAL REPORT collect-stop`
- 上报采集结果到飞书多维表格采集结果表（收尾步骤）：
  `export FEISHU_APP_ID=...`
  `export FEISHU_APP_SECRET=...`
  `export RESULT_BITABLE_URL="https://.../wiki/...?...table=tbl_xxx&view=vew_xxx"`
  `REPORT report --db-path ~/.eval/records.sqlite --table capture_results --task-id TASK_ID --status 0,-1 --batch-size 30 --limit 100`

## 启动微信
- 查看当前前台应用：
  `ADB -s SERIAL get-current-app`
- 返回手机桌面（内部执行 2 次 BACK + 0~1s 随机间隔，直到到达桌面）：
  `ADB -s SERIAL back-home`
- 启动微信：
  `ADB -s SERIAL launch com.tencent.mm`

## 截图与定位点击
- 截图（有 `TASK_ID` 用对应目录，否则用 debug；文件名前缀无需固定）：
  `SCREENSHOT=~/.eval/<TASK_ID>/$(date +"%Y%m%d_%H%M%S").png`
  `SCREENSHOT=~/.eval/debug/$(date +"%Y%m%d_%H%M%S").png`
  `ADB -s SERIAL screenshot -out "$SCREENSHOT"`
- 通过 ai-vision 获取下一步点击坐标：
  `VISION plan-next --screenshot "$SCREENSHOT" --prompt "<你的操作指令>"`
- 点击坐标（ai-vision 输出为 0-1000 相对坐标，adb_helpers 会自动转换为绝对坐标）：
  `ADB -s SERIAL tap X Y`

## 进入搜索页前的滑动
- 当搜索框被视频 UI 遮挡时，先滑动一屏：
  `ADB -s SERIAL swipe 540 1400 540 600 600`

## 输入并触发搜索
- 清空输入框：
  `ADB -s SERIAL clear-text`
- 输入文本（使用 ADBKeyboard）：
  `ADB -s SERIAL text --adb-keyboard "QUERY"`
- 触发搜索：
  `ADB -s SERIAL keyevent KEYCODE_ENTER`
- 若未进入结果页，重试 `KEYCODE_ENTER`。

## 结果滚动到底
- 滑动一屏：
  `ADB -s SERIAL swipe 540 1800 540 400 800`
- 触底判定（推荐：基于采集埋点增量，而非视觉判断）：
  - 思路：每滑动 5 次后，用 `result-bitable-reporter stat` 查询当前 `TaskID` 的总行数是否增加；连续多次无新增则判定触底。
  - 示例（bash 伪代码，关键点是“滑动 5 次 -> 查一次”）：
    ```bash
    TASK_ID="20260206001"
    NO_PROGRESS=0
    MAX_NO_PROGRESS=3

    LAST_COUNT="$(REPORT stat --task-id "$TASK_ID")"
    while true; do
      for i in {1..5}; do
        ADB -s SERIAL swipe 540 1800 540 400 --duration-ms 800
        sleep 0.2
      done

      CUR_COUNT="$(REPORT stat --task-id "$TASK_ID")"
      if [[ "$CUR_COUNT" == "$LAST_COUNT" ]]; then
        NO_PROGRESS=$((NO_PROGRESS+1))
      else
        NO_PROGRESS=0
        LAST_COUNT="$CUR_COUNT"
      fi

      if [[ "$NO_PROGRESS" -ge "$MAX_NO_PROGRESS" ]]; then
        break
      fi
    done
    ```
- 触底判定（fallback：用 ai-vision 判断是否触底）：
  - 推荐用 `assert`（二值判断更稳）：`VISION assert --screenshot "$SCREENSHOT" --prompt "<断言提示词>"`
  - 断言提示词示例（要求只输出 JSON，且不确定时必须判定为 false）：
    `VISION assert --screenshot "$SCREENSHOT" --prompt '判断“搜索结果列表是否已滑动到底”。满足任一即 pass=true：出现“没有更多/已到底/到底线”等文案；或底部出现明显空白且列表不再延伸/内容不再变化。若不确定必须 pass=false。仅输出JSON：{\"pass\":true|false,\"reason\":\"...\",\"evidence\":[\"...\"]}'`

## Feishu 任务拉取

### 综合页搜索
- 拉取任务：
  `export FEISHU_APP_ID=...`
  `export FEISHU_APP_SECRET=...`
  `export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID"`
  `TASK claim --app com.tencent.mm --scene 综合页搜索 --device-serial SERIAL --status pending,failed --date Today --log-level debug`
- 映射参数：
  `TaskID -> TASK_ID`
  `Params -> KEYWORDS`（逗号或换行拆分）
 - `claim` 成功后任务已进入 `running` 且绑定设备，无需再次 update

### 个人页搜索
- 拉取任务：
  `export FEISHU_APP_ID=...`
  `export FEISHU_APP_SECRET=...`
  `export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=TABLE_ID"`
  `TASK claim --app com.tencent.mm --scene 个人页搜索 --device-serial SERIAL --status pending,failed --date Today --log-level debug`
- 映射参数：
  `TaskID -> TASK_ID`
  `UserName -> ACCOUNT_NAME`
  `Params -> KEYWORDS`（逗号或换行拆分）
 - `claim` 成功后任务已进入 `running` 且绑定设备，无需再次 update

## Feishu 任务完成收尾
- 任务成功完成：
  `TASK update --task-id TASK_ID --status success --completed-at now`
- 任务失败/中断：
  `TASK update --task-id TASK_ID --status failed --completed-at now`

## 综合页任务后置（piracy-handler）
- 综合页任务成功后触发盗版编排（SQLite 驱动，三步）：
  `export FEISHU_APP_ID=...`
  `export FEISHU_APP_SECRET=...`
  `export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=tbl_task"`
  `export DRAMA_BITABLE_URL="https://.../base/APP_TOKEN?table=tbl_drama"`
  `export WEBHOOK_BITABLE_URL="https://.../base/APP_TOKEN?table=tbl_webhook"`
  `PIRACY_DETECT --task-id TASK_ID --db-path ~/.eval/records.sqlite --threshold 0.5`
  `PIRACY_CREATE_SUBTASKS --task-id TASK_ID`
  `PIRACY_UPSERT_WEBHOOK_PLANS --task-id TASK_ID`

## Group 任务后置（group-webhook-dispatch）
- Group 内任务完成后（个人页/合集/锚点等）触发 ready 检查与 webhook 推送：
  `export FEISHU_APP_ID=...`
  `export FEISHU_APP_SECRET=...`
  `export TASK_BITABLE_URL="https://.../base/APP_TOKEN?table=tbl_task"`
  `export WEBHOOK_BITABLE_URL="https://.../base/APP_TOKEN?table=tbl_webhook"`
  `export CRAWLER_SERVICE_BASE_URL="http://content-web-crawler:8000"`
  `WEBHOOK_DISPATCH --task-id TASK_ID --db-path ~/.eval/records.sqlite`
- 单次补偿（run-once reconcile）：
  `WEBHOOK_RECONCILE --date 2026-02-07 --limit 50 --db-path ~/.eval/records.sqlite`

## 弹窗处理
- 截图后让 ai-vision 识别关闭按钮（优先关闭或取消）：
  `VISION plan-next --screenshot "$SCREENSHOT" --prompt "识别并关闭当前弹窗（优先关闭或取消），若无弹窗则提示继续原流程"`
- 关闭弹窗：
  `ADB -s SERIAL tap X Y`
