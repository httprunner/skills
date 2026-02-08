---
name: piracy-handler
description: 综合页搜索后的盗版检测与任务编排（SQLite 驱动）。从本地 capture_results 聚类并按阈值筛选；子任务创建通过 feishu-bitable-task-manager（npx）写入任务状态表；webhook 计划创建/更新通过 group-webhook-dispatch（npx）写入推送计划表；适用于 wechat-search-collector 后置流程。
---

# Piracy Handler

用于“综合页搜索完成后”的后置编排：

1. 从本地 sqlite `capture_results` 读取采集结果。
2. 通过 TaskID 从任务状态表查询 BookID（因为 capture_results 无 BookID）——委托 `feishu-bitable-task-manager`。
3. 用 GroupID=`{MapAppValue(任务App)}_{BookID}_{UserKey}` 聚类（平台前缀跟随任务 App）。
4. 从原始剧单按 BookID 查询 `短剧总时长（分钟）`，换算秒后计算阈值。
5. 对命中阈值的分组创建子任务（个人页必建，合集/锚点按条件）——委托 `feishu-bitable-task-manager`。
6. 创建/更新 `BizType=piracy_general_search` 的 webhook 推送计划——委托 `group-webhook-dispatch`。

## 运行

1) 盗版检测（只读，输出 JSON）：

```bash
npx tsx scripts/piracy_detect.ts --task-id <TASK_ID>
```

2) 创建子任务（读取 detect 输出）：

```bash
npx tsx scripts/piracy_create_subtasks.ts --task-id <TASK_ID>
```

3) 创建/更新 webhook 计划（读取 detect 输出）：

```bash
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id <TASK_ID>
```

默认输出/输入目录：
- 不指定 `--output` 时，`piracy_detect.ts` 会写入 `~/.eval/<TaskID>/detect.json`，避免多设备并行时文件冲突。
- 不指定 `--input` 时，后续命令可用 `--task-id <TaskID>` 从 `~/.eval/<TaskID>/detect.json` 读取。

## 环境变量

- `FEISHU_APP_ID`, `FEISHU_APP_SECRET`
- `TASK_BITABLE_URL`（任务状态表）
- `DRAMA_BITABLE_URL`（原始剧单表）
- `WEBHOOK_BITABLE_URL`（推送计划表）

## 依赖说明

- 任务状态表读写：进入 `feishu-bitable-task-manager` 目录执行 `npx tsx scripts/bitable_task.ts ...`。
- 原始剧单/元信息表查询（`DRAMA_BITABLE_URL`）：进入 `feishu-bitable-task-manager` 目录执行 `npx tsx scripts/bitable_lookup.ts fetch ...`。
- webhook 计划表 upsert：进入 `group-webhook-dispatch` 目录执行 `npx tsx scripts/upsert_webhook_plan.ts ...`。

## 资源

- `scripts/piracy_detect.ts`: 阈值检测（输出 JSON）
- `scripts/piracy_create_subtasks.ts`: 子任务创建（写任务状态表）
- `scripts/piracy_upsert_webhook_plans.ts`: webhook 计划 upsert（写推送计划表）
- `references/commands.md`: 命令示例与字段约定
