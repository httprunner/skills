---
name: piracy-handler
description: 综合页搜索完成后，从本地 sqlite capture_results 聚类/阈值筛选盗版分组；并通过 feishu-bitable-task-manager 创建子任务、通过 group-webhook-dispatch upsert webhook 计划。用于 wechat-search-collector 综合页搜索后置流程（detect.json 产出/读取、子任务创建、webhook 计划写入）。
---

# Piracy Handler

用于“综合页搜索完成后”的后置编排：盗版检测（只读）→ 子任务创建 → webhook 计划 upsert。

## Quick start

在 `piracy-handler/` 目录运行：

```bash
npx tsx scripts/piracy_detect.ts --task-id <TASK_ID>
npx tsx scripts/piracy_create_subtasks.ts --task-id <TASK_ID>
npx tsx scripts/piracy_upsert_webhook_plans.ts --task-id <TASK_ID>
```

更完整命令与调试示例见 `references/commands.md`。

## Workflow（高层）

1) 从本地 sqlite `capture_results` 读取综合页采集结果。
2) 通过 `TaskID` 从任务状态表补齐 `BookID`（`capture_results` 不含 BookID）——委托 `feishu-bitable-task-manager`。
3) 用 `GroupID={MapAppValue(任务App)}_{BookID}_{UserKey}` 聚类。
4) 从原始剧单按 BookID 查询总时长，换算后计算阈值并筛选命中分组。
5) 为命中分组创建子任务（个人页必建，合集/锚点按条件）——委托 `feishu-bitable-task-manager`。
6) 创建/更新 `BizType=piracy_general_search` 的 webhook 推送计划——委托 `group-webhook-dispatch`。

## I/O（默认路径约定）

- `piracy_detect.ts` 默认写入 `~/.eval/<TaskID>/detect.json`（避免多设备并行冲突）。
- 未指定 `--input` 时，后续命令可用 `--task-id <TaskID>` 从 `~/.eval/<TaskID>/detect.json` 读取。

## Required env

- `FEISHU_APP_ID`, `FEISHU_APP_SECRET`
- `TASK_BITABLE_URL`（任务状态表）
- `DRAMA_BITABLE_URL`（原始剧单/元信息表）
- `WEBHOOK_BITABLE_URL`（推送计划表）

## Dependencies

- 任务状态表读写：在 `feishu-bitable-task-manager/` 目录执行 `npx tsx scripts/bitable_task.ts ...`。
- 剧单/元信息查询：在 `feishu-bitable-task-manager/` 目录执行 `npx tsx scripts/bitable_lookup.ts fetch ...`。
- webhook 计划 upsert：在 `group-webhook-dispatch/` 目录执行 `npx tsx scripts/upsert_webhook_plan.ts ...`。

## Resources

- `scripts/piracy_detect.ts`: 阈值检测（输出 JSON）
- `scripts/piracy_create_subtasks.ts`: 子任务创建（写任务状态表）
- `scripts/piracy_upsert_webhook_plans.ts`: webhook 计划 upsert（写推送计划表）
- Read `references/commands.md` for field conventions and debug flags.
