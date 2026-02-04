package cli

import (
	"flag"
	"os"
	"strings"
)

func Run(args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		printRootUsage()
		return 0
	}

	switch args[0] {
	case "fetch":
		return runFetch(args[1:])
	case "update":
		return runUpdate(args[1:])
	case "create":
		return runCreate(args[1:])
	default:
		errLogger.Error("unknown command", "command", args[0])
		printRootUsage()
		return 2
	}
}

func printRootUsage() {
	logUsage("Usage:")
	logUsage("  bitable-task fetch [flags]")
	logUsage("  bitable-task update [flags]")
	logUsage("  bitable-task create [flags]")
	logUsage("")
	logUsage("Environment:")
	logUsage("  FEISHU_APP_ID, FEISHU_APP_SECRET, TASK_BITABLE_URL (required)")
	logUsage("  FEISHU_BASE_URL (optional, default: https://open.feishu.cn)")
	logUsage("  TASK_FIELD_* overrides (optional)")
}

func runFetch(args []string) int {
	opts := FetchOptions{
		TaskURL:    os.Getenv("TASK_BITABLE_URL"),
		Status:     "pending",
		Date:       "Today",
		PageSize:   200,
		IgnoreView: true,
	}
	var useView bool
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&opts.TaskURL, "task-url", opts.TaskURL, "Bitable task table URL")
	fs.StringVar(&opts.App, "app", "", "App value for filter (required)")
	fs.StringVar(&opts.Scene, "scene", "", "Scene value for filter (required)")
	fs.StringVar(&opts.Status, "status", opts.Status, "Task status filter (default: pending)")
	fs.StringVar(&opts.Date, "date", opts.Date, "Date preset: Today/Yesterday/Any")
	fs.IntVar(&opts.Limit, "limit", 0, "Max tasks to return (0 = no cap)")
	fs.IntVar(&opts.PageSize, "page-size", opts.PageSize, "Page size (max 500)")
	fs.IntVar(&opts.MaxPages, "max-pages", 0, "Max pages to fetch (0 = no cap)")
	fs.BoolVar(&opts.IgnoreView, "ignore-view", true, "Ignore view_id when searching (default: true)")
	fs.BoolVar(&useView, "use-view", false, "Use view_id from URL")
	fs.StringVar(&opts.ViewID, "view-id", "", "Override view_id when searching")
	fs.BoolVar(&opts.JSONL, "jsonl", false, "Output JSONL (one task per line)")
	fs.BoolVar(&opts.Raw, "raw", false, "Include raw fields in output")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if useView {
		opts.IgnoreView = false
	}
	opts.App = strings.TrimSpace(opts.App)
	opts.Scene = strings.TrimSpace(opts.Scene)
	if opts.App == "" || opts.Scene == "" {
		errLogger.Error("--app and --scene are required")
		return 2
	}
	return FetchTasks(opts)
}

func runUpdate(args []string) int {
	opts := UpdateOptions{
		TaskURL:    os.Getenv("TASK_BITABLE_URL"),
		IgnoreView: true,
	}
	var useView bool
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&opts.TaskURL, "task-url", opts.TaskURL, "Bitable task table URL")
	fs.StringVar(&opts.InputPath, "input", "", "Input JSON or JSONL file (use - for stdin)")
	fs.IntVar(&opts.TaskID, "task-id", 0, "Single task id to update")
	fs.StringVar(&opts.BizTaskID, "biz-task-id", "", "Single biz task id to update")
	fs.StringVar(&opts.RecordID, "record-id", "", "Single record id to update")
	fs.StringVar(&opts.Status, "status", "", "Status to set")
	fs.StringVar(&opts.Date, "date", "", "Date to set (string or epoch/ISO)")
	fs.StringVar(&opts.DeviceSerial, "device-serial", "", "Dispatched device serial")
	fs.StringVar(&opts.DispatchedAt, "dispatched-at", "", "Dispatch time (ms/seconds/ISO/now)")
	fs.StringVar(&opts.StartAt, "start-at", "", "Start time (ms/seconds/ISO)")
	fs.StringVar(&opts.CompletedAt, "completed-at", "", "Completion time (ms/seconds/ISO)")
	fs.StringVar(&opts.EndAt, "end-at", "", "End time (ms/seconds/ISO)")
	fs.StringVar(&opts.ElapsedSeconds, "elapsed-seconds", "", "Elapsed seconds (int)")
	fs.StringVar(&opts.ItemsCollected, "items-collected", "", "Items collected (int)")
	fs.StringVar(&opts.Logs, "logs", "", "Logs path or identifier")
	fs.StringVar(&opts.RetryCount, "retry-count", "", "Retry count (int)")
	fs.StringVar(&opts.Extra, "extra", "", "Extra JSON string")
	fs.StringVar(&opts.SkipStatus, "skip-status", "", "Skip updates when current status matches (comma-separated)")
	fs.BoolVar(&opts.IgnoreView, "ignore-view", true, "Ignore view_id when searching (default: true)")
	fs.BoolVar(&useView, "use-view", false, "Use view_id from URL")
	fs.StringVar(&opts.ViewID, "view-id", "", "Override view_id when searching")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if useView {
		opts.IgnoreView = false
	}
	return UpdateTasks(opts)
}

func runCreate(args []string) int {
	opts := CreateOptions{
		TaskURL: os.Getenv("TASK_BITABLE_URL"),
	}
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&opts.TaskURL, "task-url", opts.TaskURL, "Bitable task table URL")
	fs.StringVar(&opts.InputPath, "input", "", "Input JSON or JSONL file (use - for stdin)")
	fs.StringVar(&opts.BizTaskID, "biz-task-id", "", "Biz task id to create")
	fs.StringVar(&opts.ParentTaskID, "parent-task-id", "", "Parent task id")
	fs.StringVar(&opts.App, "app", "", "App value")
	fs.StringVar(&opts.Scene, "scene", "", "Scene value")
	fs.StringVar(&opts.Params, "params", "", "Task params")
	fs.StringVar(&opts.ItemID, "item-id", "", "Item id")
	fs.StringVar(&opts.BookID, "book-id", "", "Book id")
	fs.StringVar(&opts.URL, "url", "", "URL")
	fs.StringVar(&opts.UserID, "user-id", "", "User id")
	fs.StringVar(&opts.UserName, "user-name", "", "User name")
	fs.StringVar(&opts.Date, "date", "", "Date value (string or epoch/ISO)")
	fs.StringVar(&opts.Status, "status", "", "Status")
	fs.StringVar(&opts.DeviceSerial, "device-serial", "", "Dispatched device serial")
	fs.StringVar(&opts.DispatchedDevice, "dispatched-device", "", "Dispatched device (override device-serial)")
	fs.StringVar(&opts.DispatchedAt, "dispatched-at", "", "Dispatch time (ms/seconds/ISO/now)")
	fs.StringVar(&opts.StartAt, "start-at", "", "Start time (ms/seconds/ISO)")
	fs.StringVar(&opts.CompletedAt, "completed-at", "", "Completion time (ms/seconds/ISO)")
	fs.StringVar(&opts.EndAt, "end-at", "", "End time (ms/seconds/ISO)")
	fs.StringVar(&opts.ElapsedSeconds, "elapsed-seconds", "", "Elapsed seconds (int)")
	fs.StringVar(&opts.ItemsCollected, "items-collected", "", "Items collected (int)")
	fs.StringVar(&opts.Logs, "logs", "", "Logs path or identifier")
	fs.StringVar(&opts.RetryCount, "retry-count", "", "Retry count (int)")
	fs.StringVar(&opts.LastScreenshot, "last-screenshot", "", "Last screenshot reference")
	fs.StringVar(&opts.GroupID, "group-id", "", "Group id")
	fs.StringVar(&opts.Extra, "extra", "", "Extra JSON string")
	fs.StringVar(&opts.SkipExisting, "skip-existing", "", "Skip create when existing records match these fields (comma-separated, all must match)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	return CreateTasks(opts)
}
