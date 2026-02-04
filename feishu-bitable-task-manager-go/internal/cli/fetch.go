package cli

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"feishu-bitable-task-manager-go/internal/common"
)

type searchResp struct {
	common.FeishuResp
	Data struct {
		Items     []map[string]any `json:"items"`
		HasMore   bool             `json:"has_more"`
		PageToken string           `json:"page_token"`
	} `json:"data"`
}

type pageInfo struct {
	HasMore       bool   `json:"has_more"`
	NextPageToken string `json:"next_page_token"`
	Pages         int    `json:"pages"`
}

type fetchOutput struct {
	Tasks          []Task   `json:"tasks"`
	Count          int      `json:"count"`
	ElapsedSeconds float64  `json:"elapsed_seconds"`
	PageInfo       pageInfo `json:"page_info"`
}

type FetchOptions struct {
	TaskURL    string
	App        string
	Scene      string
	Status     string
	Date       string
	Limit      int
	PageSize   int
	MaxPages   int
	IgnoreView bool
	ViewID     string
	JSONL      bool
	Raw        bool
}

func buildFilter(fields map[string]string, app, scene, status, datePreset string) map[string]any {
	conds := []map[string]any{}
	add := func(fieldKey, value string) {
		name := strings.TrimSpace(fields[fieldKey])
		val := strings.TrimSpace(value)
		if name != "" && val != "" {
			conds = append(conds, map[string]any{"field_name": name, "operator": "is", "value": []string{val}})
		}
	}
	add("App", app)
	add("Scene", scene)
	add("Status", status)
	if datePreset != "" && datePreset != "Any" {
		add("Date", datePreset)
	}
	if len(conds) == 0 {
		return nil
	}
	return map[string]any{"conjunction": "and", "conditions": conds}
}

func decodeTask(fieldsRaw map[string]any, mapping map[string]string) (Task, bool) {
	if len(fieldsRaw) == 0 {
		return Task{}, false
	}
	taskID := common.FieldInt(fieldsRaw, mapping["TaskID"])
	if taskID == 0 {
		return Task{}, false
	}
	get := func(name string) string {
		return strings.TrimSpace(common.NormalizeBitableValue(fieldsRaw[mapping[name]]))
	}
	t := Task{
		TaskID:           taskID,
		BizTaskID:        get("BizTaskID"),
		ParentTaskID:     get("ParentTaskID"),
		App:              get("App"),
		Scene:            get("Scene"),
		Params:           get("Params"),
		ItemID:           get("ItemID"),
		BookID:           get("BookID"),
		URL:              get("URL"),
		UserID:           get("UserID"),
		UserName:         get("UserName"),
		Date:             get("Date"),
		Status:           get("Status"),
		Extra:            get("Extra"),
		Logs:             get("Logs"),
		LastScreenshot:   get("LastScreenShot"),
		GroupID:          get("GroupID"),
		DeviceSerial:     get("DeviceSerial"),
		DispatchedDevice: get("DispatchedDevice"),
		DispatchedAt:     get("DispatchedAt"),
		StartAt:          get("StartAt"),
		EndAt:            get("EndAt"),
		ElapsedSeconds:   get("ElapsedSeconds"),
		ItemsCollected:   get("ItemsCollected"),
		RetryCount:       get("RetryCount"),
	}
	if t.Params == "" && t.ItemID == "" && t.BookID == "" && t.URL == "" && t.UserID == "" && t.UserName == "" {
		return Task{}, false
	}
	return t, true
}

func FetchTasks(opts FetchOptions) int {
	taskURL := strings.TrimSpace(opts.TaskURL)
	if taskURL == "" {
		errLogger.Error("TASK_BITABLE_URL is required")
		return 2
	}
	appID := common.Env("FEISHU_APP_ID", "")
	appSecret := common.Env("FEISHU_APP_SECRET", "")
	if appID == "" || appSecret == "" {
		errLogger.Error("FEISHU_APP_ID/FEISHU_APP_SECRET are required")
		return 2
	}
	baseURL := common.Env("FEISHU_BASE_URL", common.DefaultBaseURL)

	ref, err := common.ParseBitableURL(taskURL)
	if err != nil {
		errLogger.Error("parse bitable URL failed", "err", err)
		return 2
	}
	fields := common.LoadTaskFieldsFromEnv()
	filterObj := buildFilter(fields, opts.App, opts.Scene, opts.Status, opts.Date)

	token, err := common.GetTenantAccessToken(baseURL, appID, appSecret)
	if err != nil {
		errLogger.Error("get tenant access token failed", "err", err)
		return 2
	}
	if ref.AppToken == "" {
		if ref.WikiToken == "" {
			errLogger.Error("bitable URL missing app_token and wiki_token")
			return 2
		}
		appToken, err := common.ResolveWikiAppToken(baseURL, token, ref.WikiToken)
		if err != nil {
			errLogger.Error("resolve wiki app token failed", "err", err)
			return 2
		}
		ref.AppToken = appToken
	}

	viewID := strings.TrimSpace(opts.ViewID)
	if viewID == "" {
		viewID = ref.ViewID
	}

	pageSize := common.ClampPageSize(opts.PageSize)
	if opts.Limit > 0 && opts.Limit < pageSize {
		pageSize = opts.Limit
	}

	items := []map[string]any{}
	pageToken := ""
	pages := 0

	start := time.Now()
	for {
		q := url.Values{}
		q.Set("page_size", fmt.Sprintf("%d", pageSize))
		if pageToken != "" {
			q.Set("page_token", pageToken)
		}
		urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/search?%s",
			strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID, q.Encode(),
		)
		var body map[string]any
		if (!opts.IgnoreView && viewID != "") || filterObj != nil {
			body = map[string]any{}
			if !opts.IgnoreView && viewID != "" {
				body["view_id"] = viewID
			}
			if filterObj != nil {
				body["filter"] = filterObj
			}
		}
		var resp searchResp
		if err := common.RequestJSON("POST", urlStr, token, body, &resp); err != nil {
			errLogger.Error("search records request failed", "err", err)
			return 2
		}
		if resp.Code != 0 {
			errLogger.Error("search records failed", "code", resp.Code, "msg", resp.Msg)
			return 2
		}
		items = append(items, resp.Data.Items...)
		pages++
		pageToken = strings.TrimSpace(resp.Data.PageToken)

		if opts.Limit > 0 && len(items) >= opts.Limit {
			items = items[:opts.Limit]
			break
		}
		if opts.MaxPages > 0 && pages >= opts.MaxPages {
			break
		}
		if !resp.Data.HasMore || pageToken == "" {
			break
		}
	}
	elapsed := time.Since(start).Seconds()

	tasks := []Task{}
	for _, it := range items {
		recordID, _ := it["record_id"].(string)
		fieldsRaw, _ := it["fields"].(map[string]any)
		t, ok := decodeTask(fieldsRaw, fields)
		if !ok {
			continue
		}
		t.RecordID = strings.TrimSpace(recordID)
		if opts.Raw {
			t.RawFields = fieldsRaw
		}
		tasks = append(tasks, t)
	}

	if opts.JSONL {
		for _, t := range tasks {
			logger.Info("task", "task", t)
		}
		return 0
	}
	out := fetchOutput{
		Tasks:          tasks,
		Count:          len(tasks),
		ElapsedSeconds: float64(int(elapsed*1000)) / 1000,
		PageInfo:       pageInfo{HasMore: pageToken != "", NextPageToken: pageToken, Pages: pages},
	}
	logger.Info("tasks", "data", out)
	return 0
}
