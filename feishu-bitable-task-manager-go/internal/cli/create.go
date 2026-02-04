package cli

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"feishu-bitable-task-manager-go/internal/common"
)

const (
	createMaxBatchSize    = 500
	createMaxFilterValues = 50
)

var appGroupLabels = map[string]string{
	"com.smile.gifmaker": "快手",
}

type CreateOptions struct {
	TaskURL   string
	InputPath string

	BizTaskID    string
	ParentTaskID string
	App          string
	Scene        string
	Params       string
	ItemID       string
	BookID       string
	URL          string
	UserID       string
	UserName     string
	Date         string
	Status       string

	DeviceSerial     string
	DispatchedDevice string
	DispatchedAt     string
	StartAt          string
	CompletedAt      string
	EndAt            string
	ElapsedSeconds   string
	ItemsCollected   string
	Logs             string
	RetryCount       string
	LastScreenshot   string
	GroupID          string
	Extra            string

	SkipExisting string
}

type createReport struct {
	Created        int      `json:"created"`
	Requested      int      `json:"requested"`
	Skipped        int      `json:"skipped"`
	Failed         int      `json:"failed"`
	Errors         []string `json:"errors"`
	ElapsedSeconds float64  `json:"elapsed_seconds"`
}

func CreateTasks(opts CreateOptions) int {
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
	fieldsMap := common.LoadTaskFieldsFromEnv()

	creates, err := loadCreates(opts, fieldsMap)
	if err != nil {
		errLogger.Error("load creates failed", "err", err)
		return 2
	}
	if len(creates) == 0 {
		errLogger.Error("no tasks provided")
		return 2
	}

	ref, err := common.ParseBitableURL(taskURL)
	if err != nil {
		errLogger.Error("parse bitable URL failed", "err", err)
		return 2
	}
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
		appTok, err := common.ResolveWikiAppToken(baseURL, token, ref.WikiToken)
		if err != nil {
			errLogger.Error("resolve wiki app token failed", "err", err)
			return 2
		}
		ref.AppToken = appTok
	}

	skipFields := normalizeSkipFields(opts.SkipExisting)
	existingByField := map[string]map[string]string{}
	existingRecordIDs := map[string]bool{}

	if len(skipFields) > 0 {
		fieldMap := map[string]string{}
		for _, f := range skipFields {
			if f == "RecordID" {
				continue
			}
			mapped := fieldsMap[f]
			if mapped == "" {
				mapped = f
			}
			fieldMap[f] = mapped
		}

		// collect all unique values to check
		for _, item := range creates {
			for _, f := range skipFields {
				if f == "RecordID" {
					rid := strings.TrimSpace(common.BitableValueToString(item["record_id"]))
					if rid != "" && !existingRecordIDs[rid] {
						if recordExists(baseURL, token, ref, rid) {
							existingRecordIDs[rid] = true
						}
					}
					continue
				}
				val := extractItemValue(item, f)
				if val == "" {
					continue
				}
				if _, ok := existingByField[f]; !ok {
					existingByField[f] = map[string]string{}
				}
				existingByField[f][val] = ""
			}
		}

		for f, valuesMap := range existingByField {
			values := make([]string, 0, len(valuesMap))
			for v := range valuesMap {
				values = append(values, v)
			}
			mappedField := fieldMap[f]
			resolved, err := resolveExistingByField(baseURL, token, ref, mappedField, values)
			if err != nil {
				errLogger.Error("resolve existing records failed", "err", err)
				return 2
			}
			existingByField[f] = resolved
		}
	}

	type createRec struct {
		Fields map[string]any
	}

	records := []createRec{}
	errorsList := []string{}
	skipped := 0

	for _, item := range creates {
		if len(skipFields) > 0 {
			allMatch := true
			for _, f := range skipFields {
				if f == "RecordID" {
					rid := strings.TrimSpace(common.BitableValueToString(item["record_id"]))
					if rid == "" || !existingRecordIDs[rid] {
						allMatch = false
						break
					}
					continue
				}
				val := extractItemValue(item, f)
				if val == "" {
					allMatch = false
					break
				}
				if _, ok := existingByField[f][val]; !ok {
					allMatch = false
					break
				}
			}
			if allMatch {
				skipped++
				continue
			}
		}

		fields := buildCreateFields(fieldsMap, item)
		if len(fields) == 0 {
			errorsList = append(errorsList, "task: no fields to create")
			continue
		}
		records = append(records, createRec{Fields: fields})
	}

	start := time.Now()
	created := 0
	if len(records) > 0 {
		if len(records) == 1 {
			if err := createRecord(baseURL, token, ref, records[0].Fields); err != nil {
				errorsList = append(errorsList, err.Error())
			} else {
				created = 1
			}
		} else {
			for i := 0; i < len(records); i += createMaxBatchSize {
				j := i + createMaxBatchSize
				if j > len(records) {
					j = len(records)
				}
				batch := make([]map[string]any, 0, j-i)
				for _, r := range records[i:j] {
					batch = append(batch, map[string]any{"fields": r.Fields})
				}
				if err := batchCreateRecords(baseURL, token, ref, batch); err != nil {
					errorsList = append(errorsList, err.Error())
					break
				}
				created += (j - i)
			}
		}
	}

	elapsed := time.Since(start).Seconds()
	report := createReport{
		Created:        created,
		Requested:      len(records),
		Skipped:        skipped,
		Failed:         len(errorsList),
		Errors:         errorsList,
		ElapsedSeconds: float64(int(elapsed*1000)) / 1000,
	}
	printJSON(report)
	if len(errorsList) > 0 {
		return 1
	}
	return 0
}

func loadCreates(opts CreateOptions, fieldsMap map[string]string) ([]map[string]any, error) {
	var items []map[string]any
	if strings.TrimSpace(opts.InputPath) != "" {
		raw, err := readAllInput(opts.InputPath)
		if err != nil {
			return nil, err
		}
		mode := detectInputFormat(opts.InputPath, raw)
		if mode == "jsonl" {
			items, err = parseJSONLItems(raw)
		} else {
			items, err = parseJSONItems(raw)
		}
		if err != nil {
			return nil, err
		}
	} else {
		items = []map[string]any{
			{
				"biz_task_id":       opts.BizTaskID,
				"parent_task_id":    opts.ParentTaskID,
				"app":               opts.App,
				"scene":             opts.Scene,
				"params":            opts.Params,
				"item_id":           opts.ItemID,
				"book_id":           opts.BookID,
				"url":               opts.URL,
				"user_id":           opts.UserID,
				"user_name":         opts.UserName,
				"date":              opts.Date,
				"status":            opts.Status,
				"device_serial":     opts.DeviceSerial,
				"dispatched_device": opts.DispatchedDevice,
				"dispatched_at":     opts.DispatchedAt,
				"start_at":          opts.StartAt,
				"completed_at":      opts.CompletedAt,
				"end_at":            opts.EndAt,
				"elapsed_seconds":   opts.ElapsedSeconds,
				"items_collected":   opts.ItemsCollected,
				"logs":              opts.Logs,
				"retry_count":       opts.RetryCount,
				"last_screenshot":   opts.LastScreenshot,
				"group_id":          opts.GroupID,
				"extra":             opts.Extra,
				"record_id":         "",
			},
		}
	}

	knownKeys := map[string]bool{
		"task_id":           true,
		"taskID":            true,
		"TaskID":            true,
		"biz_task_id":       true,
		"bizTaskId":         true,
		"BizTaskID":         true,
		"record_id":         true,
		"recordId":          true,
		"RecordID":          true,
		"parent_task_id":    true,
		"parentTaskId":      true,
		"ParentTaskID":      true,
		"app":               true,
		"App":               true,
		"scene":             true,
		"Scene":             true,
		"params":            true,
		"Params":            true,
		"item_id":           true,
		"itemId":            true,
		"ItemID":            true,
		"book_id":           true,
		"bookId":            true,
		"BookID":            true,
		"url":               true,
		"URL":               true,
		"user_id":           true,
		"userId":            true,
		"UserID":            true,
		"user_name":         true,
		"userName":          true,
		"UserName":          true,
		"date":              true,
		"Date":              true,
		"status":            true,
		"Status":            true,
		"device_serial":     true,
		"DeviceSerial":      true,
		"dispatched_device": true,
		"DispatchedDevice":  true,
		"dispatched_at":     true,
		"DispatchedAt":      true,
		"start_at":          true,
		"StartAt":           true,
		"completed_at":      true,
		"end_at":            true,
		"EndAt":             true,
		"elapsed_seconds":   true,
		"ElapsedSeconds":    true,
		"items_collected":   true,
		"ItemsCollected":    true,
		"logs":              true,
		"Logs":              true,
		"retry_count":       true,
		"RetryCount":        true,
		"last_screenshot":   true,
		"LastScreenShot":    true,
		"group_id":          true,
		"GroupID":           true,
		"extra":             true,
		"Extra":             true,
		"fields":            true,
		"CDNURL":            true,
		"cdn_url":           true,
		"cdnUrl":            true,
		"cdnurl":            true,
	}

	allowedFieldNames := map[string]bool{}
	for _, v := range fieldsMap {
		if strings.TrimSpace(v) != "" {
			allowedFieldNames[v] = true
		}
	}

	pick := func(item map[string]any, key string, fallback any) any {
		if v, ok := item[key]; ok && v != nil {
			return v
		}
		return fallback
	}

	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}

		cdnURL := ""
		for _, k := range []string{"CDNURL", "cdn_url", "cdnUrl", "cdnurl"} {
			if s, ok := item[k].(string); ok && strings.TrimSpace(s) != "" {
				cdnURL = strings.TrimSpace(s)
				break
			}
		}

		extra := pick(item, "extra", opts.Extra)
		forceExtra := false
		if cdnURL != "" {
			extra = map[string]any{"cdn_url": cdnURL}
			forceExtra = true
		}

		extraFields := map[string]any{}
		for k, v := range item {
			if knownKeys[k] {
				continue
			}
			if allowedFieldNames[k] && v != nil {
				extraFields[k] = v
			}
		}
		if rawFields, ok := item["fields"].(map[string]any); ok {
			for k, v := range rawFields {
				if strings.TrimSpace(k) != "" && v != nil {
					extraFields[k] = v
				}
			}
		}

		merged := map[string]any{
			"task_id":           firstNonNil(item["task_id"], item["taskID"], item["TaskID"]),
			"biz_task_id":       firstNonNil(item["biz_task_id"], item["bizTaskId"], item["BizTaskID"]),
			"record_id":         firstNonNil(item["record_id"], item["recordId"], item["RecordID"]),
			"parent_task_id":    firstNonNil(item["parent_task_id"], item["parentTaskId"], item["ParentTaskID"]),
			"app":               firstNonNil(pick(item, "app", opts.App), item["App"]),
			"scene":             firstNonNil(pick(item, "scene", opts.Scene), item["Scene"]),
			"params":            firstNonNil(pick(item, "params", opts.Params), item["Params"]),
			"item_id":           firstNonNil(pick(item, "item_id", opts.ItemID), item["ItemID"]),
			"book_id":           firstNonNil(pick(item, "book_id", opts.BookID), item["BookID"]),
			"url":               firstNonNil(pick(item, "url", opts.URL), item["URL"]),
			"user_id":           firstNonNil(pick(item, "user_id", opts.UserID), item["UserID"]),
			"user_name":         firstNonNil(pick(item, "user_name", opts.UserName), item["UserName"]),
			"date":              firstNonNil(pick(item, "date", opts.Date), item["Date"]),
			"status":            firstNonNil(pick(item, "status", opts.Status), item["Status"]),
			"device_serial":     firstNonNil(pick(item, "device_serial", opts.DeviceSerial), item["DeviceSerial"]),
			"dispatched_device": firstNonNil(pick(item, "dispatched_device", opts.DispatchedDevice), item["DispatchedDevice"]),
			"dispatched_at":     firstNonNil(pick(item, "dispatched_at", opts.DispatchedAt), item["DispatchedAt"]),
			"start_at":          firstNonNil(pick(item, "start_at", opts.StartAt), item["StartAt"]),
			"completed_at":      pick(item, "completed_at", opts.CompletedAt),
			"end_at":            firstNonNil(pick(item, "end_at", opts.EndAt), item["EndAt"]),
			"elapsed_seconds":   firstNonNil(pick(item, "elapsed_seconds", opts.ElapsedSeconds), item["ElapsedSeconds"]),
			"items_collected":   firstNonNil(pick(item, "items_collected", opts.ItemsCollected), item["ItemsCollected"]),
			"logs":              firstNonNil(pick(item, "logs", opts.Logs), item["Logs"]),
			"retry_count":       firstNonNil(pick(item, "retry_count", opts.RetryCount), item["RetryCount"]),
			"last_screenshot":   firstNonNil(pick(item, "last_screenshot", opts.LastScreenshot), item["LastScreenShot"]),
			"group_id":          firstNonNil(pick(item, "group_id", opts.GroupID), item["GroupID"]),
			"extra":             extra,
			"force_extra":       forceExtra,
			"fields":            extraFields,
		}
		out = append(out, merged)
	}
	return out, nil
}

func buildCreateFields(fieldsMap map[string]string, item map[string]any) map[string]any {
	out := map[string]any{}

	setStr := func(jsonKey, colKey string) {
		v := strings.TrimSpace(common.BitableValueToString(item[jsonKey]))
		if v == "" {
			return
		}
		col := strings.TrimSpace(fieldsMap[colKey])
		if col == "" {
			return
		}
		out[col] = v
	}

	setStr("biz_task_id", "BizTaskID")
	setStr("parent_task_id", "ParentTaskID")

	appValue := strings.TrimSpace(common.BitableValueToString(item["app"]))
	sceneValue := strings.TrimSpace(common.BitableValueToString(item["scene"]))
	paramsValue := strings.TrimSpace(common.BitableValueToString(item["params"]))
	itemIDValue := strings.TrimSpace(common.BitableValueToString(item["item_id"]))
	bookIDValue := strings.TrimSpace(common.BitableValueToString(item["book_id"]))
	urlValue := strings.TrimSpace(common.BitableValueToString(item["url"]))
	userIDValue := strings.TrimSpace(common.BitableValueToString(item["user_id"]))
	userNameValue := strings.TrimSpace(common.BitableValueToString(item["user_name"]))
	statusValue := strings.TrimSpace(common.BitableValueToString(item["status"]))
	logsValue := strings.TrimSpace(common.BitableValueToString(item["logs"]))
	lastScreenshotValue := strings.TrimSpace(common.BitableValueToString(item["last_screenshot"]))
	groupIDValue := strings.TrimSpace(common.BitableValueToString(item["group_id"]))

	for _, kv := range []struct {
		field string
		value string
	}{
		{"App", appValue},
		{"Scene", sceneValue},
		{"Params", paramsValue},
		{"ItemID", itemIDValue},
		{"BookID", bookIDValue},
		{"URL", urlValue},
		{"UserID", userIDValue},
		{"UserName", userNameValue},
		{"Status", statusValue},
		{"Logs", logsValue},
		{"LastScreenShot", lastScreenshotValue},
		{"GroupID", groupIDValue},
	} {
		if kv.value == "" {
			continue
		}
		if col := strings.TrimSpace(fieldsMap[kv.field]); col != "" {
			out[col] = kv.value
		}
	}

	if groupIDValue == "" && appValue != "" && bookIDValue != "" && userIDValue != "" && strings.TrimSpace(fieldsMap["GroupID"]) != "" {
		label := appGroupLabels[appValue]
		if label == "" {
			label = appValue
		}
		out[fieldsMap["GroupID"]] = fmt.Sprintf("%s_%s_%s", label, bookIDValue, userIDValue)
	}

	if fieldsMap["Date"] != "" {
		if v, ok := item["date"]; ok && v != nil {
			if payload, ok := common.CoerceDatePayload(v); ok {
				out[fieldsMap["Date"]] = payload
			}
		}
	}

	deviceSerial := strings.TrimSpace(common.BitableValueToString(item["device_serial"]))
	if deviceSerial != "" && fieldsMap["DeviceSerial"] != "" {
		out[fieldsMap["DeviceSerial"]] = deviceSerial
	}

	dispatchedDevice := strings.TrimSpace(common.BitableValueToString(item["dispatched_device"]))
	if dispatchedDevice == "" {
		dispatchedDevice = deviceSerial
	}
	if dispatchedDevice != "" && fieldsMap["DispatchedDevice"] != "" {
		out[fieldsMap["DispatchedDevice"]] = dispatchedDevice
	}

	var dispatchedMS *int64
	if v, ok := item["dispatched_at"]; ok && v != nil && fieldsMap["DispatchedAt"] != "" {
		if ms, ok := common.CoerceMillis(v); ok {
			dispatchedMS = &ms
			out[fieldsMap["DispatchedAt"]] = ms
		}
	}

	var startMS *int64
	if v, ok := item["start_at"]; ok && v != nil && fieldsMap["StartAt"] != "" {
		if ms, ok := common.CoerceMillis(v); ok {
			startMS = &ms
			out[fieldsMap["StartAt"]] = ms
		}
	}
	if startMS == nil && dispatchedMS != nil && fieldsMap["StartAt"] != "" {
		out[fieldsMap["StartAt"]] = *dispatchedMS
		startMS = dispatchedMS
	}

	var endMS *int64
	if v, ok := item["completed_at"]; ok && v != nil {
		if ms, ok := common.CoerceMillis(v); ok {
			endMS = &ms
		}
	}
	if endMS == nil {
		if v, ok := item["end_at"]; ok && v != nil {
			if ms, ok := common.CoerceMillis(v); ok {
				endMS = &ms
			}
		}
	}
	if endMS != nil && fieldsMap["EndAt"] != "" {
		out[fieldsMap["EndAt"]] = *endMS
	}

	elapsed, hasElapsed := common.CoerceInt(item["elapsed_seconds"])
	if !hasElapsed && startMS != nil && endMS != nil {
		derived := int((*endMS - *startMS) / 1000)
		if derived < 0 {
			derived = 0
		}
		elapsed, hasElapsed = derived, true
	}
	if hasElapsed && fieldsMap["ElapsedSeconds"] != "" {
		out[fieldsMap["ElapsedSeconds"]] = elapsed
	}

	if itemsCollected, ok := common.CoerceInt(item["items_collected"]); ok && fieldsMap["ItemsCollected"] != "" {
		out[fieldsMap["ItemsCollected"]] = itemsCollected
	}

	if retryCount, ok := common.CoerceInt(item["retry_count"]); ok && fieldsMap["RetryCount"] != "" {
		out[fieldsMap["RetryCount"]] = retryCount
	}

	extra := item["extra"]
	forceExtra, _ := item["force_extra"].(bool)
	if fieldsMap["Extra"] != "" && extra != nil {
		extraPayload := common.NormalizeExtra(extra)
		if strings.TrimSpace(extraPayload) != "" || forceExtra {
			out[fieldsMap["Extra"]] = extraPayload
		}
	}

	if extraFields, ok := item["fields"].(map[string]any); ok {
		for k, v := range extraFields {
			if strings.TrimSpace(k) == "" || v == nil {
				continue
			}
			out[k] = v
		}
	}

	return out
}

func batchCreateRecords(baseURL, token string, ref common.BitableRef, records []map[string]any) error {
	urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_create",
		strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID,
	)
	payload := map[string]any{"records": records}
	var resp common.FeishuResp
	if err := common.RequestJSON("POST", urlStr, token, payload, &resp); err != nil {
		return err
	}
	if resp.Code != 0 {
		return fmt.Errorf("batch create failed: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func createRecord(baseURL, token string, ref common.BitableRef, fields map[string]any) error {
	urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records",
		strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID,
	)
	payload := map[string]any{"fields": fields}
	var resp common.FeishuResp
	if err := common.RequestJSON("POST", urlStr, token, payload, &resp); err != nil {
		return err
	}
	if resp.Code != 0 {
		return fmt.Errorf("create record failed: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func resolveExistingByField(baseURL, token string, ref common.BitableRef, fieldName string, values []string) (map[string]string, error) {
	out := map[string]string{}
	if len(values) == 0 {
		return out, nil
	}
	for _, batch := range chunkStrings(values, createMaxFilterValues) {
		filterObj := buildIDFilter(fieldName, batch)
		if filterObj == nil {
			continue
		}
		items, err := fetchRecordsForCreate(baseURL, token, ref, filterObj, minInt(common.MaxPageSize, maxInt(len(batch), 1)))
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			recordID := strings.TrimSpace(common.BitableValueToString(item["record_id"]))
			fieldsRaw, _ := item["fields"].(map[string]any)
			val := strings.TrimSpace(common.BitableValueToString(fieldsRaw[fieldName]))
			if recordID != "" && val != "" {
				if _, ok := out[val]; !ok {
					out[val] = recordID
				}
			}
		}
	}
	return out, nil
}

func fetchRecordsForCreate(baseURL, token string, ref common.BitableRef, filterObj map[string]any, pageSize int) ([]map[string]any, error) {
	pageSize = common.ClampPageSize(pageSize)
	urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/search?page_size=%d",
		strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID, pageSize,
	)
	var body any
	if filterObj != nil {
		body = map[string]any{"filter": filterObj}
	}
	var resp searchItemsResp
	if err := common.RequestJSON("POST", urlStr, token, body, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("search records failed: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return resp.Data.Items, nil
}

func recordExists(baseURL, token string, ref common.BitableRef, recordID string) bool {
	recordID = strings.TrimSpace(recordID)
	if recordID == "" {
		return false
	}
	urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/%s",
		strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID, url.PathEscape(recordID),
	)
	var resp common.FeishuResp
	if err := common.RequestJSON("GET", urlStr, token, nil, &resp); err != nil {
		return false
	}
	return resp.Code == 0
}

func normalizeSkipFields(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := []string{}
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			parts = append(parts, p)
		}
	}
	aliases := map[string]string{
		"task_id":     "TaskID",
		"taskid":      "TaskID",
		"biz_task_id": "BizTaskID",
		"biztaskid":   "BizTaskID",
		"record_id":   "RecordID",
		"recordid":    "RecordID",
		"book_id":     "BookID",
		"bookid":      "BookID",
		"user_id":     "UserID",
		"userid":      "UserID",
		"app":         "App",
		"scene":       "Scene",
	}
	seen := map[string]bool{}
	out := []string{}
	for _, p := range parts {
		key := p
		if mapped := aliases[strings.ToLower(p)]; mapped != "" {
			key = mapped
		}
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, key)
	}
	return out
}

func extractItemValue(item map[string]any, fieldName string) string {
	switch fieldName {
	case "TaskID":
		if id, ok := common.CoerceInt(item["task_id"]); ok && id > 0 {
			return fmt.Sprintf("%d", id)
		}
		return ""
	case "BizTaskID":
		return strings.TrimSpace(common.BitableValueToString(item["biz_task_id"]))
	case "RecordID":
		return strings.TrimSpace(common.BitableValueToString(item["record_id"]))
	case "BookID":
		return strings.TrimSpace(common.BitableValueToString(item["book_id"]))
	case "UserID":
		return strings.TrimSpace(common.BitableValueToString(item["user_id"]))
	case "App":
		return strings.TrimSpace(common.BitableValueToString(item["app"]))
	case "Scene":
		return strings.TrimSpace(common.BitableValueToString(item["scene"]))
	default:
		return strings.TrimSpace(common.BitableValueToString(item[fieldName]))
	}
}
