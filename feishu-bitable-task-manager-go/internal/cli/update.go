package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"feishu-bitable-task-manager-go/internal/common"
)

const (
	updateMaxBatchSize    = 500
	updateMaxFilterValues = 50
)

type UpdateOptions struct {
	TaskURL string

	InputPath string
	TaskID    int
	BizTaskID string
	RecordID  string

	Status         string
	Date           string
	DeviceSerial   string
	DispatchedAt   string
	StartAt        string
	CompletedAt    string
	EndAt          string
	ElapsedSeconds string
	ItemsCollected string
	Logs           string
	RetryCount     string
	Extra          string
	SkipStatus     string

	IgnoreView bool
	ViewID     string
}

type updateReport struct {
	Updated        int      `json:"updated"`
	Requested      int      `json:"requested"`
	Skipped        int      `json:"skipped"`
	Failed         int      `json:"failed"`
	Errors         []string `json:"errors"`
	ElapsedSeconds float64  `json:"elapsed_seconds"`
}

type searchItemsResp struct {
	common.FeishuResp
	Data struct {
		Items []map[string]any `json:"items"`
	} `json:"data"`
}

type getRecordResp struct {
	common.FeishuResp
	Data struct {
		Record struct {
			Fields map[string]any `json:"fields"`
		} `json:"record"`
	} `json:"data"`
}

func UpdateTasks(opts UpdateOptions) int {
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

	updates, err := loadUpdates(opts, fieldsMap)
	if err != nil {
		errLogger.Error("load updates failed", "err", err)
		return 2
	}
	if len(updates) == 0 {
		errLogger.Error("no updates provided")
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

	viewID := strings.TrimSpace(opts.ViewID)
	if viewID == "" {
		viewID = ref.ViewID
	}

	taskIDsToResolve := []int{}
	bizIDsToResolve := []string{}
	for _, upd := range updates {
		recordID := strings.TrimSpace(common.BitableValueToString(upd["record_id"]))
		taskID, _ := common.CoerceInt(upd["task_id"])
		bizID := strings.TrimSpace(common.BitableValueToString(upd["biz_task_id"]))
		if recordID == "" && taskID > 0 {
			taskIDsToResolve = append(taskIDsToResolve, taskID)
		}
		if recordID == "" && taskID == 0 && bizID != "" {
			bizIDsToResolve = append(bizIDsToResolve, bizID)
		}
	}

	resolvedTask := map[int]string{}
	resolvedBiz := map[string]string{}
	statusByRecord := map[string]string{}

	if len(taskIDsToResolve) > 0 {
		m, st, err := resolveRecordIDsByTaskID(baseURL, token, ref, fieldsMap, taskIDsToResolve, opts.IgnoreView, viewID)
		if err != nil {
			errLogger.Error("resolve record IDs by task id failed", "err", err)
			return 2
		}
		resolvedTask = m
		for k, v := range st {
			statusByRecord[k] = v
		}
	}
	if len(bizIDsToResolve) > 0 {
		m, st, err := resolveRecordIDsByBizTaskID(baseURL, token, ref, fieldsMap, bizIDsToResolve, opts.IgnoreView, viewID)
		if err != nil {
			errLogger.Error("resolve record IDs by biz task id failed", "err", err)
			return 2
		}
		resolvedBiz = m
		for k, v := range st {
			statusByRecord[k] = v
		}
	}

	skipStatuses := parseCSVSet(opts.SkipStatus)
	if len(skipStatuses) > 0 {
		recordIDsNeeded := []string{}
		for _, upd := range updates {
			recordID := resolveUpdateRecordID(upd, resolvedTask, resolvedBiz)
			if recordID != "" {
				if _, ok := statusByRecord[recordID]; !ok {
					recordIDsNeeded = append(recordIDsNeeded, recordID)
				}
			}
		}
		if len(recordIDsNeeded) > 0 {
			fetched, err := fetchRecordStatuses(baseURL, token, ref, recordIDsNeeded, fieldsMap["Status"])
			if err != nil {
				errLogger.Error("fetch record statuses failed", "err", err)
				return 2
			}
			for k, v := range fetched {
				statusByRecord[k] = v
			}
		}
	}

	type recordUpdate struct {
		RecordID string
		Fields   map[string]any
	}

	records := []recordUpdate{}
	errorsList := []string{}
	skipped := 0

	for _, upd := range updates {
		recordID := resolveUpdateRecordID(upd, resolvedTask, resolvedBiz)
		if recordID == "" {
			errorsList = append(errorsList, "missing record_id for update")
			continue
		}

		if len(skipStatuses) > 0 {
			cur := strings.ToLower(strings.TrimSpace(statusByRecord[recordID]))
			if cur != "" && skipStatuses[cur] {
				skipped++
				continue
			}
		}

		fields := buildUpdateFields(fieldsMap, upd)
		if len(fields) == 0 {
			errorsList = append(errorsList, fmt.Sprintf("record %s: no fields to update", recordID))
			continue
		}
		records = append(records, recordUpdate{RecordID: recordID, Fields: fields})
	}

	start := time.Now()
	updated := 0
	if len(records) > 0 {
		if len(records) == 1 {
			if err := updateRecord(baseURL, token, ref, records[0].RecordID, records[0].Fields); err != nil {
				errorsList = append(errorsList, err.Error())
			} else {
				updated = 1
			}
		} else {
			for i := 0; i < len(records); i += updateMaxBatchSize {
				j := i + updateMaxBatchSize
				if j > len(records) {
					j = len(records)
				}
				batch := make([]map[string]any, 0, j-i)
				for _, r := range records[i:j] {
					batch = append(batch, map[string]any{
						"record_id": r.RecordID,
						"fields":    r.Fields,
					})
				}
				if err := batchUpdateRecords(baseURL, token, ref, batch); err != nil {
					errorsList = append(errorsList, err.Error())
					break
				}
				updated += (j - i)
			}
		}
	}

	elapsed := time.Since(start).Seconds()
	report := updateReport{
		Updated:        updated,
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

func resolveUpdateRecordID(upd map[string]any, resolvedTask map[int]string, resolvedBiz map[string]string) string {
	recordID := strings.TrimSpace(common.BitableValueToString(upd["record_id"]))
	if recordID != "" {
		return recordID
	}
	if taskID, ok := common.CoerceInt(upd["task_id"]); ok && taskID > 0 {
		return strings.TrimSpace(resolvedTask[taskID])
	}
	bizID := strings.TrimSpace(common.BitableValueToString(upd["biz_task_id"]))
	if bizID != "" {
		return strings.TrimSpace(resolvedBiz[bizID])
	}
	return ""
}

func loadUpdates(opts UpdateOptions, fieldsMap map[string]string) ([]map[string]any, error) {
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
				"task_id":         opts.TaskID,
				"biz_task_id":     opts.BizTaskID,
				"record_id":       opts.RecordID,
				"status":          opts.Status,
				"device_serial":   opts.DeviceSerial,
				"dispatched_at":   opts.DispatchedAt,
				"start_at":        opts.StartAt,
				"completed_at":    opts.CompletedAt,
				"end_at":          opts.EndAt,
				"elapsed_seconds": opts.ElapsedSeconds,
				"items_collected": opts.ItemsCollected,
				"logs":            opts.Logs,
				"retry_count":     opts.RetryCount,
				"extra":           opts.Extra,
				"date":            opts.Date,
			},
		}
	}

	knownKeys := map[string]bool{
		"task_id":         true,
		"taskID":          true,
		"TaskID":          true,
		"biz_task_id":     true,
		"bizTaskId":       true,
		"BizTaskID":       true,
		"record_id":       true,
		"recordId":        true,
		"RecordID":        true,
		"status":          true,
		"date":            true,
		"Date":            true,
		"device_serial":   true,
		"dispatched_at":   true,
		"start_at":        true,
		"completed_at":    true,
		"end_at":          true,
		"elapsed_seconds": true,
		"items_collected": true,
		"logs":            true,
		"retry_count":     true,
		"extra":           true,
		"fields":          true,
		"CDNURL":          true,
		"cdn_url":         true,
		"cdnUrl":          true,
		"cdnurl":          true,
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
			"task_id":         firstNonNil(item["task_id"], item["taskID"], item["TaskID"]),
			"biz_task_id":     firstNonNil(item["biz_task_id"], item["bizTaskId"], item["BizTaskID"]),
			"record_id":       firstNonNil(item["record_id"], item["recordId"], item["RecordID"]),
			"status":          pick(item, "status", opts.Status),
			"date":            firstNonNil(pick(item, "date", opts.Date), item["Date"]),
			"device_serial":   pick(item, "device_serial", opts.DeviceSerial),
			"dispatched_at":   pick(item, "dispatched_at", opts.DispatchedAt),
			"start_at":        pick(item, "start_at", opts.StartAt),
			"completed_at":    pick(item, "completed_at", opts.CompletedAt),
			"end_at":          pick(item, "end_at", opts.EndAt),
			"elapsed_seconds": pick(item, "elapsed_seconds", opts.ElapsedSeconds),
			"items_collected": pick(item, "items_collected", opts.ItemsCollected),
			"logs":            pick(item, "logs", opts.Logs),
			"retry_count":     pick(item, "retry_count", opts.RetryCount),
			"extra":           extra,
			"force_extra":     forceExtra,
			"fields":          extraFields,
		}
		out = append(out, merged)
	}
	return out, nil
}

func resolveRecordIDsByTaskID(baseURL, token string, ref common.BitableRef, fieldsMap map[string]string, taskIDs []int, ignoreView bool, viewID string) (map[int]string, map[string]string, error) {
	result := map[int]string{}
	statuses := map[string]string{}
	values := []string{}
	for _, id := range taskIDs {
		if id > 0 {
			values = append(values, fmt.Sprintf("%d", id))
		}
	}
	if len(values) == 0 {
		return result, statuses, nil
	}

	taskField := fieldsMap["TaskID"]
	statusField := fieldsMap["Status"]
	for _, batch := range chunkStrings(values, updateMaxFilterValues) {
		filterObj := buildIDFilter(taskField, batch)
		if filterObj == nil {
			continue
		}
		items, err := searchItems(baseURL, token, ref, filterObj, minInt(common.MaxPageSize, maxInt(len(batch), 1)), ignoreView, viewID)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range items {
			recordID := strings.TrimSpace(common.BitableValueToString(item["record_id"]))
			fieldsRaw, _ := item["fields"].(map[string]any)
			taskID := common.FieldInt(fieldsRaw, taskField)
			if recordID != "" && taskID > 0 {
				if _, ok := result[taskID]; !ok {
					result[taskID] = recordID
				}
			}
		}
		for k, v := range extractStatusesFromItems(items, statusField) {
			statuses[k] = v
		}
	}
	return result, statuses, nil
}

func resolveRecordIDsByBizTaskID(baseURL, token string, ref common.BitableRef, fieldsMap map[string]string, bizIDs []string, ignoreView bool, viewID string) (map[string]string, map[string]string, error) {
	result := map[string]string{}
	statuses := map[string]string{}
	values := []string{}
	for _, id := range bizIDs {
		if s := strings.TrimSpace(id); s != "" {
			values = append(values, s)
		}
	}
	if len(values) == 0 {
		return result, statuses, nil
	}

	bizField := fieldsMap["BizTaskID"]
	statusField := fieldsMap["Status"]
	for _, batch := range chunkStrings(values, updateMaxFilterValues) {
		filterObj := buildIDFilter(bizField, batch)
		if filterObj == nil {
			continue
		}
		items, err := searchItems(baseURL, token, ref, filterObj, minInt(common.MaxPageSize, maxInt(len(batch), 1)), ignoreView, viewID)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range items {
			recordID := strings.TrimSpace(common.BitableValueToString(item["record_id"]))
			fieldsRaw, _ := item["fields"].(map[string]any)
			bizID := strings.TrimSpace(common.BitableValueToString(fieldsRaw[bizField]))
			if recordID != "" && bizID != "" {
				if _, ok := result[bizID]; !ok {
					result[bizID] = recordID
				}
			}
		}
		for k, v := range extractStatusesFromItems(items, statusField) {
			statuses[k] = v
		}
	}
	return result, statuses, nil
}

func extractStatusesFromItems(items []map[string]any, statusField string) map[string]string {
	out := map[string]string{}
	for _, item := range items {
		recordID := strings.TrimSpace(common.BitableValueToString(item["record_id"]))
		fieldsRaw, _ := item["fields"].(map[string]any)
		status := strings.TrimSpace(common.BitableValueToString(fieldsRaw[statusField]))
		if recordID != "" && status != "" {
			out[recordID] = status
		}
	}
	return out
}

func fetchRecordStatuses(baseURL, token string, ref common.BitableRef, recordIDs []string, statusField string) (map[string]string, error) {
	out := map[string]string{}
	for _, recordID := range recordIDs {
		recordID = strings.TrimSpace(recordID)
		if recordID == "" {
			continue
		}
		if _, ok := out[recordID]; ok {
			continue
		}
		urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/%s",
			strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID, url.PathEscape(recordID),
		)
		var resp getRecordResp
		if err := common.RequestJSON("GET", urlStr, token, nil, &resp); err != nil {
			return nil, err
		}
		if resp.Code != 0 {
			return nil, fmt.Errorf("get record failed: code=%d msg=%s", resp.Code, resp.Msg)
		}
		status := strings.TrimSpace(common.BitableValueToString(resp.Data.Record.Fields[statusField]))
		if status != "" {
			out[recordID] = status
		}
	}
	return out, nil
}

func buildIDFilter(fieldName string, values []string) map[string]any {
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "" {
		return nil
	}
	seen := map[string]bool{}
	conds := []map[string]any{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		conds = append(conds, map[string]any{"field_name": fieldName, "operator": "is", "value": []string{v}})
	}
	if len(conds) == 0 {
		return nil
	}
	return map[string]any{"conjunction": "or", "conditions": conds}
}

func searchItems(baseURL, token string, ref common.BitableRef, filterObj map[string]any, pageSize int, ignoreView bool, viewID string) ([]map[string]any, error) {
	pageSize = common.ClampPageSize(pageSize)
	q := url.Values{}
	q.Set("page_size", fmt.Sprintf("%d", pageSize))
	urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/search?%s",
		strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID, q.Encode(),
	)
	var body map[string]any
	if (!ignoreView && strings.TrimSpace(viewID) != "") || filterObj != nil {
		body = map[string]any{}
		if !ignoreView && strings.TrimSpace(viewID) != "" {
			body["view_id"] = strings.TrimSpace(viewID)
		}
		if filterObj != nil {
			body["filter"] = filterObj
		}
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

func hasCdnURL(extra any) bool {
	raw := common.NormalizeExtra(extra)
	if strings.TrimSpace(raw) == "" {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return false
	}
	v, ok := payload["cdn_url"].(string)
	if !ok {
		return false
	}
	return strings.TrimSpace(v) != ""
}

func buildUpdateFields(fieldsMap map[string]string, upd map[string]any) map[string]any {
	out := map[string]any{}

	status := strings.TrimSpace(common.BitableValueToString(upd["status"]))
	if status != "" && fieldsMap["Status"] != "" {
		out[fieldsMap["Status"]] = status
	}

	if fieldsMap["Date"] != "" {
		if v, ok := upd["date"]; ok && v != nil {
			if payload, ok := common.CoerceDatePayload(v); ok {
				out[fieldsMap["Date"]] = payload
			}
		}
	}

	deviceSerial := strings.TrimSpace(common.BitableValueToString(upd["device_serial"]))
	if deviceSerial != "" && fieldsMap["DispatchedDevice"] != "" {
		out[fieldsMap["DispatchedDevice"]] = deviceSerial
	}

	var dispatchedMS *int64
	if v, ok := upd["dispatched_at"]; ok && v != nil && fieldsMap["DispatchedAt"] != "" {
		if ms, ok := common.CoerceMillis(v); ok {
			dispatchedMS = &ms
			out[fieldsMap["DispatchedAt"]] = ms
		}
	}

	var startMS *int64
	if v, ok := upd["start_at"]; ok && v != nil && fieldsMap["StartAt"] != "" {
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
	if v, ok := upd["completed_at"]; ok && v != nil {
		if ms, ok := common.CoerceMillis(v); ok {
			endMS = &ms
		}
	}
	if endMS == nil {
		if v, ok := upd["end_at"]; ok && v != nil {
			if ms, ok := common.CoerceMillis(v); ok {
				endMS = &ms
			}
		}
	}
	if endMS != nil && fieldsMap["EndAt"] != "" {
		out[fieldsMap["EndAt"]] = *endMS
	}

	elapsed, hasElapsed := common.CoerceInt(upd["elapsed_seconds"])
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

	if itemsCollected, ok := common.CoerceInt(upd["items_collected"]); ok && fieldsMap["ItemsCollected"] != "" {
		out[fieldsMap["ItemsCollected"]] = itemsCollected
	}

	logs := strings.TrimSpace(common.BitableValueToString(upd["logs"]))
	if logs != "" && fieldsMap["Logs"] != "" {
		out[fieldsMap["Logs"]] = logs
	}

	if retryCount, ok := common.CoerceInt(upd["retry_count"]); ok && fieldsMap["RetryCount"] != "" {
		out[fieldsMap["RetryCount"]] = retryCount
	}

	extra := upd["extra"]
	forceExtra, _ := upd["force_extra"].(bool)
	if fieldsMap["Extra"] != "" && extra != nil {
		if forceExtra || (status == "success" && hasCdnURL(extra)) {
			extraPayload := common.NormalizeExtra(extra)
			if strings.TrimSpace(extraPayload) != "" {
				out[fieldsMap["Extra"]] = extraPayload
			}
		}
	}

	if extraFields, ok := upd["fields"].(map[string]any); ok {
		for k, v := range extraFields {
			if strings.TrimSpace(k) == "" || v == nil {
				continue
			}
			out[k] = v
		}
	}

	return out
}

func updateRecord(baseURL, token string, ref common.BitableRef, recordID string, fields map[string]any) error {
	urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/%s",
		strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID, url.PathEscape(recordID),
	)
	payload := map[string]any{"fields": fields}
	var resp common.FeishuResp
	if err := common.RequestJSON("PUT", urlStr, token, payload, &resp); err != nil {
		return err
	}
	if resp.Code != 0 {
		return fmt.Errorf("update record failed: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func batchUpdateRecords(baseURL, token string, ref common.BitableRef, records []map[string]any) error {
	urlStr := fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_update",
		strings.TrimRight(baseURL, "/"), ref.AppToken, ref.TableID,
	)
	payload := map[string]any{"records": records}
	var resp common.FeishuResp
	if err := common.RequestJSON("POST", urlStr, token, payload, &resp); err != nil {
		return err
	}
	if resp.Code != 0 {
		return fmt.Errorf("batch update failed: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func chunkStrings(values []string, size int) [][]string {
	if size <= 0 {
		return [][]string{values}
	}
	out := [][]string{}
	for i := 0; i < len(values); i += size {
		j := i + size
		if j > len(values) {
			j = len(values)
		}
		out = append(out, values[i:j])
	}
	return out
}

func parseCSVSet(s string) map[string]bool {
	out := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		p := strings.ToLower(strings.TrimSpace(part))
		if p != "" {
			out[p] = true
		}
	}
	return out
}

func printJSON(v any) {
	logger.Info("result", "data", v)
}

func firstNonNil(values ...any) any {
	for _, v := range values {
		if v != nil {
			if s, ok := v.(string); ok {
				if strings.TrimSpace(s) == "" {
					continue
				}
			}
			return v
		}
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
