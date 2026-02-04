package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	envArkBaseURL  = "ARK_BASE_URL"
	envArkAPIKey   = "ARK_API_KEY"
	envArkModel    = "ARK_MODEL_NAME"
	defaultTimeout = 120 * time.Second
)

var (
	logger    = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	errLogger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
)

type modelConfig struct {
	BaseURL string
	APIKey  string
	Model   string
}

type size struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

type queryResult struct {
	Content string          `json:"content"`
	Thought string          `json:"thought"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type assertionResult struct {
	Pass    bool   `json:"pass"`
	Thought string `json:"thought"`
	Content string `json:"content,omitempty"`
}

type planningJSONResponse struct {
	Actions []action `json:"actions"`
	Thought string   `json:"thought"`
	Error   string   `json:"error"`
}

type action struct {
	ActionType   string                 `json:"action_type"`
	ActionInputs map[string]interface{} `json:"action_inputs"`
}

type plannedAction struct {
	Action    string                 `json:"action"`
	X         float64                `json:"x,omitempty"`
	Y         float64                `json:"y,omitempty"`
	ToX       float64                `json:"to_x,omitempty"`
	ToY       float64                `json:"to_y,omitempty"`
	Text      string                 `json:"text,omitempty"`
	Key       string                 `json:"key,omitempty"`
	Direction string                 `json:"direction,omitempty"`
	Raw       map[string]interface{} `json:"raw,omitempty"`
}

type planResult struct {
	Thought string          `json:"thought"`
	Actions []plannedAction `json:"actions"`
	Content string          `json:"content"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "query":
		os.Exit(runQuery(os.Args[2:]))
	case "assert":
		os.Exit(runAssert(os.Args[2:]))
	case "plan-next":
		os.Exit(runPlanNext(os.Args[2:]))
	case "help", "-h", "--help":
		printUsage()
		os.Exit(0)
	default:
		errLogger.Error("Unknown command", "command", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	logger.Info("usage", "line", "AI Vision helper")
	logger.Info("usage", "line", "Usage: ai_vision <command> [args]")
	logger.Info("usage", "line", "Commands:")
	logger.Info("usage", "line", "  query --screenshot <file> --prompt <text> [--model <name>]")
	logger.Info("usage", "line", "  assert --screenshot <file> --assertion <text> [--model <name>]")
	logger.Info("usage", "line", "  plan-next --screenshot <file> --instruction <text> [--model <name>]")
	logger.Info("usage", "line", "")
	logger.Info("usage", "line", "Env config (Doubao):")
	logger.Info("usage", "line", "  ARK_BASE_URL, ARK_API_KEY, ARK_MODEL_NAME")
	logger.Info("usage", "line", "For other providers, pass --base-url/--api-key/--model")
}

func runQuery(args []string) int {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	screenshot := fs.String("screenshot", "", "screenshot path (png/jpg)")
	prompt := fs.String("prompt", "", "query prompt")
	model := fs.String("model", "", "model name")
	baseURL := fs.String("base-url", "", "override base url")
	apiKey := fs.String("api-key", "", "override api key")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *screenshot == "" || *prompt == "" {
		errLogger.Error("query requires --screenshot and --prompt")
		return 2
	}

	cfg, err := getModelConfig(*model, *baseURL, *apiKey)
	if err != nil {
		errLogger.Error("get model config failed", "err", err)
		return 1
	}

	imgB64, sz, err := loadImage(*screenshot)
	if err != nil {
		errLogger.Error("load image failed", "err", err)
		return 1
	}

	systemPrompt := defaultQueryPrompt

	content, err := callModel(cfg, systemPrompt, *prompt, imgB64)
	if err != nil {
		errLogger.Error("call model failed", "err", err)
		return 1
	}

	var result queryResult
	if err := parseStructuredResponse(content, &result); err != nil {
		// fallback: keep raw content
		result.Content = content
		result.Thought = "Failed to parse structured response"
	}
	normalizeQueryResult(&result, sz, content)

	output := map[string]interface{}{
		"size":   sz,
		"result": result,
		"model":  cfg.Model,
	}
	return printJSON(output)
}

func runAssert(args []string) int {
	fs := flag.NewFlagSet("assert", flag.ContinueOnError)
	screenshot := fs.String("screenshot", "", "screenshot path (png/jpg)")
	assertion := fs.String("assertion", "", "assertion text")
	model := fs.String("model", "", "model name")
	baseURL := fs.String("base-url", "", "override base url")
	apiKey := fs.String("api-key", "", "override api key")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *screenshot == "" || *assertion == "" {
		errLogger.Error("assert requires --screenshot and --assertion")
		return 2
	}

	cfg, err := getModelConfig(*model, *baseURL, *apiKey)
	if err != nil {
		errLogger.Error("get model config failed", "err", err)
		return 1
	}

	imgB64, sz, err := loadImage(*screenshot)
	if err != nil {
		errLogger.Error("load image failed", "err", err)
		return 1
	}

	systemPrompt := defaultAssertionPrompt

	content, err := callModel(cfg, systemPrompt, *assertion, imgB64)
	if err != nil {
		errLogger.Error("call model failed", "err", err)
		return 1
	}

	var result assertionResult
	if err := parseStructuredResponse(content, &result); err != nil {
		result.Content = content
		result.Thought = "Failed to parse structured response"
	}

	output := map[string]interface{}{
		"size":   sz,
		"result": result,
		"model":  cfg.Model,
	}
	return printJSON(output)
}

func runPlanNext(args []string) int {
	fs := flag.NewFlagSet("plan-next", flag.ContinueOnError)
	screenshot := fs.String("screenshot", "", "screenshot path (png/jpg)")
	instruction := fs.String("instruction", "", "instruction text (for action)")
	history := fs.String("history", "", "optional action history text")
	model := fs.String("model", "", "model name")
	baseURL := fs.String("base-url", "", "override base url")
	apiKey := fs.String("api-key", "", "override api key")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	prompt := strings.TrimSpace(*instruction)
	if *screenshot == "" || prompt == "" {
		errLogger.Error("plan-next requires --screenshot and --instruction")
		return 2
	}

	cfg, err := getModelConfig(*model, *baseURL, *apiKey)
	if err != nil {
		errLogger.Error("get model config failed", "err", err)
		return 1
	}

	imgB64, sz, err := loadImage(*screenshot)
	if err != nil {
		errLogger.Error("load image failed", "err", err)
		return 1
	}

	systemPrompt := doubaoThinkingVisionPrompt

	userPrompt := prompt
	if strings.TrimSpace(*history) != "" {
		userPrompt = fmt.Sprintf("Instruction:\n%s\n\nHistory:\n%s", prompt, strings.TrimSpace(*history))
	}

	content, err := callModel(cfg, systemPrompt, userPrompt, imgB64)
	if err != nil {
		errLogger.Error("call model failed", "err", err)
		return 1
	}

	result, err := parseJSONPlanning(content, sz)
	if err != nil {
		errLogger.Error("parse JSON planning failed", "err", err)
		return 1
	}
	return printJSON(result)
}

func getModelConfig(modelName, baseURL, apiKey string) (*modelConfig, error) {
	if modelName == "" {
		modelName = os.Getenv(envArkModel)
	}
	if modelName == "" {
		modelName = "doubao-seed-1-6-vision-250815"
	}

	if baseURL == "" {
		baseURL = os.Getenv(envArkBaseURL)
	}
	if apiKey == "" {
		apiKey = os.Getenv(envArkAPIKey)
	}

	if baseURL == "" {
		return nil, errors.New("missing base URL (set ARK_BASE_URL or pass --base-url)")
	}
	if apiKey == "" {
		return nil, errors.New("missing API key (set ARK_API_KEY or pass --api-key)")
	}

	return &modelConfig{BaseURL: baseURL, APIKey: apiKey, Model: modelName}, nil
}

func callModel(cfg *modelConfig, systemPrompt, userPrompt, imgB64 string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	url := strings.TrimRight(cfg.BaseURL, "/")
	url = url + "/responses"

	reqBody := map[string]interface{}{
		"model":        cfg.Model,
		"temperature":  0,
		"instructions": systemPrompt,
		"input": []map[string]interface{}{
			{
				"role": "user",
				"content": []map[string]interface{}{
					{
						"type":      "input_image",
						"image_url": "data:image/png;base64," + imgB64,
					},
					{
						"type": "input_text",
						"text": userPrompt,
					},
				},
			},
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("model request failed: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	text, err := extractResponseText(data)
	if err != nil {
		return "", err
	}
	return text, nil
}

func loadImage(path string) (string, size, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", size{}, err
	}
	defer file.Close()

	cfg, _, err := image.DecodeConfig(file)
	if err != nil {
		return "", size{}, err
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", size{}, err
	}

	raw, err := io.ReadAll(file)
	if err != nil {
		return "", size{}, err
	}

	return base64.StdEncoding.EncodeToString(raw), size{Width: cfg.Width, Height: cfg.Height}, nil
}

func parseJSONPlanning(content string, sz size) (*planResult, error) {
	var resp planningJSONResponse
	if err := parseStructuredResponse(content, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	var actions []plannedAction
	for _, act := range resp.Actions {
		processed, err := processActionArguments(act.ActionInputs, sz)
		if err != nil {
			return nil, err
		}
		actions = append(actions, convertAction(action{
			ActionType:   act.ActionType,
			ActionInputs: processed,
		}))
	}
	return &planResult{
		Thought: resp.Thought,
		Actions: actions,
		Content: content,
	}, nil
}

func convertAction(act action) plannedAction {
	out := plannedAction{Action: act.ActionType, Raw: act.ActionInputs}
	switch act.ActionType {
	case "click", "left_double", "right_single", "long_press", "scroll":
		if pt, ok := act.ActionInputs["start_box"].([]float64); ok && len(pt) == 2 {
			out.X = pt[0]
			out.Y = pt[1]
		}
		if dir, ok := act.ActionInputs["direction"].(string); ok {
			out.Direction = dir
		}
	case "drag":
		if pt, ok := act.ActionInputs["start_box"].([]float64); ok && len(pt) == 2 {
			out.X = pt[0]
			out.Y = pt[1]
		}
		if pt, ok := act.ActionInputs["end_box"].([]float64); ok && len(pt) == 2 {
			out.ToX = pt[0]
			out.ToY = pt[1]
		}
	case "type":
		if txt, ok := act.ActionInputs["content"].(string); ok {
			out.Text = txt
		}
	case "hotkey":
		if key, ok := act.ActionInputs["key"].(string); ok {
			out.Key = key
		}
	}
	return out
}

func normalizeParameterName(name string) string {
	switch name {
	case "start_point":
		return "start_box"
	case "end_point":
		return "end_box"
	case "point":
		return "start_box"
	default:
		return name
	}
}

func processActionArguments(raw map[string]interface{}, sz size) (map[string]interface{}, error) {
	processed := make(map[string]interface{})
	for k, v := range raw {
		value, err := processArgument(k, v, sz)
		if err != nil {
			return nil, err
		}
		processed[k] = value
	}
	return processed, nil
}

func processArgument(name string, value interface{}, sz size) (interface{}, error) {
	switch name {
	case "start_box", "end_box":
		switch v := value.(type) {
		case string:
			box, err := parseBoxString(v)
			if err != nil {
				return nil, err
			}
			if len(box) == 4 && maxFloat(box) <= 1000 {
				box = scaleRelativeBox(box, sz)
			}
			if len(box) == 2 && maxFloat(box) <= 1000 {
				box = scaleRelativePoint(box, sz)
			}
			if len(box) == 2 {
				return []float64{box[0], box[1]}, nil
			}
			center := boxCenter(box)
			return []float64{center[0], center[1]}, nil
		case []float64:
			if len(v) == 4 && maxFloat(v) <= 1000 {
				v = scaleRelativeBox(v, sz)
			}
			if len(v) == 2 && maxFloat(v) <= 1000 {
				v = scaleRelativePoint(v, sz)
			}
			if len(v) == 2 {
				return v, nil
			}
			if len(v) == 4 {
				center := boxCenter(v)
				return []float64{center[0], center[1]}, nil
			}
		case []interface{}:
			coords, err := parseInterfaceCoords(v)
			if err != nil {
				return nil, err
			}
			if len(coords) == 4 && maxFloat(coords) <= 1000 {
				coords = scaleRelativeBox(coords, sz)
			}
			if len(coords) == 2 && maxFloat(coords) <= 1000 {
				coords = scaleRelativePoint(coords, sz)
			}
			if len(coords) == 2 {
				return coords, nil
			}
			if len(coords) == 4 {
				center := boxCenter(coords)
				return []float64{center[0], center[1]}, nil
			}
		}
	case "direction", "content", "key":
		return value, nil
	}
	return value, nil
}

func parseBoxString(s string) ([]float64, error) {
	s = normalizeCoordinatesFormat(s)
	re := regexp.MustCompile(`-?\d+(\.\d+)?`)
	nums := re.FindAllString(s, -1)
	if len(nums) < 2 {
		return nil, errors.New("invalid box string")
	}
	var values []float64
	for _, n := range nums {
		v, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	if len(values) == 2 {
		return values, nil
	}
	if len(values) >= 4 {
		values = values[:4]
	}
	return values, nil
}

func normalizeCoordinatesFormat(text string) string {
	if strings.Contains(text, "<point>") {
		re := regexp.MustCompile(`<point>(\d+)\s+(\d+)(?:\s+(\d+)\s+(\d+))?</point>`)
		text = re.ReplaceAllStringFunc(text, func(match string) string {
			sub := re.FindStringSubmatch(match)
			if sub[3] != "" && sub[4] != "" {
				return fmt.Sprintf("(%s,%s,%s,%s)", sub[1], sub[2], sub[3], sub[4])
			}
			return fmt.Sprintf("(%s,%s)", sub[1], sub[2])
		})
	}
	if strings.Contains(text, "<bbox>") {
		re := regexp.MustCompile(`<bbox>(\d+)\s+(\d+)\s+(\d+)\s+(\d+)</bbox>`)
		text = re.ReplaceAllStringFunc(text, func(match string) string {
			sub := re.FindStringSubmatch(match)
			return fmt.Sprintf("(%s,%s,%s,%s)", sub[1], sub[2], sub[3], sub[4])
		})
	}
	if strings.Contains(text, "[") && strings.Contains(text, "]") {
		re := regexp.MustCompile(`\[(\d+),\s*(\d+),\s*(\d+),\s*(\d+)\]`)
		text = re.ReplaceAllStringFunc(text, func(match string) string {
			sub := re.FindStringSubmatch(match)
			return fmt.Sprintf("(%s,%s,%s,%s)", sub[1], sub[2], sub[3], sub[4])
		})
	}
	return text
}

func boxCenter(box []float64) []float64 {
	if len(box) < 4 {
		return box
	}
	return []float64{(box[0] + box[2]) / 2, (box[1] + box[3]) / 2}
}

func maxFloat(vals []float64) float64 {
	max := vals[0]
	for _, v := range vals {
		if v > max {
			max = v
		}
	}
	return max
}

func scaleRelativePoint(pt []float64, sz size) []float64 {
	if len(pt) != 2 {
		return pt
	}
	return []float64{
		pt[0] / 1000 * float64(sz.Width),
		pt[1] / 1000 * float64(sz.Height),
	}
}

func scaleRelativeBox(box []float64, sz size) []float64 {
	if len(box) != 4 {
		return box
	}
	return []float64{
		box[0] / 1000 * float64(sz.Width),
		box[1] / 1000 * float64(sz.Height),
		box[2] / 1000 * float64(sz.Width),
		box[3] / 1000 * float64(sz.Height),
	}
}

func parseInterfaceCoords(values []interface{}) ([]float64, error) {
	if len(values) == 0 {
		return nil, errors.New("empty coordinate array")
	}
	coords := make([]float64, 0, len(values))
	for _, v := range values {
		switch t := v.(type) {
		case float64:
			coords = append(coords, t)
		case float32:
			coords = append(coords, float64(t))
		case int:
			coords = append(coords, float64(t))
		case int64:
			coords = append(coords, float64(t))
		case json.Number:
			f, err := t.Float64()
			if err != nil {
				return nil, err
			}
			coords = append(coords, f)
		case string:
			parsed, err := parseBoxString(t)
			if err != nil {
				return nil, err
			}
			coords = append(coords, parsed...)
		default:
			return nil, fmt.Errorf("unsupported coordinate value type: %T", v)
		}
	}
	return coords, nil
}

func parseStructuredResponse(content string, result interface{}) error {
	clean := strings.TrimSpace(content)
	jsonContent := extractJSONFromContent(clean)
	if jsonContent == "" {
		jsonContent = clean
	}
	if err := json.Unmarshal([]byte(jsonContent), result); err == nil {
		return nil
	}
	cleaned := cleanJSONContent(jsonContent)
	return json.Unmarshal([]byte(cleaned), result)
}

func normalizeQueryResult(result *queryResult, sz size, content string) {
	raw := extractJSONFromContent(strings.TrimSpace(content))
	if raw == "" {
		raw = strings.TrimSpace(content)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		if result.Content == "" {
			result.Content = strings.TrimSpace(content)
		}
		return
	}

	x, xOK := asFloat(payload["x"])
	y, yOK := asFloat(payload["y"])
	if !xOK || !yOK {
		if result.Content == "" {
			result.Content = strings.TrimSpace(content)
		}
		return
	}
	w, _ := asFloat(payload["w"])
	h, _ := asFloat(payload["h"])

	maxVal := maxFloat([]float64{x, y, w, h})
	if maxVal > 0 && maxVal <= 1000 && sz.Width > 0 && sz.Height > 0 {
		x = x / 1000 * float64(sz.Width)
		y = y / 1000 * float64(sz.Height)
		if w > 0 {
			w = w / 1000 * float64(sz.Width)
		}
		if h > 0 {
			h = h / 1000 * float64(sz.Height)
		}
		payload["x"] = x
		payload["y"] = y
		if w > 0 {
			payload["w"] = w
		}
		if h > 0 {
			payload["h"] = h
		}
	}

	normalized, err := json.Marshal(payload)
	if err == nil {
		result.Content = string(normalized)
		result.Data = normalized
	} else if result.Content == "" {
		result.Content = strings.TrimSpace(content)
	}
}

func asFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func extractResponseText(data []byte) (string, error) {
	var parsed struct {
		Output []struct {
			Content []struct {
				Text       string `json:"text"`
				OutputText string `json:"output_text"`
			} `json:"content"`
		} `json:"output"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", err
	}
	if len(parsed.Output) > 0 {
		for _, item := range parsed.Output {
			for _, content := range item.Content {
				if strings.TrimSpace(content.Text) != "" {
					return content.Text, nil
				}
				if strings.TrimSpace(content.OutputText) != "" {
					return content.OutputText, nil
				}
			}
		}
	}
	if len(parsed.Choices) > 0 {
		return parsed.Choices[0].Message.Content, nil
	}
	return "", errors.New("empty model response")
}

func extractJSONFromContent(content string) string {
	if strings.Contains(content, "```json") {
		start := strings.Index(content, "```json")
		if start != -1 {
			start += 7
			end := strings.Index(content[start:], "```")
			if end != -1 {
				return strings.TrimSpace(content[start : start+end])
			}
		}
	}
	if strings.HasPrefix(content, "```") && strings.HasSuffix(content, "```") {
		lines := strings.Split(content, "\n")
		if len(lines) >= 3 {
			jsonLines := lines[1 : len(lines)-1]
			jsonContent := strings.TrimSpace(strings.Join(jsonLines, "\n"))
			if strings.HasPrefix(jsonContent, "{") && strings.HasSuffix(jsonContent, "}") {
				return jsonContent
			}
		}
	}
	start := strings.Index(content, "{")
	if start != -1 {
		braceCount := 0
		inString := false
		escaped := false
		for i := start; i < len(content); i++ {
			ch := content[i]
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' && inString {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = !inString
				continue
			}
			if !inString {
				switch ch {
				case '{':
					braceCount++
				case '}':
					braceCount--
					if braceCount == 0 {
						return strings.TrimSpace(content[start : i+1])
					}
				}
			}
		}
	}
	return ""
}

func cleanJSONContent(content string) string {
	cleaned := strings.ReplaceAll(content, ",}", "}")
	cleaned = strings.ReplaceAll(cleaned, ",]", "]")
	return cleaned
}

func printJSON(v interface{}) int {
	logger.Info("result", "data", v)
	return 0
}

const defaultQueryPrompt = `You are an AI assistant specialized in analyzing images and extracting information. User will provide a screenshot and a query asking for specific information to be extracted from the image. Please analyze the image carefully and provide the requested information.`

const defaultAssertionPrompt = `You are a senior testing engineer. User will give an assertion and a screenshot of a page. By carefully viewing the screenshot, please tell whether the assertion is truthy.`

const doubaoThinkingVisionPrompt = `You are a GUI agent. You are given a task and your action history, with screenshots. You need to perform the next action to complete the task.

Target: User will give you a screenshot, an instruction and some previous logs indicating what have been done. Please tell what the next one action is (or null if no action should be done) to do the tasks the instruction requires.

Restriction:
- Don't give extra actions or plans beyond the instruction. ONLY plan for what the instruction requires. For example, don't try to submit the form if the instruction is only to fill something.
- Don't repeat actions in the previous logs.
- Bbox is the bounding box of the element to be located. It's an array of 4 numbers, representing [x1, y1, x2, y2] coordinates in 1000x1000 relative coordinates system.

Supporting actions:
- click: { action_type: "click", action_inputs: { start_box: [x1, y1, x2, y2] } }
- long_press: { action_type: "long_press", action_inputs: { start_box: [x1, y1, x2, y2] } }
- type: { action_type: "type", action_inputs: { content: string } } // If you want to submit your input, use "\\n" at the end of content.
- scroll: { action_type: "scroll", action_inputs: { start_box: [x1, y1, x2, y2], direction: "down" | "up" | "left" | "right" } }
- drag: { action_type: "drag", action_inputs: { start_box: [x1, y1, x2, y2], end_box: [x3, y3, x4, y4] } }
- press_home: { action_type: "press_home", action_inputs: {} }
- press_back: { action_type: "press_back", action_inputs: {} }
- wait: { action_type: "wait", action_inputs: {} } // Sleep for 5s and take a screenshot to check for any changes.
- finished: { action_type: "finished", action_inputs: { content: string } } // Use escape characters \\\\', \\\", and \\\\n in content part to ensure we can parse the content in normal python string format.

Field description:
* The ` + "`start_box`" + ` and ` + "`end_box`" + ` fields represent the bounding box coordinates of the target element in 1000x1000 relative coordinate system.
* Use Chinese in log and thought fields.

Return in JSON format:
{
  "actions": [
    {
      "action_type": "...",
      "action_inputs": { ... }
    }
  ],
  "thought": "string", // Log what the next action you can do according to the screenshot and the instruction. Use Chinese.
  "error": "string" | null, // Error messages about unexpected situations, if any. Use Chinese.
}

## User Instruction
`
