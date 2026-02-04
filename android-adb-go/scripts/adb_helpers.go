package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const defaultTimeout = 10 * time.Second

var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

func adbPrefix(serial string) []string {
	if serial != "" {
		return []string{"adb", "-s", serial}
	}
	return []string{"adb"}
}

type cmdResult struct {
	stdout   string
	stderr   string
	exitCode int
}

func runCmd(cmd []string, capture bool, timeout time.Duration) cmdResult {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	execCmd := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	var stdoutBuf bytes.Buffer
	var stderrBuf bytes.Buffer

	if capture {
		execCmd.Stdout = &stdoutBuf
		execCmd.Stderr = &stderrBuf
	} else {
		execCmd.Stdout = os.Stdout
		execCmd.Stderr = os.Stderr
	}

	err := execCmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return cmdResult{stdout: stdoutBuf.String(), stderr: "Command timed out", exitCode: 124}
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return cmdResult{stdout: stdoutBuf.String(), stderr: stderrBuf.String(), exitCode: exitErr.ExitCode()}
		}
		return cmdResult{stdout: stdoutBuf.String(), stderr: stderrBuf.String(), exitCode: 1}
	}
	return cmdResult{stdout: stdoutBuf.String(), stderr: stderrBuf.String(), exitCode: 0}
}

func runAdb(serial string, capture bool, args ...string) cmdResult {
	cmd := append(adbPrefix(serial), args...)
	logger.Debug("exec adb", "cmd", strings.Join(cmd, " "))
	return runCmd(cmd, capture, defaultTimeout)
}

func cmdDevices(serial string, _ []string) int {
	result := runAdb(serial, false, "devices", "-l")
	return result.exitCode
}

func cmdStartServer() int {
	result := runCmd([]string{"adb", "start-server"}, false, defaultTimeout)
	return result.exitCode
}

func cmdKillServer() int {
	result := runCmd([]string{"adb", "kill-server"}, false, defaultTimeout)
	return result.exitCode
}

func cmdConnect(_ string, args []string) int {
	if len(args) < 1 {
		logger.Error("connect requires <address>")
		return 2
	}
	result := runCmd([]string{"adb", "connect", args[0]}, false, defaultTimeout)
	return result.exitCode
}

func cmdDisconnect(_ string, args []string) int {
	cmd := []string{"adb", "disconnect"}
	if len(args) >= 1 {
		cmd = append(cmd, args[0])
	}
	result := runCmd(cmd, false, defaultTimeout)
	return result.exitCode
}

func cmdGetIP(serial string, _ []string) int {
	result := runAdb(serial, true, "shell", "ip", "route")
	for _, line := range strings.Split(result.stdout, "\n") {
		if strings.Contains(line, "src") {
			parts := strings.Fields(line)
			for i, part := range parts {
				if part == "src" && i+1 < len(parts) {
					fmt.Println(parts[i+1])
					return 0
				}
			}
		}
	}

	result = runAdb(serial, true, "shell", "ip", "addr", "show", "wlan0")
	for _, line := range strings.Split(result.stdout, "\n") {
		if strings.Contains(line, "inet ") {
			parts := strings.Fields(strings.TrimSpace(line))
			if len(parts) >= 2 {
				fmt.Println(strings.Split(parts[1], "/")[0])
				return 0
			}
		}
	}

	logger.Error("IP not found")
	return 1
}

func cmdEnableTCPIP(serial string, args []string) int {
	port := 5555
	if len(args) >= 1 {
		parsed, err := strconv.Atoi(args[0])
		if err != nil {
			logger.Error("invalid port", "value", args[0])
			return 2
		}
		port = parsed
	}

	result := runAdb(serial, true, "tcpip", strconv.Itoa(port))
	output := strings.TrimSpace(result.stdout + result.stderr)
	if output != "" {
		fmt.Println(output)
	}
	if strings.Contains(strings.ToLower(output), "restarting") || result.exitCode == 0 {
		return 0
	}
	return 1
}

func cmdShell(serial string, args []string) int {
	cmdArgs := append([]string{"shell"}, args...)
	result := runAdb(serial, false, cmdArgs...)
	return result.exitCode
}

func cmdTap(serial string, args []string) int {
	if len(args) < 2 {
		logger.Error("tap requires <x> <y>")
		return 2
	}
	result := runAdb(serial, false, "shell", "input", "tap", args[0], args[1])
	return result.exitCode
}

func cmdDoubleTap(serial string, args []string) int {
	if len(args) < 2 {
		logger.Error("double-tap requires <x> <y>")
		return 2
	}
	cmd := []string{"shell", "input", "tap", args[0], args[1]}
	_ = runAdb(serial, false, cmd...)
	time.Sleep(100 * time.Millisecond)
	result := runAdb(serial, false, cmd...)
	return result.exitCode
}

func cmdSwipe(serial string, args []string, durationMs int) int {
	if len(args) < 4 {
		logger.Error("swipe requires <x1> <y1> <x2> <y2>")
		return 2
	}
	cmdArgs := []string{"shell", "input", "swipe", args[0], args[1], args[2], args[3]}
	if durationMs >= 0 {
		cmdArgs = append(cmdArgs, strconv.Itoa(durationMs))
	}
	result := runAdb(serial, false, cmdArgs...)
	return result.exitCode
}

func cmdLongPress(serial string, args []string, durationMs int) int {
	if len(args) < 2 {
		logger.Error("long-press requires <x> <y>")
		return 2
	}
	cmdArgs := []string{"shell", "input", "swipe", args[0], args[1], args[0], args[1], strconv.Itoa(durationMs)}
	result := runAdb(serial, false, cmdArgs...)
	return result.exitCode
}

func cmdKeyEvent(serial string, args []string) int {
	if len(args) < 1 {
		logger.Error("keyevent requires <keycode>")
		return 2
	}
	result := runAdb(serial, false, "shell", "input", "keyevent", args[0])
	return result.exitCode
}

func escapeInputText(text string) string {
	text = strings.ReplaceAll(text, " ", "%s")
	text = strings.ReplaceAll(text, "\\", "\\\\")
	text = strings.ReplaceAll(text, "'", "\\'")
	text = strings.ReplaceAll(text, "(", "\\(")
	text = strings.ReplaceAll(text, ")", "\\)")
	return text
}

func getCurrentIME(serial string) string {
	result := runAdb(serial, true, "shell", "settings", "get", "secure", "default_input_method")
	return strings.TrimSpace(result.stdout + result.stderr)
}

func setIME(serial, ime string) {
	_ = runAdb(serial, true, "shell", "ime", "set", ime)
}

func cmdText(serial string, args []string, useAdbKeyboard bool) int {
	if len(args) < 1 {
		logger.Error("text requires <text>")
		return 2
	}
	// Allow flags to appear after text by stripping known flags from args.
	filtered := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg {
		case "--adb-keyboard":
			useAdbKeyboard = true
		default:
			filtered = append(filtered, arg)
		}
	}
	if len(filtered) < 1 {
		logger.Error("text requires <text>")
		return 2
	}
	text := strings.Join(filtered, " ")

	var result cmdResult
	if useAdbKeyboard {
		originalIME := getCurrentIME(serial)
		if !strings.Contains(originalIME, "com.android.adbkeyboard/.AdbIME") {
			setIME(serial, "com.android.adbkeyboard/.AdbIME")
			time.Sleep(1 * time.Second)
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(text))
		result = runAdb(serial, false, "shell", "am", "broadcast", "-a", "ADB_INPUT_B64", "--es", "msg", encoded)
	} else {
		escaped := escapeInputText(text)
		result = runAdb(serial, false, "shell", "input", "text", escaped)
	}

	return result.exitCode
}

func cmdClearText(serial string) int {
	result := runAdb(serial, false, "shell", "am", "broadcast", "-a", "ADB_CLEAR_TEXT")
	return result.exitCode
}

func cmdScreenshot(serial string, outPath string) int {
	if outPath == "" {
		outPath = "screen.png"
	}
	absPath, err := filepath.Abs(outPath)
	if err == nil {
		outPath = absPath
	}

	file, err := os.Create(outPath)
	if err != nil {
		logger.Error("create output file failed", "path", outPath, "err", err)
		return 1
	}
	defer file.Close()

	cmd := append(adbPrefix(serial), "exec-out", "screencap", "-p")
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	adbCmd := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	adbCmd.Stdout = file
	var stderrBuf bytes.Buffer
	adbCmd.Stderr = &stderrBuf

	err = adbCmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		logger.Error("adb command timed out")
		return 124
	}
	if err != nil {
		if stderrBuf.Len() > 0 {
			logger.Error("adb stderr", "stderr", strings.TrimSpace(stderrBuf.String()))
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		return 1
	}
	return 0
}

func cmdLaunch(serial string, args []string) int {
	if len(args) < 1 {
		logger.Error("launch requires <package>")
		return 2
	}
	result := runAdb(serial, false, "shell", "monkey", "-p", args[0], "-c", "android.intent.category.LAUNCHER", "1")
	return result.exitCode
}

func cmdGetCurrentApp(serial string) int {
	result := runAdb(serial, true, "shell", "dumpsys", "window")
	output := result.stdout

	re := regexp.MustCompile(`([a-zA-Z0-9_.]+\.[a-zA-Z0-9_.]+)/`)
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "mCurrentFocus") || strings.Contains(line, "mFocusedApp") {
			if match := re.FindStringSubmatch(line); len(match) >= 2 {
				fmt.Println(match[1])
				return 0
			}
			parts := strings.Fields(line)
			for _, part := range parts {
				if strings.Contains(part, "/") {
					fmt.Println(strings.Split(part, "/")[0])
					return 0
				}
			}
		}
	}

	logger.Error("system home (or unknown)")
	return 1
}

func cmdForceStop(serial string, args []string) int {
	if len(args) < 1 {
		logger.Error("force-stop requires <package>")
		return 2
	}
	result := runAdb(serial, false, "shell", "am", "force-stop", args[0])
	return result.exitCode
}

type uiHierarchy struct {
	Nodes []uiNode `xml:"node"`
}

type uiNode struct {
	Attrs []xml.Attr `xml:",any,attr"`
	Nodes []uiNode   `xml:"node"`
}

func attrValue(attrs []xml.Attr, name string) string {
	for _, attr := range attrs {
		if attr.Name.Local == name {
			return attr.Value
		}
	}
	return ""
}

func parseUINode(node uiNode, tappable, inputs, texts *[]string) {
	bounds := attrValue(node.Attrs, "bounds")
	text := attrValue(node.Attrs, "text")
	contentDesc := attrValue(node.Attrs, "content-desc")
	resourceID := attrValue(node.Attrs, "resource-id")
	className := attrValue(node.Attrs, "class")
	clickable := attrValue(node.Attrs, "clickable") == "true"

	displayName := text
	if displayName == "" {
		displayName = contentDesc
	}
	if displayName == "" {
		displayName = resourceID
	}

	if bounds != "" {
		re := regexp.MustCompile(`\[(\d+),(\d+)\]\[(\d+),(\d+)\]`)
		if match := re.FindStringSubmatch(bounds); len(match) == 5 {
			x1, _ := strconv.Atoi(match[1])
			y1, _ := strconv.Atoi(match[2])
			x2, _ := strconv.Atoi(match[3])
			y2, _ := strconv.Atoi(match[4])
			centerX := (x1 + x2) / 2
			centerY := (y1 + y2) / 2
			coords := fmt.Sprintf("(%d, %d)", centerX, centerY)

			if className == "android.widget.EditText" {
				*inputs = append(*inputs, fmt.Sprintf("  INPUT \"%s\" @ %s", displayName, coords))
			} else if clickable {
				*tappable = append(*tappable, fmt.Sprintf("  TAP \"%s\" @ %s", displayName, coords))
			} else if displayName != "" && len(displayName) < 50 {
				*texts = append(*texts, fmt.Sprintf("  TEXT \"%s\" @ %s", displayName, coords))
			}
		}
	}

	for _, child := range node.Nodes {
		parseUINode(child, tappable, inputs, texts)
	}
}

func parseUIXML(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		logger.Error("error reading XML", "err", err)
		return
	}

	var hierarchy uiHierarchy
	if err := xml.Unmarshal(data, &hierarchy); err != nil {
		logger.Error("error parsing XML", "err", err)
		return
	}

	tappable := []string{}
	inputs := []string{}
	texts := []string{}

	for _, node := range hierarchy.Nodes {
		parseUINode(node, &tappable, &inputs, &texts)
	}

	if len(tappable) > 0 {
		fmt.Println("\nTAPPABLE (clickable=true):")
		limit := 20
		if len(tappable) < limit {
			limit = len(tappable)
		}
		for _, line := range tappable[:limit] {
			fmt.Println(line)
		}
		if len(tappable) > limit {
			fmt.Printf("  ... (%d more)\n", len(tappable)-limit)
		}
	}

	if len(inputs) > 0 {
		fmt.Println("\nINPUT FIELDS (EditText):")
		for _, line := range inputs {
			fmt.Println(line)
		}
	}

	if len(texts) > 0 {
		fmt.Println("\nTEXT/INFO (readable):")
		limit := 20
		if len(texts) < limit {
			limit = len(texts)
		}
		for _, line := range texts[:limit] {
			fmt.Println(line)
		}
	}
}

func cmdDumpUI(serial string, outPath string, parse bool) int {
	remotePath := "/sdcard/window_dump.xml"
	localPath := outPath
	if localPath == "" {
		localPath = "window_dump.xml"
	}

	result := runAdb(serial, true, "shell", "uiautomator", "dump", remotePath)
	if result.exitCode != 0 {
		logger.Error("dump failed", "stderr", strings.TrimSpace(result.stderr))
		return result.exitCode
	}

	result = runAdb(serial, true, "pull", remotePath, localPath)
	if result.exitCode != 0 {
		logger.Error("pull failed", "stderr", strings.TrimSpace(result.stderr))
		return result.exitCode
	}

	fmt.Printf("UI dumped to %s\n", localPath)
	if parse {
		parseUIXML(localPath)
	}
	return 0
}

func cmdWmSize(serial string) int {
	result := runAdb(serial, false, "shell", "wm", "size")
	return result.exitCode
}

func main() {
	global, serial, logJSON := rootFlagSet(os.Stderr)
	if hasHelpArg(os.Args[1:]) {
		global.SetOutput(os.Stdout)
	}
	if err := global.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		os.Exit(2)
	}
	setLoggerJSON(*logJSON)
	args := global.Args()
	if len(args) == 0 || args[0] == "help" {
		global.SetOutput(os.Stdout)
		global.Usage()
		os.Exit(0)
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "devices":
		os.Exit(cmdDevices(*serial, cmdArgs))
	case "start-server":
		os.Exit(cmdStartServer())
	case "kill-server":
		os.Exit(cmdKillServer())
	case "connect":
		os.Exit(cmdConnect(*serial, cmdArgs))
	case "disconnect":
		os.Exit(cmdDisconnect(*serial, cmdArgs))
	case "get-ip":
		os.Exit(cmdGetIP(*serial, cmdArgs))
	case "enable-tcpip":
		os.Exit(cmdEnableTCPIP(*serial, cmdArgs))
	case "shell":
		os.Exit(cmdShell(*serial, cmdArgs))
	case "tap":
		os.Exit(cmdTap(*serial, cmdArgs))
	case "double-tap":
		os.Exit(cmdDoubleTap(*serial, cmdArgs))
	case "swipe":
		fs := flag.NewFlagSet("swipe", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		setFlagUsage(fs, "adb_helpers swipe [flags] <x1> <y1> <x2> <y2>")
		duration := fs.Int("duration-ms", -1, "swipe duration in ms")
		if err := fs.Parse(cmdArgs); err != nil {
			if err == flag.ErrHelp {
				fs.SetOutput(os.Stdout)
				fs.Usage()
				os.Exit(0)
			}
			os.Exit(2)
		}
		os.Exit(cmdSwipe(*serial, fs.Args(), *duration))
	case "long-press":
		fs := flag.NewFlagSet("long-press", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		setFlagUsage(fs, "adb_helpers long-press [flags] <x> <y>")
		duration := fs.Int("duration-ms", 3000, "press duration in ms")
		if err := fs.Parse(cmdArgs); err != nil {
			if err == flag.ErrHelp {
				fs.SetOutput(os.Stdout)
				fs.Usage()
				os.Exit(0)
			}
			os.Exit(2)
		}
		os.Exit(cmdLongPress(*serial, fs.Args(), *duration))
	case "keyevent":
		os.Exit(cmdKeyEvent(*serial, cmdArgs))
	case "text":
		fs := flag.NewFlagSet("text", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		setFlagUsage(fs, "adb_helpers text [flags] <text>")
		useAdbKeyboard := fs.Bool("adb-keyboard", false, "use ADB Keyboard broadcast")
		if err := fs.Parse(cmdArgs); err != nil {
			if err == flag.ErrHelp {
				fs.SetOutput(os.Stdout)
				fs.Usage()
				os.Exit(0)
			}
			os.Exit(2)
		}
		os.Exit(cmdText(*serial, fs.Args(), *useAdbKeyboard))
	case "clear-text":
		os.Exit(cmdClearText(*serial))
	case "screenshot":
		fs := flag.NewFlagSet("screenshot", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		setFlagUsage(fs, "adb_helpers screenshot [flags]")
		outPath := fs.String("out", "", "output path")
		if err := fs.Parse(cmdArgs); err != nil {
			if err == flag.ErrHelp {
				fs.SetOutput(os.Stdout)
				fs.Usage()
				os.Exit(0)
			}
			os.Exit(2)
		}
		os.Exit(cmdScreenshot(*serial, *outPath))
	case "launch":
		os.Exit(cmdLaunch(*serial, cmdArgs))
	case "get-current-app":
		os.Exit(cmdGetCurrentApp(*serial))
	case "force-stop":
		os.Exit(cmdForceStop(*serial, cmdArgs))
	case "dump-ui":
		fs := flag.NewFlagSet("dump-ui", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		setFlagUsage(fs, "adb_helpers dump-ui [flags]")
		outPath := fs.String("out", "", "output path")
		parse := fs.Bool("parse", false, "parse UI hierarchy")
		if err := fs.Parse(cmdArgs); err != nil {
			if err == flag.ErrHelp {
				fs.SetOutput(os.Stdout)
				fs.Usage()
				os.Exit(0)
			}
			os.Exit(2)
		}
		os.Exit(cmdDumpUI(*serial, *outPath, *parse))
	case "wm-size":
		os.Exit(cmdWmSize(*serial))
	case "help", "-h", "--help":
		global.SetOutput(os.Stdout)
		global.Usage()
		os.Exit(0)
	default:
		logger.Error("unknown command", "command", cmd)
		global.SetOutput(os.Stdout)
		global.Usage()
		os.Exit(2)
	}
}

func hasHelpArg(args []string) bool {
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return true
		}
	}
	return false
}

func setFlagUsage(fs *flag.FlagSet, usageLine string) {
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:")
		fmt.Fprintln(fs.Output(), "  "+usageLine)
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
}

func rootFlagSet(out *os.File) (*flag.FlagSet, *string, *bool) {
	fs := flag.NewFlagSet("adb_helpers", flag.ContinueOnError)
	fs.SetOutput(out)
	serial := fs.String("s", "", "device serial/id")
	fs.StringVar(serial, "serial", "", "device serial/id")
	logJSON := fs.Bool("log-json", false, "Output logs in JSON")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:")
		fmt.Fprintln(fs.Output(), "  adb_helpers [--log-json] [flags] <command> [args]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Commands:")
		fmt.Fprintln(fs.Output(), "  devices")
		fmt.Fprintln(fs.Output(), "  start-server")
		fmt.Fprintln(fs.Output(), "  kill-server")
		fmt.Fprintln(fs.Output(), "  connect <address>")
		fmt.Fprintln(fs.Output(), "  disconnect [address]")
		fmt.Fprintln(fs.Output(), "  get-ip")
		fmt.Fprintln(fs.Output(), "  enable-tcpip [port]")
		fmt.Fprintln(fs.Output(), "  shell <cmd...>")
		fmt.Fprintln(fs.Output(), "  tap <x> <y>")
		fmt.Fprintln(fs.Output(), "  double-tap <x> <y>")
		fmt.Fprintln(fs.Output(), "  swipe <x1> <y1> <x2> <y2> [--duration-ms N]")
		fmt.Fprintln(fs.Output(), "  long-press <x> <y> [--duration-ms N]")
		fmt.Fprintln(fs.Output(), "  keyevent <keycode>")
		fmt.Fprintln(fs.Output(), "  text <text> [--adb-keyboard]")
		fmt.Fprintln(fs.Output(), "  clear-text")
		fmt.Fprintln(fs.Output(), "  screenshot [--out path]")
		fmt.Fprintln(fs.Output(), "  launch <package>")
		fmt.Fprintln(fs.Output(), "  get-current-app")
		fmt.Fprintln(fs.Output(), "  force-stop <package>")
		fmt.Fprintln(fs.Output(), "  dump-ui [--out path] [--parse]")
		fmt.Fprintln(fs.Output(), "  wm-size")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Global Flags:")
		fs.PrintDefaults()
	}
	return fs, serial, logJSON
}

func setLoggerJSON(enabled bool) {
	if enabled {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
		return
	}
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
