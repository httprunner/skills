package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const defaultTimeout = 15 * time.Second

func hdcPrefix(serial string) []string {
	if serial != "" {
		return []string{"hdc", "-t", serial}
	}
	return []string{"hdc"}
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

func runHdc(serial string, capture bool, args ...string) cmdResult {
	cmd := append(hdcPrefix(serial), args...)
	return runCmd(cmd, capture, defaultTimeout)
}

func cmdDevices(serial string) int {
	result := runHdc(serial, true, "list", "targets")
	if result.exitCode != 0 {
		if result.stderr != "" {
			fmt.Fprintln(os.Stderr, strings.TrimSpace(result.stderr))
		}
		return result.exitCode
	}

	for _, line := range strings.Split(result.stdout, "\n") {
		if strings.TrimSpace(line) != "" {
			fmt.Println(strings.TrimSpace(line))
		}
	}
	return 0
}

func cmdConnect(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "connect requires <address>")
		return 2
	}
	addr := args[0]
	if !strings.Contains(addr, ":") {
		addr = addr + ":5555"
	}
	result := runCmd([]string{"hdc", "tconn", addr}, false, defaultTimeout)
	return result.exitCode
}

func cmdDisconnect(args []string) int {
	cmd := []string{"hdc", "tdisconn"}
	if len(args) >= 1 {
		cmd = append(cmd, args[0])
	}
	result := runCmd(cmd, false, defaultTimeout)
	return result.exitCode
}

func cmdGetIP(serial string) int {
	result := runHdc(serial, true, "shell", "ifconfig")

	for _, line := range strings.Split(result.stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Contains(line, "inet addr:") {
			parts := strings.Fields(line)
			for _, part := range parts {
				if strings.HasPrefix(part, "addr:") {
					ip := strings.TrimPrefix(part, "addr:")
					if ip != "" && !strings.HasPrefix(ip, "127.") {
						fmt.Println(ip)
						return 0
					}
				}
			}
		}
		if strings.Contains(line, "inet ") {
			parts := strings.Fields(line)
			for i, part := range parts {
				if part == "inet" && i+1 < len(parts) {
					ip := strings.Split(parts[i+1], "/")[0]
					if ip != "" && !strings.HasPrefix(ip, "127.") {
						fmt.Println(ip)
						return 0
					}
				}
			}
		}
	}

	fmt.Fprintln(os.Stderr, "IP not found")
	return 1
}

func cmdShell(serial string, args []string) int {
	cmdArgs := append([]string{"shell"}, args...)
	result := runHdc(serial, false, cmdArgs...)
	return result.exitCode
}

func cmdTap(serial string, args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "tap requires <x> <y>")
		return 2
	}
	result := runHdc(serial, false, "shell", "uitest", "uiInput", "click", args[0], args[1])
	return result.exitCode
}

func cmdDoubleTap(serial string, args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "double-tap requires <x> <y>")
		return 2
	}
	result := runHdc(serial, false, "shell", "uitest", "uiInput", "doubleClick", args[0], args[1])
	return result.exitCode
}

func cmdSwipe(serial string, args []string, durationMs int) int {
	if len(args) < 4 {
		fmt.Fprintln(os.Stderr, "swipe requires <x1> <y1> <x2> <y2>")
		return 2
	}
	cmdArgs := []string{"shell", "uitest", "uiInput", "swipe", args[0], args[1], args[2], args[3]}
	if durationMs >= 0 {
		cmdArgs = append(cmdArgs, strconv.Itoa(durationMs))
	}
	result := runHdc(serial, false, cmdArgs...)
	return result.exitCode
}

func cmdKeyEvent(serial string, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "keyevent requires <keycode>")
		return 2
	}
	result := runHdc(serial, false, "shell", "uitest", "uiInput", "keyEvent", args[0])
	return result.exitCode
}

func cmdText(serial string, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "text requires <text>")
		return 2
	}
	text := strings.Join(args, " ")
	text = strings.ReplaceAll(text, "\"", "\\\"")
	text = strings.ReplaceAll(text, "$", "\\$")

	result := runHdc(serial, false, "shell", "uitest", "uiInput", "text", text)
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

	remotePath := "/data/local/tmp/tmp_screenshot.jpeg"
	prefix := hdcPrefix(serial)

	res := runCmd(append(prefix, "shell", "screenshot", remotePath), true, defaultTimeout)
	resOut := strings.ToLower(res.stdout)
	if strings.Contains(resOut, "fail") || strings.Contains(resOut, "error") || strings.Contains(resOut, "not found") {
		_ = runCmd(append(prefix, "shell", "snapshot_display", "-f", remotePath), true, defaultTimeout)
	}

	resPull := runCmd(append(prefix, "file", "recv", remotePath, outPath), false, defaultTimeout)
	_ = runCmd(append(prefix, "shell", "rm", remotePath), false, defaultTimeout)
	return resPull.exitCode
}

func cmdLaunch(serial string, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "launch requires <bundle> or <bundle/ability>")
		return 2
	}
	bundle := args[0]
	ability := "EntryAbility"
	if strings.Contains(bundle, "/") {
		parts := strings.SplitN(bundle, "/", 2)
		bundle = parts[0]
		ability = parts[1]
	}

	result := runHdc(serial, false, "shell", "aa", "start", "-b", bundle, "-a", ability)
	return result.exitCode
}

func cmdForceStop(serial string, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "force-stop requires <bundle>")
		return 2
	}
	result := runHdc(serial, false, "shell", "aa", "force-stop", args[0])
	return result.exitCode
}

func cmdGetCurrentApp(serial string) int {
	result := runHdc(serial, true, "shell", "aa", "dump", "-l")
	output := result.stdout

	lines := strings.Split(output, "\n")
	foregroundBundle := ""
	currentBundle := ""
	re := regexp.MustCompile(`\[([^\]]+)\]`)

	for _, line := range lines {
		if strings.Contains(line, "app name [") {
			if match := re.FindStringSubmatch(line); len(match) >= 2 {
				currentBundle = match[1]
			}
		}

		if strings.Contains(line, "state #FOREGROUND") || strings.Contains(strings.ToLower(line), "state #foreground") {
			if currentBundle != "" {
				foregroundBundle = currentBundle
				break
			}
		}

		if strings.Contains(line, "Mission ID") {
			currentBundle = ""
		}
	}

	if foregroundBundle != "" {
		fmt.Println(foregroundBundle)
		return 0
	}
	fmt.Fprintln(os.Stderr, "System Home")
	return 1
}

func main() {
	global, serial := rootFlagSet(os.Stderr)
	if hasHelpArg(os.Args[1:]) {
		global.SetOutput(os.Stdout)
	}
	if err := global.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		os.Exit(2)
	}
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
		os.Exit(cmdDevices(*serial))
	case "connect":
		os.Exit(cmdConnect(cmdArgs))
	case "disconnect":
		os.Exit(cmdDisconnect(cmdArgs))
	case "get-ip":
		os.Exit(cmdGetIP(*serial))
	case "shell":
		os.Exit(cmdShell(*serial, cmdArgs))
	case "tap":
		os.Exit(cmdTap(*serial, cmdArgs))
	case "double-tap":
		os.Exit(cmdDoubleTap(*serial, cmdArgs))
	case "swipe":
		fs := flag.NewFlagSet("swipe", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		setFlagUsage(fs, "hdc_helpers swipe [flags] <x1> <y1> <x2> <y2>")
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
	case "keyevent":
		os.Exit(cmdKeyEvent(*serial, cmdArgs))
	case "text":
		os.Exit(cmdText(*serial, cmdArgs))
	case "screenshot":
		fs := flag.NewFlagSet("screenshot", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		setFlagUsage(fs, "hdc_helpers screenshot [flags]")
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
	case "force-stop":
		os.Exit(cmdForceStop(*serial, cmdArgs))
	case "get-current-app":
		os.Exit(cmdGetCurrentApp(*serial))
	case "help", "-h", "--help":
		global.SetOutput(os.Stdout)
		global.Usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
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

func rootFlagSet(out *os.File) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet("hdc_helpers", flag.ContinueOnError)
	fs.SetOutput(out)
	serial := fs.String("t", "", "device serial/id")
	fs.StringVar(serial, "s", "", "device serial/id")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:")
		fmt.Fprintln(fs.Output(), "  hdc_helpers [flags] <command> [args]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Commands:")
		fmt.Fprintln(fs.Output(), "  devices")
		fmt.Fprintln(fs.Output(), "  connect <address>")
		fmt.Fprintln(fs.Output(), "  disconnect [address]")
		fmt.Fprintln(fs.Output(), "  get-ip")
		fmt.Fprintln(fs.Output(), "  shell <cmd...>")
		fmt.Fprintln(fs.Output(), "  tap <x> <y>")
		fmt.Fprintln(fs.Output(), "  double-tap <x> <y>")
		fmt.Fprintln(fs.Output(), "  swipe <x1> <y1> <x2> <y2> [--duration-ms N]")
		fmt.Fprintln(fs.Output(), "  keyevent <keycode>")
		fmt.Fprintln(fs.Output(), "  text <text>")
		fmt.Fprintln(fs.Output(), "  screenshot [--out path]")
		fmt.Fprintln(fs.Output(), "  launch <bundle>[/Ability]")
		fmt.Fprintln(fs.Output(), "  force-stop <bundle>")
		fmt.Fprintln(fs.Output(), "  get-current-app")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Global Flags:")
		fs.PrintDefaults()
	}
	return fs, serial
}
