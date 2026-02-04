package cli

import (
	"log/slog"
	"os"
)

var (
	logger    = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	errLogger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
)

func logUsage(line string) {
	logger.Info("usage", "line", line)
}
