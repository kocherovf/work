package work

import (
	"context"
	"io"
	"log"
	"log/slog"
)

// Deprecated: use StructuredLogger.
// StdLogger is used to log error messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Deprecated: provide a logger using the WithLogger option.
// Logger is the instance of a StdLogger interface that Worker writes connection
// management events to. By default it is set to discard all log messages via
// io.Discard, but you can set it to redirect wherever you want.
var Logger StdLogger = log.New(io.Discard, "[Work] ", log.LstdFlags)

type StructuredLogger interface {
	Error(msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
	Warn(msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	Info(msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	Debug(msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
}

var noopLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func errAttr(e error) slog.Attr {
	return slog.Any("error", e)
}
