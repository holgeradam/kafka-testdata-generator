// Package zaplog is the one zap-backed adapter of logging.Logger: human
// console text on a writer, one line per record. No zap type leaves it.
package zaplog

import (
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/holgeradam/kafka-testdata-generator/internal/logging"
)

type logger struct{ s *zap.SugaredLogger }

// New returns a Logger that writes records at or above level to w as console
// text: the level, the message, then its attrs. There is no timestamp or
// caller; this is a command-line tool's diagnostic, not a service log.
func New(w io.Writer, level logging.Level) logging.Logger {
	enc := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		LevelKey:    "level",
		MessageKey:  "msg",
		LineEnding:  zapcore.DefaultLineEnding,
		EncodeLevel: zapcore.CapitalLevelEncoder,
	})
	core := zapcore.NewCore(enc, zapcore.AddSync(w), zapLevel(level))
	return logger{zap.New(core).Sugar()}
}

func zapLevel(l logging.Level) zapcore.Level {
	switch l {
	case logging.Debug:
		return zapcore.DebugLevel
	case logging.Info:
		return zapcore.InfoLevel
	case logging.Warn:
		return zapcore.WarnLevel
	}
	return zapcore.ErrorLevel
}

func (l logger) Debug(msg string, attrs ...any) { l.s.Debugw(msg, attrs...) }
func (l logger) Info(msg string, attrs ...any)  { l.s.Infow(msg, attrs...) }
func (l logger) Warn(msg string, attrs ...any)  { l.s.Warnw(msg, attrs...) }
func (l logger) Error(msg string, attrs ...any) { l.s.Errorw(msg, attrs...) }
