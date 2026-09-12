// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ZapLoggerType string

const (
	Fast   ZapLoggerType = "logger"
	Simple ZapLoggerType = "sugar"
)

var (
	simpleOnce           sync.Once
	fastOnce             sync.Once
	simpleLoggerInstance *zap.SugaredLogger
	fastLoggerInstance   *zap.Logger
)

func getLogger(loggerType ZapLoggerType) interface{} {
	config := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.InfoLevel),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:      "time",
			LevelKey:     "level",
			MessageKey:   "message",
			CallerKey:    "caller",
			EncodeLevel:  zapcore.CapitalLevelEncoder,
			EncodeTime:   zapcore.ISO8601TimeEncoder,
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	if loggerType == Fast {
		return zap.Must(config.Build())
	}
	return zap.Must(config.Build()).Sugar()
}

func S() *zap.SugaredLogger {
	simpleOnce.Do(func() {
		simpleLoggerInstance = getLogger(Simple).(*zap.SugaredLogger)
	})
	return simpleLoggerInstance
}

func F() *zap.Logger {
	fastOnce.Do(func() {
		fastLoggerInstance = getLogger(Fast).(*zap.Logger)
	})
	return fastLoggerInstance
}
