// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"wolvekafka/go/configs"
)

func LoadConfigFromFile(path string) (*configs.Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parseConfig(contents)
}

func parseConfig(contents []byte) (*configs.Config, error) {
	protoCfg, err := configs.ParseConfigsJson(contents)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return configs.NewConfigFromProto(protoCfg)
}

// WatchConfig follows ConfigMap symlink swaps and compares file contents, so
// replacements with unchanged modification times are detected. Invalid updates
// leave the active configuration running and are retried on the next tick.
func (w *WolveKafkaService) WatchConfig(ctx context.Context, path string, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var accepted []byte
	for {
		if ctx.Err() != nil || w.ctx.Err() != nil {
			return
		}
		contents, err := os.ReadFile(path)
		if err == nil && !bytes.Equal(contents, accepted) {
			var cfg *configs.Config
			cfg, err = parseConfig(contents)
			if err == nil {
				err = w.updateConfig(cfg)
			}
			if err == nil {
				accepted = contents
				w.log.Info("config watcher: configuration applied")
			}
		}
		if err != nil {
			w.log.Errorf("config watcher: configuration not applied; will retry (teardown failures may leave old workers stopped): %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-w.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
