// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"fmt"

	"wolvekafka/go/common/logger"

	"go.uber.org/zap"
	configs_pb "wolvekafka/proto/configs"
)

type Config struct {
	proto *configs_pb.Config

	// logger.
	log *zap.SugaredLogger
}

func NewConfigFromProto(proto *configs_pb.Config) (*Config, error) {
	if proto == nil {
		return nil, fmt.Errorf("nil config proto")
	}

	cfg := &Config{
		proto: proto,

		log: logger.S(),
	}

	err := Validate(cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid config proto: %v", err)
	}

	return cfg, nil
}

func (c *Config) Consumers() []*configs_pb.Consumer {
	return c.proto.GetConsumers()
}

func (c *Config) Cluster() *configs_pb.Cluster {
	return c.proto.GetCluster()
}
