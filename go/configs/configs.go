package configs

import (
	"context"
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

func NewConfigFromProto(ctx context.Context, proto *configs_pb.Config) (*Config, error) {
	if proto == nil {
		return nil, fmt.Errorf("nil config proto")
	}

	return &Config{
		proto: proto,

		log: logger.S(),
	}, nil
}
