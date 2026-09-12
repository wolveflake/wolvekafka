// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"google.golang.org/protobuf/encoding/protojson"

	configs_pb "wolvekafka/proto/configs"
)

func ParseConfigsJson(jsonBytes []byte) (*configs_pb.Config, error) {
	cfg := &configs_pb.Config{}
	err := protojson.Unmarshal(jsonBytes, cfg)
	return cfg, err
}
