// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"github.com/stretchr/testify/assert"
	"testing"
	configs_pb "wolvekafka/proto/configs"
)

func TestSecurityValidation(t *testing.T) {
	for _, cluster := range []*configs_pb.Cluster{
		{SaslUsername: "user"}, {SaslPassword: "secret"},
		{SaslMechanism: "unknown"}, {SaslMechanism: "SCRAM-SHA-256"},
		{Tls: &configs_pb.TLS{CaFile: "ca.pem"}},
		{Tls: &configs_pb.TLS{Enabled: true, CertFile: "cert.pem"}},
	} {
		cluster.BootstrapServers = []string{"localhost:9092"}
		assert.Error(t, ValidateCluster(cluster))
	}
}

func TestProtoJSONPreservesPoliciesAndRejectsTypos(t *testing.T) {
	cfg, err := ParseConfigsJson([]byte(`{"cluster":{"bootstrap_servers":["localhost:9092"],"tls":{"enabled":true}},"consumers":[{"group_id":"g","topics":["in"],"lambdas":[{"url":"http://localhost"}],"batch":{"max_messages":5},"retry_policy":{"dlq":{"topic":"dead"}},"kafka":{"topic":"out"}}]}`))
	assert.NoError(t, err)
	if assert.Len(t, cfg.GetConsumers(), 1) {
		assert.EqualValues(t, 5, cfg.Consumers[0].GetBatch().GetMaxMessages())
		assert.Equal(t, "dead", cfg.Consumers[0].GetRetryPolicy().GetDlq().GetTopic())
		assert.Equal(t, "out", cfg.Consumers[0].GetKafka().GetTopic())
	}
	_, err = ParseConfigsJson([]byte(`{"cluster":{"sasl_mechanim":"PLAIN"}}`))
	assert.Error(t, err)
}
