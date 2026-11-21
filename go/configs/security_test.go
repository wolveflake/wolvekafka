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
		// OAUTHBEARER needs complete client-credentials settings over https.
		{SaslMechanism: "OAUTHBEARER"},
		{SaslMechanism: "OAUTHBEARER", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "https://idp/token", ClientId: "id"}},
		{SaslMechanism: "OAUTHBEARER", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "http://idp.example.com/token", ClientId: "id", ClientSecret: "s"}},
		{SaslMechanism: "OAUTHBEARER", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "/token", ClientId: "id", ClientSecret: "s"}},
		{SaslMechanism: "OAUTHBEARER", SaslUsername: "u", SaslPassword: "p", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "https://idp/token", ClientId: "id", ClientSecret: "s"}},
		{SaslUsername: "u", SaslPassword: "p", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "https://idp/token", ClientId: "id", ClientSecret: "s"}},
		// AWS_MSK_IAM takes no static credentials and requires TLS.
		{SaslMechanism: "AWS_MSK_IAM"},
		{SaslMechanism: "AWS_MSK_IAM", SaslUsername: "u", SaslPassword: "p", Tls: &configs_pb.TLS{Enabled: true}},
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

func TestSecurityValidationAcceptsOAuthAndMSKIAM(t *testing.T) {
	for _, cluster := range []*configs_pb.Cluster{
		{SaslMechanism: "OAUTHBEARER", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "https://idp.example.com/token", ClientId: "id", ClientSecret: "s"}},
		{SaslMechanism: "oauthbearer", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "http://127.0.0.1:8080/token", ClientId: "id", ClientSecret: "s"}},
		{SaslMechanism: "OAUTHBEARER", Oauthbearer: &configs_pb.OAuthBearer{TokenUrl: "http://localhost/token", ClientId: "id", ClientSecret: "s"}},
		{SaslMechanism: "AWS_MSK_IAM", Tls: &configs_pb.TLS{Enabled: true}},
	} {
		cluster.BootstrapServers = []string{"localhost:9092"}
		assert.NoError(t, ValidateCluster(cluster))
	}
}

func TestOAuthBearerConfigFromJSON(t *testing.T) {
	cfg, err := ParseConfigsJson([]byte(`{"cluster":{"bootstrap_servers":["b:9093"],"sasl_mechanism":"OAUTHBEARER","tls":{"enabled":true},"oauthbearer":{"token_url":"https://idp/token","client_id":"id","client_secret":"s","scopes":["kafka"],"audience":"aud","extensions":{"logicalCluster":"lkc-1"}}},"consumers":[]}`))
	assert.NoError(t, err)
	assert.Equal(t, "lkc-1", cfg.GetCluster().GetOauthbearer().GetExtensions()["logicalCluster"])
	assert.NoError(t, ValidateCluster(cfg.GetCluster()))
}
