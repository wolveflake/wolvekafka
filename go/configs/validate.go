// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"fmt"
	"strings"

	configs_pb "wolvekafka/proto/configs"
)

// ValidateCluster to ensure required fields are present.
func ValidateCluster(cluster *configs_pb.Cluster) error {
	if cluster == nil {
		return fmt.Errorf("cluster configuration is required")
	}
	if len(cluster.BootstrapServers) == 0 {
		return fmt.Errorf("cluster bootstrap_servers is required")
	}
	for _, broker := range cluster.BootstrapServers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("cluster bootstrap_servers contains an empty broker")
		}
	}
	mechanism := strings.ToUpper(cluster.GetSaslMechanism())
	if mechanism != "" && mechanism != "PLAIN" && mechanism != "SCRAM-SHA-256" && mechanism != "SCRAM-SHA-512" {
		return fmt.Errorf("unsupported SASL mechanism %q", mechanism)
	}
	if mechanism != "" || cluster.GetSaslUsername() != "" || cluster.GetSaslPassword() != "" {
		if cluster.GetSaslUsername() == "" || cluster.GetSaslPassword() == "" {
			return fmt.Errorf("SASL requires both sasl_username and sasl_password")
		}
	}
	if tls := cluster.GetTls(); tls != nil {
		if !tls.GetEnabled() && (tls.GetCaFile() != "" || tls.GetCertFile() != "" || tls.GetKeyFile() != "" || tls.GetServerName() != "") {
			return fmt.Errorf("TLS options require tls.enabled=true")
		}
		if (tls.GetCertFile() == "") != (tls.GetKeyFile() == "") {
			return fmt.Errorf("TLS requires both cert_file and key_file for client authentication")
		}
	}
	return nil
}

// ValidateConsumers to ensure each consumer has required fields and no duplicates.
func ValidateConsumers(consumers []*configs_pb.Consumer) error {
	groupIds := make(map[string]bool)
	for _, consumer := range consumers {
		if consumer.GetGroupId() == "" {
			return fmt.Errorf("consumer group_id is required")
		}
		if _, exists := groupIds[consumer.GetGroupId()]; exists {
			return fmt.Errorf("duplicate consumer group_id: %s", consumer.GetGroupId())
		}
		groupIds[consumer.GetGroupId()] = true
		if dlq := consumer.GetRetryPolicy().GetDlq(); dlq != nil && strings.TrimSpace(dlq.GetTopic()) == "" {
			return fmt.Errorf("consumer %s DLQ topic is required", consumer.GetGroupId())
		}
		if output := consumer.GetKafka(); output != nil && strings.TrimSpace(output.GetTopic()) == "" {
			return fmt.Errorf("consumer %s output topic is required", consumer.GetGroupId())
		}

		topics := consumer.GetTopics()
		if len(topics) == 0 {
			return fmt.Errorf("consumer %s must have at least one topic", consumer.GetGroupId())
		}

		lambdas := consumer.GetLambdas()
		if len(lambdas) == 0 {
			return fmt.Errorf("consumer %s must have at least one lambda", consumer.GetGroupId())
		}
		lambdasUrls := make(map[string]bool)
		for _, lambda := range lambdas {
			if lambda.GetUrl() == "" {
				return fmt.Errorf("consumer %s has a lambda with empty url", consumer.GetGroupId())
			}
			if _, exists := lambdasUrls[lambda.GetUrl()]; exists {
				return fmt.Errorf("consumer %s has duplicate lambda url: %s", consumer.GetGroupId(), lambda.GetUrl())
			}
			lambdasUrls[lambda.GetUrl()] = true
		}
	}

	return nil
}

// Validate the entire configuration.
func Validate(cfg *Config) error {
	err := ValidateCluster(cfg.proto.GetCluster())
	if err != nil {
		return err
	}

	err = ValidateConsumers(cfg.proto.GetConsumers())
	if err != nil {
		return err
	}

	return nil
}
