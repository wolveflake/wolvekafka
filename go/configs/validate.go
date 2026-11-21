// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"fmt"
	"net"
	"net/url"
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
	hasUserPassword := cluster.GetSaslUsername() != "" || cluster.GetSaslPassword() != ""
	switch auth := cluster.GetSaslAuth().(type) {
	case nil:
		if mechanism != "" && mechanism != "PLAIN" && mechanism != "SCRAM-SHA-256" && mechanism != "SCRAM-SHA-512" {
			return fmt.Errorf("unsupported SASL mechanism %q (use oauthbearer or aws_msk_iam for token-based SASL)", mechanism)
		}
		if mechanism != "" || hasUserPassword {
			if cluster.GetSaslUsername() == "" || cluster.GetSaslPassword() == "" {
				return fmt.Errorf("SASL requires both sasl_username and sasl_password")
			}
		}
	case *configs_pb.Cluster_Oauthbearer:
		if mechanism != "" || hasUserPassword {
			return fmt.Errorf("oauthbearer cannot be combined with sasl_username, sasl_password, or sasl_mechanism")
		}
		if err := validateOAuthBearer(auth.Oauthbearer); err != nil {
			return err
		}
	case *configs_pb.Cluster_AwsMskIam:
		if mechanism != "" || hasUserPassword {
			return fmt.Errorf("aws_msk_iam cannot be combined with sasl_username, sasl_password, or sasl_mechanism")
		}
		if !cluster.GetTls().GetEnabled() {
			return fmt.Errorf("aws_msk_iam requires tls.enabled=true")
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

func validateOAuthBearer(oauth *configs_pb.OAuthBearer) error {
	if oauth == nil {
		return fmt.Errorf("oauthbearer settings are required")
	}
	if oauth.GetClientId() == "" || oauth.GetClientSecret() == "" {
		return fmt.Errorf("oauthbearer requires client_id and client_secret")
	}
	u, err := url.Parse(oauth.GetTokenUrl())
	if err != nil || u.Host == "" {
		return fmt.Errorf("oauthbearer token_url must be an absolute URL")
	}
	// The client secret is sent to this endpoint, so only loopback may skip TLS.
	if !strings.EqualFold(u.Scheme, "https") && !(strings.EqualFold(u.Scheme, "http") && isLoopback(u.Hostname())) {
		return fmt.Errorf("oauthbearer token_url must use https")
	}
	return nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// MaxLambdaTimeoutMs is the largest accepted lambda timeout_ms (15 minutes).
const MaxLambdaTimeoutMs = 15 * 60 * 1000

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
			if err := validateGRPCTarget(lambda.GetUrl()); err != nil {
				return fmt.Errorf("consumer %s: %w", consumer.GetGroupId(), err)
			}
			if lambda.GetTimeoutMs() > MaxLambdaTimeoutMs {
				return fmt.Errorf("consumer %s lambda %s timeout_ms %d exceeds the maximum of %d", consumer.GetGroupId(), lambda.GetUrl(), lambda.GetTimeoutMs(), MaxLambdaTimeoutMs)
			}
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

// validateGRPCTarget checks grpc:// and grpcs:// lambda URLs, which address a
// host and port only. Other URLs are HTTP and are checked when called.
func validateGRPCTarget(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}
	switch strings.ToLower(u.Scheme) {
	case "grpc", "grpcs":
	default:
		return nil
	}
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("gRPC lambda url %q must be %s://host:port with no path, query, or credentials", u.Redacted(), strings.ToLower(u.Scheme))
	}
	if host, port, err := net.SplitHostPort(u.Host); err != nil || host == "" || port == "" {
		return fmt.Errorf("gRPC lambda url %q must include a host and port", u.Redacted())
	}
	return nil
}
