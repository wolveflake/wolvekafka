// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"wolvekafka/go/configs"
	configs_pb "wolvekafka/proto/configs"
)

// ConnectionOptions is shared by consumers, output producers and DLQ producers.
// Invalid security configuration fails before a Kafka client is started.
func ConnectionOptions(cluster *configs_pb.Cluster) ([]kgo.Opt, error) {
	if err := configs.ValidateCluster(cluster); err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(cluster.GetBootstrapServers()...)}
	mechanism, err := saslMechanism(cluster)
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	if cfg := cluster.GetTls(); cfg.GetEnabled() {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.GetServerName()}
		if cfg.GetCaFile() != "" {
			pem, err := os.ReadFile(cfg.GetCaFile())
			if err != nil {
				return nil, fmt.Errorf("read Kafka TLS CA: %w", err)
			}
			tlsCfg.RootCAs = x509.NewCertPool()
			if !tlsCfg.RootCAs.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("Kafka TLS CA file contains no certificates")
			}
		}
		if cfg.GetCertFile() != "" {
			cert, err := tls.LoadX509KeyPair(cfg.GetCertFile(), cfg.GetKeyFile())
			if err != nil {
				return nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	return opts, nil
}
