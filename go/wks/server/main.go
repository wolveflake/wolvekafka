// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"wolvekafka/go/common/logger"
	server "wolvekafka/go/common/server"
	"wolvekafka/go/wks"
)

var (
	serviceID   = flag.String("service_id", "wolvekafka", "Unique service identifier used for registration and logs.")
	configPath  = flag.String("config_path", "/etc/wks/config/config.json", "Path to the JSON configuration consumed by the WKS service.")
	showVersion = flag.Bool("version", false, "Print the WKS version and source revision, then exit.")
	lagMetrics  = flag.String("lag_metrics", wks.LagByPartition, "Consumer lag reporting: \"partition\" (one series per partition), \"topic\" (summed per topic), or \"off\".")
	lagRefresh  = flag.Duration("lag_refresh_interval", 15*time.Second, "How often each worker reads the newest partition offsets from Kafka for lag metrics.")
	version     = "dev"
	commit      = "unknown"
)

func main() {
	flag.Parse()
	if *showVersion {
		fmt.Printf("wks %s (%s)\n", version, commit)
		return
	}

	cfg, err := wks.LoadConfigFromFile(*configPath)
	if err != nil {
		logger.S().Fatalf("failed to load config: %v", err)
	}

	ctx := context.Background()
	// Initialize the composite server (which configures OpenTelemetry) before
	// starting the WKS service so the instrumentation binds to the real meter.
	srv := server.SetUpServer(ctx, server.PROTO_GRPC, *serviceID)

	wksService, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{
		Config: cfg,
		LagMetrics: wks.LagMetricsOptions{
			Granularity:     *lagMetrics,
			RefreshInterval: *lagRefresh,
		},
	})
	if err != nil {
		srv.Stop()
		logger.S().Fatalf("failed to start WKS service: %v", err)
	}
	defer func() {
		if closeErr := wksService.Close(); closeErr != nil {
			logger.S().Warnf("failed to stop WKS service: %v", closeErr)
		}
	}()
	go wksService.WatchConfig(ctx, *configPath, 10*time.Second)
	if err := srv.Serve(); err != nil {
		logger.S().Fatalf("server stopped: %v", err)
	}
}
