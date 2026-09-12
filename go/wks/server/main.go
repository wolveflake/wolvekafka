///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2025 [wolveflake LLC], All Rights Reserved.
///
/// NOTICE:  All information contained herein is, and remains the property of COMPANY. The intellectual and technical concepts contained
/// herein are proprietary to COMPANY and may be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
/// Dissemination of this information or reproduction of this material is strictly forbidden unless prior written permission is obtained
/// from COMPANY.  Access to the source code contained herein is hereby forbidden to anyone except current COMPANY employees, managers or contractors who have executed
/// Confidentiality and Non-disclosure agreements explicitly covering such access.
///
/// The copyright notice above does not evidence any actual or intended publication or disclosure  of  this source code, which includes
/// information that is confidential and/or proprietary, and is a trade secret, of  COMPANY.   ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE,
/// OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE CODE  WITHOUT  THE EXPRESS WRITTEN CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
/// LAWS AND INTERNATIONAL TREATIES.  THE RECEIPT OR POSSESSION OF  THIS SOURCE CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS
/// TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT  MAY DESCRIBE, IN WHOLE OR IN PART.
///

package main

import (
	"context"
	"flag"
	"time"

	"wolvekafka/go/common/logger"
	server "wolvekafka/go/common/server"
	"wolvekafka/go/wks"
)

var (
	serviceID  = flag.String("service_id", "wolvekafka", "Unique service identifier used for registration and logs.")
	configPath = flag.String("config_path", "/etc/wks/config/config.json", "Path to the JSON configuration consumed by the WKS service.")
)

func main() {
	flag.Parse()

	cfg, err := wks.LoadConfigFromFile(*configPath)
	if err != nil {
		logger.S().Fatalf("failed to load config: %v", err)
	}

	ctx := context.Background()
	// Initialize the composite server (which configures OpenTelemetry) before
	// starting the WKS service so the instrumentation binds to the real meter.
	srv := server.SetUpServer(ctx, server.PROTO_GRPC, *serviceID)

	wksService, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
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
