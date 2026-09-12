// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"net"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewConn returns grpc.Conn instance for discovered serverName in SD.
func NewConn(address string, port int) (*grpc.ClientConn, error) {
	if port == 0 {
		port = defaultGrpcPort
	}
	dialAddr := net.JoinHostPort(address, strconv.Itoa(port))
	dialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.NewClient(dialAddr, dialOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %v, error: %w", dialAddr, err)
	}

	return conn, nil
}
