// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

// A WolveKafka handler as a gRPC service.
package main

import (
	"context"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "example.com/grpc-handler/handlerv1"
)

type handler struct {
	pb.UnimplementedHandlerServiceServer
}

func (handler) Handle(ctx context.Context, req *pb.LambdaRequest) (*pb.LambdaResponse, error) {
	resp := &pb.LambdaResponse{}
	for _, m := range req.GetMessages() {
		log.Printf("[go] %s[%d]@%d: %s", m.GetTopic(), m.GetPartition(), m.GetOffset(), m.GetValue()) // your business logic
		resp.Messages = append(resp.Messages, &pb.ResponseMessage{Status: pb.ResponseStatus_STATUS_SUCCESS})
	}
	return resp, nil
}

func main() {
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}
	// Recycling connections lets WolveKafka pick up new handler replicas.
	srv := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 5 * time.Minute}))
	pb.RegisterHandlerServiceServer(srv, handler{})
	log.Print("[go] gRPC handler listening on :9090")
	log.Fatal(srv.Serve(lis))
}
