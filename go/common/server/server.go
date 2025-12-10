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

package server

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"wolvekafka/go/common/logger"
	"wolvekafka/go/common/net"
)

// list of protocols supported by server library.
type Protocol int

const (
	PROTO_GRPC Protocol = iota
	PROTO_HTTP
)

const (
	defaultGrpcPort = 8090
	defaultHttpPort = 8080
)

var (
	//list of signals to watch and stop server when it is triggered.
	StopSignals = []os.Signal{
		syscall.SIGINT,
		syscall.SIGTERM,
	}

	// body of healthcheck handler.
	httpHealthStatus = []byte("OK")

	// server related flags.
	grpcPort = flag.Int("grpc_port", defaultGrpcPort, "gRPC server port.")
	httpPort = flag.Int("http_port", defaultHttpPort, "http server port.")
)

type Params struct {
	// port on which the server is running
	GrpcPort int
	HttpPort int

	// unique id of the server
	Id string

	// Proto (grpc, http, ...)
	Proto Protocol
}

// Server represents all that is needed to run a gRPC server
type Server struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	params *Params

	// grpc server
	GRPCServer *grpc.Server
	// http server.
	HTTPServer *http.Server
	HTTPRouter *mux.Router

	// Address listened by server.
	grpcAddr string

	log          *zap.SugaredLogger
	grpcListener net.Listener
	grpcPort     int
	otelShutdown func(context.Context) error

	// http server related configuration
	httpPort     int
	httpAddr     string
	httpListener net.Listener

	sigChan chan os.Signal
}

// NewCompositeServer will register a gRPC server and HTTP server for the endpoint.
// We will encourage this set up where we start both http and gRPC routers for a given
// process as this eases operations when we need to integrate with external 3rd parties
// like Prometheus as Prometheus only supports HTTP protocol for scarping metrics
func NewCompositeServer(ctx context.Context, params Params) (*Server, error) {
	log := logger.S()
	log.Infof("Setting up server with service id: '%s', grpc port: %v, http port: %v", params.Id, params.GrpcPort, params.HttpPort)
	if err := validateParams(params.GrpcPort, params.HttpPort, params.Id); err != nil {
		return nil, err
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, StopSignals...)
	addr, err := network.GetIPAddress()
	gRPCListener, gRPCListenerPort, err := startCompositeListener(PROTO_GRPC, params)
	if err != nil {
		return nil, err
	}
	httpListener, httpListenerPort, err := startCompositeListener(PROTO_HTTP, params)
	if err != nil {
		return nil, err
	}

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return nil, err
	}

	ctxSrv, ctxSrvCancel := context.WithCancel(ctx)
	srv := &Server{
		ctx:          ctxSrv,
		ctxCancel:    ctxSrvCancel,
		log:          log,
		grpcListener: gRPCListener,
		grpcAddr:     addr,
		grpcPort:     gRPCListenerPort,
		params:       &params,
		httpAddr:     addr,
		httpListener: httpListener,
		httpPort:     httpListenerPort,
		sigChan:      sigChan,
		otelShutdown: otelShutdown,
	}

	srv.GRPCServer = grpc.NewServer()

	// hook the http
	srv.HTTPRouter = mux.NewRouter()
	srv.HTTPServer = &http.Server{
		Handler: srv.HTTPRouter,
	}
	if handler := metricsHTTPHandler(); handler != nil {
		srv.HTTPRouter.Handle("/metrics", handler)
	}

	// Register the handler for the default path
	srv.HTTPRouter.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
	})
	//srv.HTTPRouter.Use(httpMetricsInterceptor)
	// Expose the /metrics endpoint for Prometheus to scrape for both gRPC and http routers
	// srv.HTTPRouter.Handle("/metrics", promhttp.Handler())
	return srv, err
}

// startCompositeListener would start listeners for both http and gRPC server
func startCompositeListener(protocol Protocol, params Params) (net.Listener, int, error) {
	listenerType := "gRPC"
	listenPort := params.GrpcPort
	if protocol == PROTO_HTTP {
		listenerType = "http"
		listenPort = params.HttpPort
	}

	listener, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", strconv.Itoa(listenPort)))
	if err != nil {
		return nil, 0, errors.Wrapf(err, "fails to listen for listener type: %s", listenerType)
	}

	listenAddr := listener.Addr().String()
	logger.S().Infof("%s server listening addr: %v", listenerType, listenAddr)
	_, listenerPortStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to parse address:%v for listener type: %s", listenAddr, listenerType)
	}
	listenerPort, err := strconv.Atoi(listenerPortStr)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to parse port: %v for listenerType: %s", listenerPortStr, listenerType)
	}
	return listener, listenerPort, nil
}

func (s *Server) AddShutdownHook(closers ...io.Closer) {
	<-s.sigChan
	logger.S().Infof("graceful shutdown ...")
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			logger.S().Errorf("Failed to stop closer: %s", err)
		}
	}
	logger.S().Infof("complete graceful shutdown ...")
	_ = logger.S().Sync()
}
func (s *Server) Stop() {
	if s == nil {
		return
	}
	s.ctxCancel()
	if s.GRPCServer != nil {
		s.GRPCServer.Stop()
	}
	if s.HTTPServer != nil {
		_ = s.HTTPServer.Close()
	}
	s.shutdownTelemetry()
}

func (s *Server) shutdownTelemetry() {
	if s == nil || s.otelShutdown == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.otelShutdown(ctx); err != nil {
		log := s.log
		if log == nil {
			log = logger.S()
		}
		log.Warnf("failed to shut down OpenTelemetry: %v", err)
	}
	s.otelShutdown = nil
}
func (s *Server) monitorStopSignal(listener net.Listener) {
	select {
	case <-s.ctx.Done():
		s.log.Info("context has been cancelled.")
	case sig := <-s.sigChan:
		s.log.Infof("received %v signal.", sig)
		s.ctxCancel()
	}

	// close listener.
	if listener != nil {
		listener.Close()
	}
}

// Address listened by server.
func (s *Server) AddrPort() string {
	return net.JoinHostPort(s.grpcAddr, strconv.Itoa(s.grpcPort))
}
func (s *Server) Addr() string {
	return net.JoinHostPort(s.grpcAddr, strconv.Itoa(s.grpcPort))
}
func (s *Server) HttpAddrPort() string {

	return net.JoinHostPort(s.httpAddr, strconv.Itoa(s.httpPort))
}

// wait waits context cancellation or provided timeout.
// It returns true when conext has been cancelled, otherwise false.
func (s *Server) wait(delay time.Duration) bool {
	select {
	case <-time.After(delay):
		logger.S().Infof("waited: %v", delay)
	case <-s.ctx.Done():
		logger.S().Infof("context has been cancelled.")
		return true
	}

	return false
}

// grpcServe accepts incoming http connections.
func (s *Server) httpServe() error {
	logger.S().Infof("Starting HTTP Server...")

	// signal watcher
	go s.monitorStopSignal(s.httpListener)
	s.HTTPRouter.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.Write(httpHealthStatus)
	})

	// extract ip from the address
	// keep retrying till signal received.
	done := false
	for done != true {
		// serve requests.
		if err := s.HTTPServer.Serve(s.httpListener); err != nil {
			logger.S().Errorf("failed to serve HTTP server:  %v", err)
			done = s.wait(time.Second)
			continue
		}
		break
	}
	return nil
}

// grpcServe accepts incoming grpc connections.
func (s *Server) grpcServe() error {
	logger.S().Infof("Starting gRPC Server...")
	// signal watcher
	go s.monitorStopSignal(s.grpcListener)

	// register health check for the service
	healthServer := health.NewServer()
	healthServer.SetServingStatus(s.params.Id,
		grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s.GRPCServer, healthServer)

	// keep retrying till signal received.
	done := false
	for done != true {
		// serve requests.
		if err := s.GRPCServer.Serve(s.grpcListener); err != nil {
			logger.S().Errorf("failed to gRPC server:  %v", err)
			done = s.wait(time.Second)
			continue
		}
		break
	}
	return nil
}

// Serve accepts incoming connections (blocking call).
func (s *Server) Serve() error {
	defer s.shutdownTelemetry()
	go func() {
		s.grpcServe()
	}()

	go func() {
		s.httpServe()
	}()

	s.monitorStopSignal(nil)
	return nil
}

// SetUpServer will set up the gRPC server
func SetUpServer(ctx context.Context, proto Protocol, serviceId string) *Server {
	params := Params{
		GrpcPort: *grpcPort,
		HttpPort: *httpPort,
		Id:       serviceId,
		Proto:    proto,
	}

	srv, err := NewCompositeServer(ctx, params)
	if err != nil {
		logger.S().Fatalf("failed to listen: %v", err)
	}
	return srv
}

// validateParams validates params.
func validateParams(grpcPort int, httpPort int, serviceId string) error {
	if grpcPort < 0 || grpcPort > math.MaxUint32 {
		return fmt.Errorf("please provide a valid value for grpc port in start up params: %v", grpcPort)
	}
	if httpPort < 0 || httpPort > math.MaxUint32 {
		return fmt.Errorf("please provide a valid value for http port in start up params: %v", httpPort)
	}
	return nil
}
