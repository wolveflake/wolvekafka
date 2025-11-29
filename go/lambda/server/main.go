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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"wolvekafka/go/common/logger"
	lambda "wolvekafka/go/lambda"
	model_pb "wolvekafka/proto/model"
)

var (
	listenAddr = flag.String("listen", "", "address for the lambda HTTP server")
)

func main() {
	flag.Parse()
	addr := resolveListenAddr(*listenAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", helloHandler)
	mux.HandleFunc("/health", healthHandler)

	srv := &http.Server{Addr: addr, Handler: mux}

	logger.S().Infow("starting lambda service", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.S().Fatalf("lambda service stopped: %v", err)
	}
}

func resolveListenAddr(override string) string {
	if override != "" {
		return override
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	return ":" + port
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.S().Warnf("failed to read request body: %v", err)
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var lambdaReq model_pb.LambdaRequest
	if err := protojson.Unmarshal(body, &lambdaReq); err != nil {
		logger.S().Warnf("invalid lambda request: %v", err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if len(lambdaReq.GetMessages()) == 0 {
		http.Error(w, "lambda request payload missing messages", http.StatusBadRequest)
		return
	}

	for _, msg := range lambdaReq.GetMessages() {
		lambda.PrintMessage(fmt.Sprintf("[%s:%d offset=%d] %s", msg.GetTopic(), msg.GetPartition(), msg.GetOffset(), string(msg.GetValue())))
	}

	writeProtoJSON(w, http.StatusOK, &model_pb.LambdaResponse{
		Messages: []*model_pb.ResponseMessage{
			{Status: model_pb.ResponseStatus_STATUS_SUCCESS},
		},
	})
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		logger.S().Warnf("failed to encode response: %v", err)
	}
}

func writeProtoJSON(w http.ResponseWriter, status int, msg proto.Message) {
	payload, err := protojson.Marshal(msg)
	if err != nil {
		logger.S().Warnf("failed to marshal response: %v", err)
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(payload); err != nil {
		logger.S().Warnf("failed to write response: %v", err)
	}
}
