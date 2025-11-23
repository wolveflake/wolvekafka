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
	"net/http"
	"os"
	"strings"

	"wolvekafka/go/common/logger"
	lambda "wolvekafka/go/lambda"
)

var (
	listenAddr = flag.String("listen", "", "address for the lambda HTTP server")
)

type helloRequest struct {
	Message string `json:"message"`
}

type helloResponse struct {
	Message string `json:"message"`
}

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

	var req helloRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.S().Warnf("invalid hello request: %v", err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		http.Error(w, "message is required", http.StatusBadRequest)
		return
	}

	lambda.PrintMessage(req.Message)

	writeJSON(w, http.StatusOK, helloResponse{Message: req.Message})
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
