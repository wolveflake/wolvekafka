// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestHttpServerNoSD tests http server initialization without SD part.
func TestHttpServerNoSD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	params := Params{
		HttpPort: 0, // auto-assigned.
		Id:       "test",
		Proto:    PROTO_HTTP,
	}
	srv, err := NewCompositeServer(ctx, params)
	assert.NoError(t, err)

	go func() {
		err := srv.Serve()
		assert.NoError(t, err)
	}()
	resp, err := http.Get(fmt.Sprintf("http://%s/health", srv.HttpAddrPort()))
	assert.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.Equal(t, "OK", string(body))
}
