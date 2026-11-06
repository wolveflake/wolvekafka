// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraceExporterKind(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
		err  bool
	}{
		{name: "default keeps console", env: nil, want: traceExporterConsole},
		{name: "otlp endpoint selects otlp", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318"}, want: traceExporterOTLP},
		{name: "traces endpoint selects otlp", env: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://collector:4318/v1/traces"}, want: traceExporterOTLP},
		{name: "explicit otlp", env: map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}, want: traceExporterOTLP},
		{name: "explicit console wins over endpoint", env: map[string]string{"OTEL_TRACES_EXPORTER": "console", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318"}, want: traceExporterConsole},
		{name: "none disables export", env: map[string]string{"OTEL_TRACES_EXPORTER": " None "}, want: traceExporterNone},
		{name: "unknown is rejected", env: map[string]string{"OTEL_TRACES_EXPORTER": "zipkin"}, err: true},
		{name: "lists are rejected", env: map[string]string{"OTEL_TRACES_EXPORTER": "otlp,console"}, err: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := traceExporterKind(func(key string) string { return tc.env[key] })
			if tc.err {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNewTracerProviderModes(t *testing.T) {
	for _, mode := range []string{"none", "console", "otlp"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_EXPORTER", mode)
			// Point OTLP at a closed local port; creation must not dial.
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
			tp, err := newTracerProvider(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, tp)
			assert.NoError(t, tp.Shutdown(context.Background()))
		})
	}

	t.Setenv("OTEL_TRACES_EXPORTER", "zipkin")
	_, err := newTracerProvider(context.Background())
	assert.Error(t, err)
}
