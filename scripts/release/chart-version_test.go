// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestChartVersionCommand(t *testing.T) {
	chart := filepath.Join(t.TempDir(), "Chart.yaml")
	initial := "apiVersion: v2\nname: wks\nversion: 0.1.0\nappVersion: \"v0.1.0\"\n"
	if err := os.WriteFile(chart, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}
	runChartCommand(t, true, "-chart", chart, "-check")
	runChartCommand(t, true, "-chart", chart, "v0.2.0")
	runChartCommand(t, true, "-chart", chart, "-check")
	runChartCommand(t, true, "-chart", chart, "-check", "v0.2.0")
	runChartCommand(t, false, "-chart", chart, "-check", "v0.1.0")
	content, err := os.ReadFile(chart)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "version: 0.2.0\nappVersion: \"v0.2.0\"") {
		t.Fatalf("chart metadata not updated: %s", content)
	}
}

func runChartCommand(t *testing.T, wantSuccess bool, args ...string) {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "chart-version.go"}, args...)...)
	output, err := cmd.CombinedOutput()
	if (err == nil) != wantSuccess {
		t.Fatalf("command with %v: err=%v output=%s", args, err, output)
	}
}
