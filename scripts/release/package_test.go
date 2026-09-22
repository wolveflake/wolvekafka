// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteArchiveDeterministic(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "binary")
	if err := os.WriteFile(binary, []byte("binary-content"), 0o755); err != nil {
		t.Fatal(err)
	}
	entries := []entry{{name: "wks", path: binary, mode: 0o755}}
	first := filepath.Join(dir, "first.tar.gz")
	second := filepath.Join(dir, "second.tar.gz")
	const epoch = int64(1735689600)
	for _, path := range []string{first, second} {
		if err := writeArchive(path, epoch, entries); err != nil {
			t.Fatal(err)
		}
	}
	a, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("archives differ despite identical inputs")
	}
	gz, err := gzip.NewReader(bytes.NewReader(a))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	if !gz.ModTime.Equal(time.Unix(epoch, 0)) {
		t.Fatalf("gzip timestamp = %v", gz.ModTime)
	}
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != "wks" || hdr.Mode != 0o755 || hdr.Uid != 0 || hdr.Gid != 0 || !hdr.ModTime.Equal(time.Unix(epoch, 0)) {
		t.Fatalf("unexpected tar header: %+v", hdr)
	}
	content, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "binary-content" {
		t.Fatalf("unexpected content: %q", content)
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("expected end of archive, got %v", err)
	}
}

func TestWriteArchiveRemovesPartialOutput(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "release.tar.gz")
	missing := filepath.Join(dir, "missing")
	if err := writeArchive(output, 1735689600, []entry{{name: "wks", path: missing, mode: 0o755}}); err == nil {
		t.Fatal("expected missing input error")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("partial archive remains: %v", err)
	}
	valid := filepath.Join(dir, "wks")
	if err := os.WriteFile(valid, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeArchive(output, 1735689600, []entry{{name: "wks", path: valid, mode: 0o755}}); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
}
