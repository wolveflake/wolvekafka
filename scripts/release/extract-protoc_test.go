// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/zip"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestExtractProtocKeepsStandardIncludes(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "protoc.zip")
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(archiveFile)
	for name, body := range map[string]string{
		"bin/protoc":                                    "binary",
		"include/google/protobuf/any.proto":             "standard type",
		"include/google/protobuf/compiler/plugin.proto": "plugin type",
		"readme.txt":                                    "not installed",
	} {
		file, err := archive.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archiveFile.Close(); err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(dir, "install")
	cmd := exec.Command("go", "run", "extract-protoc.go", archivePath, prefix)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("extractor failed: %v\n%s", err, output)
	}
	for name, want := range map[string]string{
		"bin/protoc":                                    "binary",
		"include/google/protobuf/any.proto":             "standard type",
		"include/google/protobuf/compiler/plugin.proto": "plugin type",
	} {
		got, err := os.ReadFile(filepath.Join(prefix, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	if _, err := os.Stat(filepath.Join(prefix, "readme.txt")); !os.IsNotExist(err) {
		t.Fatalf("unexpected unrelated file: %v", err)
	}
}
