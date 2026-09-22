// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

//go:build ignore

// extract-protoc.go extracts protoc and its standard proto includes from the
// verified upstream zip, without installing distribution packages.
package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: go run extract-protoc.go PROTOC_ZIP PREFIX")
		os.Exit(2)
	}
	if err := extractProtoc(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func extractProtoc(archivePath, prefix string) error {
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	foundBinary := false
	foundIncludes := false
	for _, file := range archive.File {
		if file.Name != "bin/protoc" && !strings.HasPrefix(file.Name, "include/google/protobuf/") {
			continue
		}
		if file.FileInfo().IsDir() {
			continue
		}
		if path.Clean(file.Name) != file.Name {
			return fmt.Errorf("unsafe archive path %q", file.Name)
		}
		outputPath := filepath.Join(prefix, filepath.FromSlash(file.Name))
		if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
			return err
		}
		input, err := file.Open()
		if err != nil {
			return err
		}
		mode := os.FileMode(0o644)
		if file.Name == "bin/protoc" {
			mode = 0o755
			foundBinary = true
		} else {
			foundIncludes = true
		}
		output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		inputErr := input.Close()
		outputErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		if inputErr != nil {
			return inputErr
		}
		if outputErr != nil {
			return outputErr
		}
	}
	if !foundBinary || !foundIncludes {
		return fmt.Errorf("protoc binary or standard includes missing from %s", archivePath)
	}
	return nil
}
