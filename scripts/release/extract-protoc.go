// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

//go:build ignore

// extract-protoc.go extracts only the protoc executable from the verified
// upstream zip, without installing distribution packages during the build.
package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: go run extract-protoc.go PROTOC_ZIP OUTPUT")
		os.Exit(2)
	}
	if err := extractProtoc(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func extractProtoc(archivePath, outputPath string) error {
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	for _, file := range archive.File {
		if file.Name != "bin/protoc" {
			continue
		}
		input, err := file.Open()
		if err != nil {
			return err
		}
		output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
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
		return outputErr
	}
	return fmt.Errorf("bin/protoc not found in %s", archivePath)
}
