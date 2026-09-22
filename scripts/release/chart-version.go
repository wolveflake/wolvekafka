// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

//go:build ignore

// chart-version.go keeps the source Helm chart's version and appVersion in
// sync. Run it before creating a release tag; CI verifies the committed file.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var releaseTag = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`)

func main() {
	check := flag.Bool("check", false, "verify chart metadata without changing it")
	chart := flag.String("chart", "services/wks/Chart.yaml", "path to Chart.yaml")
	flag.Parse()
	if flag.NArg() > 1 || (!*check && flag.NArg() != 1) {
		fatal("usage: go run chart-version.go [-check] [-chart PATH] [vX.Y.Z]")
	}
	input, err := os.ReadFile(*chart)
	if err != nil {
		fatal("read chart: %v", err)
	}
	version, appVersion, err := chartVersions(input)
	if err != nil {
		fatal("read chart metadata: %v", err)
	}
	tag := "v" + version
	if flag.NArg() == 1 {
		tag = flag.Arg(0)
	}
	if !releaseTag.MatchString(tag) {
		fatal("invalid release tag %q", tag)
	}
	if *check {
		if version != strings.TrimPrefix(tag, "v") || appVersion != tag {
			fatal("chart version %q and appVersion %q must match %q; run go run scripts/release/chart-version.go %s before tagging", version, appVersion, tag, tag)
		}
		return
	}
	updated := updateChartVersions(input, tag)
	if bytes.Equal(input, updated) {
		return
	}
	if err := writeAtomically(*chart, updated); err != nil {
		fatal("write chart: %v", err)
	}
}

func chartVersions(input []byte) (version, appVersion string, err error) {
	versionCount, appCount := 0, 0
	for _, line := range strings.Split(string(input), "\n") {
		switch {
		case strings.HasPrefix(line, "version:"):
			version = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "version:")), `"'`)
			versionCount++
		case strings.HasPrefix(line, "appVersion:"):
			appVersion = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "appVersion:")), `"'`)
			appCount++
		}
	}
	if versionCount != 1 || appCount != 1 {
		return "", "", fmt.Errorf("expected exactly one root version and appVersion field")
	}
	return version, appVersion, nil
}

func updateChartVersions(input []byte, tag string) []byte {
	lines := strings.Split(string(input), "\n")
	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "version:"):
			lines[i] = "version: " + strings.TrimPrefix(tag, "v")
		case strings.HasPrefix(line, "appVersion:"):
			lines[i] = "appVersion: \"" + tag + "\""
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

func writeAtomically(path string, contents []byte) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".chart-version-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(info.Mode().Perm()); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(contents); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
