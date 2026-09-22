// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

// package.go writes a deterministic release tarball: fixed member order,
// permissions, ownership, and timestamps, with no gzip filename or clock time.
package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	binary := flag.String("binary", "", "path to the WKS binary")
	license := flag.String("license", "", "path to LICENSE")
	notice := flag.String("notice", "", "path to NOTICE")
	output := flag.String("output", "", "tar.gz output path")
	epoch := flag.Int64("epoch", 0, "SOURCE_DATE_EPOCH")
	flag.Parse()
	if *binary == "" || *license == "" || *notice == "" || *output == "" || *epoch <= 0 {
		fmt.Fprintln(os.Stderr, "-binary, -license, -notice, -output, and positive -epoch are required")
		os.Exit(2)
	}
	if err := writeArchive(*output, *epoch, []entry{
		{name: "LICENSE", path: *license, mode: 0o644},
		{name: "NOTICE", path: *notice, mode: 0o644},
		{name: "wks", path: *binary, mode: 0o755},
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type entry struct {
	name string
	path string
	mode int64
}

func writeArchive(path string, epoch int64, entries []entry) (err error) {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	gz := gzip.NewWriter(out)
	gz.Header.ModTime = time.Unix(epoch, 0).UTC()
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	for _, item := range entries {
		in, openErr := os.Open(item.path)
		if openErr != nil {
			return openErr
		}
		info, statErr := in.Stat()
		if statErr != nil {
			in.Close()
			return statErr
		}
		header := &tar.Header{
			Name: item.name, Mode: item.mode, Size: info.Size(),
			ModTime: time.Unix(epoch, 0).UTC(), Typeflag: tar.TypeReg,
			Uid: 0, Gid: 0, Uname: "", Gname: "", Format: tar.FormatUSTAR,
		}
		if writeErr := tw.WriteHeader(header); writeErr != nil {
			in.Close()
			return writeErr
		}
		_, copyErr := io.Copy(tw, in)
		closeErr := in.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}
