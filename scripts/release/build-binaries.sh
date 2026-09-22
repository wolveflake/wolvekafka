#!/usr/bin/env bash
# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

tag=${1:?usage: build-binaries.sh vX.Y.Z[.-prerelease] COMMIT [OUTPUT_DIR]}
commit=${2:?full commit SHA is required}
out_dir=${3:-dist}

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "release tag must be a SemVer tag such as v0.1.0 or v0.1.0-rc.1" >&2
  exit 2
fi
if [[ ! "$commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "commit must be a full 40-character SHA" >&2
  exit 2
fi

repo_dir=$(git rev-parse --show-toplevel)
epoch=${SOURCE_DATE_EPOCH:-$(git -C "$repo_dir" show -s --format=%ct "$commit")}
if [[ ! "$epoch" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be a Unix timestamp" >&2
  exit 2
fi

mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)
build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT

for arch in amd64 arm64; do
  archive="$out_dir/wks_${tag}_linux_${arch}.tar.gz"
  if [[ -e "$archive" ]]; then
    echo "refusing to overwrite $archive" >&2
    exit 2
  fi
  docker buildx build \
    --file "$repo_dir/examples/quickstart/Dockerfile" \
    --target binary \
    --platform "linux/$arch" \
    --build-arg "VERSION=$tag" \
    --build-arg "VCS_REF=$commit" \
    --output "type=local,dest=$build_dir/$arch" \
    "$repo_dir"
  go run "$repo_dir/scripts/release/package.go" \
    -binary "$build_dir/$arch/wks" \
    -license "$repo_dir/LICENSE" \
    -notice "$repo_dir/NOTICE" \
    -output "$archive" \
    -epoch "$epoch"
done
