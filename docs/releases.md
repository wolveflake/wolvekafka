# Releases and reproducible artifacts

WolveKafka publishes versioned WKS artifacts from a `vX.Y.Z` or
`vX.Y.Z-prerelease` GitHub release. The release commit must be on `main`. The
release workflow builds all artifacts from that exact commit; no local build or
developer workstation is part of publication.

Each published release is intended to contain:

| Artifact | Location |
| --- | --- |
| Linux `amd64` and `arm64` binaries (with `LICENSE` and `NOTICE`) | GitHub release assets, `wks_<tag>_linux_<arch>.tar.gz` |
| SHA-256 manifest and SPDX source SBOM | GitHub release assets, `SHA256SUMS` and `source.spdx.json` |
| Multi-platform container image, with Buildx provenance and SBOM | `ghcr.io/dguptawolveflake/wolvekafka:<tag>` |
| Matching OCI Helm chart | `oci://ghcr.io/dguptawolveflake/charts/wks`, chart version without the `v` |

The Docker builder/runtime bases, `protoc` downloads, Go plugin, and release
action revisions are pinned. Go builds use `-trimpath` and a fixed build ID;
the archives normalize timestamps, member order, ownership, and permissions.
The tag and full source commit are embedded in `wks --version` and container
labels. The tarball timestamp derives from the source commit (or an explicit
`SOURCE_DATE_EPOCH`). This makes the release inputs traceable and the binary
archives repeatable for the same toolchain and inputs. It does not promise that
an independently rebuilt OCI manifest or provenance envelope will have the
same digest.

## Verify a release

Download the two files for your architecture and `SHA256SUMS` from the same
GitHub release, then run:

```sh
sha256sum -c SHA256SUMS --ignore-missing
tar -xzf wks_v0.1.0_linux_amd64.tar.gz
./wks --version
```

Replace the example tag and architecture with the release you downloaded. The
checksum manifest also lists the other architecture, chart, and SBOM. The
`--ignore-missing` option checks only the assets you downloaded.
After the repository is public, GitHub artifact attestations can additionally
be verified with `gh attestation verify <archive> --repo
dguptawolveflake/wolvekafka`. Image provenance and an image SBOM are attached
to the GHCR image at build time.

To inspect the container without deploying it:

```sh
docker run --rm ghcr.io/dguptawolveflake/wolvekafka:v0.1.0 --version
```

See the [Helm chart guide](../services/wks/README.md) for Kubernetes installs.
The quickstart [Compose file](../examples/quickstart/compose.yaml) builds the
same Dockerfile locally and does not depend on registry availability.

## Maintainer release procedure

1. On a release-preparation branch, run
   `go run scripts/release/chart-version.go vX.Y.Z` with the intended tag.
   Commit the updated `services/wks/Chart.yaml` with the other release changes,
   let CI pass, and merge to `main`. The release workflow rejects tags that do
   not match the committed chart version and `appVersion`.
2. Create a SemVer tag pointing at that commit on `main`, then publish its
   GitHub release. The publication workflow runs on the published release
   event.
3. Wait for `Publish release artifacts` to finish. Verify the release assets,
   multi-platform image, and matching chart before announcing the release.
4. Make the GHCR image and chart packages public if GitHub created them as
   private packages. Repository visibility does not by itself guarantee
   package visibility.

Do not publish a release until the repository and intended package visibility
have been decided. Artifact attestations are enabled for public repositories;
the workflow skips GitHub's attestation service while this repository is
private. Image Buildx provenance remains enabled. A published GitHub release
is not a substitute for making its packages accessible to users.

To reproduce archives locally from a checked-out release commit:

```sh
scripts/release/build-binaries.sh v0.1.0 "$(git rev-parse HEAD)" dist
```

The script refuses to overwrite existing archives. Use an empty output
directory when repeating a build. The release requires Docker Buildx, Go
1.25.1, and Helm 3.19.0. After updating and committing chart metadata,
`helm package services/wks --destination dist` produces the matching chart.
