# Contributing to WolveKafka

Thanks for helping improve WolveKafka. This is an early-stage, Apache-2.0
project maintained by Wolveflake LLC. Issues and pull requests are welcome;
review and release timing is best-effort, not guaranteed.

## Before you start

- Search existing issues and pull requests before opening a duplicate. For a
  substantial behavior or configuration change, open an issue first so the
  design and compatibility impact can be discussed.
- Keep WKS focused on Kafka-in → HTTP handler → optional Kafka-out. Read the
  [project scope](README.md#when-not-to-use-wks) and [delivery contract](docs/delivery-and-security.md)
  before changing commit, retry, DLQ, batching, or rebalance behavior.
- Do not put passwords, tokens, real customer data, or unredacted logs in
  issues, pull requests, tests, or example configuration. Report a security
  vulnerability through the private channel in [SECURITY.md](SECURITY.md), not
  a public issue.
- Ensure you have the right to contribute the material. Contributions are
  submitted under the repository's [Apache-2.0 license](LICENSE); do not add
  code or assets with incompatible terms.

## Development

Prerequisites are Bazelisk (the version is pinned in `.bazeliskrc`), a Go
toolchain, and Docker for the Kafka integration suite. The
[quickstart](README.md#quickstart) is a useful first smoke test.

Run the checks that CI runs before requesting review:

```sh
bazel test //go/common/server:server_test //go/configs:configs_test \
  //go/kafka:kafka_test //go/wks:wks_test --@rules_go//go/config:race
docker pull apache/kafka:4.1.0
bazel test //go/itest/wks_e2e:wks_e2e_test \
  --test_strategy=standalone \
  --test_env=WKS_TEST_KAFKA_IMAGE=apache/kafka:4.1.0 \
  --local_test_jobs=1
```

The integration fixture uses localhost ports 9092 and 9093; run it serially
and stop any other local Kafka process first. See [CI and merge policy](docs/ci.md)
for the full check list. If your change affects releases or deployment, also
run `helm lint services/wks` and the release checks relevant to your change.

Format Go changes with `gofmt`. Add focused unit tests for new behavior and a
real-Kafka regression test when delivery behavior changes. Update the README,
configuration examples, Helm chart, and operational docs when their behavior
changes. Generated protobuf code and Bazel build files should stay consistent
with the sources; `./gen_builds.sh` runs Gazelle when build metadata needs
regeneration. Keep a PR focused enough to review and explain any breaking
configuration or delivery change explicitly.

## Pull requests

Use the [PR template](.github/PULL_REQUEST_TEMPLATE.md) to describe the
problem, approach, compatibility impact, and validation. CI runs on every PR.
Maintainers may request changes or decline a proposal that is outside the
project's scope. A merged PR does not imply an immediate release; see the
[release process](docs/releases.md).

By participating, you agree to the [code of conduct](CODE_OF_CONDUCT.md).
