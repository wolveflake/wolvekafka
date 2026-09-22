# Continuous integration and merge policy

Every pull request, `main` push, and merge-queue group runs two independent CI
checks:

- **Unit and race tests:** Bazel tests for config, Kafka, WKS, and the HTTP/gRPC
  server, with Go race detection enabled.
- **Kafka integration tests:** WKS end-to-end tests against a real Kafka Docker
  container. The workflow pulls the Kafka image before the tests so a missing
  Docker installation cannot silently skip the suite.

The existing **Check release artifacts / release-check** workflow also builds
both Linux binary archives and the Helm chart on pull requests, `main`, and
merge-queue groups. Workflow status is linked from the README badges and from
each pull request's Checks tab. CI needs no repository secrets and has
read-only repository permission.

Run the core checks locally:

```sh
bazel test //go/common/server:server_test //go/configs:configs_test \
  //go/kafka:kafka_test //go/wks:wks_test --@rules_go//go/config:race
docker pull apache/kafka:4.1.0
bazel test //go/itest/wks_e2e:wks_e2e_test \
  --test_strategy=standalone \
  --test_env=WKS_TEST_KAFKA_IMAGE=apache/kafka:4.1.0 \
  --local_test_jobs=1
```

The integration fixture binds localhost ports 9092 and 9093. Run its test
target serially and stop any other local Kafka instance first.

## Make checks mandatory

An administrator should add an active `main` branch ruleset (or equivalent
branch protection) that requires a pull request and the following passing
checks before merge:

| Workflow | Check |
| --- | --- |
| CI | Unit and race tests |
| CI | Kafka integration tests |
| Check release artifacts | release-check |

Select the exact check names emitted by the first PR run and set GitHub
Actions as their expected source. Apply the rule to administrators as well,
with no routine bypass actor; disable force pushes and branch deletion. If a
merge queue is enabled, keep the `merge_group` triggers in both workflows.
Do not mark a check required until it has run and passed on a PR.

As of September 2026, this repository is private and its ruleset API responds
`403` with “Upgrade to GitHub Pro or make this repository public to enable this
feature.” CI runs and is visible, but passing checks cannot yet be enforced
as a merge requirement on the current plan. Making the repository public or
upgrading the plan, then applying the rule above, is an outstanding repository
administrator action. Do not describe merges as CI-gated until the rule is
active and verified with a failing-check PR.
