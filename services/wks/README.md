# WolveKafka Helm chart

The chart deploys WKS to Kubernetes. It defaults to the versioned GHCR image
matching the chart's `appVersion`; it does not require a private registry or
image-pull secret once the package is public.

After a version is published, install its matching OCI chart:

Replace `0.1.0` below with the published chart version you want to install.

```sh
helm install wks oci://ghcr.io/wolveflake/charts/wks \
  --version 0.1.0 \
  --namespace wks --create-namespace \
  --set-file config.data=config.json
```

`config.json` must use Kafka bootstrap and handler addresses reachable from the
cluster. Start with the [configuration example](../../docs/reference.md#configuration)
and [delivery/security reference](../../docs/delivery-and-security.md). Do not
store Kafka credentials in a ConfigMap; mount a Kubernetes Secret and set
`config.enabled=false`, `config.path`, and `extraVolumes`/`extraVolumeMounts`
instead.

Inspect the chart before applying it:

```sh
helm show chart oci://ghcr.io/wolveflake/charts/wks --version 0.1.0
helm template wks ./services/wks --set-file config.data=config.json
```

The chart exposes HTTP health and Prometheus metrics on port 8080. The image
repository can be overridden with `image.repository`; `image.tag` defaults to
the chart release's `appVersion`. Set `image.pullSecrets` only for a private
registry. See [`values.yaml`](values.yaml) for replicas, resource limits,
ingress, autoscaling, volume mounts, and other options.
