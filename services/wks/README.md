# WKS Server Helm Chart

This chart deploys the `go/wks/server` binary (packaged into the OCI image built via Bazel) to a DigitalOcean Kubernetes cluster.

## Prerequisites

- The container image `registry.digitalocean.com/wolveflake/wks:<tag>` is available and the cluster can pull from the registry (create an image pull secret if needed).
- `helm` 3.10+ installed locally.
- Access to the desired DigitalOcean Kubernetes context (`kubectl config use-context <do-cluster>`).

## Usage

```bash
# Package the container image first
bazel run //go/wks/server:image_push -- --tag <tag>

# Install / upgrade the chart
helm upgrade --install wks ./services/wks \
  --namespace wks --create-namespace \
  --set image.repository=registry.digitalocean.com/wolveflake/wks \
  --set image.tag=<tag>
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of pods (ignored when autoscaling enabled) | `1` |
| `image.repository` | OCI repository for the server image | `registry.digitalocean.com/wolveflake/wks` |
| `image.tag` | Image tag | `latest` |
| `image.pullSecrets` | List of image pull secrets | `[]` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | Service port | `8080` |
| `ingress.enabled` | Enable ingress | `false` |
| `autoscaling.enabled` | Enable HPA | `false` |
| `resources` | Pod resource requests/limits | See `values.yaml` |
| `env` | Additional environment variables passed to the container | `[LOG_LEVEL, PORT]` |

Refer to `values.yaml` for the full list of configurable settings (ingress, probes, PDB, volume mounts, etc.).

## DigitalOcean Registry Auth

If the cluster must pull from a private DigitalOcean registry, create a secret:

```bash
doctl registry kubernetes-manifest | kubectl apply -f -
kubectl create secret docker-registry do-registry \
  --docker-server=registry.digitalocean.com \
  --docker-username=<token> --docker-password=<token> \
  --namespace wks
```

Then set `image.pullSecrets[0].name=do-registry` when installing the chart.

## Local Development

Use `helm template` to preview manifests:

```bash
helm template wks ./services/wks --set replicaCount=2
```
