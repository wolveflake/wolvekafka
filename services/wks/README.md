# WKS Server Helm Chart

This chart deploys the `go/wks/server` binary (packaged into the OCI image built via Bazel) to a DigitalOcean Kubernetes cluster.

## Prerequisites

- The container image `ghcr.io/wolveflake/wks:<tag>` is available and the cluster can pull from the registry (create an image pull secret if needed).
- `helm` 3.10+ installed locally.
- Access to the desired DigitalOcean Kubernetes context (`kubectl config use-context <do-cluster>`).

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of pods (ignored when autoscaling enabled) | `1` |
| `image.repository` | OCI repository for the server image | `ghcr.io/wolveflake/wks` |
| `image.tag` | Image tag | `latest` |
| `image.pullSecrets` | List of image pull secrets | `[demo-regcred]` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | Service port | `8080` |
| `ingress.enabled` | Enable ingress | `false` |
| `autoscaling.enabled` | Enable HPA | `false` |
| `resources` | Pod resource requests/limits | See `values.yaml` |
| `env` | Additional environment variables passed to the container | `[LOG_LEVEL, PORT]` |

Refer to `values.yaml` for the full list of configurable settings (ingress, probes, PDB, volume mounts, etc.).

## End-to-End Deployment Flow

1. **Prepare GHCR Docker config** (rerun when the PAT changes):
   ```bash
   DOCKER_GHCR_CONFIG=${DOCKER_GHCR_CONFIG:-/tmp/docker-ghcr}
   ./services/wks/setup-ghcr-config.sh <ghcr-personal-access-token> <github-username>
   export DOCKER_CONFIG=$DOCKER_GHCR_CONFIG
   ```

2. **Build & push the image** (publishes `ghcr.io/wolveflake/wks:<tag>`):
   ```bash
   bazel run //go/wks/server:image_push_ghcr -- --tag <tag>
   ```

3. **Create / update the cluster pull secret** in each namespace (delete first if it exists):
   ```bash
   kubectl delete secret demo-regcred -n wks-demo --ignore-not-found
   kubectl create secret docker-registry demo-regcred \
     --namespace wks-demo \
     --docker-server=ghcr.io \
     --docker-username=<github-username> \
     --docker-password=<ghcr-personal-access-token>
   ```

4. **Install / upgrade (or uninstall) the Helm release**:
   ```bash
   helm upgrade --install demo-wks ./services/wks \
     --namespace wks-demo --create-namespace \
     --set image.tag=<tag> \
     --set image.pullPolicy=Always

   # Uninstall
   helm uninstall demo-wks --namespace wks-demo
   ```

5. **Monitor the rollout / access the service**:
   ```bash
   kubectl -n wks-demo get pods -w
   kubectl -n wks-demo logs deploy/demo-wks-wks

   POD=$(kubectl -n wks-demo get pods -l app.kubernetes.io/name=wks -o jsonpath='{.items[0].metadata.name}')
   kubectl -n wks-demo port-forward "$POD" 8080:8080
   # Visit http://127.0.0.1:8080/health
   ```

## Local Development

Use `helm template` to preview manifests:

```bash
helm template wks ./services/wks --set replicaCount=2
```
