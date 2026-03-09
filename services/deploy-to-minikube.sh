#!/usr/bin/env bash
set -euo pipefail

# When run via `bazel run`, ensure we're in the workspace root.
if [ -n "${BUILD_WORKSPACE_DIRECTORY:-}" ]; then
    cd "${BUILD_WORKSPACE_DIRECTORY}"
fi

NAMESPACE="${NAMESPACE:-wolvekafka}"
CHART_DIR="services"
STRIMZI_NAMESPACE="${STRIMZI_NAMESPACE:-strimzi}"

ALL_SERVICES=(kafka)

# Services that are helm-only (no Bazel build step).
helm_only() {
    local service="$1"
    case "${service}" in
        kafka) return 0 ;;
        *)     return 1 ;;
    esac
}

# Maps service name to its Helm chart directory.
chart_path() {
    local service="$1"
    echo "${CHART_DIR}/${service}"
}

# Maps service name to its bazel server target path.
bazel_target() {
    local service="$1"
    case "${service}" in
        *) echo "//go/${service}/server" ;;
    esac
}

usage() {
    echo "Usage: $0 <service_name> [-- <extra helm args>]"
    echo "       $0 --all"
    echo ""
    echo "Deploy a service to minikube via Helm."
    echo ""
    echo "Options:"
    echo "  --all       Deploy all services in dependency order:"
    echo "              ${ALL_SERVICES[*]}"
    echo ""
    echo "Examples:"
    echo "  $0 kafka"
    echo "  $0 kafka -- --set kafka.replicas=3"
    echo "  $0 --all"
    echo ""
    echo "Environment variables:"
    echo "  NAMESPACE         Application namespace (default: wolvekafka)"
    echo "  STRIMZI_NAMESPACE Strimzi operator namespace (default: strimzi)"
    echo ""
    echo "This will:"
    echo "  1. Verify minikube is running"
    echo "  2. Install the Strimzi operator (if not present)"
    echo "  3. Helm install/upgrade the requested service"
    echo "  4. Wait for rollout to complete"
    exit 1
}

# ── Strimzi operator management ──────────────────────────────────────────────

ensure_strimzi_operator() {
    if helm list -n "${STRIMZI_NAMESPACE}" -q 2>/dev/null | grep -q '^strimzi$'; then
        echo "==> Strimzi operator already installed in namespace ${STRIMZI_NAMESPACE}."
        return 0
    fi

    echo "==> Installing Strimzi Kafka operator..."

    if ! helm repo list 2>/dev/null | grep -q 'strimzi'; then
        helm repo add strimzi https://strimzi.io/charts/
    fi
    helm repo update strimzi

    kubectl create namespace "${STRIMZI_NAMESPACE}" 2>/dev/null || true

    helm install strimzi strimzi/strimzi-kafka-operator \
        --namespace "${STRIMZI_NAMESPACE}" \
        --set watchAnyNamespace=true \
        --wait --timeout 120s

    echo "==> Strimzi operator is ready."
}

# ── Main ─────────────────────────────────────────────────────────────────────

if [ $# -lt 1 ] || [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    usage
fi

# Handle --all: deploy every service in dependency order.
if [ "$1" = "--all" ]; then
    for svc in "${ALL_SERVICES[@]}"; do
        echo ""
        echo "########## Deploying ${svc} ##########"
        NAMESPACE="${NAMESPACE}" STRIMZI_NAMESPACE="${STRIMZI_NAMESPACE}" "$0" "${svc}" || exit 1
    done
    echo ""
    echo "==> All services deployed successfully."
    exit 0
fi

SERVICE_NAME="$1"
shift

# Consume the optional '--' separator.
EXTRA_ARGS=()
if [ $# -gt 0 ] && [ "$1" = "--" ]; then
    shift
    EXTRA_ARGS=("$@")
fi

CHART_PATH="$(chart_path "${SERVICE_NAME}")"

# Verify chart exists.
if [ ! -f "${CHART_PATH}/Chart.yaml" ]; then
    echo "Error: Helm chart not found at ${CHART_PATH}/Chart.yaml"
    echo ""
    echo "Available charts:"
    for d in "${CHART_DIR}"/*/Chart.yaml; do
        [ -f "$d" ] && basename "$(dirname "$d")"
    done
    exit 1
fi

# Verify minikube is running.
if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
    echo "Error: minikube is not running. Start it with: minikube start --memory=4096"
    exit 1
fi

# Install Strimzi operator if this is the kafka service.
if [ "${SERVICE_NAME}" = "kafka" ]; then
    ensure_strimzi_operator
fi

# Build and load image (skip for helm-only services).
if helm_only "${SERVICE_NAME}"; then
    echo "==> ${SERVICE_NAME} is helm-only, skipping Bazel build..."
else
    SERVER_TARGET="$(bazel_target "${SERVICE_NAME}")"

    if ! bazel query "${SERVER_TARGET}:local_image" >/dev/null 2>&1; then
        echo "Error: target ${SERVER_TARGET}:local_image not found."
        echo "Add an oci_load target named 'local_image' to ${SERVER_TARGET}/BUILD.bazel"
        exit 1
    fi

    TARBALL="bazel-bin/${SERVER_TARGET#//}/local_image/tarball.tar"

    echo "==> Building image for ${SERVICE_NAME}..."
    bazel build "${SERVER_TARGET}:local_image" --output_groups=+tarball

    echo "==> Loading image into minikube..."
    minikube image load "${TARBALL}" --overwrite=true
fi

# Ensure namespace exists.
if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "==> Creating namespace ${NAMESPACE}..."
    kubectl create namespace "${NAMESPACE}"
fi

# Update subchart dependencies if Chart.yaml declares any.
if grep -q "^dependencies:" "${CHART_PATH}/Chart.yaml"; then
    echo "==> Updating Helm dependencies for ${SERVICE_NAME}..."
    helm dependency update "${CHART_PATH}"
fi

# Release name: use service name with underscores replaced by hyphens.
RELEASE_NAME="${SERVICE_NAME//_/-}"

HELM_ARGS=()
if ! helm_only "${SERVICE_NAME}"; then
    HELM_ARGS+=(--set image.pullPolicy=Never)
fi

echo "==> Deploying ${SERVICE_NAME} to minikube (namespace: ${NAMESPACE})..."
helm upgrade --install "${RELEASE_NAME}" "${CHART_PATH}" \
    --namespace "${NAMESPACE}" \
    "${HELM_ARGS[@]+"${HELM_ARGS[@]}"}" \
    "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"

# Wait for resources to become ready.
echo "==> Waiting for rollout..."
case "${SERVICE_NAME}" in
    kafka)
        echo "==> Waiting for Strimzi to reconcile the Kafka cluster (this may take a few minutes)..."
        # Wait for the Kafka CR to report Ready.
        CLUSTER_NAME=$(helm get values "${RELEASE_NAME}" -n "${NAMESPACE}" -o json 2>/dev/null \
            | grep -o '"clusterName":"[^"]*"' | cut -d'"' -f4)
        CLUSTER_NAME="${CLUSTER_NAME:-wolvekafka}"

        kubectl wait kafka/"${CLUSTER_NAME}" \
            --for=condition=Ready \
            --timeout=300s \
            --namespace "${NAMESPACE}"
        ;;
    *)
        kubectl rollout status deployment/"${RELEASE_NAME}" \
            --namespace "${NAMESPACE}" \
            --timeout=120s
        ;;
esac

echo "==> Done. ${SERVICE_NAME} deployed successfully."
kubectl get pods -n "${NAMESPACE}" -l "strimzi.io/cluster=${CLUSTER_NAME:-${RELEASE_NAME}}" -o wide 2>/dev/null \
    || kubectl get pods -n "${NAMESPACE}" -l "app=${RELEASE_NAME}" -o wide
