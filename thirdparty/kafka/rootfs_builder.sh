#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: rootfs_builder.sh <jre_url> <kafka_url> <scala_version> <kafka_version> <output>" >&2
  exit 1
fi

JRE_URL="$1"
KAFKA_URL="$2"
SCALA_VERSION="$3"
KAFKA_VERSION="$4"
OUTPUT="$5"

TMP=$(mktemp -d)
cleanup() {
  rm -rf "$TMP"
}
trap cleanup EXIT

ROOT="$TMP/root"
mkdir -p "$ROOT/opt" "$ROOT/usr/local/bin" "$ROOT/var/lib/kafka" "$ROOT/var/log/kafka"

curl -fsSL "$JRE_URL" -o "$TMP/jre.tar.gz"
tar -xzf "$TMP/jre.tar.gz" -C "$ROOT/opt"
JRE_DIR=$(find "$ROOT/opt" -mindepth 1 -maxdepth 1 -type d | head -n1)
mv "$JRE_DIR" "$ROOT/opt/jre"

curl -fsSL "$KAFKA_URL" -o "$TMP/kafka.tgz"
tar -xzf "$TMP/kafka.tgz" -C "$ROOT/opt"
mv "$ROOT/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$ROOT/opt/kafka"

cat <<'SCRIPT' > "$ROOT/usr/local/bin/kafka-entrypoint.sh"
#!/usr/bin/env bash
set -euo pipefail
exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
SCRIPT
chmod +x "$ROOT/usr/local/bin/kafka-entrypoint.sh"

tar -C "$ROOT" -czf "$OUTPUT" .
