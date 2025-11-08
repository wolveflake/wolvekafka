package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

const (
	kafkaImageRef      = "kafka:4.1.0"
	kafkaBootstrapHost = "127.0.0.1:9092"
	kafkaStartupScript = `set -euo pipefail
CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
cat <<'CONFIG' >/tmp/server.properties
process.roles=broker,controller
node.id=1
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.quorum.voters=1@127.0.0.1:9093
inter.broker.listener.name=PLAINTEXT
log.dirs=/var/lib/kafka/data
metadata.log.dir=/var/lib/kafka/metadata
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
CONFIG
mkdir -p /var/lib/kafka/data /var/lib/kafka/metadata
/opt/kafka/bin/kafka-storage.sh format --ignore-formatted --cluster-id ${CLUSTER_ID} --config /tmp/server.properties
exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties`
)

func TestKafkaStartupAndConnection(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is required for this test: %v", err)
	}

	loader := findKafkaLoaderScript(t)
	ensureKafkaImageLoaded(t, loader)

	containerName := fmt.Sprintf("wolvekafka-test-kafka-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	stopContainer := startKafkaContainer(ctx, t, containerName)
	defer stopContainer()

	waitForBootstrap(ctx, t, containerName)
	verifyKafkaConnection(t)
}

func findKafkaLoaderScript(t *testing.T) string {
	t.Helper()

	const runfilePath = "thirdparty/kafka/kafka_image_tarball.sh"

	candidates := make([]string, 0, 6)
	seen := make(map[string]struct{})
	addCandidate := func(path string) {
		if path == "" {
			return
		}
		if _, exists := seen[path]; exists {
			return
		}
		seen[path] = struct{}{}
		candidates = append(candidates, path)
	}

	runfilesDir := os.Getenv("RUNFILES_DIR")
	workspace := os.Getenv("TEST_WORKSPACE")
	if runfilesDir != "" {
		addCandidate(filepath.Join(runfilesDir, runfilePath))
		if workspace != "" {
			addCandidate(filepath.Join(runfilesDir, workspace, runfilePath))
		}
	}

	testSrcDir := os.Getenv("TEST_SRCDIR")
	if testSrcDir != "" {
		addCandidate(filepath.Join(testSrcDir, runfilePath))
		if workspace != "" {
			addCandidate(filepath.Join(testSrcDir, workspace, runfilePath))
		}
	}

	if root := repoRoot(t); root != "" {
		addCandidate(filepath.Join(root, "bazel-bin", "thirdparty", "kafka", "kafka_image_tarball.sh"))
		addCandidate(filepath.Join(root, "bazel-bin", "thirdparty", "kafka", "kafka_image_tarball", "load.sh"))
	}

	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}

	t.Skip("Kafka image loader script not found; build //thirdparty/kafka:kafka_image_tarball first")
	return ""
}

func repoRoot(t *testing.T) string {
	t.Helper()

	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		return dir
	}

	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}

	return ""
}

func ensureKafkaImageLoaded(t *testing.T, loader string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, loader)
	cmd.Env = os.Environ()

	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to load Kafka image using %s: %v\n%s", loader, err, string(output))
	}
}

func startKafkaContainer(ctx context.Context, t *testing.T, containerName string) func() {
	t.Helper()

	args := []string{"run", "-d"}
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		args = append(args, "--platform", "linux/arm64")
	}

	args = append(args,
		"--name", containerName,
		"-p", "127.0.0.1:9092:9092",
		"-p", "127.0.0.1:9093:9093",
		"--entrypoint", "bash",
		kafkaImageRef,
		"-lc", kafkaStartupScript,
	)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to start Kafka container: %v\n%s", err, string(output))
	}

	return func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		stopCmd := exec.CommandContext(stopCtx, "docker", "stop", containerName)
		stopCmd.Env = os.Environ()
		if output, err := stopCmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			t.Logf("docker stop failed: %v\n%s", err, string(output))
		}

		rmCmd := exec.CommandContext(stopCtx, "docker", "rm", containerName)
		rmCmd.Env = os.Environ()
		if output, err := rmCmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			t.Logf("docker rm failed: %v\n%s", err, string(output))
		}
	}
}

func waitForBootstrap(ctx context.Context, t *testing.T, containerName string) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	for {
		if time.Now().After(deadline) {
			logs := dockerLogs(t, containerName)
			t.Fatalf("Kafka did not become ready in time. Logs:\n%s", logs)
		}

		select {
		case <-ctx.Done():
			logs := dockerLogs(t, containerName)
			t.Fatalf("context cancelled while waiting for Kafka: %v\nLogs:\n%s", ctx.Err(), logs)
		default:
		}

		conn, err := net.DialTimeout("tcp", kafkaBootstrapHost, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			if err := checkKafkaMetadata(); err == nil {
				return
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func verifyKafkaConnection(t *testing.T) {
	t.Helper()

	require.NoError(t, checkKafkaMetadata())
}

func checkKafkaMetadata() error {
	client, err := ckafka.NewAdminClient(&ckafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapHost,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	metadata, err := client.GetMetadata(nil, true, 10_000)
	if err != nil {
		return err
	}

	if metadata == nil || len(metadata.Brokers) == 0 {
		return errors.New("empty metadata returned from Kafka")
	}

	return nil
}

func dockerLogs(t *testing.T, containerName string) string {
	t.Helper()

	cmd := exec.Command("docker", "logs", containerName)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("unable to retrieve docker logs: %v\n%s", err, string(output))
	}

	return string(output)
}
