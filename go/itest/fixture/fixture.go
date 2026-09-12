// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package fixture

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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultKafkaImageRef      = "kafka:4.1.0"
	defaultKafkaBootstrapHost = "127.0.0.1:9092"
	kafkaStartupScript        = `set -euo pipefail
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
if [ ! -f /var/lib/kafka/data/meta.properties ] && [ ! -f /var/lib/kafka/metadata/meta.properties ]; then
  CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
  /opt/kafka/bin/kafka-storage.sh format --cluster-id ${CLUSTER_ID} --config /tmp/server.properties
fi
exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties`
)

// DockerFixture spins up a single-node Kafka cluster inside Docker for integration testing.
type DockerFixture struct {
	tb testing.TB

	imageRef      string
	bootstrapHost string
	containerName string
	loaderScript  string
	startupScript string

	stop func()
}

// DockerFixtureOption configures the fixture.
type DockerFixtureOption func(*DockerFixture)

// WithImageRef overrides the Docker image reference used for Kafka.
func WithImageRef(imageRef string) DockerFixtureOption {
	return func(f *DockerFixture) {
		if imageRef != "" {
			f.imageRef = imageRef
		}
	}
}

// WithBootstrapHost overrides the bootstrap host used for client connections.
func WithBootstrapHost(host string) DockerFixtureOption {
	return func(f *DockerFixture) {
		if host != "" {
			f.bootstrapHost = host
		}
	}
}

// WithContainerName overrides the Docker container name.
func WithContainerName(name string) DockerFixtureOption {
	return func(f *DockerFixture) {
		if name != "" {
			f.containerName = name
		}
	}
}

// WithLoaderScript explicitly sets the path to the Kafka image loader script.
func WithLoaderScript(path string) DockerFixtureOption {
	return func(f *DockerFixture) {
		if path != "" {
			f.loaderScript = path
		}
	}
}

// WithStartupScript overrides the default script used to start Kafka inside the container.
func WithStartupScript(script string) DockerFixtureOption {
	return func(f *DockerFixture) {
		if script != "" {
			f.startupScript = script
		}
	}
}

// NewDockerFixture creates a fixture instance with sane defaults.
func NewDockerFixture(tb testing.TB, opts ...DockerFixtureOption) *DockerFixture {
	tb.Helper()

	fixture := &DockerFixture{
		tb:            tb,
		imageRef:      defaultKafkaImageRef,
		bootstrapHost: defaultKafkaBootstrapHost,
		containerName: fmt.Sprintf("wolvekafka-test-kafka-%d", time.Now().UnixNano()),
		startupScript: kafkaStartupScript,
	}

	for _, opt := range opts {
		opt(fixture)
	}

	return fixture
}

// Start boots the Kafka container, waits for it to become available, and registers cleanup.
func (f *DockerFixture) Start(ctx context.Context) {
	f.tb.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		f.tb.Skipf("docker is required for Kafka integration tests: %v", err)
	}

	if image := os.Getenv("WKS_TEST_KAFKA_IMAGE"); image != "" {
		// Explicit opt-in for a preloaded image with the fixture's /opt/kafka
		// layout. Useful when running regression tests without rebuilding it.
		f.imageRef = image
	} else {
		loader := f.loaderScript
		if loader == "" {
			loader = findKafkaLoaderScript(f.tb)
		}
		ensureKafkaImageLoaded(f.tb, loader)
	}

	stopContainer := startKafkaContainer(ctx, f.tb, f.containerName, f.imageRef, f.startupScript)
	f.stop = func() {
		if stopContainer != nil {
			stopContainer()
		}
	}
	f.tb.Cleanup(f.Stop)

	waitForBootstrap(ctx, f.tb, f.bootstrapHost, f.containerName)
}

// Stop tears down the Docker container if it is running.
func (f *DockerFixture) Stop() {
	f.tb.Helper()

	if f.stop != nil {
		f.stop()
		f.stop = nil
	}
}

// Restart stops and starts the Kafka container, then waits for bootstrap again.
func (f *DockerFixture) Restart(ctx context.Context) {
	f.tb.Helper()

	if f.containerName == "" {
		f.tb.Fatal("Kafka fixture has no container name")
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, 45*time.Second)
	defer stopCancel()
	stopCmd := exec.CommandContext(stopCtx, "docker", "stop", "--time", "1", f.containerName)
	stopCmd.Env = os.Environ()
	if output, err := stopCmd.CombinedOutput(); err != nil {
		f.tb.Fatalf("failed to stop Kafka container for restart: %v\n%s", err, string(output))
	}

	startCtx, startCancel := context.WithTimeout(ctx, 45*time.Second)
	defer startCancel()
	startCmd := exec.CommandContext(startCtx, "docker", "start", f.containerName)
	startCmd.Env = os.Environ()
	if output, err := startCmd.CombinedOutput(); err != nil {
		f.tb.Fatalf("failed to start Kafka container after restart: %v\n%s", err, string(output))
	}

	waitForBootstrap(ctx, f.tb, f.bootstrapHost, f.containerName)
}

// VerifyConnection checks that Kafka responds with metadata using confluent-kafka-go.
func (f *DockerFixture) VerifyConnection() error {
	return checkKafkaMetadata(f.bootstrapHost)
}

// BootstrapHost returns the address clients should use to reach the fixture.
func (f *DockerFixture) BootstrapHost() string {
	return f.bootstrapHost
}

func findKafkaLoaderScript(tb testing.TB) string {
	tb.Helper()

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

	if root := repoRoot(tb); root != "" {
		addCandidate(filepath.Join(root, "bazel-bin", "thirdparty", "kafka", "kafka_image_tarball.sh"))
		addCandidate(filepath.Join(root, "bazel-bin", "thirdparty", "kafka", "kafka_image_tarball", "load.sh"))
	}

	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}

	tb.Skip("Kafka image loader script not found; build //thirdparty/kafka:kafka_image_tarball first")
	return ""
}

func repoRoot(tb testing.TB) string {
	tb.Helper()

	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		return dir
	}

	dir, err := os.Getwd()
	if err != nil {
		tb.Fatalf("failed to determine working directory: %v", err)
	}

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
}

func ensureKafkaImageLoaded(tb testing.TB, loader string) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, loader)
	cmd.Env = os.Environ()

	if output, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("failed to load Kafka image using %s: %v\n%s", loader, err, string(output))
	}
}

func startKafkaContainer(ctx context.Context, tb testing.TB, containerName, imageRef, startupScript string) func() {
	tb.Helper()

	args := []string{"run", "-d"}
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		args = append(args, "--platform", "linux/arm64")
	}

	args = append(args,
		"--name", containerName,
		"-p", "127.0.0.1:9092:9092",
		"-p", "127.0.0.1:9093:9093",
		"--entrypoint", "bash",
		imageRef,
		"-lc", startupScript,
	)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		tb.Fatalf("failed to start Kafka container: %v\n%s", err, string(output))
	}

	return func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		stopCmd := exec.CommandContext(stopCtx, "docker", "stop", containerName)
		stopCmd.Env = os.Environ()
		if output, err := stopCmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			tb.Logf("docker stop failed: %v\n%s", err, string(output))
		}

		rmCmd := exec.CommandContext(stopCtx, "docker", "rm", containerName)
		rmCmd.Env = os.Environ()
		if output, err := rmCmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			tb.Logf("docker rm failed: %v\n%s", err, string(output))
		}
	}
}

func waitForBootstrap(ctx context.Context, tb testing.TB, bootstrapHost, containerName string) {
	tb.Helper()

	deadline := time.Now().Add(90 * time.Second)
	for {
		if time.Now().After(deadline) {
			logs := dockerLogs(tb, containerName)
			tb.Fatalf("Kafka did not become ready in time. Logs:\n%s", logs)
		}

		select {
		case <-ctx.Done():
			logs := dockerLogs(tb, containerName)
			tb.Fatalf("context cancelled while waiting for Kafka: %v\nLogs:\n%s", ctx.Err(), logs)
		default:
		}

		conn, err := net.DialTimeout("tcp", bootstrapHost, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			if err := checkKafkaMetadata(bootstrapHost); err == nil {
				return
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// checkKafkaMetadata verifies a Kafka connection by fetching metadata.
func checkKafkaMetadata(bootstrapHost string) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapHost),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	brokers, err := adminClient.Metadata(ctx)
	if err != nil {
		return err
	}

	if len(brokers.Brokers) == 0 {
		return errors.New("empty metadata returned from Kafka")
	}

	return nil
}

func dockerLogs(tb testing.TB, containerName string) string {
	tb.Helper()

	cmd := exec.Command("docker", "logs", containerName)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("unable to retrieve docker logs: %v\n%s", err, string(output))
	}

	return string(output)
}
