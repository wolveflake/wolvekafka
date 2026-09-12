package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	configs_pb "wolvekafka/proto/configs"
)

func TestKafkaClientsUseSecurityAndManualCommits(t *testing.T) {
	for _, mechanism := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(mechanism, func(t *testing.T) {
			cfg := &configs_pb.Cluster{BootstrapServers: []string{"localhost:9092"}, SaslUsername: "user", SaslPassword: "secret", SaslMechanism: mechanism, Tls: &configs_pb.TLS{Enabled: true}}
			consumer, err := NewConsumer(context.Background(), ConsumerParams{Name: "test", Cluster: cfg, Topics: []string{"in"}, RequireBatchAck: true})
			require.NoError(t, err)
			defer consumer.Close()
			producer, err := NewProducer(context.Background(), ProducerParams{Name: "out", Cluster: cfg})
			require.NoError(t, err)
			defer producer.Close()
			require.Equal(t, true, consumer.franzClient.OptValue(kgo.DisableAutoCommit))
			require.Equal(t, true, consumer.franzClient.OptValue(kgo.BlockRebalanceOnPoll))
			for _, client := range []*kgo.Client{consumer.franzClient, producer.kafkaProducer} {
				mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
				require.Len(t, mechanisms, 1)
				require.Equal(t, mechanism, mechanisms[0].Name())
				tlsCfg := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
				require.False(t, tlsCfg.InsecureSkipVerify)
				require.EqualValues(t, tls.VersionTLS12, tlsCfg.MinVersion)
			}
		})
	}
}

func TestKafkaTLSVerifiesCustomCAAndHostname(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
	cfg := &configs_pb.Cluster{BootstrapServers: []string{"localhost:9092"}, Tls: &configs_pb.TLS{Enabled: true, CaFile: ca}}
	opts, err := ConnectionOptions(cfg)
	require.NoError(t, err)
	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	defer client.Close()
	tlsCfg := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
	transport := &http.Transport{TLSClientConfig: tlsCfg}
	defer transport.CloseIdleConnections()
	resp, err := (&http.Client{Transport: transport}).Get(server.URL)
	require.NoError(t, err)
	resp.Body.Close()
	badTLS := tlsCfg.Clone()
	badTLS.ServerName = "wrong.example"
	badTransport := &http.Transport{TLSClientConfig: badTLS}
	defer badTransport.CloseIdleConnections()
	_, err = (&http.Client{Transport: badTransport}).Get(server.URL)
	require.Error(t, err)

	// Prove that a configured client key pair is actually presented during TLS.
	keyFile := filepath.Join(t.TempDir(), "client-key.pem")
	keyDER, err := x509.MarshalPKCS8PrivateKey(server.TLS.Certificates[0].PrivateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600))
	cfg.Tls.CertFile, cfg.Tls.KeyFile = ca, keyFile
	mtlsOpts, err := ConnectionOptions(cfg)
	require.NoError(t, err)
	mtlsClient, err := kgo.NewClient(mtlsOpts...)
	require.NoError(t, err)
	defer mtlsClient.Close()
	mutualServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "missing client certificate", 401)
			return
		}
		w.WriteHeader(200)
	}))
	mutualServer.TLS = &tls.Config{Certificates: server.TLS.Certificates, ClientAuth: tls.RequireAnyClientCert}
	mutualServer.StartTLS()
	defer mutualServer.Close()
	mutualTransport := &http.Transport{TLSClientConfig: mtlsClient.OptValue(kgo.DialTLSConfig).(*tls.Config)}
	defer mutualTransport.CloseIdleConnections()
	mutualResponse, err := (&http.Client{Transport: mutualTransport}).Get(mutualServer.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, mutualResponse.StatusCode)
	mutualResponse.Body.Close()
	require.NoError(t, os.WriteFile(ca, []byte("not a certificate"), 0600))
	_, err = ConnectionOptions(cfg)
	require.Error(t, err)
}
