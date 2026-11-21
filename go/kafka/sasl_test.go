// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	configs_pb "wolvekafka/proto/configs"
)

// tokenServer is an OAuth token endpoint for the client-credentials grant.
func tokenServer(t *testing.T, status int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		require.NoError(t, r.ParseForm())
		require.Equal(t, "client_credentials", r.PostForm.Get("grant_type"))
		require.Equal(t, "kafka.write kafka.read", r.PostForm.Get("scope"))
		require.Equal(t, "wks-test", r.PostForm.Get("audience"))
		id, secret, ok := r.BasicAuth()
		if !ok {
			id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
		}
		require.Equal(t, "wks", id)
		require.Equal(t, "s3cret", secret)
		if status != http.StatusOK {
			http.Error(w, `{"error":"invalid_client"}`, status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("tok-%d", n), "token_type": "bearer", "expires_in": 3600})
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func oauthCluster(tokenURL string) *configs_pb.Cluster {
	return &configs_pb.Cluster{
		BootstrapServers: []string{"localhost:9092"},
		SaslMechanism:    "OAUTHBEARER",
		Oauthbearer: &configs_pb.OAuthBearer{
			TokenUrl: tokenURL, ClientId: "wks", ClientSecret: "s3cret",
			Scopes: []string{"kafka.write", "kafka.read"}, Audience: "wks-test",
			Extensions: map[string]string{"logicalCluster": "lkc-1", "identityPoolId": "pool-1"},
		},
	}
}

func clientMechanism(t *testing.T, cluster *configs_pb.Cluster) sasl.Mechanism {
	t.Helper()
	opts, err := ConnectionOptions(cluster)
	require.NoError(t, err)
	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
	require.Len(t, mechanisms, 1)
	return mechanisms[0]
}

func TestOAuthBearerFetchesCachesAndSendsToken(t *testing.T) {
	srv, calls := tokenServer(t, http.StatusOK)
	cluster := oauthCluster(srv.URL + "/token")

	first := clientMechanism(t, cluster)
	require.Equal(t, "OAUTHBEARER", first.Name())
	_, msg, err := first.Authenticate(context.Background(), "broker:9092")
	require.NoError(t, err)
	require.Equal(t, "n,,\x01auth=Bearer tok-1\x01identityPoolId=pool-1\x01logicalCluster=lkc-1\x01\x01", string(msg))

	// A second client of the same cluster, and re-authentication, reuse the
	// cached token instead of calling the token endpoint again.
	second := clientMechanism(t, cluster)
	_, msg, err = second.Authenticate(context.Background(), "broker:9092")
	require.NoError(t, err)
	require.Contains(t, string(msg), "auth=Bearer tok-1\x01")
	require.EqualValues(t, 1, calls.Load())
}

func TestOAuthBearerReportsTokenEndpointErrors(t *testing.T) {
	srv, _ := tokenServer(t, http.StatusUnauthorized)
	mechanism := clientMechanism(t, oauthCluster(srv.URL+"/token"))
	_, _, err := mechanism.Authenticate(context.Background(), "broker:9092")
	require.ErrorContains(t, err, "fetch OAUTHBEARER token")
}

func TestAWSMSKIAMSignsWithDefaultCredentialChain(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDWKSTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "session-token")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	awsCredsMu.Lock()
	awsCreds = nil
	awsCredsMu.Unlock()
	t.Cleanup(func() { awsCredsMu.Lock(); awsCreds = nil; awsCredsMu.Unlock() })

	mechanism := clientMechanism(t, &configs_pb.Cluster{
		BootstrapServers: []string{"b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098"},
		SaslMechanism:    "AWS_MSK_IAM",
		Tls:              &configs_pb.TLS{Enabled: true},
	})
	require.Equal(t, "AWS_MSK_IAM", mechanism.Name())
	_, msg, err := mechanism.Authenticate(context.Background(), "b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098")
	require.NoError(t, err)
	var payload map[string]string
	require.NoError(t, json.Unmarshal(msg, &payload))
	require.Equal(t, "kafka-cluster:Connect", payload["action"])
	require.Regexp(t, `^AKIDWKSTEST/\d{8}/us-east-1/kafka-cluster/aws4_request$`, payload["x-amz-credential"])
	require.Equal(t, "session-token", payload["x-amz-security-token"])
	require.NotEmpty(t, payload["x-amz-signature"])
}
