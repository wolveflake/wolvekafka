// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks_e2e

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"

	"wolvekafka/go/configs"
	kafka_fixture "wolvekafka/go/itest/fixture"
	"wolvekafka/go/wks"
	configs_pb "wolvekafka/proto/configs"
)

const oauthAudience = "wks-test"

// oauthKafkaStartupScript runs a broker whose client listener accepts only
// OAUTHBEARER tokens signed by the key in the JWKS file and issued for
// oauthAudience. Broker-to-broker traffic stays on a plaintext listener.
//
// On Kafka 4.1 the validator must be set explicitly (and globally, not per
// listener): the default DefaultJwtValidator is built without a key resolver
// and falls back to ClientJwtValidator, which does not check signatures. The
// impostor check below guards against that.
const oauthKafkaStartupScript = `set -euo pipefail
cat <<'JWKS' >/tmp/jwks.json
%s
JWKS
cat <<'CONFIG' >/tmp/server.properties
process.roles=broker,controller
node.id=1
controller.listener.names=CONTROLLER
listeners=SASL_PLAINTEXT://:9092,CONTROLLER://:9093,INTERNAL://:9094
advertised.listeners=SASL_PLAINTEXT://127.0.0.1:9092,INTERNAL://localhost:9094
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
controller.quorum.voters=1@127.0.0.1:9093
inter.broker.listener.name=INTERNAL
sasl.enabled.mechanisms=OAUTHBEARER
listener.name.sasl_plaintext.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;
listener.name.sasl_plaintext.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler
sasl.oauthbearer.jwks.endpoint.url=file:///tmp/jwks.json
sasl.oauthbearer.expected.audience=` + oauthAudience + `
sasl.oauthbearer.jwt.validator.class=org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator
log.dirs=/var/lib/kafka/data
metadata.log.dir=/var/lib/kafka/metadata
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
CONFIG
mkdir -p /var/lib/kafka/data /var/lib/kafka/metadata
CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
/opt/kafka/bin/kafka-storage.sh format --cluster-id ${CLUSTER_ID} --config /tmp/server.properties
export KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///tmp/jwks.json"
exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties`

type jwtIssuer struct {
	key *rsa.PrivateKey
	kid string
}

func newJWTIssuer(t *testing.T) *jwtIssuer {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return &jwtIssuer{key: key, kid: "wks-test-key"}
}

func (i *jwtIssuer) jwks() string {
	b64 := base64.RawURLEncoding.EncodeToString
	keys := map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": i.kid,
		"n": b64(i.key.N.Bytes()), "e": b64(big.NewInt(int64(i.key.E)).Bytes()),
	}}}
	out, _ := json.Marshal(keys)
	return string(out)
}

func (i *jwtIssuer) token(t *testing.T, subject, audience string) string {
	b64 := base64.RawURLEncoding.EncodeToString
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": i.kid})
	now := time.Now()
	claims, _ := json.Marshal(map[string]any{
		"sub": subject, "aud": audience, "scope": "kafka",
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	})
	signingInput := b64(header) + "." + b64(claims)
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, i.key, crypto.SHA256, digest[:])
	require.NoError(t, err)
	return signingInput + "." + b64(sig)
}

// WKS authenticates to a real broker with OAUTHBEARER, fetching its token from
// a client-credentials endpoint, and processes and commits a record.
func TestOAuthBearerAuthenticatesToRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	issuer := newJWTIssuer(t)
	var tokenRequests atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, secret, ok := r.BasicAuth()
		if !ok || id != "wks" || secret != "s3cret" || r.PostForm.Get("grant_type") != "client_credentials" {
			http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
			return
		}
		tokenRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": issuer.token(t, "wks", r.PostForm.Get("audience")),
			"token_type":   "bearer", "expires_in": 3600,
		})
	}))
	defer tokenSrv.Close()

	// The test's own clients use a token minted directly by the issuer.
	testAuth := kgo.SASL(oauth.Auth{Token: issuer.token(t, "test", oauthAudience)}.AsMechanism())
	fixture := kafka_fixture.NewDockerFixture(t,
		kafka_fixture.WithStartupScript(fmt.Sprintf(oauthKafkaStartupScript, issuer.jwks())),
		kafka_fixture.WithClientOpts(testAuth),
	)
	fixture.Start(ctx)

	// The broker really enforces authentication: a token from another key fails.
	impostor := newJWTIssuer(t)
	bad, err := kgo.NewClient(kgo.SeedBrokers(fixture.BootstrapHost()),
		kgo.SASL(oauth.Auth{Token: impostor.token(t, "intruder", oauthAudience)}.AsMechanism()))
	require.NoError(t, err)
	badCtx, badCancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = kadm.NewClient(bad).Metadata(badCtx)
	badCancel()
	bad.Close()
	require.Error(t, err, "a token signed by an unknown key must be rejected")

	client, err := kgo.NewClient(kgo.SeedBrokers(fixture.BootstrapHost()), testAuth)
	require.NoError(t, err)
	defer client.Close()
	admin := kadm.NewClient(client)
	topic := fmt.Sprintf("oauth-%d", time.Now().UnixNano())
	group := topic + "-group"
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
	require.NoError(t, created.Error())

	received := make(chan string, 1)
	handler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages []struct {
				Value []byte `json:"value"`
			} `json:"messages"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		for _, m := range req.Messages {
			select {
			case received <- string(m.Value):
			default:
			}
		}
		results := strings.TrimSuffix(strings.Repeat(`{"status":2},`, len(req.Messages)), ",")
		_, _ = fmt.Fprintf(w, `{"messages":[%s]}`, results)
	}))
	defer handler.Close()

	cfg, err := configs.NewConfigFromProto(&configs_pb.Config{
		Cluster: &configs_pb.Cluster{
			BootstrapServers: []string{fixture.BootstrapHost()},
			SaslAuth: &configs_pb.Cluster_Oauthbearer{Oauthbearer: &configs_pb.OAuthBearer{
				TokenUrl: tokenSrv.URL + "/token", ClientId: "wks", ClientSecret: "s3cret",
				Audience: oauthAudience,
			}},
		},
		Consumers: []*configs_pb.Consumer{{
			GroupId: group,
			Topics:  []string{topic},
			Lambdas: []*configs_pb.Lambda{{Url: handler.URL}},
		}},
	})
	require.NoError(t, err)
	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	defer service.Close()

	require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("authenticated")}).FirstErr())
	select {
	case v := <-received:
		require.Equal(t, "authenticated", v)
	case <-time.After(60 * time.Second):
		t.Fatal("handler was not called; did WKS authenticate?")
	}
	require.Eventually(t, func() bool {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}
		o, ok := offsets.Lookup(topic, 0)
		return ok && o.At == 1
	}, 30*time.Second, 250*time.Millisecond, "offset committed with OAUTHBEARER")
	require.Positive(t, tokenRequests.Load())
	require.LessOrEqual(t, tokenRequests.Load(), int32(1), "all WKS clients share one cached token")
}
