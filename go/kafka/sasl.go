// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/sasl"
	saslaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	configs_pb "wolvekafka/proto/configs"
)

// oauthTokenTimeout bounds one request to the OAuth token endpoint.
const oauthTokenTimeout = 30 * time.Second

// saslMechanism returns the SASL mechanism for a validated cluster, or nil
// when the cluster does not use SASL.
func saslMechanism(cluster *configs_pb.Cluster) (sasl.Mechanism, error) {
	switch auth := cluster.GetSaslAuth().(type) {
	case *configs_pb.Cluster_Oauthbearer:
		return oauthBearerMechanism(auth.Oauthbearer), nil
	case *configs_pb.Cluster_AwsMskIam:
		return awsMSKIAMMechanism()
	}
	user, password := cluster.GetSaslUsername(), cluster.GetSaslPassword()
	switch strings.ToUpper(cluster.GetSaslMechanism()) {
	case "", "PLAIN":
		if user == "" {
			return nil, nil
		}
		return plain.Auth{User: user, Pass: password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", cluster.GetSaslMechanism())
	}
}

// oauthSources shares one cached token source per OAuth configuration, so
// every consumer and producer of a cluster reuses the same token.
var oauthSources sync.Map // string -> oauth2.TokenSource

func oauthTokenSource(cfg *configs_pb.OAuthBearer) oauth2.TokenSource {
	scopes := append([]string(nil), cfg.GetScopes()...)
	sort.Strings(scopes)
	key := strings.Join([]string{cfg.GetTokenUrl(), cfg.GetClientId(), cfg.GetClientSecret(), cfg.GetAudience(), strings.Join(scopes, " ")}, "\x00")
	if ts, ok := oauthSources.Load(key); ok {
		return ts.(oauth2.TokenSource)
	}
	cc := clientcredentials.Config{
		ClientID:     cfg.GetClientId(),
		ClientSecret: cfg.GetClientSecret(),
		TokenURL:     cfg.GetTokenUrl(),
		Scopes:       cfg.GetScopes(),
	}
	if aud := cfg.GetAudience(); aud != "" {
		cc.EndpointParams = url.Values{"audience": {aud}}
	}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Timeout: oauthTokenTimeout})
	// TokenSource caches the token and fetches a new one shortly before expiry.
	ts, _ := oauthSources.LoadOrStore(key, cc.TokenSource(ctx))
	return ts.(oauth2.TokenSource)
}

func oauthBearerMechanism(cfg *configs_pb.OAuthBearer) sasl.Mechanism {
	ts := oauthTokenSource(cfg)
	extensions := cfg.GetExtensions()
	// Called for every SASL handshake, including broker re-authentication.
	return oauth.Oauth(func(context.Context) (oauth.Auth, error) {
		tok, err := ts.Token()
		if err != nil {
			return oauth.Auth{}, fmt.Errorf("fetch OAUTHBEARER token: %w", err)
		}
		return oauth.Auth{Token: tok.AccessToken, Extensions: extensions}, nil
	})
}

var (
	awsCredsMu sync.Mutex
	awsCreds   awssdk.CredentialsProvider
)

// awsCredentials loads the default AWS credential chain once: environment,
// shared config, web identity (IRSA on EKS), container, and instance roles.
func awsCredentials() (awssdk.CredentialsProvider, error) {
	awsCredsMu.Lock()
	defer awsCredsMu.Unlock()
	if awsCreds != nil {
		return awsCreds, nil
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration for AWS_MSK_IAM: %w", err)
	}
	if cfg.Credentials == nil {
		return nil, fmt.Errorf("no AWS credentials found for AWS_MSK_IAM")
	}
	awsCreds = cfg.Credentials
	return awsCreds, nil
}

func awsMSKIAMMechanism() (sasl.Mechanism, error) {
	creds, err := awsCredentials()
	if err != nil {
		return nil, err
	}
	// The region is taken from the MSK broker hostname when signing.
	return saslaws.ManagedStreamingIAM(func(ctx context.Context) (saslaws.Auth, error) {
		c, err := creds.Retrieve(ctx)
		if err != nil {
			return saslaws.Auth{}, fmt.Errorf("retrieve AWS credentials for AWS_MSK_IAM: %w", err)
		}
		return saslaws.Auth{AccessKey: c.AccessKeyID, SecretKey: c.SecretAccessKey, SessionToken: c.SessionToken}, nil
	}), nil
}
