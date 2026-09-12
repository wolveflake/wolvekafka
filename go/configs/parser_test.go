// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserSimpleConfig(t *testing.T) {
	jsonCfg := `{
		"cluster": {
			"bootstrap_servers": [
				"broker1:9092",
				"broker2:9092"
			],
			"sasl_username": "test_user",
			"sasl_password": "test_password"
		},
		"consumers": [
			{
				"group_id": "group1",
				"topics": ["topic1", "topic2"],
				"lambdas": [
					{
						"url": "https://lambda1.example.com",
						"weight": 10
					},
					{
						"url": "https://lambda2.example.com",
						"weight": 90
					}
				]
			},
			{
				"group_id": "group1",
				"topics": ["topic1", "topic3"],
				"lambdas": [
					{
						"url": "https://lambda1.example.com"
					},
					{
						"url": "https://lambda2.example.com"
					}
				]
			}
		]
	}`

	cfg, err := ParseConfigsJson([]byte(jsonCfg))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(cfg.GetConsumers()))
}
