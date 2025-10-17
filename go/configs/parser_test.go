///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2025 [wolveflake LLC], All Rights Reserved.
///
/// NOTICE:  All information contained herein is, and remains the property of COMPANY. The intellectual and technical concepts contained
/// herein are proprietary to COMPANY and may be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
/// Dissemination of this information or reproduction of this material is strictly forbidden unless prior written permission is obtained
/// from COMPANY.  Access to the source code contained herein is hereby forbidden to anyone except current COMPANY employees, managers or contractors who have executed
/// Confidentiality and Non-disclosure agreements explicitly covering such access.
///
/// The copyright notice above does not evidence any actual or intended publication or disclosure  of  this source code, which includes
/// information that is confidential and/or proprietary, and is a trade secret, of  COMPANY.   ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE,
/// OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE CODE  WITHOUT  THE EXPRESS WRITTEN CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
/// LAWS AND INTERNATIONAL TREATIES.  THE RECEIPT OR POSSESSION OF  THIS SOURCE CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS
/// TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT  MAY DESCRIBE, IN WHOLE OR IN PART.
///

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
				"retry_policy": {
					"max_retries": 3,
					"initial_backoff_ms": 100,
					"max_backoff_ms": 1000,
					"drop": {}
				},
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
				"retry_policy": {
					"max_retries": 3,
					"drop": {}
				},
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
