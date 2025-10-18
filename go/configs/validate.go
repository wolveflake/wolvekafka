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
	"fmt"

	configs_pb "wolvekafka/proto/configs"
)

// ValidateCluster to ensure required fields are present.
func ValidateCluster(cluster *configs_pb.Cluster) error {
	if cluster == nil {
		return fmt.Errorf("cluster configuration is required")
	}
	if len(cluster.BootstrapServers) == 0 {
		return fmt.Errorf("cluster bootstrap_servers is required")
	}
	return nil
}

// ValidateConsumers to ensure each consumer has required fields and no duplicates.
func ValidateConsumers(consumers []*configs_pb.Consumer) error {
	groupIds := make(map[string]bool)
	for _, consumer := range consumers {
		if consumer.GetGroupId() == "" {
			return fmt.Errorf("consumer group_id is required")
		}
		if _, exists := groupIds[consumer.GetGroupId()]; exists {
			return fmt.Errorf("duplicate consumer group_id: %s", consumer.GetGroupId())
		}
		groupIds[consumer.GetGroupId()] = true

		topics := consumer.GetTopics()
		if len(topics) == 0 {
			return fmt.Errorf("consumer %s must have at least one topic", consumer.GetGroupId())
		}

		lambdas := consumer.GetLambdas()
		if len(lambdas) == 0 {
			return fmt.Errorf("consumer %s must have at least one lambda", consumer.GetGroupId())
		}
		lambdasUrls := make(map[string]bool)
		for _, lambda := range lambdas {
			if lambda.GetUrl() == "" {
				return fmt.Errorf("consumer %s has a lambda with empty url", consumer.GetGroupId())
			}
			if _, exists := lambdasUrls[lambda.GetUrl()]; exists {
				return fmt.Errorf("consumer %s has duplicate lambda url: %s", consumer.GetGroupId(), lambda.GetUrl())
			}
			lambdasUrls[lambda.GetUrl()] = true
		}
	}

	return nil
}

// Validate the entire configuration.
func Validate(cfg *Config) error {
	err := ValidateCluster(cfg.proto.GetCluster())
	if err != nil {
		return err
	}

	err = ValidateConsumers(cfg.proto.GetConsumers())
	if err != nil {
		return err
	}

	return nil
}
