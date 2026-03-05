///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2024 [wolveflake LLC], All Rights Reserved.
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

package wks

import (
	"math/rand/v2"
	"sort"

	configs_pb "wolvekafka/proto/configs"
)

var (
	// default lambda weight.
	defaultLambdaWeight = uint32(100)
)

type LambdaSelector struct {
	totalWeight int64

	lambdas []*configs_pb.Lambda
}

func NewLambdaSelector(lambdas []*configs_pb.Lambda) *LambdaSelector {
	// total lambas weight
	totalWeight := int64(0)
	for _, lambda := range lambdas {
		weight := lambda.GetWeight()
		if weight <= 0 {
			weight = defaultLambdaWeight
		}
		totalWeight += int64(weight)
	}

	// sort c.consumerCfg by weight.
	sort.SliceStable(lambdas, func(i, j int) bool {
		return lambdas[i].GetWeight() > lambdas[j].GetWeight()
	})

	return &LambdaSelector{
		totalWeight: totalWeight,
		lambdas:     lambdas,
	}
}

// pick lambda based on weight.
func (l *LambdaSelector) Select() *configs_pb.Lambda {
	randWeight := rand.Int64N(l.totalWeight)
	currentWeight := int64(0)
	for _, lambda := range l.lambdas {
		weight := lambda.GetWeight()
		if weight <= 0 {
			weight = defaultLambdaWeight
		}
		currentWeight += int64(weight)
		if randWeight < currentWeight {
			return lambda
		}
	}
	// fallback to the first lambda, but it should not be the case.
	return l.lambdas[0]
}
