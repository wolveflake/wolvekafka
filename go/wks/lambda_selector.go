// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

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
	// Each worker owns its ordering; never sort the shared config slice.
	lambdas = append([]*configs_pb.Lambda(nil), lambdas...)
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
