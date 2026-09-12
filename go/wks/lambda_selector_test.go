// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	configs_pb "wolvekafka/proto/configs"
)

// Validate that lambdas are sorted by weight in descending order which is required by the selector logic.
func TestSorting(t *testing.T) {
	s := NewLambdaSelector([]*configs_pb.Lambda{
		{Weight: 50},
		{Weight: 150},
		{Weight: 100},
	})

	weight := int64(150)
	for _, lambda := range s.lambdas {
		assert.GreaterOrEqual(t, weight, int64(lambda.GetWeight()))
		weight = int64(lambda.GetWeight())
	}
}

// TestSelectDistribution verifies that weighted selection produces a distribution
// roughly proportional to the configured weights.
func TestSelectDistribution(t *testing.T) {
	lambdas := []*configs_pb.Lambda{
		{Url: "http://a", Weight: 75},
		{Url: "http://b", Weight: 25},
	}
	s := NewLambdaSelector(lambdas)

	counts := make(map[string]int)
	const iterations = 10_000
	for i := 0; i < iterations; i++ {
		selected := s.Select()
		counts[selected.GetUrl()]++
	}

	ratioA := float64(counts["http://a"]) / float64(iterations)
	// Expected ~0.75 for "a". Allow 10% tolerance.
	assert.Less(t, math.Abs(ratioA-0.75), 0.10, "expected ~75%% selection for lambda a, got %.2f%%", ratioA*100)
}
