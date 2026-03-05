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
