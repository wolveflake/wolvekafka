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

package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerProduceUsesProducerFunc(t *testing.T) {
	ctx := context.Background()
	msg := &kgo.Record{
		Value: []byte("payload"),
	}

	var captured *Message
	producer := newProducer(ctx, ProducerParams{
		Name: "test-producer",
		ProducerFunc: func(m *Message) error {
			captured = m
			return nil
		},
	})

	err := producer.Produce(ctx, msg)

	require.NoError(t, err)
	assert.Equal(t, msg, captured)
}

func TestProducerProducePropagatesProducerFuncError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("produce failure")
	producer := newProducer(ctx, ProducerParams{
		Name: "test-producer",
		ProducerFunc: func(m *Message) error {
			return expectedErr
		},
	})

	err := producer.Produce(ctx, &kgo.Record{})

	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}

func TestProducerCloseCancelsContextWithoutKafkaProducer(t *testing.T) {
	ctx := context.Background()
	producer := newProducer(ctx, ProducerParams{Name: "test-producer"})

	err := producer.Close()
	require.NoError(t, err)

	select {
	case <-producer.ctx.Done():
	default:
		t.Fatalf("producer context should be canceled after Close")
	}
}
