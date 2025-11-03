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
	"testing"

	"github.com/stretchr/testify/assert"

	model_pb "wolvekafka/proto/model"
)

func TestSimpleConversion(t *testing.T) {
	req := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			&model_pb.KafkaMessage{
				Topic:     "test_topic",
				Partition: 0,
				Offset:    12345,
				Headers: []*model_pb.Header{
					{Key: "header1", Value: []byte("value1")},
					{Key: "header2", Value: []byte("value2")},
				},
				Key:   []byte("test_key"),
				Value: []byte("test_value"),
			},
		},
	}
	lambdaReqJson, err := ConvertLambdaRequestToJson(req)
	assert.NoError(t, err)
	assert.NotEmpty(t, lambdaReqJson)

	parsedReq, err := ParseLambdaRequestJson(lambdaReqJson)
	assert.NoError(t, err)
	assert.Equal(t, len(req.Messages), len(parsedReq.Messages))
	assert.Equal(t, req.Messages[0].Topic, parsedReq.Messages[0].Topic)
	assert.Equal(t, req.Messages[0].Partition, parsedReq.Messages[0].Partition)
	assert.Equal(t, req.Messages[0].Offset, parsedReq.Messages[0].Offset)
	assert.Equal(t, len(req.Messages[0].Headers), len(parsedReq.Messages[0].Headers))
	assert.Equal(t, string(req.Messages[0].Headers[0].Value), string(parsedReq.Messages[0].Headers[0].Value))
	assert.Equal(t, string(req.Messages[0].Key), string(parsedReq.Messages[0].Key))
	assert.Equal(t, string(req.Messages[0].Value), string(parsedReq.Messages[0].Value))
}
