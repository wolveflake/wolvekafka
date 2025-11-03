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
	"google.golang.org/protobuf/encoding/protojson"
	model_pb "wolvekafka/proto/model"
)

var (
	// Configure protojson to match the legacy jsonpb behaviour we relied on.
	marshaler = protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseEnumNumbers:  true,
	}
	// Allow round-tripping JSON back into the proto message.
	unmarshaler = protojson.UnmarshalOptions{}
)

// ConvertLambdaRequestToJson converts a LambdaRequest protobuf message to its JSON representation.
func ConvertLambdaRequestToJson(lambdaReq *model_pb.LambdaRequest) ([]byte, error) {
	return marshaler.Marshal(lambdaReq)
}

// ParseLambdaRequestJson parses a JSON representation of a LambdaRequest into its protobuf message.
func ParseLambdaRequestJson(payload []byte) (*model_pb.LambdaRequest, error) {
	result := &model_pb.LambdaRequest{}
	if err := unmarshaler.Unmarshal(payload, result); err != nil {
		return nil, err
	}
	return result, nil
}
