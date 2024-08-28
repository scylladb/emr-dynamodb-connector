/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.dynamodb.preader;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class ScanRecordReadRequest extends AbstractRecordReadRequest {

  public ScanRecordReadRequest(AbstractReadManager readMgr, DynamoDBRecordReaderContext context,
      int segment, Map<String, AttributeValue> lastEvaluatedKey) {
    super(readMgr, context, segment, lastEvaluatedKey);
  }

  @Override
  protected AbstractRecordReadRequest buildNextReadRequest(PageResults<Map<String,
      AttributeValue>> pageResults) {
    return new ScanRecordReadRequest(readMgr, context, segment, pageResults.lastEvaluatedKey);
  }

  @Override
  protected PageResults<Map<String, AttributeValue>> fetchPage(RequestLimit lim) {
    // Read from DynamoDB
    RetryResult<ScanResponse> retryResult = context.getClient()
            .scanTable(tableName, queryFilter().orElse(null), segment,
                context.getSplit().getTotalSegments(), lastEvaluatedKey, lim.items,
                context.getReporter());

    ScanResponse response = retryResult.result;
    int retries = retryResult.retries;

    double consumedCapacityUnits = 0.0;
    if (response.consumedCapacity() != null) {
      consumedCapacityUnits = response.consumedCapacity().capacityUnits();
    }
    return new PageResults<>(response.items(),
        // Default value of ScanResponse.lastEvaluatedKey is changed from NULL to
        // SdkAutoConstructMap in AWS SDK 2.x.
        // Translate the default value to NULL here, to keep this assumption in other classes.
        response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null,
        consumedCapacityUnits,
        retries);
  }

  /**
   * Set a Scan query filter to skip expired records if the configuration
   * provides the TTL attribute name
   */
  Optional<DynamoDBQueryFilter> queryFilter() {
    Optional<String> maybeTtlAttribute =
        Optional.ofNullable(context.getConf().get(DynamoDBConstants.TTL_ATTRIBUTE_NAME));
    return maybeTtlAttribute.map(attributeName -> {
      long now = Instant.now().getEpochSecond();
      DynamoDBQueryFilter filter = new DynamoDBQueryFilter();
      filter.addScanFilter(new DynamoDBFilter() {
        @Override
        public String getColumnName() {
          return attributeName;
        }

        @Override
        public String getColumnType() {
          throw new Error();
        }

        @Override
        public DynamoDBFilterOperator getOperator() {
          throw new Error();
        }

        @Override
        public Condition getDynamoDBCondition() {
          return Condition
              .builder()
              .comparisonOperator(ComparisonOperator.GT)
              .attributeValueList(AttributeValue.fromN(String.valueOf(now)))
              .build();
        }
      });
      return filter;
    });
  }

}
