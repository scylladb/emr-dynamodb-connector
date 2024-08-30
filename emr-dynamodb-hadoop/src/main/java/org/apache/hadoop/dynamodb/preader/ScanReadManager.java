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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.util.AbstractTimeSource;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;

public class ScanReadManager extends AbstractReadManager {

  public ScanReadManager(RateController rateController, AbstractTimeSource time,
      DynamoDBRecordReaderContext context) {
    super(rateController, time, context);
  }

  @Override
  protected void initializeReadRequests() {
    // Create a temporary copy of the segments, as we're about to shuffle it
    List<Integer> shuffleSgments = new ArrayList<>(context.getSplit().getSegments());
    if (shuffleSgments.isEmpty()) {
      String errorMsg = "0 segment. Need at least one segment to work with.";
      log.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    // Shuffle the segments.
    Collections.shuffle(shuffleSgments, new Random());

    // Keep track of how many segments remain to be scanned. Used by the
    // record readers to signal completion once all segments have been fully
    // scanned.
    segmentsRemaining.set(shuffleSgments.size());

    // Set a Scan query filter to skip expired records if the configuration
    // provides the TTL attribute name
    Optional<DynamoDBQueryFilter> maybeScanFilter =
        Optional.ofNullable(context.getConf().get(DynamoDBConstants.TTL_ATTRIBUTE_NAME))
            .map(attributeName -> {
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

    // Queue up segment scan requests
    for (Integer segment : shuffleSgments) {
      enqueueReadRequestToTail(new ScanRecordReadRequest(this, context, segment, maybeScanFilter,
          null /* lastEvaluatedKey */));
    }
  }
}
