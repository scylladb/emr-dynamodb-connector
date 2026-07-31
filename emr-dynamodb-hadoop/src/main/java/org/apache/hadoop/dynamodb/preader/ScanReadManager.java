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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.util.AbstractTimeSource;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ScanReadManager extends AbstractReadManager {

  public static final String TTL_FILTER_EXPRESSION =
      "attribute_not_exists(#ttl) OR NOT attribute_type(#ttl, :ttlType) OR #ttl > :now";

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
              DynamoDBQueryFilter filter = new DynamoDBQueryFilter();

              filter.setFilterExpression(TTL_FILTER_EXPRESSION);

              Map<String, String> expressionAttributeNames = new HashMap<>();
              expressionAttributeNames.put("#ttl", attributeName);
              filter.setExpressionAttributeNames(expressionAttributeNames);

              Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
              // attribute_type() expects a type code ("N", "S", "B", etc.) as an S-typed value
              expressionAttributeValues.put(":ttlType", AttributeValue.fromS("N"));
              long now = Instant.now().getEpochSecond();
              expressionAttributeValues.put(":now", AttributeValue.fromN(String.valueOf(now)));
              filter.setExpressionAttributeValues(expressionAttributeValues);

              return filter;
            });

    // Queue up segment scan requests
    for (Integer segment : shuffleSgments) {
      enqueueReadRequestToTail(new ScanRecordReadRequest(this, context, segment, maybeScanFilter,
          null /* lastEvaluatedKey */));
    }
  }
}
