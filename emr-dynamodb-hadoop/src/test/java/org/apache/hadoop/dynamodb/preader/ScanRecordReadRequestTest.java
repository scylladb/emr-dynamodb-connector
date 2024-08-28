package org.apache.hadoop.dynamodb.preader;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;
import org.apache.hadoop.dynamodb.split.DynamoDBSegmentsSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RunWith(MockitoJUnitRunner.class)
public final class ScanRecordReadRequestTest {

  @Mock
  DynamoDBRecordReaderContext context;
  @Mock
  DynamoDBClient client;

  @Test
  public void fetchPageReturnsZeroConsumedCapacityWhenResultsConsumedCapacityIsNull() {
    RetryResult stubbedResult = new RetryResult<>(ScanResponse.builder()
        .consumedCapacity((ConsumedCapacity) null)
        .items(new HashMap<String, AttributeValue>())
        .build(), 0);
    stubScanTableWith(stubbedResult);

    when(context.getClient()).thenReturn(client);
    when(context.getConf()).thenReturn(new JobConf());
    when(context.getSplit()).thenReturn(new DynamoDBSegmentsSplit());
    ScanReadManager readManager = Mockito.mock(ScanReadManager.class);
    ScanRecordReadRequest readRequest = new ScanRecordReadRequest(readManager, context, 0, null);
    PageResults<Map<String, AttributeValue>> pageResults =
        readRequest.fetchPage(new RequestLimit(0, 0));
    assertEquals(0.0, pageResults.consumedRcu, 0.0);
  }

  @Test
  public void queryFilterIsEmptyWhenTtlAttributeNameIsEmpty() {
    when(context.getClient()).thenReturn(client);
    when(context.getConf()).thenReturn(new JobConf());
    when(context.getSplit()).thenReturn(new DynamoDBSegmentsSplit());
    ScanReadManager readManager = Mockito.mock(ScanReadManager.class);
    ScanRecordReadRequest readRequest = new ScanRecordReadRequest(readManager, context, 0, null);
    assertFalse(readRequest.queryFilter().isPresent());
  }

  @Test
  public void queryFilterIsDefinedWhenTtlAttributeNameIsDefined() {
    final String ttlAttributeName = "foo";
    when(context.getClient()).thenReturn(client);
    JobConf conf = new JobConf();
    conf.set(DynamoDBConstants.TTL_ATTRIBUTE_NAME, ttlAttributeName);
    when(context.getConf()).thenReturn(conf);
    when(context.getSplit()).thenReturn(new DynamoDBSegmentsSplit());
    ScanReadManager readManager = Mockito.mock(ScanReadManager.class);
    ScanRecordReadRequest readRequest = new ScanRecordReadRequest(readManager, context, 0, null);
    Optional<DynamoDBQueryFilter> maybeQueryFilter = readRequest.queryFilter();
    assertTrue(maybeQueryFilter.isPresent());
    Map<String, Condition> scanFilter = maybeQueryFilter.get().getScanFilter();
    assertNotNull(scanFilter.get(ttlAttributeName));
  }

  private void stubScanTableWith(RetryResult<ScanResponse> scanResultRetryResult) {
    when(client.scanTable(
        anyString(),
        any(DynamoDBQueryFilter.class),
        anyInt(),
        anyInt(),
        any(Map.class),
        anyLong(),
        any(Reporter.class))
    ).thenReturn(scanResultRetryResult);
  }

}
