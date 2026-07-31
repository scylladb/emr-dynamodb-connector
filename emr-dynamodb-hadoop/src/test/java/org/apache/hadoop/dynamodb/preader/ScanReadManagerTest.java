package org.apache.hadoop.dynamodb.preader;

import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.split.DynamoDBSegmentsSplit;
import org.apache.hadoop.dynamodb.util.MockTimeSource;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class ScanReadManagerTest {

  @Mock
  DynamoDBRecordReaderContext context;

  @Test
  public void queryFilterIsEmptyWhenTtlAttributeNameIsEmpty() {
    when(context.getConf()).thenReturn(new JobConf());
    when(context.getSplit()).thenReturn(new DynamoDBSegmentsSplit(null, 1, 1, Collections.singletonList(1), 1, 0, null));
    ScanReadManager readManager = new ScanReadManager(Mockito.mock(RateController.class), new MockTimeSource(), context);
    ScanRecordReadRequest readRequest = (ScanRecordReadRequest) readManager.dequeueReadRequest();
    assertFalse(readRequest.maybeScanFilter.isPresent());
  }

  @Test
  public void queryFilterIsDefinedWhenTtlAttributeNameIsDefined() {
    final String ttlAttributeName = "foo";
    JobConf conf = new JobConf();
    conf.set(DynamoDBConstants.TTL_ATTRIBUTE_NAME, ttlAttributeName);
    when(context.getConf()).thenReturn(conf);
    when(context.getSplit()).thenReturn(new DynamoDBSegmentsSplit(null, 1, 1, Collections.singletonList(1), 1, 0, null));
    ScanReadManager readManager = new ScanReadManager(Mockito.mock(RateController.class), new MockTimeSource(), context);
    ScanRecordReadRequest readRequest = (ScanRecordReadRequest) readManager.dequeueReadRequest();
    assertTrue(readRequest.maybeScanFilter.isPresent());
    org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter filter = readRequest.maybeScanFilter.get();
    assertEquals(ScanReadManager.TTL_FILTER_EXPRESSION, filter.getFilterExpression());
    assertEquals(ttlAttributeName, filter.getExpressionAttributeNames().get("#ttl"));
    assertEquals("N", filter.getExpressionAttributeValues().get(":ttlType").s());
    assertNotNull(filter.getExpressionAttributeValues().get(":now"));
  }

}
