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

package org.apache.hadoop.dynamodb.split;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DynamoDBSplitGeneratorTest {

  DynamoDBSplitGenerator splitGenerator = new DynamoDBSplitGenerator();

  @Test
  public void testGenerateEvenSplits() {
    InputSplit[] splits = splitGenerator.generateSplits(1, 1, generateSegments(1), getTestConf());
    verifySplits(splits, 1, 1);

    splits = splitGenerator.generateSplits(1000, 1000, generateSegments(1000), getTestConf());
    verifySplits(splits, 1000, 1000);
  }

  @Test
  public void testGenerateFewerSegmentsThanMappers() {
    InputSplit[] splits = splitGenerator.generateSplits(10, 1, generateSegments(1), getTestConf());
    verifySplits(splits, 1, 1);
  }

  @Test
  public void testGenerateMoreSegmentsThanMappersEvenly() {
    InputSplit[] splits = splitGenerator.generateSplits(10, 20, generateSegments(20), getTestConf());
    verifySplits(splits, 20, 10);
  }

  @Test
  public void testGenerateMoreSegmentsThanMappersUnevenly() {
    InputSplit[] splits = splitGenerator.generateSplits(10, 27, generateSegments(27), getTestConf());
    verifySplits(splits, 27, 10);
  }

  @Test
  public void testExcludedSegments1() {
    int maxClusterMapTasks = 10;
    int numTotalSegments = 10;
    int skippedSegments = 5;
    List<Integer> effectiveSegments = IntStream.range(0, 10).skip(skippedSegments).boxed().collect(Collectors.toList());
    InputSplit[] splits = splitGenerator.generateSplits(maxClusterMapTasks, numTotalSegments, effectiveSegments, getTestConf());
    // We expect as many mappers as the number of effective segments since maxClusterMapTasks is greater than
    // the number of effective segments
    int numMappers = numTotalSegments - skippedSegments;
    verifySkippedSegments(splits, effectiveSegments, numTotalSegments, numMappers);
  }

  @Test
  public void testExcludedSegments2() {
    int maxClusterMapTasks = 10;
    int numTotalSegments = 10;
    List<Integer> effectiveSegments = generateSegments(numTotalSegments);
    // Remove arbitrary segments
    effectiveSegments.remove(9);
    effectiveSegments.remove(7);
    effectiveSegments.remove(5);
    effectiveSegments.remove(1);
    int skippedSegments = numTotalSegments - effectiveSegments.size();
    InputSplit[] splits = splitGenerator.generateSplits(maxClusterMapTasks, numTotalSegments, effectiveSegments, getTestConf());
    // We expect as many mappers as the number of effective segments since maxClusterMapTasks is greater than
    // the number of effective segments
    int numMappers = numTotalSegments - skippedSegments;
    verifySkippedSegments(splits, effectiveSegments, numTotalSegments, numMappers);
  }

  @Test
  public void testExcludedSegments3() {
    int maxClusterMapTasks = 4;
    int numTotalSegments = 10;
    List<Integer> effectiveSegments = generateSegments(numTotalSegments);
    // Remove arbitrary segments
    effectiveSegments.remove(6);
    effectiveSegments.remove(5);
    effectiveSegments.remove(2);
    InputSplit[] splits = splitGenerator.generateSplits(maxClusterMapTasks, numTotalSegments, effectiveSegments, getTestConf());
    verifySkippedSegments(splits, effectiveSegments, numTotalSegments, maxClusterMapTasks);
  }

  private JobConf getTestConf() {
    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "abc");
    return conf;
  }

  private void verifySplits(InputSplit[] splits, int numSegments, int numMappers) {
    assertEquals(numMappers, splits.length);

    boolean[] segments = new boolean[numSegments];
    for (int i = 0; i < segments.length; i++) {
      segments[i] = false;
    }

    int numSegmentsPerSplit = numSegments / splits.length;
    for (InputSplit split1 : splits) {
      DynamoDBSplit split = (DynamoDBSplit) split1;
      assertEquals(segments.length, split.getTotalSegments());
      for (Integer segment : split.getSegments()) {
        assertFalse(segments[segment]);
        segments[segment] = true;
      }
      // Make sure no segment has way more than anyone else
      int numSegmentsThisSplit = split.getSegments().size();
      assertTrue(Math.abs(numSegmentsThisSplit - numSegmentsPerSplit) <= 1);
    }

    // Make sure every segment is accounted for
    for (int i = 0; i < segments.length; i++) {
      assertTrue(segments[i]);
    }
  }

  private void verifySkippedSegments(InputSplit[] splits, List<Integer> effectiveSegments, int numTotalSegments, int numMappers) {
    assertEquals(numMappers, splits.length);
    for (InputSplit split1 : splits) {
      DynamoDBSplit split = (DynamoDBSplit) split1;
      // Yet, each splits remembers correctly the number of total segments so that Scan requests are
      // correctly constructed
      assertEquals(numTotalSegments, split.getTotalSegments());
      for (Integer segment : split.getSegments()) {
        // Check that every segment in the split is part of the effective segments
        assertTrue(effectiveSegments.remove(segment));
      }
    }
    // Check that all the effective segments have been allocated to a split
    assertTrue(effectiveSegments.isEmpty());
  }

  private List<Integer> generateSegments(int numSegments) {
    return IntStream.range(0, numSegments).boxed().collect(Collectors.toList());
  }

}
