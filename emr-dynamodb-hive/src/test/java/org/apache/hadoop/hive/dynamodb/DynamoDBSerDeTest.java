/**
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hive.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONObject;
import org.junit.Test;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class DynamoDBSerDeTest {

  private static final ObjectInspector STRING_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
  private static final ObjectInspector DOUBLE_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
  private static final ObjectInspector LONG_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
  private static final ObjectInspector BOOLEAN_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
  private static final ObjectInspector STRING_LIST_OBJECT_INSPECTOR = ObjectInspectorFactory
          .getStandardListObjectInspector(STRING_OBJECT_INSPECTOR);
  private static final ObjectInspector STRING_MAP_OBJECT_INSPECTOR = ObjectInspectorFactory
          .getStandardMapObjectInspector(STRING_OBJECT_INSPECTOR, STRING_OBJECT_INSPECTOR);
  private static final ObjectInspector LONG_MAP_OBJECT_INSPECTOR = ObjectInspectorFactory
          .getStandardMapObjectInspector(STRING_OBJECT_INSPECTOR, LONG_OBJECT_INSPECTOR);
  private static final ObjectInspector LIST_MAP_OBJECT_INSPECTOR = ObjectInspectorFactory
          .getStandardMapObjectInspector(STRING_OBJECT_INSPECTOR, STRING_LIST_OBJECT_INSPECTOR);

  @Test
  public void testPrimitives() throws SerDeException {
    List<String> attributeNames = Lists.newArrayList("animal", "height", "weight", "endangered");
    List<ObjectInspector> colOIs = Lists.newArrayList(STRING_OBJECT_INSPECTOR, DOUBLE_OBJECT_INSPECTOR,
            LONG_OBJECT_INSPECTOR, BOOLEAN_OBJECT_INSPECTOR);

    List<String> data = Lists.newArrayList("giraffe", "5.5", "1360", "true");

    Map<String, AttributeValue> expectedItemMap = Maps.newHashMap();
    expectedItemMap.put(attributeNames.get(0), new AttributeValue(data.get(0)));
    expectedItemMap.put(attributeNames.get(1), new AttributeValue().withN(data.get(1)));
    expectedItemMap.put(attributeNames.get(2), new AttributeValue().withN(data.get(2)));
    expectedItemMap.put(attributeNames.get(3), new AttributeValue().withBOOL(Boolean.valueOf(data.get(3))));

    List<Object> rowData = Lists.newArrayList();
    rowData.add(data.get(0));
    rowData.add(Double.parseDouble(data.get(1)));
    rowData.add(Long.parseLong(data.get(2)));
    rowData.add(Boolean.valueOf(data.get(3)));
    Map<String, AttributeValue> actualItemMap = getSerializedItem(attributeNames, colOIs, rowData);

    assertEquals(expectedItemMap, actualItemMap);
  }

  @Test
  public void testArray() throws SerDeException {
    List<String> attributeNames = Lists.newArrayList("list", "items");
    List<ObjectInspector> colOIs = Lists.newArrayList(STRING_OBJECT_INSPECTOR, STRING_LIST_OBJECT_INSPECTOR);

    String list = "groceries";
    List<String> items = Lists.newArrayList("milk", "bread", "eggs", "milk");
    List<AttributeValue> itemsAV = Lists.newArrayList();
    for (String item : items) {
      itemsAV.add(new AttributeValue(item));
    }

    Map<String, AttributeValue> expectedItemMap = Maps.newHashMap();
    expectedItemMap.put(attributeNames.get(0), new AttributeValue(list));
    expectedItemMap.put(attributeNames.get(1), new AttributeValue().withL(itemsAV));

    List<Object> rowData = Lists.newArrayList();
    rowData.add(list);
    rowData.add(items);
    Map<String, AttributeValue> actualItemMap = getSerializedItem(attributeNames, colOIs, rowData);

    assertEquals(expectedItemMap, actualItemMap);
  }

  @Test
  public void testMap() throws SerDeException {
    List<String> attributeNames = Lists.newArrayList("map", "ids", "lists");
    List<ObjectInspector> colOIs = Lists.newArrayList(STRING_OBJECT_INSPECTOR, LONG_MAP_OBJECT_INSPECTOR,
            LIST_MAP_OBJECT_INSPECTOR);

    String map = "people";
    List<String> people = Lists.newArrayList("bob", "linda", "tina", "gene", "louise");
    List<AttributeValue> peopleAV = Lists.newArrayList();
    Map<String, Long> ids = Maps.newHashMap();
    Map<String, AttributeValue> idsAV = Maps.newHashMap();
    Map<String, List<String>> lists = Maps.newHashMap();
    Map<String, AttributeValue> listsAV = Maps.newHashMap();
    for (int i = 0; i < people.size(); i++) {
      String person = people.get(i);
      long id = (long) i;

      peopleAV.add(new AttributeValue(person));
      ids.put(person, id);
      idsAV.put(person, new AttributeValue().withN(Long.toString(id)));
      lists.put(person, people.subList(0, i + 1));
      listsAV.put(person, new AttributeValue().withL(Lists.newArrayList(peopleAV)));
    }

    Map<String, AttributeValue> expectedItemMap = Maps.newHashMap();
    expectedItemMap.put(attributeNames.get(0), new AttributeValue(map));
    expectedItemMap.put(attributeNames.get(1), new AttributeValue().withM(idsAV));
    expectedItemMap.put(attributeNames.get(2), new AttributeValue().withM(listsAV));

    List<Object> rowData = Lists.newArrayList();
    rowData.add(map);
    rowData.add(ids);
    rowData.add(lists);

    Map<String, AttributeValue> actualItemMap = getSerializedItem(attributeNames, colOIs, rowData);

    assertEquals(expectedItemMap, actualItemMap);
  }

  @Test
  public void testItem() throws SerDeException {
    List<String> colNames = Lists.newArrayList("ddbitem");
    List<ObjectInspector> colOIs = Lists.newArrayList(STRING_MAP_OBJECT_INSPECTOR);

    List<String> attributeNames = Lists.newArrayList("animal", "height", "weight", "endangered");
    List<String> attributeTypes = Lists.newArrayList("s", "n", "n", "bOOL");
    List<String> data = Lists.newArrayList("giraffe", "5.5", "1360", "true");
    Map<String, AttributeValue> expectedItemMap = Maps.newHashMap();
    expectedItemMap.put(attributeNames.get(0), new AttributeValue(data.get(0)));
    expectedItemMap.put(attributeNames.get(1), new AttributeValue().withN(data.get(1)));
    expectedItemMap.put(attributeNames.get(2), new AttributeValue().withN(data.get(2)));
    expectedItemMap.put(attributeNames.get(3), new AttributeValue().withBOOL(Boolean.valueOf(data.get(3))));

    Map<String, String> itemCol = Maps.newHashMap();
    for (int i = 0; i < attributeNames.size(); i++) {
      String type = attributeTypes.get(i);
      Object value = data.get(i);
      if (type.equalsIgnoreCase("bool")) {
        value = Boolean.valueOf(data.get(i));
      }
      itemCol.put(attributeNames.get(i), new JSONObject().put(type, value).toString());
    }
    List<Object> rowData = Lists.newArrayList();
    rowData.add(itemCol);
    Map<String, AttributeValue> actualItemMap = getSerializedItem(colNames, colOIs, rowData);

    assertEquals(expectedItemMap, actualItemMap);

    // backup item
    String animal = "tiger";
    colNames.add(attributeNames.get(0));
    colOIs.add(STRING_OBJECT_INSPECTOR);
    rowData.add(animal);
    expectedItemMap.put(attributeNames.get(0), new AttributeValue(animal));
    actualItemMap = getSerializedItem(colNames, colOIs, rowData);

    assertEquals(expectedItemMap, actualItemMap);
  }

  private Map<String, AttributeValue> getSerializedItem(List<String> attributeNames, List<ObjectInspector> colOIs,
                                                        List<Object> rowData)
          throws SerDeException {
    List<String> colTypes = Lists.newArrayList();
    List<String> colMappings = Lists.newArrayList();
    for (int i = 0; i < attributeNames.size(); i++) {
      colTypes.add(colOIs.get(i).getTypeName());
      if (!HiveDynamoDBTypeFactory.isHiveDynamoDBItemMapType(colOIs.get(i))) {
        colMappings.add(attributeNames.get(i) + ":" + attributeNames.get(i));
      }
    }

    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(attributeNames, ","));
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, StringUtils.join(colTypes, ","));
    props.setProperty(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, StringUtils.join(colMappings, ","));

    DynamoDBSerDe serde = new DynamoDBSerDe();
    serde.initialize(null, props);

    StructObjectInspector rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(attributeNames, colOIs);
    DynamoDBItemWritable item = (DynamoDBItemWritable) serde.serialize(rowData, rowOI);
    return item.getItem();
  }
}