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

package org.apache.hadoop.dynamodb;

import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_MAX_ITEM_SIZE;
import static org.mockito.Mockito.mock;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Capacity;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDBClientTest {

  private static final String TEST_PROXY_HOST = "test.proxy.host";
  private static final int TEST_PROXY_PORT = 5555;
  private static final String TEST_USERNAME = "username";
  private static final String TEST_PASSWORD = "password";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  DynamoDbClient mockClient = mock(DynamoDbClient.class);
  Configuration conf = new Configuration();
  DynamoDBClient client;

  @Before
  public void setup() {
    conf.clear();
    client = new DynamoDBClient(mockClient, conf);
  }

  @Test
  public void testDynamoDBCredentials() {
    final String DYNAMODB_ACCESS_KEY = "abc";
    final String DYNAMODB_SECRET_KEY = "xyz";
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, DYNAMODB_ACCESS_KEY);
    conf.set(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, DYNAMODB_SECRET_KEY);

    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    AwsCredentialsProvider provider = dynamoDBClient.getAwsCredentialsProvider(conf);
    Assert.assertEquals(DYNAMODB_ACCESS_KEY, provider.resolveCredentials().accessKeyId());
    Assert.assertEquals(DYNAMODB_SECRET_KEY, provider.resolveCredentials().secretAccessKey());
  }

  @Test
  public void testDefaultCredentials() {
    final String DEFAULT_ACCESS_KEY = "abc";
    final String DEFAULT_SECRET_KEY = "xyz";
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.DEFAULT_ACCESS_KEY_CONF, DEFAULT_ACCESS_KEY);
    conf.set(DynamoDBConstants.DEFAULT_SECRET_KEY_CONF, DEFAULT_SECRET_KEY);

    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    AwsCredentialsProvider provider = dynamoDBClient.getAwsCredentialsProvider(conf);
    Assert.assertEquals(DEFAULT_ACCESS_KEY, provider.resolveCredentials().accessKeyId());
    Assert.assertEquals(DEFAULT_SECRET_KEY, provider.resolveCredentials().secretAccessKey());
  }

  @Test
  public void testDefaultCredentialsWithSessionToken() {
    final String DEFAULT_ACCESS_KEY = "abc";
    final String DEFAULT_SECRET_KEY = "xyz";
    final String DEFAULT_SESSION_TOKEN = "007";
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.DEFAULT_ACCESS_KEY_CONF, DEFAULT_ACCESS_KEY);
    conf.set(DynamoDBConstants.DEFAULT_SECRET_KEY_CONF, DEFAULT_SECRET_KEY);
    conf.set(DynamoDBConstants.DEFAULT_SESSION_TOKEN_CONF, DEFAULT_SESSION_TOKEN);

    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    AwsCredentialsProvider provider = dynamoDBClient.getAwsCredentialsProvider(conf);
    AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) provider.resolveCredentials();
    Assert.assertEquals(DEFAULT_ACCESS_KEY, sessionCredentials.accessKeyId());
    Assert.assertEquals(DEFAULT_SECRET_KEY, sessionCredentials.secretAccessKey());
    Assert.assertEquals(DEFAULT_SESSION_TOKEN, sessionCredentials.sessionToken());
  }

  @Test
  public void testCustomCredentialsProviderWithConstructor() {
    final String MY_ACCESS_KEY = "abc";
    final String MY_SECRET_KEY = "xyz";
    Configuration conf = new Configuration();
    conf.set("my.accessKey", MY_ACCESS_KEY);
    conf.set("my.secretKey", MY_SECRET_KEY);
    conf.set(DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF, MyAWSCredentialsProvider.class
        .getName());

    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    AwsCredentialsProvider provider = dynamoDBClient.getAwsCredentialsProvider(conf);
    Assert.assertEquals(MY_ACCESS_KEY, provider.resolveCredentials().accessKeyId());
    Assert.assertEquals(MY_SECRET_KEY, provider.resolveCredentials().secretAccessKey());
  }

  @Test
  public void testCustomCredentialsProviderWithMethod() {
    final String MY_ACCESS_KEY = "abc";
    final String MY_SECRET_KEY = "xyz";
    Configuration conf = new Configuration();
    conf.set("my.accessKey", MY_ACCESS_KEY);
    conf.set("my.secretKey", MY_SECRET_KEY);
    conf.set(DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF, MyFactoryCredentialsProvider.class
        .getName());

    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    AwsCredentialsProvider provider = dynamoDBClient.getAwsCredentialsProvider(conf);
    Assert.assertEquals(MY_ACCESS_KEY, provider.resolveCredentials().accessKeyId());
    Assert.assertEquals(MY_SECRET_KEY, provider.resolveCredentials().secretAccessKey());
  }

  @Test
  public void testCustomProviderNotFound() {
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF, "org.apache.hadoop.dynamodb" +
        ".NonExistentCredentialsProvider");
    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    expectedException.expectCause(Is.isA(ClassNotFoundException.class));
    dynamoDBClient.getAwsCredentialsProvider(conf);
  }

  @Test
  public void testCustomProviderCannotCast() {
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF, Object.class.getName());
    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    expectedException.expect(ClassCastException.class);
    dynamoDBClient.getAwsCredentialsProvider(conf);
  }

  @Test
  public void testCustomDynamoDbClientBuilderTransformer() {
    final String localDynamoDBEndpoint = "http://localhost:8000";
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.CUSTOM_CLIENT_BUILDER_TRANSFORMER,
            MyDynamoDbClientBuilderTransformer.class.getName());
    conf.set("my.uri", localDynamoDBEndpoint);
    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    MyDynamoDbClientBuilderTransformer transformer =
            (MyDynamoDbClientBuilderTransformer) dynamoDBClient.getDynamoDbClientBuilderTransformer(conf);
    Assert.assertEquals(localDynamoDBEndpoint, transformer.uri);
  }

  @Test
  public void testDynamoDbClientBuilderTransformerNotFound() {
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.CUSTOM_CLIENT_BUILDER_TRANSFORMER, "org.foo.NonExistentTransformer");
    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    expectedException.expectCause(Is.isA(ClassNotFoundException.class));
    dynamoDBClient.getDynamoDbClientBuilderTransformer(conf);
  }

  @Test
  public void testBasicSessionCredentials(){
    final String DYNAMODB_ACCESS_KEY = "abc";
    final String DYNAMODB_SECRET_KEY = "xyz";
    final String DYNAMODB_SESSION_KEY = "007";
    Configuration conf = new Configuration();
    conf.set(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, DYNAMODB_ACCESS_KEY);
    conf.set(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, DYNAMODB_SECRET_KEY);
    conf.set(DynamoDBConstants.DYNAMODB_SESSION_TOKEN_CONF, DYNAMODB_SESSION_KEY);

    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    AwsCredentialsProvider provider = dynamoDBClient.getAwsCredentialsProvider(conf);
    AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) provider.resolveCredentials();
    Assert.assertEquals(DYNAMODB_ACCESS_KEY, sessionCredentials.accessKeyId());
    Assert.assertEquals(DYNAMODB_SECRET_KEY, sessionCredentials.secretAccessKey());
    Assert.assertEquals(DYNAMODB_SESSION_KEY, sessionCredentials.sessionToken());

  }

  @Test
  public void testDefaultCredentialProvider() {
    DynamoDBClient dynamoDBClient = new DynamoDBClient();
    AwsCredentialsProvider provider = dynamoDBClient.getAwsCredentialsProvider(conf);
    Assert.assertTrue(provider instanceof AwsCredentialsProviderChain);
    AwsCredentialsProviderChain providerChain = (AwsCredentialsProviderChain) provider;
    try {
      Field providersField = AwsCredentialsProviderChain.class.getDeclaredField("credentialsProviders");
      providersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<AwsCredentialsProvider> providers = (List<AwsCredentialsProvider>) providersField.get(providerChain);
      Assert.assertEquals(1, providers.size());
      Assert.assertTrue(providers.get(0) instanceof DefaultCredentialsProvider);
    } catch (Exception e) {
      Assert.fail("Unexpected error thrown: " + e.getMessage());
    }
  }

  @Test
  public void setsClientConfigurationProxyHostAndPortWhenBothAreSupplied() {
    setTestProxyHostAndPort(conf);
    ProxyConfiguration proxyConfig = client.applyProxyConfiguration(conf);
    Assert.assertEquals(TEST_PROXY_HOST, proxyConfig.host());
    Assert.assertEquals(TEST_PROXY_PORT, proxyConfig.port());
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenProxyPortIsMissing() {
    setProxyHostAndPort(conf, "test.proxy.host", 0);
    client.applyProxyConfiguration(conf);
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenProxyHostIsMissing() {
    setProxyHostAndPort(conf, null, 5555);
    client.applyProxyConfiguration(conf);
  }

  @Test
  public void
  setsClientConfigurationProxyUsernameAndPasswordWhenBothAreSuppliedWithProxyHostAndPort() {
    setTestProxyHostAndPort(conf);
    setProxyUsernameAndPassword(conf, TEST_USERNAME, TEST_PASSWORD);
    ProxyConfiguration proxyConfig = client.applyProxyConfiguration(conf);
    Assert.assertEquals(TEST_PROXY_HOST, proxyConfig.host());
    Assert.assertEquals(TEST_PROXY_PORT, proxyConfig.port());
    Assert.assertEquals(TEST_USERNAME, proxyConfig.username());
    Assert.assertEquals(TEST_PASSWORD, proxyConfig.password());
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenProxyUsernameIsMissing() {
    setTestProxyHostAndPort(conf);
    setProxyUsernameAndPassword(conf, null, TEST_PASSWORD);
    client.applyProxyConfiguration(conf);
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenProxyPasswordIsMissing() {
    setTestProxyHostAndPort(conf);
    conf.set(DynamoDBConstants.PROXY_USERNAME, TEST_USERNAME);
    client.applyProxyConfiguration(conf);
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenGivenProxyUsernameAndPasswordWithoutProxyHostAndPortAreNotSupplied() {
    setProxyUsernameAndPassword(conf, TEST_USERNAME, TEST_PASSWORD);
    client.applyProxyConfiguration(conf);
  }

  @Test(expected = RuntimeException.class)
  public void testPutBatchThrowsWhenItemIsTooLarge() throws Exception {
    Map<String, AttributeValue> item = ImmutableMap.of("",
        AttributeValue.fromS(Strings.repeat("a", (int) (DEFAULT_MAX_ITEM_SIZE + 1))));
    client.putBatch("dummyTable", item, 1, null, false);
  }

  @Test
  public void testPutBatchDoesNotThrowWhenItemIsNotTooLarge() throws Exception {
    Map<String, AttributeValue> item = ImmutableMap.of("",
        AttributeValue.fromS(Strings.repeat("a", (int) DEFAULT_MAX_ITEM_SIZE)));
    client.putBatch("dummyTable", item, 1, null, false);
  }

  @Test
  public void testPutBatchDeletionModeSuccessful() throws Exception {
    Map<String, AttributeValue> item = ImmutableMap.of("",
            AttributeValue.fromS(Strings.repeat("a", (int) DEFAULT_MAX_ITEM_SIZE)));

    client.putBatch("dummyTable", item, 1, null, true);

    for (Map.Entry<String, List<WriteRequest>> entry: client.getWriteBatchMap().entrySet()) {
      for (WriteRequest req: entry.getValue()) {
        Assert.assertNotNull(req.deleteRequest());
        Assert.assertNull(req.putRequest());
      }
    }
  }

  @Test
  public void testPutMultipleBatchDeletionModeSuccessful() throws Exception {
    Map<String, AttributeValue> item = ImmutableMap.of("",
            AttributeValue.fromS(Strings.repeat("a", (int) DEFAULT_MAX_ITEM_SIZE)));

    Mockito
            .when(mockClient.batchWriteItem(Mockito.<BatchWriteItemRequest>any()))
            .thenAnswer(i -> {
      BatchWriteItemRequest request = (BatchWriteItemRequest) i.getArguments()[0];

      ConsumedCapacity consumedCapacity = ConsumedCapacity.builder()
              .table(
                      Capacity
                              .builder()
                              .capacityUnits(100.0)
                              .build())
              .build();

      BatchWriteItemResponse response = BatchWriteItemResponse
              .builder()
              .unprocessedItems(request.requestItems())
              .consumedCapacity(consumedCapacity)
              .build();

      return response;
    });
    for (int i = 0; i < 5; ++i) {
      client.putBatch("dummyTable", item, 2, null, true);
    }

    for (Map.Entry<String, List<WriteRequest>> entry: client.getWriteBatchMap().entrySet()) {
      for (WriteRequest req: entry.getValue()) {
        Assert.assertNotNull(req.deleteRequest());
        Assert.assertNull(req.putRequest());
      }
    }
  }

  @Test
  public void testPutBatchDeletionModeSuccessfulWithAdditionalKeysInItem() throws Exception {
    Map<String, AttributeValue> item = ImmutableMap.of(
        "a", AttributeValue.fromS("a"),
        "b", AttributeValue.fromS("b")
    );

    conf.set(DynamoDBConstants.DYNAMODB_TABLE_KEY_NAMES, "a");

    client.putBatch("dummyTable", item, 1, null, true);

    for (Map.Entry<String, List<WriteRequest>> entry: client.getWriteBatchMap().entrySet()) {
      for (WriteRequest req: entry.getValue()) {
        Assert.assertNotNull(req.deleteRequest());
        Assert.assertEquals(1, req.deleteRequest().key().size());
        Assert.assertTrue(req.deleteRequest().key().containsKey("a"));
        Assert.assertNull(req.putRequest());
      }
    }
  }

  @Test
  public void testPutBatchDeletionFailsAsGivenItemDoesNotContainAnyKey() throws Exception {
    Map<String, AttributeValue> item = ImmutableMap.of(
        "c", AttributeValue.fromS("a"),
        "d", AttributeValue.fromS("b")
    );

    conf.set(DynamoDBConstants.DYNAMODB_TABLE_KEY_NAMES, "a,b");

    Assert.assertThrows(IllegalArgumentException.class, () ->
        client.putBatch("dummyTable", item, 1, null, true));
  }

  private void setTestProxyHostAndPort(Configuration conf) {
    setProxyHostAndPort(conf, TEST_PROXY_HOST, TEST_PROXY_PORT);
  }

  private void setProxyHostAndPort(Configuration conf, String host, int port) {
    if (!Strings.isNullOrEmpty(host)) {
      conf.set(DynamoDBConstants.PROXY_HOST, host);
    }
    if (port > 0) {
      conf.setInt(DynamoDBConstants.PROXY_PORT, port);
    }
  }

  private void setProxyUsernameAndPassword(Configuration conf, String username, String password) {
    if (!Strings.isNullOrEmpty(username)) {
      conf.set(DynamoDBConstants.PROXY_USERNAME, username);
    }
    if (!Strings.isNullOrEmpty(password)) {
      conf.set(DynamoDBConstants.PROXY_PASSWORD, password);
    }
  }

  // Default Constructor-based credential provider
  private static class MyAWSCredentialsProvider implements AwsCredentialsProvider, Configurable {
    private Configuration conf;
    private String accessKey;
    private String secretKey;

    private void init() {
      accessKey = conf.get("my.accessKey");
      secretKey = conf.get("my.secretKey");
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create(accessKey, secretKey);
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public void setConf(Configuration configuration) {
      this.conf = configuration;
      init();
    }
  }

  // Method-based constructor credential provider
  private static class MyFactoryCredentialsProvider implements AwsCredentialsProvider, Configurable {
    private Configuration conf;
    private String accessKey;
    private String secretKey;

    public static MyFactoryCredentialsProvider create() {
      return new MyFactoryCredentialsProvider();
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create(accessKey, secretKey);
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public void setConf(Configuration configuration) {
      this.conf = configuration;
      accessKey = conf.get("my.accessKey");
      secretKey = conf.get("my.secretKey");
    }
  }

  private static class MyDynamoDbClientBuilderTransformer implements DynamoDbClientBuilderTransformer, Configurable {

    private Configuration conf;
    protected String uri;

    @Override
    public DynamoDbClientBuilder apply(DynamoDbClientBuilder builder) {
      return builder.endpointProvider(params -> CompletableFuture.completedFuture(
              Endpoint.builder().url(URI.create(this.uri)).build()
      ));
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public void setConf(Configuration configuration) {
      this.conf = configuration;
      this.uri = conf.get("my.uri");
    }

  }
}
