/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


/**
 * MiniKafkaCluster for Kafka 3.x using Testcontainers with KRaft mode (no ZooKeeper).
 */
public final class MiniKafkaCluster implements Closeable, StreamDataServerStartable {
  private static final String KAFKA_IMAGE = "apache/kafka:3.9.1";

  private final KafkaContainer _kafkaContainer;

  public MiniKafkaCluster() {
    _kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));
  }

  @Override
  public void init(Properties props) {
    // No-op: Testcontainers manages Kafka startup via container configuration.
  }

  @Override
  public void start() {
    _kafkaContainer.start();
  }

  @Override
  public void stop() {
    _kafkaContainer.stop();
  }

  @Override
  public void close()
      throws IOException {
    stop();
  }

  public String getKafkaServerAddress() {
    return _kafkaContainer.getBootstrapServers();
  }

  @Override
  public void createTopic(String topicName, Properties topicProps) {
    int numPartitions = Integer.parseInt(topicProps.getProperty("partition", "1"));
    try {
      createTopic(topicName, numPartitions, 1);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to create topic: " + topicName, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while creating topic: " + topicName, e);
    }
  }

  @Override
  public void deleteTopic(String topicName) {
    try (AdminClient adminClient = createAdminClient()) {
      adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to delete topic: " + topicName, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while deleting topic: " + topicName, e);
    }
  }

  @Override
  public void createPartitions(String topic, int numPartitions) {
    try (AdminClient adminClient = createAdminClient()) {
      Map<String, NewPartitions> newPartitions = new HashMap<>();
      newPartitions.put(topic, NewPartitions.increaseTo(numPartitions));
      adminClient.createPartitions(newPartitions).all().get();
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to create partitions for topic: " + topic, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while creating partitions for topic: " + topic, e);
    }
  }

  @Override
  public int getPort() {
    String bootstrapServers = getKafkaServerAddress();
    int colonIndex = bootstrapServers.lastIndexOf(':');
    if (colonIndex < 0) {
      throw new IllegalStateException("Unexpected bootstrap servers: " + bootstrapServers);
    }
    return Integer.parseInt(bootstrapServers.substring(colonIndex + 1));
  }

  public void createTopic(String topicName, int numPartitions, int replicationFactor)
      throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = createAdminClient()) {
      NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
      int retries = 5;
      while (retries > 0) {
        try {
          adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
          return;
        } catch (ExecutionException e) {
          if (e.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
            retries--;
            TimeUnit.SECONDS.sleep(1);
          } else {
            throw e;
          }
        }
      }
      throw new ExecutionException("Failed to create topic after retries", null);
    }
  }

  public void deleteRecordsBeforeOffset(String topicName, int partitionId, long offset)
      throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = createAdminClient()) {
      Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
      recordsToDelete.put(new TopicPartition(topicName, partitionId), RecordsToDelete.beforeOffset(offset));
      adminClient.deleteRecords(recordsToDelete).all().get();
    }
  }

  private AdminClient createAdminClient() {
    Properties kafkaClientConfig = new Properties();
    kafkaClientConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerAddress());
    return AdminClient.create(kafkaClientConfig);
  }
}
