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
package org.apache.pinot.plugin.stream.kafka30.server;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kafka 3.x implementation of {@link StreamDataServerStartable}.
 *
 * This class can either connect to an external broker or start a managed single-node KRaft
 * container for local quickstart usage. It is not thread-safe.
 */
public class KafkaServerStartable implements StreamDataServerStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServerStartable.class);

  private static final String KAFKA_IMAGE = "apache/kafka:3.9.1";
  private static final int KAFKA_STARTUP_TIMEOUT_SECONDS = 60;

  private static final int DEFAULT_BROKER_ID = 0;
  private static final int DEFAULT_KAFKA_PORT = 19092;
  private static final String DEFAULT_KAFKA_BROKER = "localhost:" + DEFAULT_KAFKA_PORT;

  private static final String KAFKA_SERVER_BOOTSTRAP_SERVERS = "kafka.server.bootstrap.servers";
  private static final String KAFKA_SERVER_PORT = "kafka.server.port";
  private static final String KAFKA_SERVER_BROKER_ID = "kafka.server.broker.id";
  private static final String KAFKA_SERVER_OWNER_NAME = "kafka.server.owner.name";
  private static final String KAFKA_SERVER_ALLOW_MANAGED_FOR_CONFIGURED_BROKER =
      "kafka.server.allow.managed.for.configured.broker";

  private static final String LOCALHOST = "localhost";
  private static final String LOOPBACK = "127.0.0.1";

  private String _ownerName = KafkaServerStartable.class.getSimpleName();
  private String _kafkaBrokerList = DEFAULT_KAFKA_BROKER;
  private int _kafkaPort = DEFAULT_KAFKA_PORT;
  private int _kafkaBrokerId = DEFAULT_BROKER_ID;
  private boolean _allowManagedKafkaForConfiguredBroker;

  private boolean _started;
  private String _resolvedKafkaBrokerList;
  private String _managedKafkaContainerName;

  @Override
  public void init(Properties props) {
    _ownerName = props.getProperty(KAFKA_SERVER_OWNER_NAME, _ownerName);
    _kafkaBrokerList = props.getProperty(KAFKA_SERVER_BOOTSTRAP_SERVERS, _kafkaBrokerList);
    _kafkaPort = parseInt(props.getProperty(KAFKA_SERVER_PORT), parsePort(_kafkaBrokerList, DEFAULT_KAFKA_PORT));
    _kafkaBrokerId = parseInt(props.getProperty(KAFKA_SERVER_BROKER_ID), DEFAULT_BROKER_ID);
    _allowManagedKafkaForConfiguredBroker =
        Boolean.parseBoolean(props.getProperty(KAFKA_SERVER_ALLOW_MANAGED_FOR_CONFIGURED_BROKER, "false"));
  }

  @Override
  public void start() {
    if (_started) {
      return;
    }

    try {
      if (shouldStartManagedKafka(_kafkaBrokerList) && !isKafkaAvailable(_kafkaBrokerList)) {
        ensureDockerDaemonRunning();
        startManagedKafkaContainer();
      }
      _resolvedKafkaBrokerList = resolveKafkaBrokerList(_kafkaBrokerList);
      LOGGER.info("Using external Kafka at {}", _resolvedKafkaBrokerList);
      _started = true;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Kafka for " + _ownerName, e);
    }
  }

  @Override
  public void stop() {
    if (_managedKafkaContainerName != null) {
      try {
        runProcess(List.of("docker", "rm", "-f", _managedKafkaContainerName), true);
      } catch (Exception e) {
        LOGGER.warn("Failed to stop managed Kafka container {}", _managedKafkaContainerName, e);
      }
    }
    _started = false;
  }

  @Override
  public void createTopic(String topic, Properties topicProps) {
    ensureStarted();
    int numPartitions = parseInt(String.valueOf(topicProps.getOrDefault("partition", 1)), 1);
    try (AdminClient adminClient = createKafkaAdminClient()) {
      adminClient.createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, (short) 1))).all().get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create topic: " + topic, e);
    }
  }

  @Override
  public int getPort() {
    return _kafkaPort;
  }

  private AdminClient createKafkaAdminClient() {
    Properties props = new Properties();
    props.put("bootstrap.servers", _resolvedKafkaBrokerList);
    return AdminClient.create(props);
  }

  private String resolveKafkaBrokerList(String configuredBrokerList) {
    if (isKafkaAvailable(configuredBrokerList)) {
      return configuredBrokerList;
    }

    String fallback = "localhost:9092";
    if (isKafkaAvailable(fallback)) {
      LOGGER.warn("Default Kafka broker {} is not reachable; using {}", configuredBrokerList, fallback);
      return fallback;
    }

    throw new IllegalStateException("Kafka broker list is not reachable: " + configuredBrokerList
        + ". Please start Kafka or pass -kafkaBrokerList.");
  }

  private boolean shouldStartManagedKafka(String brokerList) {
    if (_allowManagedKafkaForConfiguredBroker) {
      return isLocalBroker(brokerList);
    }
    return managedKafkaBroker().equals(brokerList);
  }

  private static boolean isLocalBroker(String brokerList) {
    int separator = brokerList.lastIndexOf(':');
    if (separator <= 0 || separator == brokerList.length() - 1) {
      return false;
    }
    String host = brokerList.substring(0, separator);
    return LOCALHOST.equalsIgnoreCase(host) || LOOPBACK.equals(host);
  }

  private static int parsePort(String brokerList, int defaultPort) {
    int separator = brokerList.lastIndexOf(':');
    if (separator <= 0 || separator == brokerList.length() - 1) {
      return defaultPort;
    }
    return parseInt(brokerList.substring(separator + 1), defaultPort);
  }

  private static int parseInt(String value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private boolean isKafkaAvailable(String brokerList) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("request.timeout.ms", "2000");
    props.put("default.api.timeout.ms", "2000");
    try (AdminClient adminClient = AdminClient.create(props)) {
      adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void ensureDockerDaemonRunning()
      throws Exception {
    LOGGER.info("Checking Docker daemon availability");
    try {
      runProcess(List.of("docker", "info"), false);
    } catch (Exception e) {
      throw new IllegalStateException("Docker daemon is not running or not reachable. Quickstart starts Kafka in "
          + "Docker on " + managedKafkaBroker()
          + ". Start Docker, or pass -kafkaBrokerList <host:port> to use an external Kafka broker.", e);
    }
  }

  private void pullKafkaDockerImage()
      throws Exception {
    LOGGER.info("Pulling Kafka Docker image {}", KAFKA_IMAGE);
    runProcess(List.of("docker", "pull", KAFKA_IMAGE), false);
    LOGGER.info("Kafka Docker image {} is ready", KAFKA_IMAGE);
  }

  private void startManagedKafkaContainer()
      throws Exception {
    _managedKafkaContainerName = "pinot-qs-kafka-" + System.currentTimeMillis();
    pullKafkaDockerImage();

    List<String> runCommand = List.of("docker", "run", "-d", "--name", _managedKafkaContainerName,
        "-p", _kafkaPort + ":9092",
        "-e", "KAFKA_NODE_ID=" + _kafkaBrokerId,
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + managedKafkaBroker(),
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=" + _kafkaBrokerId + "@localhost:9093",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        KAFKA_IMAGE);

    runProcess(List.of("docker", "rm", "-f", _managedKafkaContainerName), true);
    LOGGER.info("Starting Kafka container {}", _managedKafkaContainerName);
    runProcess(runCommand, false);
    waitForKafkaReady();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        runProcess(List.of("docker", "rm", "-f", _managedKafkaContainerName), true);
      } catch (Exception e) {
        LOGGER.warn("Failed to stop managed Kafka container {}", _managedKafkaContainerName, e);
      }
    }));
  }

  private void waitForKafkaReady()
      throws Exception {
    String managedBroker = managedKafkaBroker();
    LOGGER.info("Waiting for Kafka broker to become available on {}", managedBroker);
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(KAFKA_STARTUP_TIMEOUT_SECONDS);
    while (System.currentTimeMillis() < deadline) {
      if (isKafkaAvailable(managedBroker)) {
        LOGGER.info("Kafka broker is ready on {}", managedBroker);
        return;
      }
      Thread.sleep(1000L);
    }
    throw new IllegalStateException("Kafka container did not become ready on " + managedBroker);
  }

  private static String runProcess(List<String> command, boolean ignoreFailure)
      throws Exception {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
    int code = process.waitFor();
    if (code != 0 && !ignoreFailure) {
      throw new IllegalStateException("Command failed (" + code + "): " + String.join(" ", command)
          + (output.isEmpty() ? "" : "\n" + output));
    }
    return output;
  }

  private void ensureStarted() {
    if (!_started) {
      throw new IllegalStateException("Kafka is not started for " + _ownerName
          + ". Call startKafka() explicitly before using Kafka operations.");
    }
  }

  private String managedKafkaBroker() {
    return LOCALHOST + ":" + _kafkaPort;
  }
}
