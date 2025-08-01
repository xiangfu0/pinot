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
package org.apache.pinot.spi.utils;

import java.io.File;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.query.QueryThreadContext;


public class CommonConstants {
  private CommonConstants() {
  }

  public static final String ENVIRONMENT_IDENTIFIER = "environment";
  public static final String INSTANCE_FAILURE_DOMAIN = "failureDomain";
  public static final String DEFAULT_FAILURE_DOMAIN = "No such domain";

  public static final String PREFIX_OF_SSL_SUBSET = "ssl";
  public static final String HTTP_PROTOCOL = "http";
  public static final String HTTPS_PROTOCOL = "https";

  public static final String KEY_OF_AUTH = "auth";

  public static final String TABLE_NAME = "tableName";

  public static final String UNKNOWN = "unknown";
  public static final String CONFIG_OF_METRICS_FACTORY_CLASS_NAME = "factory.className";
  public static final String CONFIG_OF_BROKER_EVENT_LISTENER_CLASS_NAME = "factory.className";
  public static final String CONFIG_OF_REQUEST_CONTEXT_TRACKED_HEADER_KEYS = "request.context.tracked.header.keys";
  public static final String DEFAULT_METRICS_FACTORY_CLASS_NAME =
      //"org.apache.pinot.plugin.metrics.compound.CompoundPinotMetricsFactory";
      "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory";
  //"org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory";
  public static final String DEFAULT_BROKER_EVENT_LISTENER_CLASS_NAME =
      "org.apache.pinot.spi.eventlistener.query.NoOpBrokerQueryEventListener";

  public static final String SWAGGER_AUTHORIZATION_KEY = "oauth";
  public static final String SWAGGER_POM_PROPERTIES_PATH = "META-INF/maven/org.webjars/swagger-ui/pom.properties";
  public static final String CONFIG_OF_SWAGGER_RESOURCES_PATH = "META-INF/resources/webjars/swagger-ui/";
  public static final String CONFIG_OF_TIMEZONE = "pinot.timezone";

  public static final String DATABASE = "database";
  public static final String DEFAULT_DATABASE = "default";
  public static final String CONFIG_OF_PINOT_INSECURE_MODE = "pinot.insecure.mode";
  @Deprecated
  public static final String DEFAULT_PINOT_INSECURE_MODE = "false";

  public static final String CONFIG_OF_EXECUTORS_FIXED_NUM_THREADS = "pinot.executors.fixed.default.numThreads";
  public static final String DEFAULT_EXECUTORS_FIXED_NUM_THREADS = "-1";

  public static final String CONFIG_OF_PINOT_TAR_COMPRESSION_CODEC_NAME = "pinot.tar.compression.codec.name";
  public static final String QUERY_WORKLOAD = "queryWorkload";

  public static class Lucene {
    public static final String CONFIG_OF_LUCENE_MAX_CLAUSE_COUNT = "pinot.lucene.max.clause.count";
    public static final int DEFAULT_LUCENE_MAX_CLAUSE_COUNT = 1024;
  }
  public static final String JFR = "pinot.jfr";

  public static final String RLS_FILTERS = "rlsFilters";

  /**
   * The state of the consumer for a given segment
   */
  public enum ConsumerState {
    CONSUMING, NOT_CONSUMING // In error state
  }

  public enum TaskTriggers {
    CRON_TRIGGER, MANUAL_TRIGGER, ADHOC_TRIGGER, UNKNOWN
  }

  public static class Table {
    public static final String PUSH_FREQUENCY_HOURLY = "hourly";
    public static final String PUSH_FREQUENCY_DAILY = "daily";
    public static final String PUSH_FREQUENCY_WEEKLY = "weekly";
    public static final String PUSH_FREQUENCY_MONTHLY = "monthly";
  }

  public static class Helix {
    public static final String IS_SHUTDOWN_IN_PROGRESS = "shutdownInProgress";
    public static final String QUERIES_DISABLED = "queriesDisabled";
    public static final String QUERY_RATE_LIMIT_DISABLED = "queryRateLimitDisabled";
    public static final String DATABASE_MAX_QUERIES_PER_SECOND = "databaseMaxQueriesPerSecond";
    public static final String APPLICATION_MAX_QUERIES_PER_SECOND = "applicationMaxQueriesPerSecond";

    public static final String INSTANCE_CONNECTED_METRIC_NAME = "helix.connected";

    public static final String PREFIX_OF_CONTROLLER_INSTANCE = "Controller_";
    public static final String PREFIX_OF_BROKER_INSTANCE = "Broker_";
    public static final String PREFIX_OF_SERVER_INSTANCE = "Server_";
    public static final String PREFIX_OF_MINION_INSTANCE = "Minion_";

    public static final int CONTROLLER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_CONTROLLER_INSTANCE.length();
    public static final int BROKER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_BROKER_INSTANCE.length();
    public static final int SERVER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_SERVER_INSTANCE.length();
    public static final int MINION_INSTANCE_PREFIX_LENGTH = PREFIX_OF_MINION_INSTANCE.length();

    public static final String BROKER_RESOURCE_INSTANCE = "brokerResource";
    public static final String LEAD_CONTROLLER_RESOURCE_NAME = "leadControllerResource";

    public static final String LEAD_CONTROLLER_RESOURCE_ENABLED_KEY = "RESOURCE_ENABLED";

    public static final String ENABLE_CASE_INSENSITIVE_KEY = "enable.case.insensitive";
    public static final boolean DEFAULT_ENABLE_CASE_INSENSITIVE = true;

    public static final String DEFAULT_HYPERLOGLOG_LOG2M_KEY = "default.hyperloglog.log2m";
    public static final int DEFAULT_HYPERLOGLOG_LOG2M = 8;
    public static final int DEFAULT_HYPERLOGLOG_PLUS_P = 14;
    public static final int DEFAULT_HYPERLOGLOG_PLUS_SP = 0;

    // 2 to the power of 14, for tradeoffs see datasketches library documentation:
    // https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html
    public static final int DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES = 16384;

    // 2 to the power of 14, for tradeoffs see datasketches library documentation:
    // https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html
    public static final int DEFAULT_TUPLE_SKETCH_LGK = 14;

    public static final int DEFAULT_CPC_SKETCH_LGK = 12;
    public static final int DEFAULT_ULTRALOGLOG_P = 12;

    // K is set to 200, for tradeoffs see datasketches library documentation:
    // https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html#:~:
    public static final int DEFAULT_KLL_SKETCH_K = 200;

    // Whether to rewrite DistinctCount to DistinctCountBitmap
    public static final String ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY = "enable.distinct.count.bitmap.override";

    // More information on why these numbers are set can be found in the following doc:
    // https://cwiki.apache.org/confluence/display/PINOT/Controller+Separation+between+Helix+and+Pinot
    public static final int NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE = 24;
    public static final int LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT = 1;
    public static final int MIN_ACTIVE_REPLICAS = 0;

    // Instance tags
    public static final String CONTROLLER_INSTANCE = "controller";
    public static final String UNTAGGED_BROKER_INSTANCE = "broker_untagged";
    public static final String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    public static final String UNTAGGED_MINION_INSTANCE = "minion_untagged";

    public static class StateModel {
      public static class SegmentStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
        public static final String CONSUMING = "CONSUMING";
      }

      public static class DisplaySegmentStatus {
        public static final String BAD = "BAD";
        public static final String GOOD = "GOOD";
        public static final String UPDATING = "UPDATING";
      }

      public static class BrokerResourceStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
      }
    }

    public static class ZkClient {
      public static final int DEFAULT_CONNECT_TIMEOUT_MS = 60_000;
      public static final int DEFAULT_SESSION_TIMEOUT_MS = 30_000;
      // Retry interval and count for ZK operations where we would rather fail than get an empty (wrong) result back
      public static final int RETRY_INTERVAL_MS = 50;
      public static final int RETRY_COUNT = 2;
      public static final String ZK_CLIENT_CONNECTION_TIMEOUT_MS_CONFIG = "zk.client.connection.timeout.ms";
      public static final String ZK_CLIENT_SESSION_TIMEOUT_MS_CONFIG = "zk.client.session.timeout.ms";
    }

    public static class DataSource {
      public enum SegmentAssignmentStrategyType {
        RandomAssignmentStrategy,
        BalanceNumSegmentAssignmentStrategy,
        BucketizedSegmentAssignmentStrategy,
        ReplicaGroupSegmentAssignmentStrategy
      }
    }

    public static class Instance {
      @Deprecated
      public static final String INSTANCE_ID_KEY = "instanceId";
      public static final String DATA_DIR_KEY = "dataDir";
      public static final String ADMIN_PORT_KEY = "adminPort";
      public static final String ADMIN_HTTPS_PORT_KEY = "adminHttpsPort";
      public static final String GRPC_PORT_KEY = "grpcPort";
      public static final String NETTY_TLS_PORT_KEY = "nettyTlsPort";

      public static final String MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY = "queryServerPort";
      public static final String MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY = "queryMailboxPort";

      public static final String SYSTEM_RESOURCE_INFO_KEY = "SYSTEM_RESOURCE_INFO";
      public static final String PINOT_VERSION_KEY = "pinotVersion";
    }

    public static final String SET_INSTANCE_ID_TO_HOSTNAME_KEY = "pinot.set.instance.id.to.hostname";

    public static final String KEY_OF_SERVER_NETTY_PORT = "pinot.server.netty.port";
    public static final int DEFAULT_SERVER_NETTY_PORT = 8098;
    public static final String KEY_OF_SERVER_NETTYTLS_PORT = Server.SERVER_NETTYTLS_PREFIX + ".port";
    public static final int DEFAULT_SERVER_NETTYTLS_PORT = 8091;
    public static final String KEY_OF_BROKER_QUERY_PORT = "pinot.broker.client.queryPort";
    public static final int DEFAULT_BROKER_QUERY_PORT = 8099;
    public static final String KEY_OF_SERVER_NETTY_HOST = "pinot.server.netty.host";
    public static final String KEY_OF_MINION_HOST = "pinot.minion.host";
    public static final String KEY_OF_MINION_PORT = "pinot.minion.port";

    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    public static final String CONFIG_OF_CONTROLLER_FLAPPING_TIME_WINDOW_MS = "pinot.controller.flapping.timeWindowMs";
    public static final String CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS = "pinot.broker.flapping.timeWindowMs";
    public static final String CONFIG_OF_SERVER_FLAPPING_TIME_WINDOW_MS = "pinot.server.flapping.timeWindowMs";
    public static final String CONFIG_OF_MINION_FLAPPING_TIME_WINDOW_MS = "pinot.minion.flapping.timeWindowMs";
    public static final String CONFIG_OF_HELIX_INSTANCE_MAX_STATE_TRANSITIONS =
        "pinot.helix.instance.state.maxStateTransitions";
    public static final String DEFAULT_HELIX_INSTANCE_MAX_STATE_TRANSITIONS = "100000";
    public static final String DEFAULT_FLAPPING_TIME_WINDOW_MS = "1";
    public static final String PINOT_SERVICE_ROLE = "pinot.service.role";
    public static final String CONFIG_OF_CLUSTER_NAME = "pinot.cluster.name";
    public static final String CONFIG_OF_ZOOKEEPR_SERVER = "pinot.zk.server";

    public static final String CONFIG_OF_PINOT_CONTROLLER_STARTABLE_CLASS = "pinot.controller.startable.class";
    public static final String CONFIG_OF_PINOT_BROKER_STARTABLE_CLASS = "pinot.broker.startable.class";
    public static final String CONFIG_OF_PINOT_SERVER_STARTABLE_CLASS = "pinot.server.startable.class";
    public static final String CONFIG_OF_PINOT_MINION_STARTABLE_CLASS = "pinot.minion.startable.class";

    public static final String CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED = "pinot.multistage.engine.enabled";
    public static final boolean DEFAULT_MULTI_STAGE_ENGINE_ENABLED = true;

    public static final String CONFIG_OF_MULTI_STAGE_ENGINE_TLS_ENABLED = "pinot.multistage.engine.tls.enabled";
    public static final boolean DEFAULT_MULTI_STAGE_ENGINE_TLS_ENABLED = false;

    // This is a "beta" config and can be changed or even removed in future releases.
    public static final String CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS =
        "pinot.beta.multistage.engine.max.server.query.threads";
    public static final String DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS = "-1";
    public static final String CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR =
        "pinot.beta.multistage.engine.max.server.query.threads.hardlimit.factor";
    public static final String DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR = "4";

    // Preprocess throttle configs
    public static final String CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM =
        "pinot.server.max.segment.preprocess.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM = String.valueOf(Integer.MAX_VALUE);
    // Before serving queries is enabled, we should use a higher preprocess parallelism to process segments faster
    public static final String CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.preprocess.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);

    // Preprocess throttle config specifically for StarTree index rebuild
    public static final String CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM =
        "pinot.server.max.segment.startree.preprocess.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM = String.valueOf(Integer.MAX_VALUE);
    public static final String CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.startree.preprocess.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);

    // Preprocess throttle config specifically for StarTree index rebuild
    public static final String CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM =
        "pinot.server.max.segment.multicol.text.index.preprocess.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM =
        String.valueOf(Integer.MAX_VALUE);
    public static final String CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.multicol.text.index.preprocess.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);

    // Download throttle config
    public static final String CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM =
        "pinot.server.max.segment.download.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM = String.valueOf(Integer.MAX_VALUE);
    public static final String CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.download.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    public static final String DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);
  }

  public static class Broker {
    public static final String ROUTING_TABLE_CONFIG_PREFIX = "pinot.broker.routing.table";
    public static final String ACCESS_CONTROL_CONFIG_PREFIX = "pinot.broker.access.control";
    public static final String METRICS_CONFIG_PREFIX = "pinot.broker.metrics";
    public static final String EVENT_LISTENER_CONFIG_PREFIX = "pinot.broker.event.listener";
    public static final String CONFIG_OF_METRICS_NAME_PREFIX = "pinot.broker.metrics.prefix";
    public static final String DEFAULT_METRICS_NAME_PREFIX = "pinot.broker.";

    public static final String CONFIG_OF_DELAY_SHUTDOWN_TIME_MS = "pinot.broker.delayShutdownTimeMs";
    public static final long DEFAULT_DELAY_SHUTDOWN_TIME_MS = 10_000L;
    public static final String CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS = "pinot.broker.enableTableLevelMetrics";
    public static final boolean DEFAULT_ENABLE_TABLE_LEVEL_METRICS = true;
    public static final String CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS =
        "pinot.broker.allowedTablesForEmittingMetrics";

    public static final String CONFIG_OF_BROKER_QUERY_REWRITER_CLASS_NAMES = "pinot.broker.query.rewriter.class.names";
    public static final String CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT = "pinot.broker.query.response.limit";
    public static final String CONFIG_OF_BROKER_DEFAULT_QUERY_LIMIT = "pinot.broker.default.query.limit";

    public static final int DEFAULT_BROKER_QUERY_RESPONSE_LIMIT = Integer.MAX_VALUE;

    // -1 means no limit; value of 10 aligns limit with PinotQuery's defaults.
    public static final int DEFAULT_BROKER_QUERY_LIMIT = 10;

    public static final String CONFIG_OF_BROKER_QUERY_LOG_LENGTH = "pinot.broker.query.log.length";
    public static final int DEFAULT_BROKER_QUERY_LOG_LENGTH = Integer.MAX_VALUE;
    public static final String CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND =
        "pinot.broker.query.log.maxRatePerSecond";
    public static final String CONFIG_OF_BROKER_QUERY_LOG_BEFORE_PROCESSING =
        "pinot.broker.query.log.logBeforeProcessing";
    public static final boolean DEFAULT_BROKER_QUERY_LOG_BEFORE_PROCESSING = true;
    public static final String CONFIG_OF_BROKER_QUERY_ENABLE_NULL_HANDLING = "pinot.broker.query.enable.null.handling";
    public static final String CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION = "pinot.broker.enable.query.cancellation";
    public static final double DEFAULT_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND = 10_000d;
    public static final String CONFIG_OF_BROKER_TIMEOUT_MS = "pinot.broker.timeoutMs";
    public static final long DEFAULT_BROKER_TIMEOUT_MS = 10_000L;
    public static final String CONFIG_OF_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH =
        "pinot.broker.enable.row.column.level.auth";
    public static final boolean DEFAULT_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH = false;
    public static final String CONFIG_OF_EXTRA_PASSIVE_TIMEOUT_MS = "pinot.broker.extraPassiveTimeoutMs";
    public static final long DEFAULT_EXTRA_PASSIVE_TIMEOUT_MS = 100L;
    public static final String CONFIG_OF_BROKER_ID = "pinot.broker.instance.id";
    public static final String CONFIG_OF_BROKER_INSTANCE_TAGS = "pinot.broker.instance.tags";
    public static final String CONFIG_OF_BROKER_HOSTNAME = "pinot.broker.hostname";
    public static final String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.broker.swagger.use.https";
    // Comma separated list of packages that contains javax service resources.
    public static final String BROKER_RESOURCE_PACKAGES = "broker.restlet.api.resource.packages";
    public static final String DEFAULT_BROKER_RESOURCE_PACKAGES = "org.apache.pinot.broker.api.resources";

    // Configuration to consider the broker ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this broker has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    public static final String CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.broker.startup.minResourcePercent";
    public static final double DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;
    public static final String CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE = "pinot.broker.enable.query.limit.override";

    // Config for number of threads to use for Broker reduce-phase.
    public static final String CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY = "pinot.broker.max.reduce.threads.per.query";
    public static final int DEFAULT_MAX_REDUCE_THREADS_PER_QUERY =
        Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));
    // Same logic as CombineOperatorUtils

    // Config for Jersey ThreadPoolExecutorProvider.
    // By default, Jersey uses the default unbounded thread pool to process queries.
    // By enabling it, BrokerManagedAsyncExecutorProvider will be used to create a bounded thread pool.
    public static final String CONFIG_OF_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR =
        "pinot.broker.enable.bounded.jersey.threadpool.executor";
    public static final boolean DEFAULT_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR = false;
    // Default capacities for the bounded thread pool
    public static final String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE =
        "pinot.broker.jersey.threadpool.executor.max.pool.size";
    public static final int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE =
        Runtime.getRuntime().availableProcessors() * 2;
    public static final String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE =
        "pinot.broker.jersey.threadpool.executor.core.pool.size";
    public static final int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE =
        Runtime.getRuntime().availableProcessors() * 2;
    public static final String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE =
        "pinot.broker.jersey.threadpool.executor.queue.size";
    public static final int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE = Integer.MAX_VALUE;

    // Configs for broker reduce on group-by queries (only apply to SSE)
    public static final String CONFIG_OF_BROKER_GROUPBY_TRIM_THRESHOLD = "pinot.broker.groupby.trim.threshold";
    public static final int DEFAULT_BROKER_GROUPBY_TRIM_THRESHOLD = 1_000_000;
    public static final String CONFIG_OF_BROKER_MIN_GROUP_TRIM_SIZE = "pinot.broker.min.group.trim.size";
    public static final int DEFAULT_BROKER_MIN_GROUP_TRIM_SIZE = 5000;
    public static final String CONFIG_OF_BROKER_MIN_INITIAL_INDEXED_TABLE_CAPACITY =
        "pinot.broker.min.init.indexed.table.capacity";
    public static final int DEFAULT_BROKER_MIN_INITIAL_INDEXED_TABLE_CAPACITY = 128;

    // Config for enabling group trim for MSE group-by queries. When group trim is enabled, there are 3 levels of
    // trimming: segment level (shared with SSE, disabled by default), leaf stage level (shared with SSE, enabled by
    // default), intermediate stage level (enabled by default). The group trim behavior for each level is configured on
    // the server side.
    public static final String CONFIG_OF_MSE_ENABLE_GROUP_TRIM = "pinot.broker.mse.enable.group.trim";
    public static final boolean DEFAULT_MSE_ENABLE_GROUP_TRIM = false;

    public static final String CONFIG_OF_MSE_MAX_SERVER_QUERY_THREADS = "pinot.broker.mse.max.server.query.threads";
    public static final int DEFAULT_MSE_MAX_SERVER_QUERY_THREADS = -1;
    public static final String CONFIG_OF_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY =
        "pinot.broker.mse.max.server.query.threads.exceed.strategy";
    public static final String DEFAULT_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY = "WAIT";

    // Configure the request handler type used by broker to handler inbound query request.
    // NOTE: the request handler type refers to the communication between Broker and Server.
    public static final String BROKER_REQUEST_HANDLER_TYPE = "pinot.broker.request.handler.type";
    public static final String NETTY_BROKER_REQUEST_HANDLER_TYPE = "netty";
    public static final String GRPC_BROKER_REQUEST_HANDLER_TYPE = "grpc";
    public static final String MULTI_STAGE_BROKER_REQUEST_HANDLER_TYPE = "multistage";
    public static final String DEFAULT_BROKER_REQUEST_HANDLER_TYPE = NETTY_BROKER_REQUEST_HANDLER_TYPE;

    public static final String BROKER_TLS_PREFIX = "pinot.broker.tls";
    public static final String BROKER_NETTY_PREFIX = "pinot.broker.netty";
    public static final String BROKER_NETTYTLS_ENABLED = "pinot.broker.nettytls.enabled";
    //Set to true to load all services tagged and compiled with hk2-metadata-generator. Default to False
    public static final String BROKER_SERVICE_AUTO_DISCOVERY = "pinot.broker.service.auto.discovery";

    public static final String DISABLE_GROOVY = "pinot.broker.disable.query.groovy";
    public static final boolean DEFAULT_DISABLE_GROOVY = true;
    // Rewrite potential expensive functions to their approximation counterparts
    // - DISTINCT_COUNT -> DISTINCT_COUNT_SMART_HLL
    // - PERCENTILE -> PERCENTILE_SMART_TDIGEST
    public static final String USE_APPROXIMATE_FUNCTION = "pinot.broker.use.approximate.function";

    public static final String CONTROLLER_URL = "pinot.broker.controller.url";

    public static final String CONFIG_OF_BROKER_REQUEST_CLIENT_IP_LOGGING = "pinot.broker.request.client.ip.logging";

    // TODO: Support populating clientIp for GrpcRequestIdentity.
    public static final boolean DEFAULT_BROKER_REQUEST_CLIENT_IP_LOGGING = false;

    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.broker.logger.root.dir";
    public static final String CONFIG_OF_SWAGGER_BROKER_ENABLED = "pinot.broker.swagger.enabled";
    public static final boolean DEFAULT_SWAGGER_BROKER_ENABLED = true;
    public static final String CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT =
        "pinot.broker.instance.enableThreadCpuTimeMeasurement";
    public static final String CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT =
        "pinot.broker.instance.enableThreadAllocatedBytesMeasurement";
    public static final boolean DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT = false;
    public static final boolean DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT = false;
    public static final String CONFIG_OF_BROKER_RESULT_REWRITER_CLASS_NAMES =
        "pinot.broker.result.rewriter.class.names";

    public static final String CONFIG_OF_ENABLE_PARTITION_METADATA_MANAGER =
        "pinot.broker.enable.partition.metadata.manager";
    public static final boolean DEFAULT_ENABLE_PARTITION_METADATA_MANAGER = true;
    // Whether to infer partition hint by default or not.
    // This value can always be overridden by INFER_PARTITION_HINT query option
    public static final String CONFIG_OF_INFER_PARTITION_HINT = "pinot.broker.multistage.infer.partition.hint";
    public static final boolean DEFAULT_INFER_PARTITION_HINT = false;

    /**
     * Whether to use spools in multistage query engine by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_SPOOLS} query option
     */
    public static final String CONFIG_OF_SPOOLS = "pinot.broker.multistage.spools";
    public static final boolean DEFAULT_OF_SPOOLS = false;

    /// Whether to only use servers for leaf stages as the workers for the intermediate stages.
    /// This value can always be overridden by [Request.QueryOptionKey#USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE].
    public static final String CONFIG_OF_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE =
        "pinot.broker.mse.use.leaf.server.for.intermediate.stage";
    public static final boolean DEFAULT_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE = false;

    public static final String CONFIG_OF_USE_FIXED_REPLICA = "pinot.broker.use.fixed.replica";
    public static final boolean DEFAULT_USE_FIXED_REPLICA = false;

    // Broker config indicating the maximum serialized response size across all servers for a query. This value is
    // equally divided across all servers processing the query.
    // The value can be in human readable format (e.g. '200K', '200KB', '0.2MB') or in raw bytes (e.g. '200000').
    public static final String CONFIG_OF_MAX_QUERY_RESPONSE_SIZE_BYTES = "pinot.broker.max.query.response.size.bytes";

    // Broker config indicating the maximum length of the serialized response per server for a query.
    // If both "server.response.size" and "query.response.size" are set, then the "server.response.size" takes
    // precedence over "query.response.size" (i.e., "query.response.size" will be ignored).
    public static final String CONFIG_OF_MAX_SERVER_RESPONSE_SIZE_BYTES = "pinot.broker.max.server.response.size.bytes";

    public static final String CONFIG_OF_NEW_SEGMENT_EXPIRATION_SECONDS = "pinot.broker.new.segment.expiration.seconds";
    public static final long DEFAULT_VALUE_OF_NEW_SEGMENT_EXPIRATION_SECONDS = TimeUnit.MINUTES.toSeconds(5);

    // If this config is set to true, the broker will check every query executed using the v1 query engine and attempt
    // to determine whether the query could have successfully been run on the v2 / multi-stage query engine. If not,
    // a counter metric will be incremented - if this counter remains 0 during regular query workload execution, it
    // signals that users can potentially migrate their query workload to the multistage query engine.
    public static final String CONFIG_OF_BROKER_ENABLE_MULTISTAGE_MIGRATION_METRIC =
        "pinot.broker.enable.multistage.migration.metric";
    public static final boolean DEFAULT_ENABLE_MULTISTAGE_MIGRATION_METRIC = false;
    public static final String CONFIG_OF_BROKER_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN =
        "pinot.broker.enable.dynamic.filtering.semijoin";
    public static final boolean DEFAULT_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN = true;

    /**
     * Whether to use physical optimizer by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_PHYSICAL_OPTIMIZER} query option
     */
    public static final String CONFIG_OF_USE_PHYSICAL_OPTIMIZER = "pinot.broker.multistage.use.physical.optimizer";
    public static final boolean DEFAULT_USE_PHYSICAL_OPTIMIZER = false;

    /**
     * Whether to use lite mode by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_LITE_MODE} query option
     */
    public static final String CONFIG_OF_USE_LITE_MODE = "pinot.broker.multistage.use.lite.mode";
    public static final boolean DEFAULT_USE_LITE_MODE = false;

    /**
     * Whether to run in broker by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#RUN_IN_BROKER} query option
     */
    public static final String CONFIG_OF_RUN_IN_BROKER = "pinot.broker.multistage.run.in.broker";
    public static final boolean DEFAULT_RUN_IN_BROKER = true;

    /**
     * Whether to use broker pruning by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_BROKER_PRUNING} query option
     */
    public static final String CONFIG_OF_USE_BROKER_PRUNING = "pinot.broker.multistage.use.broker.pruning";
    public static final boolean DEFAULT_USE_BROKER_PRUNING = true;

    /**
     * Default server stage limit for lite mode queries.
     * This value can always be overridden by {@link Request.QueryOptionKey#LITE_MODE_SERVER_STAGE_LIMIT} query option
     */
    public static final String CONFIG_OF_LITE_MODE_LEAF_STAGE_LIMIT =
        "pinot.broker.multistage.lite.mode.leaf.stage.limit";
    public static final int DEFAULT_LITE_MODE_LEAF_STAGE_LIMIT = 100_000;

    // Config for default hash function used in KeySelector for data shuffling
    public static final String CONFIG_OF_BROKER_DEFAULT_HASH_FUNCTION = "pinot.broker.multistage.default.hash.function";
    public static final String DEFAULT_BROKER_DEFAULT_HASH_FUNCTION = "absHashCode";

    // When the server instance's pool field is null or the pool contains multi distinguished group value, the broker
    // would set the pool to -1 in the routing table for that server.
    public static final int FALLBACK_POOL_ID = -1;
    // keep the variable to pass the compability test
    @Deprecated
    public static final int FALLBACK_REPLICA_GROUP_ID = -1;

    public static class Request {
      public static final String SQL = "sql";
      public static final String SQL_V1 = "sqlV1";
      public static final String SQL_V2 = "sqlV2";
      public static final String TRACE = "trace";
      public static final String QUERY_OPTIONS = "queryOptions";

      public static class QueryOptionKey {
        public static final String TIMEOUT_MS = "timeoutMs";
        public static final String EXTRA_PASSIVE_TIMEOUT_MS = "extraPassiveTimeoutMs";
        public static final String SKIP_UPSERT = "skipUpsert";
        public static final String SKIP_UPSERT_VIEW = "skipUpsertView";
        public static final String UPSERT_VIEW_FRESHNESS_MS = "upsertViewFreshnessMs";
        public static final String USE_STAR_TREE = "useStarTree";
        public static final String SCAN_STAR_TREE_NODES = "scanStarTreeNodes";
        public static final String ROUTING_OPTIONS = "routingOptions";
        public static final String USE_SCAN_REORDER_OPTIMIZATION = "useScanReorderOpt";
        public static final String MAX_EXECUTION_THREADS = "maxExecutionThreads";

        // For group-by queries with order-by clause, the tail groups are trimmed off to reduce the memory footprint. To
        // ensure the accuracy of the result, {@code max(limit * 5, minTrimSize)} groups are retained. When
        // {@code minTrimSize} is non-positive, trim is disabled.
        //
        // Caution:
        // Setting trim size to non-positive value (disable trim) or large value gives more accurate result, but can
        // potentially cause higher memory pressure.
        //
        // Trim can be applied in the following stages:
        // - Segment level: after getting the segment level results, before merging them into server level results.
        // - Server level: while merging the segment level results into server level results.
        // - Broker level: while merging the server level results into broker level results. (SSE only)
        // - MSE intermediate stage (MSE only)

        public static final String MIN_SEGMENT_GROUP_TRIM_SIZE = "minSegmentGroupTrimSize";
        public static final String MIN_SERVER_GROUP_TRIM_SIZE = "minServerGroupTrimSize";
        public static final String MIN_BROKER_GROUP_TRIM_SIZE = "minBrokerGroupTrimSize";
        public static final String MSE_MIN_GROUP_TRIM_SIZE = "mseMinGroupTrimSize";

        /**
         * This will help in getting accurate and correct result for queries
         * with group by and limit but  without order by
         */
        public static final String ACCURATE_GROUP_BY_WITHOUT_ORDER_BY = "accurateGroupByWithoutOrderBy";

        /** Number of threads used in the final reduce.
         * This is useful for expensive aggregation functions. E.g. Funnel queries are considered as expensive
         * aggregation functions. */
        public static final String NUM_THREADS_EXTRACT_FINAL_RESULT = "numThreadsExtractFinalResult";

        /** Number of threads used in the final reduce at broker level. */
        public static final String CHUNK_SIZE_EXTRACT_FINAL_RESULT = "chunkSizeExtractFinalResult";

        public static final String NUM_REPLICA_GROUPS_TO_QUERY = "numReplicaGroupsToQuery";

        @Deprecated
        public static final String ORDERED_PREFERRED_REPLICAS = "orderedPreferredReplicas";
        public static final String ORDERED_PREFERRED_POOLS = "orderedPreferredPools";
        public static final String USE_FIXED_REPLICA = "useFixedReplica";
        public static final String EXPLAIN_PLAN_VERBOSE = "explainPlanVerbose";
        public static final String USE_MULTISTAGE_ENGINE = "useMultistageEngine";
        public static final String INFER_PARTITION_HINT = "inferPartitionHint";
        public static final String ENABLE_NULL_HANDLING = "enableNullHandling";
        public static final String APPLICATION_NAME = "applicationName";
        public static final String USE_SPOOLS = "useSpools";
        public static final String USE_PHYSICAL_OPTIMIZER = "usePhysicalOptimizer";
        /**
         * If set, changes the explain behavior in multi-stage engine.
         *
         * {@code true} means to ask servers for the physical plan while false means to just use logical plan.
         *
         * Use false in order to mimic behavior of Pinot 1.2.0 and previous.
         */
        public static final String EXPLAIN_ASKING_SERVERS = "explainAskingServers";

        // Can be applied to aggregation and group-by queries to ask servers to directly return final results instead of
        // intermediate results for aggregations.
        public static final String SERVER_RETURN_FINAL_RESULT = "serverReturnFinalResult";
        // Can be applied to group-by queries to ask servers to directly return final results instead of intermediate
        // results for aggregations. Different from SERVER_RETURN_FINAL_RESULT, this option should be used when the
        // group key is not server partitioned, but the aggregated values are server partitioned. When this option is
        // used, server will return final results, but won't directly trim the result to the query limit.
        public static final String SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED =
            "serverReturnFinalResultKeyUnpartitioned";

        // Reorder scan based predicates based on cardinality and number of selected values
        public static final String AND_SCAN_REORDERING = "AndScanReordering";
        public static final String SKIP_INDEXES = "skipIndexes";

        // Query option key used to skip a given set of rules
        public static final String SKIP_PLANNER_RULES = "skipPlannerRules";

        // Query option key used to enable a given set of defaultly disabled rules
        public static final String USE_PLANNER_RULES = "usePlannerRules";

        public static final String ORDER_BY_ALGORITHM = "orderByAlgorithm";

        public static final String MULTI_STAGE_LEAF_LIMIT = "multiStageLeafLimit";

        // TODO: Apply this to SSE as well
        /** Throw an exception on reaching num_groups_limit instead of just setting a flag. */
        public static final String ERROR_ON_NUM_GROUPS_LIMIT = "errorOnNumGroupsLimit";

        public static final String NUM_GROUPS_LIMIT = "numGroupsLimit";
        // Not actually accepted as Query Option but faked as one during MSE
        public static final String NUM_GROUPS_WARNING_LIMIT = "numGroupsWarningLimit";
        public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY = "maxInitialResultHolderCapacity";
        public static final String MIN_INITIAL_INDEXED_TABLE_CAPACITY = "minInitialIndexedTableCapacity";
        public static final String MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY = "mseMaxInitialResultHolderCapacity";
        public static final String GROUP_TRIM_THRESHOLD = "groupTrimThreshold";
        public static final String STAGE_PARALLELISM = "stageParallelism";

        public static final String IN_PREDICATE_PRE_SORTED = "inPredicatePreSorted";
        public static final String IN_PREDICATE_LOOKUP_ALGORITHM = "inPredicateLookupAlgorithm";

        public static final String DROP_RESULTS = "dropResults";

        // Maximum number of pending results blocks allowed in the streaming operator
        public static final String MAX_STREAMING_PENDING_BLOCKS = "maxStreamingPendingBlocks";

        // Handle JOIN Overflow
        public static final String MAX_ROWS_IN_JOIN = "maxRowsInJoin";
        public static final String JOIN_OVERFLOW_MODE = "joinOverflowMode";

        // Handle WINDOW Overflow
        public static final String MAX_ROWS_IN_WINDOW = "maxRowsInWindow";
        public static final String WINDOW_OVERFLOW_MODE = "windowOverflowMode";

        // Indicates the maximum length of the serialized response per server for a query.
        public static final String MAX_SERVER_RESPONSE_SIZE_BYTES = "maxServerResponseSizeBytes";

        // Indicates the maximum length of serialized response across all servers for a query. This value is equally
        // divided across all servers processing the query.
        public static final String MAX_QUERY_RESPONSE_SIZE_BYTES = "maxQueryResponseSizeBytes";

        // If query submission causes an exception, still continue to submit the query to other servers
        public static final String SKIP_UNAVAILABLE_SERVERS = "skipUnavailableServers";

        // Indicates that a query belongs to a secondary workload when using the BinaryWorkloadScheduler. The
        // BinaryWorkloadScheduler divides queries into two workloads, primary and secondary. Primary workloads are
        // executed in an  Unbounded FCFS fashion. However, secondary workloads are executed in a constrainted FCFS
        // fashion with limited compute.des queries into two workloads, primary and secondary. Primary workloads are
        // executed in an  Unbounded FCFS fashion. However, secondary workloads are executed in a constrainted FCFS
        // fashion with limited compute.
        public static final String IS_SECONDARY_WORKLOAD = "isSecondaryWorkload";

        // For group by queries with only filtered aggregations (and no non-filtered aggregations), the default behavior
        // is to compute all groups over the rows matching the main query filter. This ensures SQL compliant results,
        // since empty groups are also expected to be returned in such queries. However, this could be quite inefficient
        // if the main query does not have a filter (since a scan would be required to compute all groups). In case
        // users are okay with skipping empty groups - i.e., only the groups matching at least one aggregation filter
        // will be returned - this query option can be set. This is useful for performance, since indexes can be used
        // for the aggregation filters and a full scan can be avoided.
        public static final String FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS = "filteredAggregationsSkipEmptyGroups";

        // When set to true, the max initial result holder capacity will be optimized based on the query. Rather than
        // using the default value. This is best-effort for now and returns the default value if the optimization is not
        // possible.
        public static final String OPTIMIZE_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
            "optimizeMaxInitialResultHolderCapacity";

        // Set to true if a cursor should be returned instead of the complete result set
        public static final String GET_CURSOR = "getCursor";
        // Number of rows that the cursor should contain
        public static final String CURSOR_NUM_ROWS = "cursorNumRows";

        // Custom Query ID provided by the client
        public static final String CLIENT_QUERY_ID = "clientQueryId";

        // Use MSE compiler when trying to fill a response with no schema metadata
        // (overrides the "pinot.broker.use.mse.to.fill.empty.response.schema" broker conf)
        public static final String USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA = "useMSEToFillEmptyResponseSchema";

        // Used by the MSE Engine when auto-inferring data partitioning. Realtime streams can often incorrectly assign
        // records to stream partitions, which can make a segment have multiple partitions. The scale of this is
        // usually low, and this query option allows the MSE Optimizer to infer the partition of a segment based on its
        // name, when that segment has multiple partitions in its columnPartitionMap.
        @Deprecated
        public static final String INFER_INVALID_SEGMENT_PARTITION = "inferInvalidSegmentPartition";
        // For realtime tables, this infers the segment partition for all segments. The partition column, function,
        // and number of partitions still rely on the Table's segmentPartitionConfig. This is useful if you have
        // scenarios where the stream doesn't guarantee 100% accuracy for stream partition assignment. In such
        // scenarios, if you don't have upsert compaction enabled, inferInvalidSegmentPartition will suffice. But when
        // you have compaction enabled, it's possible that after compaction you are only left with invalid partition
        // records, which can change the partition of a segment from something like [1, 3, 5] to [5], for a segment
        // that was supposed to be in partition-1.
        public static final String INFER_REALTIME_SEGMENT_PARTITION = "inferRealtimeSegmentPartition";
        public static final String USE_LITE_MODE = "useLiteMode";
        // Server stage limit for lite mode queries.
        public static final String LITE_MODE_SERVER_STAGE_LIMIT = "liteModeServerStageLimit";
        // Used by the MSE Engine to determine whether to use the broker pruning logic. Only supported by the
        // new MSE query optimizer.
        // TODO(mse-physical): Consider removing this query option and making this the default, since there's already
        //   a table config to enable broker pruning (it is disabled by default).
        public static final String USE_BROKER_PRUNING = "useBrokerPruning";
        // When lite mode is enabled, if this flag is set, we will run all the non-leaf stage operators within the
        // broker itself. That way, the MSE queries will model the scatter gather pattern used by the V1 Engine.
        public static final String RUN_IN_BROKER = "runInBroker";

        /// For MSE queries, when this option is set to true, only use servers for leaf stages as the workers for the
        /// intermediate stages. This is useful to control the fanout of the query and reduce data shuffling.
        public static final String USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE = "useLeafServerForIntermediateStage";

        // Option denoting the workloadName to which the query belongs. This is used to enforce resource budgets for
        // each workload if "Query Workload Isolation" feature enabled.
        public static final String WORKLOAD_NAME = "workloadName";
      }

      public static class QueryOptionValue {
        public static final int DEFAULT_MAX_STREAMING_PENDING_BLOCKS = 100;
      }
    }

    /**
     * Calcite and Pinot rule names / descriptions
     * used for enable and disabling of rules, this will be iterated through in PlannerContext
     * to check if rule is disabled.
     */
    public static class PlannerRuleNames {
      public static final String FILTER_INTO_JOIN = "FilterIntoJoin";
      public static final String FILTER_AGGREGATE_TRANSPOSE = "FilterAggregateTranspose";
      public static final String FILTER_SET_OP_TRANSPOSE = "FilterSetOpTranspose";
      public static final String PROJECT_JOIN_TRANSPOSE = "ProjectJoinTranspose";
      public static final String PROJECT_SET_OP_TRANSPOSE = "ProjectSetOpTranspose";
      public static final String FILTER_PROJECT_TRANSPOSE = "FilterProjectTranspose";
      public static final String JOIN_CONDITION_PUSH = "JoinConditionPush";
      public static final String JOIN_PUSH_TRANSITIVE_PREDICATES = "JoinPushTransitivePredicates";
      public static final String PROJECT_REMOVE = "ProjectRemove";
      public static final String PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW = "ProjectToLogicalProjectAndWindow";
      public static final String PROJECT_WINDOW_TRANSPOSE = "ProjectWindowTranspose";
      public static final String EVALUATE_LITERAL_PROJECT = "EvaluateProjectLiteral";
      public static final String EVALUATE_LITERAL_FILTER = "EvaluateFilterLiteral";
      public static final String JOIN_PUSH_EXPRESSIONS = "JoinPushExpressions";
      public static final String PROJECT_TO_SEMI_JOIN = "ProjectToSemiJoin";
      public static final String SEMIN_JOIN_DISTINCT_PROJECT = "SeminJoinDistinctProject";
      public static final String UNION_TO_DISTINCT = "UnionToDistinct";
      public static final String AGGREGATE_REMOVE = "AggregateRemove";
      public static final String AGGREGATE_JOIN_TRANSPOSE = "AggregateJoinTranspose";
      public static final String AGGREGATE_UNION_AGGREGATE = "AggregateUnionAggregate";
      public static final String AGGREGATE_REDUCE_FUNCTIONS = "AggregateReduceFunctions";
      public static final String AGGREGATE_CASE_TO_FILTER = "AggregateCaseToFilter";
      public static final String PROJECT_FILTER_TRANSPOSE = "ProjectFilterTranspose";
      public static final String PROJECT_MERGE = "ProjectMerge";
      public static final String AGGREGATE_PROJECT_MERGE = "AggregateProjectMerge";
      public static final String FILTER_MERGE = "FilterMerge";
      public static final String SORT_REMOVE = "SortRemove";
      public static final String SORT_JOIN_TRANSPOSE = "SortJoinTranspose";
      public static final String SORT_JOIN_COPY = "SortJoinCopy";
      public static final String AGGREGATE_JOIN_TRANSPOSE_EXTENDED = "AggregateJoinTransposeExtended";
      public static final String PRUNE_EMPTY_AGGREGATE = "PruneEmptyAggregate";
      public static final String PRUNE_EMPTY_FILTER = "PruneEmptyFilter";
      public static final String PRUNE_EMPTY_PROJECT = "PruneEmptyProject";
      public static final String PRUNE_EMPTY_SORT = "PruneEmptySort";
      public static final String PRUNE_EMPTY_UNION = "PruneEmptyUnion";
      public static final String PRUNE_EMPTY_CORRELATE_LEFT = "PruneEmptyCorrelateLeft";
      public static final String PRUNE_EMPTY_CORRELATE_RIGHT = "PruneEmptyCorrelateRight";
      public static final String PRUNE_EMPTY_JOIN_LEFT = "PruneEmptyJoinLeft";
      public static final String PRUNE_EMPTY_JOIN_RIGHT = "PruneEmptyJoinRight";
    }

    /**
     * Set of planner rules that will be disabled by default
     * and could be enabled by setting
     * {@link CommonConstants.Broker.Request.QueryOptionKey#USE_PLANNER_RULES}.
     *
     * If a rule is enabled and disabled at the same time,
     * it will be disabled
     */
    public static final Set<String> DEFAULT_DISABLED_RULES = Set.of(
        PlannerRuleNames.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,
        PlannerRuleNames.SORT_JOIN_TRANSPOSE,
        PlannerRuleNames.SORT_JOIN_COPY
    );

    public static class FailureDetector {
      public enum Type {
        // Do not detect any failure
        NO_OP,

        // Detect connection failures
        CONNECTION,

        // Use the custom failure detector of the configured class name
        CUSTOM
      }

      public static final String CONFIG_OF_TYPE = "pinot.broker.failure.detector.type";
      public static final String DEFAULT_TYPE = Type.NO_OP.name();
      public static final String CONFIG_OF_CLASS_NAME = "pinot.broker.failure.detector.class";

      // Exponential backoff delay of retrying an unhealthy server when a failure is detected
      public static final String CONFIG_OF_RETRY_INITIAL_DELAY_MS =
          "pinot.broker.failure.detector.retry.initial.delay.ms";
      public static final long DEFAULT_RETRY_INITIAL_DELAY_MS = 5_000L;
      public static final String CONFIG_OF_RETRY_DELAY_FACTOR = "pinot.broker.failure.detector.retry.delay.factor";
      public static final double DEFAULT_RETRY_DELAY_FACTOR = 2.0;
      public static final String CONFIG_OF_MAX_RETRIES = "pinot.broker.failure.detector.max.retries";
      public static final int DEFAULT_MAX_RETRIES = 10;
    }

    // Configs related to AdaptiveServerSelection.
    public static class AdaptiveServerSelector {
      /**
       * Adaptive Server Selection feature has 2 parts:
       * 1. Stats Collection
       * 2. Routing Strategy
       *
       * Stats Collection is controlled by the config CONFIG_OF_ENABLE_STATS_COLLECTION.
       * Routing Strategy is controlled by the config CONFIG_OF_TYPE.
       *
       *
       *
       * Stats Collection: Enabling/Disabling stats collection will dictate whether stats (like latency, # of inflight
       *                   requests) will be collected when queries are routed to/received from servers. It does not
       *                   have any impact on the Server Selection Strategy used.
       *
       * Routing Strategy: Decides what strategy should be used to pick a server. Note that this
       *                   routing strategy complements the existing Balanced/ReplicaGroup/StrictReplicaGroup
       *                   strategies and is not a replacement.The available strategies are as follows:
       *                   1. NO_OP: Uses the default behavior offered by Balanced/ReplicaGroup/StrictReplicaGroup
       *                   instance selectors. Does NOT require Stats Collection to be enabled.
       *                   2. NUM_INFLIGHT_REQ: Picks the best server based on the number of inflight requests for
       *                   each server. Requires Stats Collection to be enabled.
       *                   3. LATENCY: Picks the best server based on the Exponential Weighted Moving Averge of Latency
       *                   for each server. Requires Stats Collection to be enabled.
       *                   4. HYBRID: Picks the best server by computing a custom hybrid score based on both latency
       *                   and # inflight requests. This is based on the approach described in the paper
       *                   https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf. Requires Stats
       *                   Collection to be enabled.
       */

      public enum Type {
        NO_OP,

        NUM_INFLIGHT_REQ,

        LATENCY,

        HYBRID
      }

      private static final String CONFIG_PREFIX = "pinot.broker.adaptive.server.selector";

      // Determines the type of AdaptiveServerSelector to use.
      public static final String CONFIG_OF_TYPE = CONFIG_PREFIX + ".type";
      public static final String DEFAULT_TYPE = Type.NO_OP.name();

      // Determines whether stats collection is enabled. This can be enabled independent of CONFIG_OF_TYPE. This is
      // so that users have an option to just enable stats collection and analyze them before deciding and enabling
      // adaptive server selection.
      public static final String CONFIG_OF_ENABLE_STATS_COLLECTION = CONFIG_PREFIX + ".enable.stats.collection";
      public static final boolean DEFAULT_ENABLE_STATS_COLLECTION = false;

      // Parameters to tune exponential moving average.

      // The weightage to be given for a new incoming value. For example, alpha=0.30 will give 30% weightage to the
      // new value and 70% weightage to the existing value in the Exponential Weighted Moving Average calculation.
      public static final String CONFIG_OF_EWMA_ALPHA = CONFIG_PREFIX + ".ewma.alpha";
      public static final double DEFAULT_EWMA_ALPHA = 0.666;

      // If the EMA average has not been updated during a specified time window (defined by this property), the
      // EMA average value is automatically decayed by an incoming value of zero. This is required to bring a server
      // back to healthy state gracefully after it has experienced some form of slowness.
      public static final String CONFIG_OF_AUTODECAY_WINDOW_MS = CONFIG_PREFIX + ".autodecay.window.ms";
      public static final long DEFAULT_AUTODECAY_WINDOW_MS = 10 * 1000;

      // Determines the initial duration during which incoming values are skipped in the Exponential Moving Average
      // calculation. Until this duration has elapsed, average returned will be equal to AVG_INITIALIZATION_VAL.
      public static final String CONFIG_OF_WARMUP_DURATION_MS = CONFIG_PREFIX + ".warmup.duration";
      public static final long DEFAULT_WARMUP_DURATION_MS = 0;

      // Determines the initialization value for Exponential Moving Average.
      public static final String CONFIG_OF_AVG_INITIALIZATION_VAL = CONFIG_PREFIX + ".avg.initialization.val";
      public static final double DEFAULT_AVG_INITIALIZATION_VAL = 1.0;

      // Parameters related to Hybrid score.
      public static final String CONFIG_OF_HYBRID_SCORE_EXPONENT = CONFIG_PREFIX + ".hybrid.score.exponent";
      public static final int DEFAULT_HYBRID_SCORE_EXPONENT = 3;

      // Threadpool size of ServerRoutingStatsManager. This controls the number of threads available to update routing
      // stats for servers upon query submission and response arrival.
      public static final String CONFIG_OF_STATS_MANAGER_THREADPOOL_SIZE =
          CONFIG_PREFIX + ".stats.manager.threadpool.size";
      public static final int DEFAULT_STATS_MANAGER_THREADPOOL_SIZE = 2;
    }

    public static class Grpc {
      public static final String KEY_OF_GRPC_PORT = "pinot.broker.grpc.port";
      public static final String KEY_OF_GRPC_TLS_ENABLED = "pinot.broker.grpc.tls.enabled";
      public static final String KEY_OF_GRPC_TLS_PORT = "pinot.broker.grpc.tls.port";
      public static final String KEY_OF_GRPC_TLS_PREFIX = "pinot.broker.grpctls";

      public static final String BLOCK_ROW_SIZE = "blockRowSize";
      public static final int DEFAULT_BLOCK_ROW_SIZE = 10_000;
      public static final String COMPRESSION = "compression";
      public static final String DEFAULT_COMPRESSION = "ZSTD";
      public static final String ENCODING = "encoding";
      public static final String DEFAULT_ENCODING = "JSON";
    }

    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.broker.storage.factory";

    public static final String USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA =
        "pinot.broker.use.mse.to.fill.empty.response.schema";
    public static final boolean DEFAULT_USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA = false;
  }

  public static class Server {
    public static final String INSTANCE_DATA_MANAGER_CONFIG_PREFIX = "pinot.server.instance";
    public static final String QUERY_EXECUTOR_CONFIG_PREFIX = "pinot.server.query.executor";
    public static final String METRICS_CONFIG_PREFIX = "pinot.server.metrics";

    public static final String CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";
    public static final String DEFAULT_INSTANCE_DATA_MANAGER_CLASS =
        "org.apache.pinot.server.starter.helix.HelixInstanceDataManager";
    // Following configs are used in HelixInstanceDataManagerConfig, where the config prefix is trimmed. We keep the
    // full config for reference purpose.
    public static final String INSTANCE_ID = "id";
    public static final String CONFIG_OF_INSTANCE_ID = INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + INSTANCE_ID;
    public static final String INSTANCE_DATA_DIR = "dataDir";
    public static final String CONFIG_OF_INSTANCE_DATA_DIR =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + INSTANCE_DATA_DIR;
    public static final String DEFAULT_INSTANCE_BASE_DIR =
        FileUtils.getTempDirectoryPath() + File.separator + "PinotServer";
    public static final String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "index";
    public static final String CONSUMER_DIR = "consumerDir";
    public static final String CONFIG_OF_CONSUMER_DIR = INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + CONSUMER_DIR;
    public static final String INSTANCE_SEGMENT_TAR_DIR = "segmentTarDir";
    public static final String CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + INSTANCE_SEGMENT_TAR_DIR;
    public static final String DEFAULT_INSTANCE_SEGMENT_TAR_DIR =
        DEFAULT_INSTANCE_BASE_DIR + File.separator + "segmentTar";
    public static final String CONSUMER_CLIENT_ID_SUFFIX = "consumer.client.id.suffix";
    public static final String CONFIG_OF_CONSUMER_CLIENT_ID_SUFFIX =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + CONSUMER_CLIENT_ID_SUFFIX;
    public static final String SEGMENT_STORE_URI = "segment.store.uri";
    public static final String CONFIG_OF_SEGMENT_STORE_URI =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + SEGMENT_STORE_URI;
    public static final String TABLE_DATA_MANAGER_PROVIDER_CLASS = "table.data.manager.provider.class";
    public static final String CONFIG_OF_TABLE_DATA_MANAGER_PROVIDER_CLASS =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + TABLE_DATA_MANAGER_PROVIDER_CLASS;
    public static final String DEFAULT_TABLE_DATA_MANAGER_PROVIDER_CLASS =
        "org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider";
    public static final String READ_MODE = "readMode";
    public static final String CONFIG_OF_READ_MODE = INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + READ_MODE;
    public static final String DEFAULT_READ_MODE = "mmap";
    public static final String SEGMENT_FORMAT_VERSION = "segment.format.version";
    public static final String CONFIG_OF_SEGMENT_FORMAT_VERSION =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + SEGMENT_FORMAT_VERSION;
    public static final String REALTIME_OFFHEAP_ALLOCATION = "realtime.alloc.offheap";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + REALTIME_OFFHEAP_ALLOCATION;
    public static final boolean DEFAULT_REALTIME_OFFHEAP_ALLOCATION = true;
    public static final String REALTIME_OFFHEAP_DIRECT_ALLOCATION = "realtime.alloc.offheap.direct";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + REALTIME_OFFHEAP_DIRECT_ALLOCATION;
    public static final boolean DEFAULT_REALTIME_OFFHEAP_DIRECT_ALLOCATION = false;
    public static final String RELOAD_CONSUMING_SEGMENT = "reload.consumingSegment";
    public static final String CONFIG_OF_RELOAD_CONSUMING_SEGMENT =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + RELOAD_CONSUMING_SEGMENT;
    public static final boolean DEFAULT_RELOAD_CONSUMING_SEGMENT = true;

    // Query logger related configs
    public static final String CONFIG_OF_QUERY_LOG_MAX_RATE = "pinot.server.query.log.maxRatePerSecond";
    @Deprecated
    public static final String DEPRECATED_CONFIG_OF_QUERY_LOG_MAX_RATE =
        "pinot.query.scheduler.query.log.maxRatePerSecond";
    public static final double DEFAULT_QUERY_LOG_MAX_RATE = 10_000;
    public static final String CONFIG_OF_QUERY_LOG_DROPPED_REPORT_MAX_RATE =
        "pinot.server.query.log.droppedReportMaxRatePerSecond";
    public static final double DEFAULT_QUERY_LOG_DROPPED_REPORT_MAX_RATE = 1;

    /* Start of query executor related configs */

    public static final String CLASS = "class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_CLASS = QUERY_EXECUTOR_CONFIG_PREFIX + "." + CLASS;
    public static final String DEFAULT_QUERY_EXECUTOR_CLASS =
        "org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl";
    public static final String PRUNER = "pruner";
    public static final String QUERY_EXECUTOR_PRUNER_CONFIG_PREFIX = QUERY_EXECUTOR_CONFIG_PREFIX + "." + PRUNER;
    public static final String CONFIG_OF_QUERY_EXECUTOR_PRUNER_CLASS =
        QUERY_EXECUTOR_PRUNER_CONFIG_PREFIX + "." + CLASS;
    // The order of the pruners matters. Pruning with segment metadata ahead of those using segment data like bloom
    // filters to reduce the required data access.
    public static final List<String> DEFAULT_QUERY_EXECUTOR_PRUNER_CLASS =
        List.of("ColumnValueSegmentPruner", "BloomFilterSegmentPruner", "SelectionQuerySegmentPruner");
    public static final String PLAN_MAKER_CLASS = "plan.maker.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_PLAN_MAKER_CLASS =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + PLAN_MAKER_CLASS;
    public static final String DEFAULT_QUERY_EXECUTOR_PLAN_MAKER_CLASS =
        "org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2";
    public static final String TIMEOUT = "timeout";
    public static final String CONFIG_OF_QUERY_EXECUTOR_TIMEOUT = QUERY_EXECUTOR_CONFIG_PREFIX + "." + TIMEOUT;
    public static final long DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS = 15_000L;
    public static final String MAX_EXECUTION_THREADS = "max.execution.threads";
    public static final String CONFIG_OF_QUERY_EXECUTOR_MAX_EXECUTION_THREADS =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MAX_EXECUTION_THREADS;
    public static final int DEFAULT_QUERY_EXECUTOR_MAX_EXECUTION_THREADS = -1;  // Use number of CPU cores

    // Group-by query related configs
    public static final String NUM_GROUPS_LIMIT = "num.groups.limit";
    public static final String CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + NUM_GROUPS_LIMIT;
    public static final int DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT = 100_000;
    public static final String NUM_GROUPS_WARN_LIMIT = "num.groups.warn.limit";
    public static final String CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + NUM_GROUPS_WARN_LIMIT;
    public static final int DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT = 150_000;
    public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY = "max.init.group.holder.capacity";
    public static final String CONFIG_OF_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MAX_INITIAL_RESULT_HOLDER_CAPACITY;
    public static final int DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10_000;
    public static final String MIN_INITIAL_INDEXED_TABLE_CAPACITY = "min.init.indexed.table.capacity";
    public static final String CONFIG_OF_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MIN_INITIAL_INDEXED_TABLE_CAPACITY;
    public static final int DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY = 128;
    public static final String MSE = "mse";
    public static final String MSE_CONFIG_PREFIX = QUERY_EXECUTOR_CONFIG_PREFIX + "." + MSE;
    public static final String CONFIG_OF_MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
        MSE_CONFIG_PREFIX + "." + MAX_INITIAL_RESULT_HOLDER_CAPACITY;
    public static final String CONFIG_OF_MSE_MAX_EXECUTION_THREADS =
        MSE_CONFIG_PREFIX + "." + MAX_EXECUTION_THREADS;
    public static final int DEFAULT_MSE_MAX_EXECUTION_THREADS = -1;
    public static final String CONFIG_OF_MSE_MAX_EXECUTION_THREADS_EXCEED_STRATEGY =
        MSE_CONFIG_PREFIX + "." + MAX_EXECUTION_THREADS + ".exceed.strategy";
    public static final String DEFAULT_MSE_MAX_EXECUTION_THREADS_EXCEED_STRATEGY = "ERROR";

    // For group-by queries with order-by clause, the tail groups are trimmed off to reduce the memory footprint. To
    // ensure the accuracy of the result, {@code max(limit * 5, minTrimSize)} groups are retained. When
    // {@code minTrimSize} is non-positive, trim is disabled.
    //
    // Caution:
    // Setting trim size to non-positive value (disable trim) or large value gives more accurate result, but can
    // potentially cause higher memory pressure.
    //
    // Trim can be applied in the following stages:
    // - Segment level: after getting the segment level results, before merging them into server level results.
    // - Server level: while merging the segment level results into server level results.
    // - Broker level: while merging the server level results into broker level results. (SSE only)
    // - MSE intermediate stage (MSE only)
    public static final String MIN_SEGMENT_GROUP_TRIM_SIZE = "min.segment.group.trim.size";
    public static final String CONFIG_OF_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MIN_SEGMENT_GROUP_TRIM_SIZE;
    public static final int DEFAULT_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE = -1;
    public static final String MIN_SERVER_GROUP_TRIM_SIZE = "min.server.group.trim.size";
    public static final String CONFIG_OF_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MIN_SERVER_GROUP_TRIM_SIZE;
    // Match the value of GroupByUtils.DEFAULT_MIN_NUM_GROUPS
    public static final int DEFAULT_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE = 5000;
    public static final String GROUPBY_TRIM_THRESHOLD = "groupby.trim.threshold";
    public static final String CONFIG_OF_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + GROUPBY_TRIM_THRESHOLD;
    public static final int DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD = 1_000_000;
    public static final String CONFIG_OF_MSE_MIN_GROUP_TRIM_SIZE = MSE_CONFIG_PREFIX + ".min.group.trim.size";
    // Match the value of GroupByUtils.DEFAULT_MIN_NUM_GROUPS
    public static final int DEFAULT_MSE_MIN_GROUP_TRIM_SIZE = 5000;

    // TODO: Merge this with "mse"
    /**
     * The ExecutorServiceProvider to use for execution threads, which are the ones that execute
     * MultiStageOperators (and SSE operators in the leaf stages).
     *
     * It is recommended to use cached. In case fixed is used, it should use a large enough number of threads or
     * parent operators may consume all threads.
     * In Java 21 or newer, virtual threads are a good solution. Although Apache Pinot doesn't include this option yet,
     * it is trivial to implement that plugin.
     *
     * See QueryRunner
     */
    public static final String MULTISTAGE_EXECUTOR = "multistage.executor";
    public static final String MULTISTAGE_EXECUTOR_CONFIG_PREFIX =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MULTISTAGE_EXECUTOR;
    public static final String DEFAULT_MULTISTAGE_EXECUTOR_TYPE = "cached";
    /**
     * The ExecutorServiceProvider to be used for submission threads, which are the ones
     * that receive requests in protobuf and transform them into MultiStageOperators.
     *
     * It is recommended to use a fixed thread pool, given submission code should not block.
     *
     * See QueryServer
     */
    public static final String MULTISTAGE_SUBMISSION_EXEC_CONFIG_PREFIX =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + "multistage.submission";
    public static final String DEFAULT_MULTISTAGE_SUBMISSION_EXEC_TYPE = "fixed";
    @Deprecated
    public static final String CONFIG_OF_QUERY_EXECUTOR_OPCHAIN_EXECUTOR = MULTISTAGE_EXECUTOR_CONFIG_PREFIX;
    @Deprecated
    public static final String DEFAULT_QUERY_EXECUTOR_OPCHAIN_EXECUTOR = DEFAULT_MULTISTAGE_EXECUTOR_TYPE;

    // Enable SSE & MSE task throttling on critical heap usage.
    public static final String CONFIG_OF_ENABLE_QUERY_SCHEDULER_THROTTLING_ON_HEAP_USAGE =
        QUERY_EXECUTOR_CONFIG_PREFIX + ".enableThrottlingOnHeapUsage";
    public static final boolean DEFAULT_ENABLE_QUERY_SCHEDULER_THROTTLING_ON_HEAP_USAGE = false;

    /**
     * The ExecutorServiceProvider to be used for timeseries threads.
     *
     * It is recommended to use a cached thread pool, given timeseries endpoints are blocking.
     *
     * See QueryServer
     */
    public static final String MULTISTAGE_TIMESERIES_EXEC_CONFIG_PREFIX =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + "timeseries";
    public static final String DEFAULT_TIMESERIES_EXEC_CONFIG_PREFIX = "cached";
    /* End of query executor related configs */

    public static final String CONFIG_OF_TRANSFORM_FUNCTIONS = "pinot.server.transforms";
    public static final String CONFIG_OF_SERVER_QUERY_REWRITER_CLASS_NAMES = "pinot.server.query.rewriter.class.names";
    public static final String CONFIG_OF_SERVER_QUERY_REGEX_CLASS = "pinot.server.query.regex.class";
    public static final String DEFAULT_SERVER_QUERY_REGEX_CLASS = "JAVA_UTIL";
    public static final String CONFIG_OF_ENABLE_QUERY_CANCELLATION = "pinot.server.enable.query.cancellation";
    public static final String CONFIG_OF_NETTY_SERVER_ENABLED = "pinot.server.netty.enabled";
    public static final boolean DEFAULT_NETTY_SERVER_ENABLED = true;
    public static final String CONFIG_OF_ENABLE_GRPC_SERVER = "pinot.server.grpc.enable";
    public static final boolean DEFAULT_ENABLE_GRPC_SERVER = true;
    public static final String CONFIG_OF_GRPC_PORT = "pinot.server.grpc.port";
    public static final int DEFAULT_GRPC_PORT = 8090;
    public static final String CONFIG_OF_GRPCTLS_SERVER_ENABLED = "pinot.server.grpctls.enabled";
    public static final boolean DEFAULT_GRPCTLS_SERVER_ENABLED = false;
    public static final String CONFIG_OF_NETTYTLS_SERVER_ENABLED = "pinot.server.nettytls.enabled";
    public static final boolean DEFAULT_NETTYTLS_SERVER_ENABLED = false;
    public static final String CONFIG_OF_SWAGGER_SERVER_ENABLED = "pinot.server.swagger.enabled";
    public static final boolean DEFAULT_SWAGGER_SERVER_ENABLED = true;
    public static final String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.server.swagger.use.https";
    public static final String CONFIG_OF_ADMIN_API_PORT = "pinot.server.adminapi.port";
    public static final int DEFAULT_ADMIN_API_PORT = 8097;
    public static final String CONFIG_OF_SERVER_RESOURCE_PACKAGES = "server.restlet.api.resource.packages";
    public static final String DEFAULT_SERVER_RESOURCE_PACKAGES = "org.apache.pinot.server.api.resources";

    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.server.storage.factory";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.server.crypter";
    public static final String CONFIG_OF_VALUE_PRUNER_IN_PREDICATE_THRESHOLD =
        "pinot.server.query.executor.pruner.columnvaluesegmentpruner.inpredicate.threshold";
    public static final int DEFAULT_VALUE_PRUNER_IN_PREDICATE_THRESHOLD = 10;

    /**
     * Service token for accessing protected controller APIs.
     * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
     */
    public static final String CONFIG_OF_AUTH = KEY_OF_AUTH;

    // Configuration to consider the server ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this this server has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    public static final String CONFIG_OF_SERVER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.server.startup.minResourcePercent";
    public static final double DEFAULT_SERVER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;
    public static final String CONFIG_OF_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS =
        "pinot.server.starter.realtimeConsumptionCatchupWaitMs";
    public static final int DEFAULT_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS = 0;
    public static final String CONFIG_OF_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER =
        "pinot.server.starter.enableRealtimeOffsetBasedConsumptionStatusChecker";
    public static final boolean DEFAULT_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER = false;

    public static final String CONFIG_OF_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER =
        "pinot.server.starter.enableRealtimeFreshnessBasedConsumptionStatusChecker";
    public static final boolean DEFAULT_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER = false;
    // This configuration is in place to avoid servers getting stuck checking for freshness in
    // cases where they will never be able to reach the freshness threshold or the latest offset.
    // The only current case where we have seen this is low volume streams using read_committed
    // because of transactional publishes where the last message in the stream is an
    // un-consumable kafka control message, and it is impossible to tell if the consumer is stuck
    // or some offsets will never be consumable.
    //
    // When in doubt, do not enable this configuration as it can cause a lagged server to start
    // serving queries.
    public static final String CONFIG_OF_REALTIME_FRESHNESS_IDLE_TIMEOUT_MS =
        "pinot.server.starter.realtimeFreshnessIdleTimeoutMs";
    public static final int DEFAULT_REALTIME_FRESHNESS_IDLE_TIMEOUT_MS = 0;
    public static final String CONFIG_OF_STARTUP_REALTIME_MIN_FRESHNESS_MS =
        "pinot.server.starter.realtimeMinFreshnessMs";
    // Use 10 seconds by default so high volume stream are able to catch up.
    // This is also the default in the case a user misconfigures this by setting to <= 0.
    public static final int DEFAULT_STARTUP_REALTIME_MIN_FRESHNESS_MS = 10000;

    // Config for realtime consumption message rate limit
    public static final String CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT = "pinot.server.consumption.rate.limit";
    // Default to 0.0 (no limit)
    public static final double DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT = 0.0;

    public static final String CONFIG_OF_MMAP_DEFAULT_ADVICE = "pinot.server.mmap.advice.default";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.server.segment.fetcher";

    // Configs for server starter startup/shutdown checks
    // Startup: timeout for the startup checks
    public static final String CONFIG_OF_STARTUP_TIMEOUT_MS = "pinot.server.startup.timeoutMs";
    public static final long DEFAULT_STARTUP_TIMEOUT_MS = 600_000L;
    // Startup: enable service status check before claiming server up
    public static final String CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK =
        "pinot.server.startup.enableServiceStatusCheck";
    public static final boolean DEFAULT_STARTUP_ENABLE_SERVICE_STATUS_CHECK = true;
    // The timeouts above determine how long servers will poll their status before giving up.
    // This configuration determines what we do when we give up. By default, we will mark the
    // server as healthy and start the query server. If this is set to true, we instead throw
    // an exception and exit the server. This is useful if you want to ensure that the server
    // is always fully ready before accepting queries. But note that this can cause the server
    // to never be healthy if there is some reason that it can never reach a GOOD status.
    public static final String CONFIG_OF_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE =
        "pinot.server.startup.exitOnServiceStatusCheckFailure";
    public static final boolean DEFAULT_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE = false;
    public static final String CONFIG_OF_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS =
        "pinot.server.startup.serviceStatusCheckIntervalMs";
    public static final long DEFAULT_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS = 10_000L;
    // Shutdown: timeout for the shutdown checks
    public static final String CONFIG_OF_SHUTDOWN_TIMEOUT_MS = "pinot.server.shutdown.timeoutMs";
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 600_000L;
    // Shutdown: enable query check before shutting down the server
    //           Will drain queries (no incoming queries and all existing queries finished)
    public static final String CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK = "pinot.server.shutdown.enableQueryCheck";
    public static final boolean DEFAULT_SHUTDOWN_ENABLE_QUERY_CHECK = true;
    // Shutdown: threshold to mark that there is no incoming queries, use max query time as the default threshold
    public static final String CONFIG_OF_SHUTDOWN_NO_QUERY_THRESHOLD_MS = "pinot.server.shutdown.noQueryThresholdMs";
    // Shutdown: enable resource check before shutting down the server
    //           Will wait until all the resources in the external view are neither ONLINE nor CONSUMING
    //           No need to enable this check if startup service status check is enabled
    public static final String CONFIG_OF_SHUTDOWN_ENABLE_RESOURCE_CHECK = "pinot.server.shutdown.enableResourceCheck";
    public static final boolean DEFAULT_SHUTDOWN_ENABLE_RESOURCE_CHECK = false;
    public static final String CONFIG_OF_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS =
        "pinot.server.shutdown.resourceCheckIntervalMs";
    public static final long DEFAULT_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS = 10_000L;

    public static final String DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE = "ALL";

    public static final String PINOT_SERVER_METRICS_PREFIX = "pinot.server.metrics.prefix";

    public static final String SERVER_TLS_PREFIX = "pinot.server.tls";
    public static final String SERVER_NETTYTLS_PREFIX = "pinot.server.nettytls";
    public static final String SERVER_GRPCTLS_PREFIX = "pinot.server.grpctls";
    public static final String SERVER_NETTY_PREFIX = "pinot.server.netty";

    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.server.logger.root.dir";

    public static final String LUCENE_MAX_REFRESH_THREADS = "pinot.server.lucene.max.refresh.threads";
    public static final int DEFAULT_LUCENE_MAX_REFRESH_THREADS = 1;
    public static final String LUCENE_MIN_REFRESH_INTERVAL_MS = "pinot.server.lucene.min.refresh.interval.ms";
    public static final int DEFAULT_LUCENE_MIN_REFRESH_INTERVAL_MS = 10;

    public static final String CONFIG_OF_MESSAGES_COUNT_REFRESH_INTERVAL_SECONDS =
        "pinot.server.messagesCount.refreshIntervalSeconds";
    public static final int DEFAULT_MESSAGES_COUNT_REFRESH_INTERVAL_SECONDS = 30;

    public static class SegmentCompletionProtocol {
      public static final String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.server.segment.uploader";

      /**
       * Deprecated. Enable legacy https configs for segment upload.
       * Use server-wide TLS configs instead.
       */
      @Deprecated
      public static final String CONFIG_OF_CONTROLLER_HTTPS_ENABLED = "enabled";

      /**
       * Deprecated. Set the legacy https port for segment upload.
       * Use server-wide TLS configs instead.
       */
      @Deprecated
      public static final String CONFIG_OF_CONTROLLER_HTTPS_PORT = "controller.port";

      public static final String CONFIG_OF_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = "upload.request.timeout.ms";

      /**
       * Specify connection scheme to use for controller upload connections. Defaults to "http"
       */
      public static final String CONFIG_OF_PROTOCOL = "protocol";

      /**
       * Service token for accessing protected controller APIs.
       * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
       */
      public static final String CONFIG_OF_SEGMENT_UPLOADER_AUTH = KEY_OF_AUTH;

      public static final int DEFAULT_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = 300_000;
      public static final int DEFAULT_OTHER_REQUESTS_TIMEOUT = 10_000;
    }

    public static final String DEFAULT_METRICS_PREFIX = "pinot.server.";
    public static final String CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS = "pinot.server.enableTableLevelMetrics";
    public static final boolean DEFAULT_ENABLE_TABLE_LEVEL_METRICS = true;
    public static final String CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS =
        "pinot.server.allowedTablesForEmittingMetrics";
    public static final String ACCESS_CONTROL_FACTORY_CLASS = "pinot.server.admin.access.control.factory.class";
    public static final String DEFAULT_ACCESS_CONTROL_FACTORY_CLASS =
        "org.apache.pinot.server.access.AllowAllAccessFactory";
    public static final String PREFIX_OF_CONFIG_OF_ACCESS_CONTROL = "pinot.server.admin.access.control";

    public static final String CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT =
        "pinot.server.instance.enableThreadCpuTimeMeasurement";
    public static final String CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT =
        "pinot.server.instance.enableThreadAllocatedBytesMeasurement";
    public static final boolean DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT = false;
    public static final boolean DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT = false;

    public static final String CONFIG_OF_CURRENT_DATA_TABLE_VERSION = "pinot.server.instance.currentDataTableVersion";

    // Environment Provider Configs
    public static final String PREFIX_OF_CONFIG_OF_ENVIRONMENT_PROVIDER_FACTORY =
        "pinot.server.environmentProvider.factory";
    public static final String ENVIRONMENT_PROVIDER_CLASS_NAME = "pinot.server.environmentProvider.className";

    /// All the keys should be prefixed with {@link #INSTANCE_DATA_MANAGER_CONFIG_PREFIX}
    public static class Upsert {
      public static final String CONFIG_PREFIX = "upsert";
      public static final String DEFAULT_METADATA_MANAGER_CLASS = "default.metadata.manager.class";
      public static final String DEFAULT_ENABLE_SNAPSHOT = "default.enable.snapshot";
      public static final String DEFAULT_ENABLE_PRELOAD = "default.enable.preload";

      /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
      @Deprecated
      public static final String DEFAULT_ALLOW_PARTIAL_UPSERT_CONSUMPTION_DURING_COMMIT =
          "default.allow.partial.upsert.consumption.during.commit";
    }

    /// All the keys should be prefixed with {@link #INSTANCE_DATA_MANAGER_CONFIG_PREFIX}
    public static class Dedup {
      public static final String CONFIG_PREFIX = "dedup";
      public static final String DEFAULT_METADATA_MANAGER_CLASS = "default.metadata.manager.class";
      public static final String DEFAULT_ENABLE_PRELOAD = "default.enable.preload";
      public static final String DEFAULT_IGNORE_NON_DEFAULT_TIERS = "default.ignore.non.default.tiers";

      /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
      @Deprecated
      public static final String DEFAULT_ALLOW_DEDUP_CONSUMPTION_DURING_COMMIT =
          "default.allow.dedup.consumption.during.commit";
    }
  }

  public static class Controller {
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.controller.segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.controller.storage.factory";
    public static final String HOST_HTTP_HEADER = "Pinot-Controller-Host";
    public static final String VERSION_HTTP_HEADER = "Pinot-Controller-Version";
    public static final String SEGMENT_NAME_HTTP_HEADER = "Pinot-Segment-Name";
    public static final String TABLE_NAME_HTTP_HEADER = "Pinot-Table-Name";
    public static final String PINOT_QUERY_ERROR_CODE_HEADER = "X-Pinot-Error-Code";
    public static final String INGESTION_DESCRIPTOR = "Pinot-Ingestion-Descriptor";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.controller.crypter";

    public static final String CONFIG_OF_CONTROLLER_METRICS_PREFIX = "controller.metrics.prefix";
    // FYI this is incorrect as it generate metrics named without a dot after pinot.controller part,
    // but we keep this default for backward compatibility in case someone relies on this format
    // see Server or Broker class for correct prefix format you should use
    public static final String DEFAULT_METRICS_PREFIX = "pinot.controller.";

    public static final String CONFIG_OF_INSTANCE_ID = "pinot.controller.instance.id";
    public static final String CONFIG_OF_CONTROLLER_QUERY_REWRITER_CLASS_NAMES =
        "pinot.controller.query.rewriter.class.names";

    // Task Manager configuration
    public static final String CONFIG_OF_TASK_MANAGER_CLASS = "pinot.controller.task.manager.class";
    public static final String DEFAULT_TASK_MANAGER_CLASS =
        "org.apache.pinot.controller.helix.core.minion.PinotTaskManager";

    //Set to true to load all services tagged and compiled with hk2-metadata-generator. Default to False
    public static final String CONTROLLER_SERVICE_AUTO_DISCOVERY = "pinot.controller.service.auto.discovery";
    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.controller.logger.root.dir";
    public static final String PREFIX_OF_PINOT_CONTROLLER_SEGMENT_COMPLETION = "pinot.controller.segment.completion";
  }

  public static class Minion {
    public static final String CONFIG_OF_METRICS_PREFIX = "pinot.minion.";
    public static final String CONFIG_OF_MINION_ID = "pinot.minion.instance.id";
    public static final String METADATA_EVENT_OBSERVER_PREFIX = "metadata.event.notifier";

    // Config keys
    public static final String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.minion.swagger.use.https";
    public static final String CONFIG_OF_METRICS_PREFIX_KEY = "pinot.minion.metrics.prefix";
    @Deprecated
    public static final String DEPRECATED_CONFIG_OF_METRICS_PREFIX_KEY = "metricsPrefix";
    public static final String METRICS_REGISTRY_REGISTRATION_LISTENERS_KEY = "metricsRegistryRegistrationListeners";
    public static final String METRICS_CONFIG_PREFIX = "pinot.minion.metrics";

    // Default settings
    public static final int DEFAULT_HELIX_PORT = 9514;
    public static final String DEFAULT_INSTANCE_BASE_DIR =
        System.getProperty("java.io.tmpdir") + File.separator + "PinotMinion";
    public static final String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "data";

    // Add pinot.minion prefix on those configs to be consistent with configs of controller and server.
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.minion.storage.factory";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.minion.segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.minion.segment.uploader";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.minion.crypter";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "storage.factory";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "segment.fetcher";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "segment.uploader";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "crypter";

    /**
     * Service token for accessing protected controller APIs.
     * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
     */
    public static final String CONFIG_TASK_AUTH_NAMESPACE = "task.auth";
    public static final String MINION_TLS_PREFIX = "pinot.minion.tls";
    public static final String CONFIG_OF_MINION_QUERY_REWRITER_CLASS_NAMES = "pinot.minion.query.rewriter.class.names";
    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.minion.logger.root.dir";
    public static final String CONFIG_OF_EVENT_OBSERVER_CLEANUP_DELAY_IN_SEC =
        "pinot.minion.event.observer.cleanupDelayInSec";
    public static final char TASK_LIST_SEPARATOR = ',';
    public static final String CONFIG_OF_ALLOW_DOWNLOAD_FROM_SERVER = "pinot.minion.task.allow.download.from.server";
    public static final String DEFAULT_ALLOW_DOWNLOAD_FROM_SERVER = "false";
  }

  public static class ControllerJob {
    /**
     * Controller job ZK props
     */
    public static final String JOB_TYPE = "jobType";
    public static final String TABLE_NAME_WITH_TYPE = "tableName";
    public static final String TENANT_NAME = "tenantName";
    public static final String JOB_ID = "jobId";
    public static final String SUBMISSION_TIME_MS = "submissionTimeMs";
    public static final String MESSAGE_COUNT = "messageCount";

    public static final Integer DEFAULT_MAXIMUM_CONTROLLER_JOBS_IN_ZK = 100;

    /**
     * Segment reload job ZK props
     */
    public static final String SEGMENT_RELOAD_JOB_SEGMENT_NAME = "segmentName";
    public static final String SEGMENT_RELOAD_JOB_INSTANCE_NAME = "instanceName";
    // Force commit job ZK props
    public static final String CONSUMING_SEGMENTS_FORCE_COMMITTED_LIST = "segmentsForceCommitted";
    public static final String CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED_LIST = "segmentsYetToBeCommitted";
    public static final String NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED = "numberOfSegmentsYetToBeCommitted";
  }

  // prefix for scheduler related features, e.g. query accountant
  public static final String PINOT_QUERY_SCHEDULER_PREFIX = "pinot.query.scheduler";

  public static class Accounting {
    public static final int ANCHOR_TASK_ID = -1;
    public static final String CONFIG_OF_FACTORY_NAME = "accounting.factory.name";

    public static final String CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING = "accounting.enable.thread.cpu.sampling";
    public static final Boolean DEFAULT_ENABLE_THREAD_CPU_SAMPLING = false;

    public static final String CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING = "accounting.enable.thread.memory.sampling";
    public static final Boolean DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING = false;

    public static final String CONFIG_OF_OOM_PROTECTION_KILLING_QUERY = "accounting.oom.enable.killing.query";
    public static final boolean DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY = false;

    public static final String CONFIG_OF_PUBLISHING_JVM_USAGE = "accounting.publishing.jvm.heap.usage";
    public static final boolean DEFAULT_PUBLISHING_JVM_USAGE = false;

    public static final String CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED = "accounting.cpu.time.based.killing.enabled";
    public static final boolean DEFAULT_CPU_TIME_BASED_KILLING_ENABLED = false;

    public static final String CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS =
        "accounting.cpu.time.based.killing.threshold.ms";
    public static final int DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS = 30_000;

    public static final String CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.panic.heap.usage.ratio";
    public static final float DFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO = 0.99f;

    public static final String CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.critical.heap.usage.ratio";
    public static final float DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO = 0.96f;

    public static final String CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC =
        "accounting.oom.critical.heap.usage.ratio.delta.after.gc";
    public static final float DEFAULT_CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC = 0.15f;

    public static final String CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.alarming.usage.ratio";
    public static final float DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO = 0.75f;

    public static final String CONFIG_OF_HEAP_USAGE_PUBLISHING_PERIOD_MS = "accounting.heap.usage.publishing.period.ms";
    public static final int DEFAULT_HEAP_USAGE_PUBLISH_PERIOD = 5000;

    public static final String CONFIG_OF_SLEEP_TIME_MS = "accounting.sleep.ms";
    public static final int DEFAULT_SLEEP_TIME_MS = 30;

    public static final String CONFIG_OF_SLEEP_TIME_DENOMINATOR = "accounting.sleep.time.denominator";
    public static final int DEFAULT_SLEEP_TIME_DENOMINATOR = 3;

    public static final String CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO =
        "accounting.min.memory.footprint.to.kill.ratio";
    public static final double DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO = 0.025;

    public static final String CONFIG_OF_GC_BACKOFF_COUNT = "accounting.gc.backoff.count";
    public static final int DEFAULT_GC_BACKOFF_COUNT = 5;

    public static final String CONFIG_OF_GC_WAIT_TIME_MS = "accounting.gc.wait.time.ms";
    public static final int DEFAULT_CONFIG_OF_GC_WAIT_TIME_MS = 0;

    public static final String CONFIG_OF_QUERY_KILLED_METRIC_ENABLED = "accounting.query.killed.metric.enabled";
    public static final boolean DEFAULT_QUERY_KILLED_METRIC_ENABLED = false;

    public static final String CONFIG_OF_ENABLE_THREAD_SAMPLING_MSE = "accounting.enable.thread.sampling.mse.debug";
    public static final Boolean DEFAULT_ENABLE_THREAD_SAMPLING_MSE = true;

    public static final String CONFIG_OF_CANCEL_CALLBACK_CACHE_MAX_SIZE = "accounting.cancel.callback.cache.max.size";
    public static final int DEFAULT_CANCEL_CALLBACK_CACHE_MAX_SIZE = 500;

    public static final String CONFIG_OF_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS =
        "accounting.cancel.callback.cache.expiry.seconds";
    public static final int DEFAULT_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS = 1200;

    public static final String CONFIG_OF_THREAD_SELF_TERMINATE =
        "accounting.thread.self.terminate";
    public static final boolean DEFAULT_THREAD_SELF_TERMINATE = false;

    /**
     * QUERY WORKLOAD ISOLATION Configs
     *
     * This is a set of configs to enable query workload isolation. Queries are classified into workload based on the
     * QueryOption - WORKLOAD_NAME. The CPU and Memory cost for a workload are set globally in ZK. The CPU and memory
     * costs are for a certain time duration, called "enforcementWindow". The workload cost is split into smaller cost
     * for each instance involved in executing queries of the workload.
     *
     *
     * At each instance (broker,server), there are two parts to workload isolation:
     * 1. Workload Cost Collection
     * 2. Workload Cost Enforcement
     *
     *
     * Workload Cost collection happens at various stages of query execution. On server, the resource costs associated
     * with pruning, planning and execution are collected. On broker, the resource costs associated with compilation &
     * reduce are collected. WorkloadBudgetManager maintains the budget and usage for each workload in the instance.
     * Workload Enforcement enforces the budget for a workload if the resource usages are exceeded. The queries in the
     * workload are killed until the enforcementWindow is refreshed.
     *
     * More details in https://tinyurl.com/2p9vuzbd
     *
     * Pre-req configs for enabling Query Workload Isolation:
     *  - CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME  = ResourceUsageAccountantFactory
     *  - CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING = true
     *  - CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING = true
     *  - CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_SAMPLING_MSE = true
     *  - Instance Config: enableThreadCpuTimeMeasurement = true
     *  - Instance Config: enableThreadAllocatedBytesMeasurement = true
     */

    public static final String CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION =
        "accounting.workload.enable.cost.collection";
    public static final boolean DEFAULT_WORKLOAD_ENABLE_COST_COLLECTION = false;

    public static final String CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT =
        "accounting.workload.enable.cost.enforcement";
    public static final boolean DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT = false;

    public static final String CONFIG_OF_WORKLOAD_ENFORCEMENT_WINDOW_MS =
        "accounting.workload.enforcement.window.ms";
    public static final long DEFAULT_WORKLOAD_ENFORCEMENT_WINDOW_MS = 60_000L;

    public static final String CONFIG_OF_WORKLOAD_SLEEP_TIME_MS =
        "accounting.workload.sleep.time.ms";
    public static final int DEFAULT_WORKLOAD_SLEEP_TIME_MS = 1;

    public static final String DEFAULT_WORKLOAD_NAME = "default";
    public static final String CONFIG_OF_SECONDARY_WORKLOAD_NAME = "accounting.secondary.workload.name";
    public static final String DEFAULT_SECONDARY_WORKLOAD_NAME = "defaultSecondary";
    public static final String CONFIG_OF_SECONDARY_WORKLOAD_CPU_PERCENTAGE =
        "accounting.secondary.workload.cpu.percentage";
    public static final double DEFAULT_SECONDARY_WORKLOAD_CPU_PERCENTAGE = 0.0;
  }

  public static class ExecutorService {
    public static final String PINOT_QUERY_RUNNER_NAME_PREFIX = "pqr-";
    public static final String PINOT_QUERY_RUNNER_NAME_FORMAT = PINOT_QUERY_RUNNER_NAME_PREFIX + "%d";
    public static final String PINOT_QUERY_WORKER_NAME_PREFIX = "pqw-";
    public static final String PINOT_QUERY_WORKER_NAME_FORMAT = PINOT_QUERY_WORKER_NAME_PREFIX + "%d";
  }

  public static class Segment {
    public static class Realtime {
      public enum Status {
        IN_PROGRESS, // The segment is still consuming data
        COMMITTING, // This state will only be utilised by pauseless ingestion when the segment has been consumed but
                    // is yet to be build and uploaded by the server.
        DONE, // The segment has finished consumption and has been committed to the segment store
        UPLOADED; // The segment is uploaded by an external party

        /**
         * Returns {@code true} if the segment is completed (DONE/UPLOADED), {@code false} otherwise.
         *
         * The segment is
         * 1. still Consuming if the status is IN_PROGRESS
         * 2. just done consuming but not yet committed if the status is COMMITTING (for pauseless tables)
         */
        public boolean isCompleted() {
          return (this == DONE) || (this == UPLOADED);
        }
      }

      /**
       * During realtime segment completion, the value of this enum decides how  non-winner servers should replace
       * the completed segment.
       */
      public enum CompletionMode {
        // default behavior - if the in memory segment in the non-winner server is equivalent to the committed
        // segment, then build and replace, else download
        DEFAULT, // non-winner servers always download the segment, never build it
        DOWNLOAD
      }

      public static final String STATUS = "segment.realtime.status";
      public static final String START_OFFSET = "segment.realtime.startOffset";
      public static final String END_OFFSET = "segment.realtime.endOffset";
      public static final String NUM_REPLICAS = "segment.realtime.numReplicas";
      public static final String FLUSH_THRESHOLD_SIZE = "segment.flush.threshold.size";
      @Deprecated
      public static final String FLUSH_THRESHOLD_TIME = "segment.flush.threshold.time";

      // Deprecated, but kept for backward-compatibility of reading old segments' ZK metadata
      @Deprecated
      public static final String DOWNLOAD_URL = "segment.realtime.download.url";
    }

    // Deprecated, but kept for backward-compatibility of reading old segments' ZK metadata
    @Deprecated
    public static class Offline {
      public static final String DOWNLOAD_URL = "segment.offline.download.url";
      public static final String PUSH_TIME = "segment.offline.push.time";
      public static final String REFRESH_TIME = "segment.offline.refresh.time";
    }

    public static final String START_TIME = "segment.start.time";
    public static final String END_TIME = "segment.end.time";
    public static final String RAW_START_TIME = "segment.start.time.raw";
    public static final String RAW_END_TIME = "segment.end.time.raw";
    public static final String TIME_UNIT = "segment.time.unit";
    public static final String INDEX_VERSION = "segment.index.version";
    public static final String TOTAL_DOCS = "segment.total.docs";
    public static final String CRC = "segment.crc";
    public static final String TIER = "segment.tier";
    public static final String CREATION_TIME = "segment.creation.time";
    public static final String PUSH_TIME = "segment.push.time";
    public static final String REFRESH_TIME = "segment.refresh.time";
    public static final String DOWNLOAD_URL = "segment.download.url";
    public static final String CRYPTER_NAME = "segment.crypter";
    public static final String PARTITION_METADATA = "segment.partition.metadata";
    public static final String CUSTOM_MAP = "custom.map";
    public static final String SIZE_IN_BYTES = "segment.size.in.bytes";

    /**
     * This field is used for parallel push protection to lock the segment globally.
     * We put the segment upload start timestamp so that if the previous push failed without unlock the segment, the
     * next upload won't be blocked forever.
     */
    public static final String SEGMENT_UPLOAD_START_TIME = "segment.upload.start.time";

    public static final String SEGMENT_BACKUP_DIR_SUFFIX = ".segment.bak";
    public static final String SEGMENT_TEMP_DIR_SUFFIX = ".segment.tmp";

    public static final String LOCAL_SEGMENT_SCHEME = "file";
    public static final String PEER_SEGMENT_DOWNLOAD_SCHEME = "peer://";
    public static final String METADATA_URI_FOR_PEER_DOWNLOAD = "";

    public static class AssignmentStrategy {
      public static final String BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY = "balanced";
      public static final String REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY = "replicagroup";
      public static final String DIM_TABLE_SEGMENT_ASSIGNMENT_STRATEGY = "allservers";
    }

    public static class BuiltInVirtualColumn {
      public static final String DOCID = "$docId";
      public static final String HOSTNAME = "$hostName";
      public static final String SEGMENTNAME = "$segmentName";
      public static final Set<String> BUILT_IN_VIRTUAL_COLUMNS = Set.of(DOCID, HOSTNAME, SEGMENTNAME);
    }
  }

  public static class Tier {
    public static final String BACKEND_PROP_DATA_DIR = "dataDir";
  }

  public static class Explain {
    public static class Response {
      public static class ServerResponseStatus {
        public static final String STATUS_ERROR = "ERROR";
        public static final String STATUS_OK = "OK";
      }
    }
  }

  public static class Query {

    /**
     * Configuration keys for query context mode.
     *
     * Valid values are 'strict' (ignoring case) or empty.
     *
     * In strict mode, if the {@link QueryThreadContext} is not initialized, an {@link IllegalStateException} will be
     * thrown when setter and getter methods are used. Otherwise a warning will be logged and the fake instance will be
     * returned.
     */
    public static final String CONFIG_OF_QUERY_CONTEXT_MODE = "pinot.query.context.mode";

    public static class Request {
      public static class MetadataKeys {
        /// This is the request id, which may change during the execution.
        ///
        /// See [QueryThreadContext#getRequestId()] for more details.
        public static final String REQUEST_ID = "requestId";
        /// Ths is the correlation id, which is set when the query starts and will not change during the execution.
        /// This value is either set by the client or generated by the broker, in which case it will be equal to the
        /// original request id.
        ///
        /// See [QueryThreadContext#getCid()] for more details.
        public static final String CORRELATION_ID = "correlationId";
        public static final String BROKER_ID = "brokerId";
        public static final String ENABLE_TRACE = "enableTrace";
        public static final String ENABLE_STREAMING = "enableStreaming";
        public static final String PAYLOAD_TYPE = "payloadType";
      }

      public static class PayloadType {
        public static final String SQL = "sql";
        public static final String BROKER_REQUEST = "brokerRequest";
      }
    }

    public static class Response {
      public static class MetadataKeys {
        public static final String RESPONSE_TYPE = "responseType";
      }

      public static class ResponseType {
        // For streaming response, multiple (could be 0 if no data should be returned, or query encounters exception)
        // data responses will be returned, followed by one single metadata response
        public static final String DATA = "data";
        public static final String METADATA = "metadata";
        // For non-streaming response
        public static final String NON_STREAMING = "nonStreaming";
      }

      /**
       * Configuration keys for {@link org.apache.pinot.common.proto.Worker.QueryResponse} extra metadata.
       */
      public static class ServerResponseStatus {
        public static final String STATUS_ERROR = "ERROR";
        public static final String STATUS_OK = "OK";
      }
    }

    public static class OptimizationConstants {
      public static final int DEFAULT_AVG_MV_ENTRIES_DENOMINATOR = 2;
    }

    public static class Range {
      public static final char DELIMITER = '\0';
      public static final char LOWER_EXCLUSIVE = '(';
      public static final char LOWER_INCLUSIVE = '[';
      public static final char UPPER_EXCLUSIVE = ')';
      public static final char UPPER_INCLUSIVE = ']';
      public static final String UNBOUNDED = "*";
      public static final String LOWER_UNBOUNDED = LOWER_EXCLUSIVE + UNBOUNDED + DELIMITER;
      public static final String UPPER_UNBOUNDED = DELIMITER + UNBOUNDED + UPPER_EXCLUSIVE;
    }
  }

  public static class IdealState {
    public static final String HYBRID_TABLE_TIME_BOUNDARY = "HYBRID_TABLE_TIME_BOUNDARY";
  }

  public static class RewriterConstants {
    public static final String PARENT_AGGREGATION_NAME_PREFIX = "pinotparentagg";
    public static final String CHILD_AGGREGATION_NAME_PREFIX = "pinotchildagg";
    public static final String CHILD_AGGREGATION_SEPERATOR = "@";
    public static final String CHILD_KEY_SEPERATOR = "_";
  }

  /**
   * Configuration for setting up multi-stage query runner, this service could be running on either broker or server.
   */
  public static class MultiStageQueryRunner {
    /**
     * Configuration for mailbox data block size
     */
    public static final String KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = "pinot.query.runner.max.msg.size.bytes";
    public static final int DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

    /**
     * Enable splitting of data block payload during mailbox transfer.
     */
    public static final String KEY_OF_ENABLE_DATA_BLOCK_PAYLOAD_SPLIT =
        "pinot.query.runner.enable.data.block.payload.split";
    public static final boolean DEFAULT_ENABLE_DATA_BLOCK_PAYLOAD_SPLIT = false;

    /**
     * Configuration for server port, port that opens and accepts
     * {@link org.apache.pinot.query.runtime.plan.DistributedStagePlan} and start executing query stages.
     */
    public static final String KEY_OF_QUERY_SERVER_PORT = "pinot.query.server.port";
    public static final int DEFAULT_QUERY_SERVER_PORT = 0;

    /**
     * Configuration for mailbox hostname and port, this hostname and port opens streaming channel to receive
     * {@link org.apache.pinot.common.datablock.DataBlock}.
     */
    public static final String KEY_OF_QUERY_RUNNER_HOSTNAME = "pinot.query.runner.hostname";
    public static final String KEY_OF_QUERY_RUNNER_PORT = "pinot.query.runner.port";
    public static final int DEFAULT_QUERY_RUNNER_PORT = 0;

    /**
     * Configuration for join overflow.
     */
    public static final String KEY_OF_MAX_ROWS_IN_JOIN = "pinot.query.join.max.rows";
    public static final String KEY_OF_JOIN_OVERFLOW_MODE = "pinot.query.join.overflow.mode";

    /// Specifies the send stats mode used in MSE.
    ///
    /// Valid values are (in lower or upper case):
    /// - "SAFE": MSE will only send stats if all instances in the cluster are running 1.4.0 or later.
    /// - "ALWAYS": MSE will always send stats, regardless of the version of the instances in the cluster.
    /// - "NEVER": MSE will never send stats.
    ///
    /// The reason for this flag that versions 1.3.0 and lower have two undesired behaviors:
    /// 1. Some queries using intersection generate incorrect stats
    /// 2. When stats from other nodes are sent but are different from expected, the query fails.
    ///
    /// In 1.4.0 the first issue is solved and instead of failing when unexpected stats are received, the query
    /// continues without children stats. But if a query involves servers in versions 1.3.0 and 1.4.0, the one
    /// running 1.3.0 may fail, which breaks backward compatibility.
    public static final String KEY_OF_SEND_STATS_MODE = "pinot.query.mse.stats.mode";
    public static final String DEFAULT_SEND_STATS_MODE = "SAFE";

    /// Used to indicate that MSE stats should be logged at INFO level for successful queries.
    ///
    /// When an MSE query is executed, the stats are collected and logged.
    /// By default, successful queries are logged in the DEBUG level, while errors are logged in the INFO level.
    /// But if this property is set to true (upper or lower case), stats will be logged in the INFO level for both
    /// successful queries and errors.
    public static final String KEY_OF_LOG_STATS = "logStats";

    public enum JoinOverFlowMode {
      THROW, BREAK
    }

    /**
     * Configuration for window overflow.
     */
    public static final String KEY_OF_MAX_ROWS_IN_WINDOW = "pinot.query.window.max.rows";
    public static final String KEY_OF_WINDOW_OVERFLOW_MODE = "pinot.query.window.overflow.mode";

    public enum WindowOverFlowMode {
      THROW, BREAK
    }

    /**
     * Constants related to plan versions.
     */
    public static class PlanVersions {
      public static final int V1 = 1;
    }

    public static final String KEY_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN =
        "pinot.query.multistage.explain.include.segment.plan";
    public static final boolean DEFAULT_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN = false;

    /// Max number of rows operators stored in the op stats cache.
    /// Although the cache stores stages, each entry has a weight equal to the number of operators in the stage.
    public static final String KEY_OF_OP_STATS_CACHE_SIZE = "pinot.server.query.op.stats.cache.size";
    public static final int DEFAULT_OF_OP_STATS_CACHE_SIZE = 1000;

    /// Max time to keep the op stats in the cache.
    public static final String KEY_OF_OP_STATS_CACHE_EXPIRE_MS = "pinot.server.query.op.stats.cache.ms";
    public static final int DEFAULT_OF_OP_STATS_CACHE_EXPIRE_MS = 60 * 1000;
    /// Timeout of the cancel request, in milliseconds.
    public static final String KEY_OF_CANCEL_TIMEOUT_MS = "pinot.server.query.cancel.timeout.ms";
    public static final long DEFAULT_OF_CANCEL_TIMEOUT_MS = 1000;
  }

  public static class NullValuePlaceHolder {
    public static final int INT = 0;
    public static final long LONG = 0L;
    public static final float FLOAT = 0f;
    public static final double DOUBLE = 0d;
    public static final BigDecimal BIG_DECIMAL = BigDecimal.ZERO;
    public static final String STRING = "";
    public static final byte[] BYTES = new byte[0];
    public static final ByteArray INTERNAL_BYTES = new ByteArray(BYTES);
    public static final int[] INT_ARRAY = new int[0];
    public static final long[] LONG_ARRAY = new long[0];
    public static final float[] FLOAT_ARRAY = new float[0];
    public static final double[] DOUBLE_ARRAY = new double[0];
    public static final String[] STRING_ARRAY = new String[0];
    public static final byte[][] BYTES_ARRAY = new byte[0][];
    public static final Object MAP = Collections.emptyMap();
  }

  public static class CursorConfigs {
    public static final String PREFIX_OF_CONFIG_OF_CURSOR = "pinot.broker.cursor";
    public static final String PREFIX_OF_CONFIG_OF_RESPONSE_STORE = "pinot.broker.cursor.response.store";
    public static final String DEFAULT_RESPONSE_STORE_TYPE = "file";
    public static final String RESPONSE_STORE_TYPE = "type";
    public static final int DEFAULT_CURSOR_FETCH_ROWS = 10000;
    public static final String CURSOR_FETCH_ROWS = PREFIX_OF_CONFIG_OF_CURSOR + ".fetch.rows";
    public static final String DEFAULT_RESULTS_EXPIRATION_INTERVAL = "1h"; // 1 hour.
    public static final String RESULTS_EXPIRATION_INTERVAL = PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".expiration";

    public static final String RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD =
        "controller.cluster.response.store.cleaner.frequencyPeriod";
    public static final String DEFAULT_RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD = "1h";
    public static final String RESPONSE_STORE_CLEANER_INITIAL_DELAY =
        "controller.cluster.response.store.cleaner.initialDelay";
  }

  public static class ForwardIndexConfigs {
    public static final String CONFIG_OF_DEFAULT_RAW_INDEX_WRITER_VERSION =
        "pinot.forward.index.default.raw.index.writer.version";
    public static final String CONFIG_OF_DEFAULT_TARGET_MAX_CHUNK_SIZE =
        "pinot.forward.index.default.target.max.chunk.size";
    public static final String CONFIG_OF_DEFAULT_TARGET_DOCS_PER_CHUNK =
        "pinot.forward.index.default.target.docs.per.chunk";
  }

  public static class FieldSpecConfigs {
    public static final String CONFIG_OF_DEFAULT_JSON_MAX_LENGTH_EXCEED_STRATEGY =
        "pinot.field.spec.default.json.max.length.exceed.strategy";
    public static final String CONFIG_OF_DEFAULT_JSON_MAX_LENGTH =
        "pinot.field.spec.default.json.max.length";
  }

  /**
   * Configuration for setting up groovy static analyzer.
   * User can config different configuration for query and ingestion (table creation and update) static analyzer.
   * The all configuration is the default configuration for both query and ingestion static analyzer.
   */
  public static class Groovy {
    public static final String GROOVY_ALL_STATIC_ANALYZER_CONFIG = "pinot.groovy.all.static.analyzer";
    public static final String GROOVY_QUERY_STATIC_ANALYZER_CONFIG = "pinot.groovy.query.static.analyzer";
    public static final String GROOVY_INGESTION_STATIC_ANALYZER_CONFIG = "pinot.groovy.ingestion.static.analyzer";
  }

  /**
   * ZK paths used by Pinot.
   */
  public static class ZkPaths {
    public static final String LOGICAL_TABLE_PARENT_PATH = "/LOGICAL/TABLE";
    public static final String LOGICAL_TABLE_PATH_PREFIX = "/LOGICAL/TABLE/";
    public static final String TABLE_CONFIG_PATH_PREFIX = "/CONFIGS/TABLE/";
    public static final String SCHEMA_PATH_PREFIX = "/SCHEMAS/";
  }
}
