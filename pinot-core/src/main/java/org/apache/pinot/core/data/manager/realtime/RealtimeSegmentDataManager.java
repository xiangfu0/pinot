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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.ConsumptionRateLimiter;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.dedup.DedupContext;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamDataDecoder;
import org.apache.pinot.spi.stream.StreamDataDecoderImpl;
import org.apache.pinot.spi.stream.StreamDataDecoderResult;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants.ConsumerState;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.CompletionMode;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment data manager for low level consumer realtime segments, which manages consumption and segment completion.
 */
@SuppressWarnings("jol")
public class RealtimeSegmentDataManager extends SegmentDataManager {

  @VisibleForTesting
  public enum State {
    // The state machine starts off with this state. While in this state we consume stream events
    // and index them in memory. We continue to be in this state until the end criteria is satisfied
    // (time or number of rows)
    INITIAL_CONSUMING,

    // In this state, we consume from stream until we reach the _finalOffset (exclusive)
    CATCHING_UP,

    // In this state, we sleep for MAX_HOLDING_TIME_MS, and the make a segmentConsumed() call to the
    // controller.
    HOLDING,

    // We have been asked to go Online from Consuming state, and are trying a last attempt to catch up to the
    // target offset. In this state, we have a time constraint as well as a final offset constraint to look into
    // before we stop consuming.
    CONSUMING_TO_ONLINE,

    // We have been asked by the controller to retain the segment we have in memory at the current offset.
    // We should build the segment, and replace it with the in-memory segment.
    RETAINING,

    // We have been asked by the controller to commit the segment at the current offset. Build the segment
    // and make a segmentCommit() call to the controller.
    COMMITTING,

    // We have been asked to discard the in-memory segment we have. We will be serving queries, but not consuming
    // anymore rows from stream. We wait for a helix transition to go ONLINE, at which point, we can download the
    // segment from the controller and replace it with the in-memory segment.
    DISCARDED,

    // We have replaced our in-memory segment with the segment that has been built locally.
    RETAINED,

    // We have committed our segment to the controller, and also replaced it locally with the constructed segment.
    COMMITTED,

    // Something went wrong, we need to download the segment when we get the ONLINE transition.
    ERROR;

    public boolean shouldConsume() {
      return this.equals(INITIAL_CONSUMING) || this.equals(CATCHING_UP) || this.equals(CONSUMING_TO_ONLINE);
    }

    public boolean isFinal() {
      return this.equals(ERROR) || this.equals(COMMITTED) || this.equals(RETAINED) || this.equals(DISCARDED);
    }
  }

  @VisibleForTesting
  public class SegmentBuildDescriptor {
    final File _segmentTarFile;
    final Map<String, File> _metadataFileMap;
    final StreamPartitionMsgOffset _offset;
    final long _waitTimeMillis;
    final long _buildTimeMillis;
    final long _segmentSizeBytes;

    public SegmentBuildDescriptor(@Nullable File segmentTarFile, @Nullable Map<String, File> metadataFileMap,
        StreamPartitionMsgOffset offset, long buildTimeMillis, long waitTimeMillis, long segmentSizeBytes) {
      _segmentTarFile = segmentTarFile;
      _metadataFileMap = metadataFileMap;
      _offset = _streamPartitionMsgOffsetFactory.create(offset);
      _buildTimeMillis = buildTimeMillis;
      _waitTimeMillis = waitTimeMillis;
      _segmentSizeBytes = segmentSizeBytes;
    }

    public StreamPartitionMsgOffset getOffset() {
      return _offset;
    }

    public long getBuildTimeMillis() {
      return _buildTimeMillis;
    }

    public long getWaitTimeMillis() {
      return _waitTimeMillis;
    }

    @Nullable
    public File getSegmentTarFile() {
      return _segmentTarFile;
    }

    @Nullable
    public Map<String, File> getMetadataFiles() {
      return _metadataFileMap;
    }

    public long getSegmentSizeBytes() {
      return _segmentSizeBytes;
    }

    public void deleteSegmentFile() {
      if (_segmentTarFile != null) {
        FileUtils.deleteQuietly(_segmentTarFile);
      }
    }
  }

  public static final String RESOURCE_TEMP_DIR_NAME = "_tmp";

  private static final int MINIMUM_CONSUME_TIME_MINUTES = 10;
  private static final long TIME_THRESHOLD_FOR_LOG_MINUTES = 1;
  private static final long TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS = 1;
  private static final int MSG_COUNT_THRESHOLD_FOR_LOG = 100000;
  private static final int BUILD_TIME_LEASE_SECONDS = 30;
  private static final int MAX_CONSECUTIVE_ERROR_COUNT = 5;

  // Interrupt consumer thread every 10 seconds in case it doesn't stop, e.g. interrupt flag getting cleared somehow
  private static final int CONSUMER_THREAD_INTERRUPT_INTERVAL_MS = 10000;

  private final SegmentZKMetadata _segmentZKMetadata;
  private final TableConfig _tableConfig;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final StreamDataDecoder _streamDataDecoder;
  private final int _segmentMaxRowCount;
  private final String _resourceDataDir;
  private final Schema _schema;
  private final LLCSegmentName _llcSegmentName;
  private final AtomicBoolean _streamConsumerClosed = new AtomicBoolean(false);
  // Semaphore for each partitionGroupId only, which is to prevent two different stream consumers
  // from consuming with the same partitionGroupId in parallel in the same host.
  // See the comments in {@link RealtimeTableDataManager}.
  private final ConsumerCoordinator _consumerCoordinator;
  // A boolean flag to check whether the current thread has acquired the semaphore.
  // This boolean is needed because the semaphore is shared by threads; every thread holding this semaphore can
  // modify the permit. This boolean make sure the semaphore gets released only once when the partition group stops
  // consuming.
  private final AtomicBoolean _consumerSemaphoreAcquired = new AtomicBoolean(false);
  private final ServerMetrics _serverMetrics;
  private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private final PartitionDedupMetadataManager _partitionDedupMetadataManager;
  private final BooleanSupplier _isReadyToConsumeData;
  private final MutableSegmentImpl _realtimeSegment;
  private volatile StreamPartitionMsgOffset _currentOffset; // Next offset to be consumed
  private volatile State _state;
  private volatile int _numRowsConsumed = 0;
  private volatile int _numRowsIndexed = 0; // Can be different from _numRowsConsumed when metrics update is enabled.
  private volatile int _numRowsErrored = 0;
  private volatile long _numBytesDropped = 0;
  private volatile int _consecutiveErrorCount = 0;
  private long _startTimeMs = 0;
  private final IdleTimer _idleTimer = new IdleTimer();
  private final String _segmentNameStr;
  private final SegmentVersion _segmentVersion;
  private final SegmentBuildTimeLeaseExtender _leaseExtender;
  private SegmentBuildDescriptor _segmentBuildDescriptor;
  private boolean _segmentBuildFailedWithDeterministicError = false;
  private final StreamConsumerFactory _streamConsumerFactory;
  private final StreamPartitionMsgOffsetFactory _streamPartitionMsgOffsetFactory;

  // Segment end criteria
  private volatile long _consumeEndTime = 0;
  private volatile boolean _hasMessagesFetched = false;
  private volatile boolean _endOfPartitionGroup = false;
  private volatile boolean _forceCommitMessageReceived = false;
  private volatile StreamPartitionMsgOffset _finalOffset; // Exclusive, used when we want to catch up to this one
  private volatile boolean _shouldStop = false;

  // It takes 30s to locate controller leader, and more if there are multiple controller failures.
  // For now, we let 31s pass for this state transition.
  private static final int MAX_TIME_FOR_CONSUMING_TO_ONLINE_IN_SECONDS = 31;

  private Thread _consumerThread;
  // _partitionGroupId represents the Pinot's internal partition number which will eventually be used as part of
  // segment name.
  // _streamPartitionId represents the partition number in the stream topic, which could be derived from the
  // _partitionGroupId and identify which partition of the stream topic this consumer is consuming from.
  // Note that in traditional single topic ingestion mode, those two concepts were identical which got separated
  // in multi-topic ingestion mode.
  private final int _partitionGroupId;
  private final int _streamPartitionId;
  private final PartitionGroupConsumptionStatus _partitionGroupConsumptionStatus;
  final String _clientId;
  private final TransformPipeline _transformPipeline;
  private PartitionGroupConsumer _partitionGroupConsumer = null;
  private StreamMetadataProvider _partitionMetadataProvider = null;
  private final File _resourceTmpDir;
  private final String _tableNameWithType;
  private final Logger _segmentLogger;
  private final String _tableStreamName;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final AtomicLong _lastUpdatedRowsIndexed = new AtomicLong(0);
  private final String _instanceId;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final StreamPartitionMsgOffset _startOffset;
  private final StreamConfig _streamConfig;

  private StreamMessageMetadata _lastRowMetadata;
  private long _lastConsumedTimestampMs = -1;
  private long _consumeStartTime = -1;
  private long _lastLogTime = 0;
  private int _lastConsumedCount = 0;
  private String _stopReason = null;
  private final Semaphore _segBuildSemaphore;
  private final boolean _isOffHeap;
  /**
   * Whether null handling is enabled by default. This value is only used if
   * {@link Schema#isEnableColumnBasedNullHandling()} is false.
   */
  private final boolean _defaultNullHandlingEnabled;
  private final SegmentCommitterFactory _segmentCommitterFactory;
  private final ConsumptionRateLimiter _partitionRateLimiter;
  private final ConsumptionRateLimiter _serverRateLimiter;

  private final StreamPartitionMsgOffset _latestStreamOffsetAtStartupTime;
  private final CompletionMode _segmentCompletionMode;
  private final List<String> _filteredMessageOffsets = new ArrayList<>();
  private final boolean _trackFilteredMessageOffsets;
  private final ParallelSegmentConsumptionPolicy _parallelSegmentConsumptionPolicy;

  // TODO each time this method is called, we print reason for stop. Good to print only once.
  private boolean endCriteriaReached() {
    Preconditions.checkState(_state.shouldConsume(), "Incorrect state %s", _state);
    long now = now();
    switch (_state) {
      case INITIAL_CONSUMING:
        // The segment has been created, and we have not posted a segmentConsumed() message on the controller yet.
        // We need to consume as much data as available, until we have encountered one of the following scenarios:
        //   - the max number of rows has been reached
        //   - the max time we are allowed to consume has passed;
        //   - partition group is ended
        //   - force commit message has been received
        if (now >= _consumeEndTime) {
          if (!_hasMessagesFetched) {
            _segmentLogger.info("No events came in, extending time by {} hours", TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS);
            _consumeEndTime += TimeUnit.HOURS.toMillis(TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS);
            return false;
          }
          _segmentLogger
              .info("Stopping consumption due to time limit start={} now={} numRowsConsumed={} numRowsIndexed={}",
                  _startTimeMs, now, _numRowsConsumed, _numRowsIndexed);
          _stopReason = SegmentCompletionProtocol.REASON_TIME_LIMIT;
          return true;
        } else if (_numRowsIndexed >= _segmentMaxRowCount) {
          _segmentLogger.info("Stopping consumption due to row limit nRows={} numRowsIndexed={}, numRowsConsumed={}",
              _segmentMaxRowCount, _numRowsIndexed, _numRowsConsumed);
          _stopReason = SegmentCompletionProtocol.REASON_ROW_LIMIT;
          return true;
        } else if (_endOfPartitionGroup) {
          _segmentLogger.info("Stopping consumption due to end of partitionGroup reached nRows={} numRowsIndexed={}, "
              + "numRowsConsumed={}", _segmentMaxRowCount, _numRowsIndexed, _numRowsConsumed);
          _stopReason = SegmentCompletionProtocol.REASON_END_OF_PARTITION_GROUP;
          return true;
        } else if (_forceCommitMessageReceived) {
          _segmentLogger.info("Stopping consumption due to force commit - numRowsConsumed={} numRowsIndexed={}",
              _numRowsConsumed, _numRowsIndexed);
          _stopReason = SegmentCompletionProtocol.REASON_FORCE_COMMIT_MESSAGE_RECEIVED;
          return true;
        } else if (!canAddMore()) {
          _segmentLogger.info(
              "Stopping consumption as mutable index cannot consume more rows - numRowsConsumed={} "
                  + "numRowsIndexed={}",
              _numRowsConsumed, _numRowsIndexed);
          _stopReason = SegmentCompletionProtocol.REASON_INDEX_CAPACITY_THRESHOLD_BREACHED;
          return true;
        }
        return false;

      case CATCHING_UP:
        _stopReason = null;
        // We have posted segmentConsumed() at least once, and the controller is asking us to catch up to a certain
        // offset.
        // There is no time limit here, so just check to see that we are still within the offset we need to reach.
        // Going past the offset is an exception.
        if (_currentOffset.compareTo(_finalOffset) == 0) {
          _segmentLogger.info("Caught up to offset={}, state={}", _finalOffset, _state.toString());
          return true;
        }
        if (_currentOffset.compareTo(_finalOffset) > 0) {
          _segmentLogger.error("Offset higher in state={}, current={}, final={}", _state.toString(), _currentOffset,
              _finalOffset);
          throw new RuntimeException("Past max offset");
        }
        return false;

      case CONSUMING_TO_ONLINE:
        // We are attempting to go from CONSUMING to ONLINE state. We are making a last attempt to catch up to the
        // target offset. We have a time constraint, and need to stop consuming if we cannot get to the target offset
        // within that time.
        if (_currentOffset.compareTo(_finalOffset) == 0) {
          _segmentLogger.info("Caught up to offset={}, state={}", _finalOffset, _state.toString());
          return true;
        } else if (now >= _consumeEndTime) {
          _segmentLogger.info("Past max time budget: offset={}, state={}", _currentOffset, _state.toString());
          return true;
        }
        if (_currentOffset.compareTo(_finalOffset) > 0) {
          _segmentLogger.error("Offset higher in state={}, current={}, final={}", _state.toString(), _currentOffset,
              _finalOffset);
          throw new RuntimeException("Past max offset");
        }
        return false;
      default:
        _segmentLogger.error("Illegal state: {}", _state);
        throw new RuntimeException("Illegal state to consume");
    }
  }

  private void handleTransientStreamErrors(Exception e)
      throws Exception {
    _consecutiveErrorCount++;
    _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
    _serverMetrics.addMeteredTableValue(_tableStreamName, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS,
        1L);
    if (_consecutiveErrorCount > MAX_CONSECUTIVE_ERROR_COUNT) {
      _segmentLogger.warn("Stream transient exception when fetching messages, stopping consumption after {} attempts",
          _consecutiveErrorCount, e);
      throw e;
    } else {
      if (_shouldStop && (e instanceof InterruptedException || e.getCause() instanceof InterruptedException)) {
        _segmentLogger.debug("Interrupted to stop consumption", e);
      } else {
        _segmentLogger.warn("Stream transient exception when fetching messages, retrying (count={})",
            _consecutiveErrorCount, e);
      }
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      recreateStreamConsumer("Too many transient errors");
    }
  }

  protected boolean consumeLoop()
      throws Exception {
    // At this point, we know that we can potentially move the offset, so the old saved segment file is not valid
    // anymore. Remove the file if it exists.
    removeSegmentFile();

    _numRowsErrored = 0;
    _numBytesDropped = 0;
    long idlePipeSleepTimeMillis = 100;
    long idleTimeoutMillis = _streamConfig.getIdleTimeoutMillis();
    _idleTimer.init();

    StreamPartitionMsgOffset lastUpdatedOffset = _streamPartitionMsgOffsetFactory
        .create(_currentOffset);  // so that we always update the metric when we enter this method.

    _segmentLogger.info("Starting consumption loop start offset {}, finalOffset {}", _currentOffset, _finalOffset);
    while (!_shouldStop && !endCriteriaReached()) {
      _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING, 1);
      // Consume for the next readTime ms, or we get to final offset, whichever happens earlier,
      // Update _currentOffset upon return from this method
      MessageBatch messageBatch;
      try {
        messageBatch = _partitionGroupConsumer.fetchMessages(_currentOffset, _streamConfig.getFetchTimeoutMillis());
        //track realtime rows fetched on a table level. This included valid + invalid rows
        _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.REALTIME_ROWS_FETCHED,
            messageBatch.getUnfilteredMessageCount());
        if (_segmentLogger.isDebugEnabled()) {
          _segmentLogger.debug("message batch received. filtered={} unfiltered={} endOfPartitionGroup={}",
              messageBatch.getMessageCount(), messageBatch.getUnfilteredMessageCount(),
              messageBatch.isEndOfPartitionGroup());
        }
        // We need to check for both endOfPartitionGroup and messageCount == 0, because
        // endOfPartitionGroup can be true even if this is the last batch of messages (has been observed for kinesis)
        // To process the last batch of messages, we need to set _endOfPartitionGroup to false in such a case
        _endOfPartitionGroup = messageBatch.getMessageCount() == 0 && messageBatch.isEndOfPartitionGroup();
        _consecutiveErrorCount = 0;
      } catch (PermanentConsumerException e) {
        _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
        _serverMetrics.addMeteredTableValue(_tableStreamName, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
        _segmentLogger.warn("Permanent exception from stream when fetching messages, stopping consumption", e);
        throw e;
      } catch (Exception e) {
        //track realtime rows fetched on a table level. This included valid + invalid rows
        // all exceptions but PermanentConsumerException are handled the same way
        // can be a TimeoutException or TransientConsumerException routinely
        // Unknown exception from stream. Treat as a transient exception.
        // One such exception seen so far is java.net.SocketTimeoutException
        handleTransientStreamErrors(e);
        continue;
      } catch (Throwable t) {
        //track realtime rows fetched on a table level. This included valid + invalid rows
        _segmentLogger.warn("Stream error when fetching messages, stopping consumption", t);
        throw t;
      }

      reportDataLoss(messageBatch);

      boolean endCriteriaReached = processStreamEvents(messageBatch, idlePipeSleepTimeMillis);

      if (_currentOffset.compareTo(lastUpdatedOffset) != 0) {
        _idleTimer.markEventConsumed();
        // We consumed something. Update the highest stream offset as well as partition-consuming metric.
        // TODO Issue 5359 Need to find a way to bump metrics without getting actual offset value.
        if (_currentOffset instanceof LongMsgOffset) {
          // TODO: only LongMsgOffset supplies long offset value.
          _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED,
              ((LongMsgOffset) _currentOffset).getOffset());
        }
        lastUpdatedOffset = _streamPartitionMsgOffsetFactory.create(_currentOffset);
      } else if (endCriteriaReached) {
        // At this point current offset has not moved because processStreamEvents() has exited before processing a
        // single message
        if (_segmentLogger.isDebugEnabled()) {
          _segmentLogger.debug("No messages processed before end criteria was reached. Staying at offset {}",
              _currentOffset);
        }
        // We check this flag again further down
      } else if (messageBatch.getUnfilteredMessageCount() > 0) {
        _idleTimer.markEventConsumed();
        // we consumed something from the stream but filtered all the content out,
        // so we need to advance the offsets to avoid getting stuck
        StreamPartitionMsgOffset nextOffset = messageBatch.getOffsetOfNextBatch();
        if (_segmentLogger.isDebugEnabled()) {
          _segmentLogger.debug("Skipped empty batch. Advancing from {} to {}", _currentOffset, nextOffset);
        }
        _currentOffset = nextOffset;
        lastUpdatedOffset = _streamPartitionMsgOffsetFactory.create(nextOffset);
      } else {
        // We did not consume any rows.
        long timeSinceStreamLastCreatedOrConsumedMs = _idleTimer.getTimeSinceStreamLastCreatedOrConsumedMs();

        if (idleTimeoutMillis >= 0 && (timeSinceStreamLastCreatedOrConsumedMs > idleTimeoutMillis)) {
          // Create a new stream consumer wrapper, in case we are stuck on something.
          recreateStreamConsumer("Total idle time: " + timeSinceStreamLastCreatedOrConsumedMs
              + " ms exceeded idle timeout: " + idleTimeoutMillis + " ms");
          _idleTimer.markStreamCreated();
        }
      }

      if (endCriteriaReached) {
        // check this flag to avoid calling endCriteriaReached() at the beginning of the loop
        break;
      }
    }

    if (_numRowsErrored > 0) {
      _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.ROWS_WITH_ERRORS, _numRowsErrored);
      _serverMetrics.addMeteredTableValue(_tableStreamName, ServerMeter.ROWS_WITH_ERRORS, _numRowsErrored);
      // TODO Although the metric is called real-time, updating it at this point is not really real-time. The choice of
      // name is partly to avoid a more convoluted name and partly in anticipation of making it real-time.
      _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.REALTIME_BYTES_DROPPED, _numBytesDropped);
    }
    return true;
  }

  /**
   * @param messageBatch batch of messages to process
   * @param idlePipeSleepTimeMillis wait time in case no messages were read
   * @return returns <code>true</code> if the process loop ended before processing the batch, <code>false</code>
   * otherwise
   */
  private boolean processStreamEvents(MessageBatch messageBatch, long idlePipeSleepTimeMillis) {
    int messageCount = messageBatch.getMessageCount();
    _partitionRateLimiter.throttle(messageCount);
    _serverRateLimiter.throttle(messageCount);

    PinotMeter realtimeBytesIngestedMeter = null;
    PinotMeter realtimeBytesDroppedMeter = null;
    PinotMeter realtimeRowsConsumedMeter = null;
    PinotMeter realtimeRowsDroppedMeter = null;
    PinotMeter realtimeIncompleteRowsConsumedMeter = null;
    PinotMeter realtimeRowsSanitizedMeter = null;

    int indexedMessageCount = 0;
    int streamMessageCount = 0;
    boolean canTakeMore = true;
    boolean prematureExit = false;

    for (int index = 0; index < messageCount; index++) {
      prematureExit = _shouldStop || endCriteriaReached();
      if (prematureExit) {
        if (_segmentLogger.isDebugEnabled()) {
          _segmentLogger.debug("stop processing message batch early shouldStop: {}", _shouldStop);
        }
        break;
      }
      if (!canTakeMore) {
        // The RealtimeSegmentImpl that we are pushing rows into has indicated that it cannot accept any more
        // rows. This can happen in one of two conditions:
        // 1. We are in INITIAL_CONSUMING state, and we somehow exceeded the max number of rows we are allowed to
        // consume
        //    for this row. Something is seriously wrong, because endCriteriaReached() should have returned true when
        //    we hit the row limit.
        //    Throw an exception.
        //
        // 2. We are in CATCHING_UP state, and we legally hit this error due to unclean leader election where
        //    offsets get changed with higher generation numbers for some pinot servers but not others. So, if another
        //    server (who got a larger stream offset) asked us to catch up to that offset, but we are connected to a
        //    broker who has smaller offsets, then we may try to push more rows into the buffer than maximum. This
        //    is a rare case, and we really don't know how to handle this at this time.
        //    Throw an exception.
        //
        _segmentLogger
            .error("Buffer full with {} rows consumed (row limit {}, indexed {})", _numRowsConsumed, _numRowsIndexed,
                _segmentMaxRowCount);
        throw new RuntimeException("Realtime segment full");
      }

      // Decode message
      StreamMessage<?> streamMessage = messageBatch.getStreamMessage(index);
      StreamDataDecoderResult decodedRow = _streamDataDecoder.decode(streamMessage);
      StreamMessageMetadata metadata = streamMessage.getMetadata();
      StreamPartitionMsgOffset offset = metadata.getOffset();
      StreamPartitionMsgOffset nextOffset = metadata.getNextOffset();
      int rowSizeInBytes = metadata.getRecordSerializedSize();
      if (decodedRow.getException() != null) {
        // TODO: based on a config, decide whether the record should be silently dropped or stop further consumption on
        // decode error
        realtimeRowsDroppedMeter =
            _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                realtimeRowsDroppedMeter);
        _numRowsErrored++;
        _numBytesDropped += rowSizeInBytes;
      } else {
        TransformPipeline.Result result = null;
        try {
          result = _transformPipeline.processRow(decodedRow.getResult());
        } catch (Exception e) {
          _numRowsErrored++;
          _numBytesDropped += rowSizeInBytes;
          String errorMessage = "Caught exception while transforming the record at offset: " + offset + " , row: "
              + decodedRow.getResult();
          _segmentLogger.error(errorMessage, e);
          _realtimeTableDataManager.addSegmentError(_segmentNameStr, new SegmentErrorInfo(now(), errorMessage, e));
        }
        if (result != null) {
          if (result.getSkippedRowCount() > 0) {
            realtimeRowsDroppedMeter =
                _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.REALTIME_ROWS_FILTERED,
                    result.getSkippedRowCount(), realtimeRowsDroppedMeter);
            if (_trackFilteredMessageOffsets) {
              _filteredMessageOffsets.add(offset.toString());
            }
          }
          if (result.getIncompleteRowCount() > 0) {
            realtimeIncompleteRowsConsumedMeter =
                _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED,
                    result.getIncompleteRowCount(), realtimeIncompleteRowsConsumedMeter);
          }
          if (result.getSanitizedRowCount() > 0) {
            realtimeRowsSanitizedMeter =
                _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.REALTIME_ROWS_SANITIZED,
                    result.getSanitizedRowCount(), realtimeRowsSanitizedMeter);
          }
          List<GenericRow> transformedRows = result.getTransformedRows();
          for (GenericRow transformedRow : transformedRows) {
            try {
              canTakeMore = _realtimeSegment.index(transformedRow, metadata);
              indexedMessageCount++;
              _lastRowMetadata = metadata;
              _lastConsumedTimestampMs = System.currentTimeMillis();
              realtimeRowsConsumedMeter =
                  _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.REALTIME_ROWS_CONSUMED, 1,
                      realtimeRowsConsumedMeter);
              _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_ROWS_CONSUMED, 1L);

              int recordSerializedValueLength = _lastRowMetadata.getRecordSerializedSize();
              if (recordSerializedValueLength > 0) {
                realtimeBytesIngestedMeter =
                    _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.REALTIME_BYTES_CONSUMED,
                        recordSerializedValueLength, realtimeBytesIngestedMeter);
                _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_BYTES_CONSUMED, recordSerializedValueLength);
              }
            } catch (Exception e) {
              _numRowsErrored++;
              _numBytesDropped += rowSizeInBytes;
              String errorMessage =
                  "Caught exception while indexing the record at offset: " + offset + " , row: " + transformedRow;
              _segmentLogger.error(errorMessage, e);
              _realtimeTableDataManager.addSegmentError(_segmentNameStr, new SegmentErrorInfo(now(), errorMessage, e));
            }
          }
        }
      }
      _currentOffset = nextOffset;
      _numRowsIndexed = _realtimeSegment.getNumDocsIndexed();
      _numRowsConsumed++;
      streamMessageCount++;
    }

    updateCurrentDocumentCountMetrics();
    if (messageBatch.getUnfilteredMessageCount() > 0) {
      updateIngestionMetrics(messageBatch.getLastMessageMetadata());
      _hasMessagesFetched = true;
      if (streamMessageCount > 0 && _segmentLogger.isDebugEnabled()) {
        _segmentLogger.debug("Indexed {} messages ({} messages read from stream) current offset {}",
            indexedMessageCount, streamMessageCount, _currentOffset);
      }
    } else if (!prematureExit) {
      // Record Pinot ingestion delay as zero since we are up-to-date and no new events
      setIngestionDelayToZero();
      if (_segmentLogger.isDebugEnabled()) {
        _segmentLogger.debug("empty batch received - sleeping for {}ms", idlePipeSleepTimeMillis);
      }
      // If there were no messages to be fetched from stream, wait for a little bit as to avoid hammering the stream
      Uninterruptibles.sleepUninterruptibly(idlePipeSleepTimeMillis, TimeUnit.MILLISECONDS);
    }
    return prematureExit;
  }

  @VisibleForTesting
  boolean canAddMore() {
    return _realtimeSegment.canAddMore();
  }

  public class PartitionConsumer implements Runnable {
    public void run() {
      long initialConsumptionEnd = 0L;
      long lastCatchUpStart = 0L;
      long catchUpTimeMillis = 0L;
      _startTimeMs = now();
      try {
        if (!_isReadyToConsumeData.getAsBoolean()) {
          do {
            //noinspection BusyWait
            Thread.sleep(RealtimeTableDataManager.READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS);
          } while (!_shouldStop && !_isReadyToConsumeData.getAsBoolean());
        }

        // Acquire semaphore before consuming data
        try {
          _consumerCoordinator.acquire(_llcSegmentName);
        } catch (ConsumerCoordinator.ShouldNotConsumeException e) {
          _segmentLogger.info("Skipping consumption because: {}", e.getMessage());
          return;
        }
        _consumerSemaphoreAcquired.set(true);
        _consumerCoordinator.register(_llcSegmentName);

        _segmentLogger.info("Acquired consumer semaphore.");

        _consumeStartTime = now();
        _segmentLogger.info("Starting consumption on segment: {}, maxRowCount: {}, maxEndTime: {}.", _llcSegmentName,
            _segmentMaxRowCount, new DateTime(_consumeEndTime, DateTimeZone.UTC));

        // TODO:
        //   When reaching here, the current consuming segment has already acquired the consumer semaphore, but there is
        //   no guarantee that the previous consuming segment is already persisted (replaced with immutable segment). It
        //   can potentially cause the following problems:
        //   1. The snapshot for the previous consuming segment might not be taken since it is not persisted yet
        //   2. If the previous consuming segment is dropped but immutable segment is not downloaded and replaced yet,
        //      it might cause inconsistency (especially for partial upsert because events are not consumed in sequence)
        //   To address this problem, we should consider releasing the consumer semaphore after the consuming segment is
        //   persisted.
        // Take upsert snapshot before starting consuming events
        if (_partitionUpsertMetadataManager != null) {
          if (_partitionUpsertMetadataManager.getContext().getMetadataTTL() > 0) {
            // If upsertMetadataTTL is enabled, we will remove expired primary keys from upsertMetadata
            // AFTER taking a snapshot. Taking the snapshot first is crucial to capture the final
            // state of each key before it exits the TTL window. Out-of-TTL segments are skipped in
            // the doAddSegment flow, and the snapshot is used to enableUpsert on the immutable out-of-TTL segment.
            // If no snapshot is found, the entire segment is marked as valid and queryable.
            _partitionUpsertMetadataManager.takeSnapshot();
            _partitionUpsertMetadataManager.removeExpiredPrimaryKeys();
          } else {
            // We should remove deleted-keys first and then take a snapshot. This is because the deletedKeysTTL
            // flow removes keys from the map and updates to remove valid doc IDs. By taking the snapshot immediately
            // after this process, we save one commit cycle, ensuring that the deletion of valid doc IDs is reflected
            // immediately
            _partitionUpsertMetadataManager.removeExpiredPrimaryKeys();
            _partitionUpsertMetadataManager.takeSnapshot();
          }
        }

        if (_partitionDedupMetadataManager != null
            && _partitionDedupMetadataManager.getContext().getMetadataTTL() > 0) {
          _partitionDedupMetadataManager.removeExpiredPrimaryKeys();
        }

        // NOTE: _shouldStop is set to true when stop() is called to terminate the consumer thread. We check this flag
        //       after every operation that can take a long time, such as consuming messages, communicating with
        //       controller, holding, etc. so that we can stop the thread as soon as possible.
        while (!_shouldStop && !_state.isFinal()) {
          if (_state.shouldConsume()) {
            consumeLoop();  // Consume until we reached the end criteria, or we are stopped.
          }
          _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING, 0);
          if (_shouldStop) {
            break;
          }

          if (_state == State.INITIAL_CONSUMING) {
            initialConsumptionEnd = now();
            _serverMetrics.setValueOfTableGauge(_clientId,
                ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
                TimeUnit.MILLISECONDS.toSeconds(initialConsumptionEnd - _startTimeMs));
          } else if (_state == State.CATCHING_UP) {
            catchUpTimeMillis += now() - lastCatchUpStart;
            _serverMetrics
                .setValueOfTableGauge(_clientId, ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
                    TimeUnit.MILLISECONDS.toSeconds(catchUpTimeMillis));
          }

          // If we are sending segmentConsumed() to the controller, we are in HOLDING state.
          _state = State.HOLDING;
          SegmentCompletionProtocol.Response response = postSegmentConsumedMsg();
          if (_shouldStop) {
            break;
          }
          SegmentCompletionProtocol.ControllerResponseStatus status = response.getStatus();
          switch (status) {
            case NOT_LEADER:
              // Retain the same state
              _segmentLogger.warn("Got not leader response");
              hold();
              break;
            case CATCH_UP:
              StreamPartitionMsgOffset rspOffset = extractOffset(response);
              if (rspOffset.compareTo(_currentOffset) <= 0) {
                // Something wrong with the controller. Back off and try again.
                _segmentLogger.error("Invalid catchup offset {} in controller response, current offset {}", rspOffset,
                    _currentOffset);
                hold();
              } else {
                _state = State.CATCHING_UP;
                _finalOffset = rspOffset;
                lastCatchUpStart = now();
                // We will restart consumption when we loop back above.
              }
              break;
            case HOLD:
              hold();
              break;
            case DISCARD:
              // Keep this in memory, but wait for the online transition, and download when it comes in.
              _state = State.DISCARDED;
              break;
            case KEEP: {
              if (_segmentCompletionMode == CompletionMode.DOWNLOAD) {
                _state = State.DISCARDED;
                break;
              }
              _state = State.RETAINING;
              // Lock the segment to avoid multiple threads touching the same segment.
              Lock segmentLock = _realtimeTableDataManager.getSegmentLock(_segmentNameStr);
              // NOTE: We need to lock interruptibly because the lock might already be held by the Helix thread for the
              //       CONSUMING -> ONLINE state transition.
              segmentLock.lockInterruptibly();
              try {
                if (buildSegmentAndReplace()) {
                  _state = State.RETAINED;
                } else {
                  // Could not build segment for some reason. We can only download it.
                  _state = State.ERROR;
                  _segmentLogger.error("Could not build segment for {}", _segmentNameStr);
                }
              } finally {
                segmentLock.unlock();
              }
              break;
            }
            case COMMIT: {
              _state = State.COMMITTING;
              _currentOffset = _partitionGroupConsumer.checkpoint(_currentOffset);
              // Lock the segment to avoid multiple threads touching the same segment.
              Lock segmentLock = _realtimeTableDataManager.getSegmentLock(_segmentNameStr);
              // NOTE: We need to lock interruptibly because the lock might already be held by the Helix thread for the
              //       CONSUMING -> ONLINE state transition.
              segmentLock.lockInterruptibly();
              try {
                // For tables with pauseless consumption enabled we want to start the commit protocol that
                // 1. Updates the endOffset in the ZK metadata for the committing segment
                // 2. Creates ZK metadata for the new consuming segment
                // 3. Updates the IdealState for committing and new consuming segment to ONLINE and CONSUMING
                // respectively.
                // Refer to the PR for the new commit protocol: https://github.com/apache/pinot/pull/14741
                if (PauselessConsumptionUtils.isPauselessEnabled(_tableConfig)) {
                  _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.PAUSELESS_CONSUMPTION_ENABLED, 1);
                  if (!startSegmentCommit()) {
                    // If for any reason commit failed, we don't want to be in COMMITTING state when we hold.
                    // Change the state to HOLDING before looping around.
                    _state = State.HOLDING;
                    _segmentLogger.info("Could not commit segment: {}. Retrying after hold", _segmentNameStr);
                    hold();
                    break;
                  }
                } else {
                  _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.PAUSELESS_CONSUMPTION_ENABLED, 0);
                }
                long buildTimeSeconds = response.getBuildTimeSeconds();
                try {
                  buildSegmentForCommit(buildTimeSeconds * 1000L);
                } catch (SegmentBuildFailureException e) {
                  handleSegmentBuildFailure();
                  break;
                }
                if (_segmentBuildDescriptor == null) {
                  // This can happen if Table data manager is already shut down.
                  handleSegmentBuildFailure();
                  break;
                }
                if (commitSegment(response.getControllerVipUrl())) {
                  _state = State.COMMITTED;
                  break;
                }
              } finally {
                segmentLock.unlock();
              }
              // If for any reason commit failed, we don't want to be in COMMITTING state when we hold.
              // Change the state to HOLDING before looping around.
              _state = State.HOLDING;
              _segmentLogger.info("Could not commit segment. Retrying after hold");
              hold();
              break;
            }
            default:
              _segmentLogger.error("Holding after response from Controller: {}", response.toJsonString());
              hold();
              break;
          }
        }
      } catch (Throwable e) {
        if (_shouldStop) {
          if (_segmentLogger.isDebugEnabled()) {
            _segmentLogger.debug("Caught exception in consumer thread after stop() is invoked, ignoring the exception",
                e);
          } else {
            _segmentLogger.info(
                "Caught exception in consumer thread after stop() is invoked: {}, ignoring the exception",
                e.getMessage());
          }
        } else {
          String errorMessage = "Exception while in work";
          _segmentLogger.error(errorMessage, e);
          _state = State.ERROR;
          _realtimeTableDataManager.addSegmentError(_segmentNameStr, new SegmentErrorInfo(now(), errorMessage, e));
          _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING, 0);
          postStopConsumedMsg(e.getClass().getName());
          return;
        }
      }

      removeSegmentFile();

      if (initialConsumptionEnd != 0L) {
        _serverMetrics
            .setValueOfTableGauge(_clientId, ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
                TimeUnit.MILLISECONDS.toSeconds(now() - initialConsumptionEnd));
      }
      // There is a race condition that the destroy() method can be called which ends up calling stop on the consumer.
      // The destroy() method does not wait for the thread to terminate (and reasonably so, we dont want to wait
      // forever).
      // Since the _shouldStop variable is set to true only in stop() method, we know that the metric will be destroyed,
      // so it is ok not to mark it non-consuming, as the main thread will clean up this metric in destroy() method
      // as the final step.
      if (!_shouldStop) {
        _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING, 0);
      }
    }

    private void handleSegmentBuildFailure() {
      // We could not build the segment. Go into error state.
      _state = State.ERROR;
      _segmentLogger.error("Could not build segment for {}", _segmentNameStr);
      if (_segmentBuildFailedWithDeterministicError && (_tableConfig.getIngestionConfig() != null)
          && _tableConfig.getIngestionConfig().isRetryOnSegmentBuildPrecheckFailure()) {
        _segmentLogger.error(
            "Found non-recoverable segment build error for {}, from offset {} to {}, sending notifyCannotBuild event.",
            _segmentNameStr, _startOffset, _currentOffset);
        notifySegmentBuildFailedWithDeterministicError();
      }
    }
  }

  private boolean startSegmentCommit() {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withSegmentName(_segmentNameStr).withStreamPartitionMsgOffset(_currentOffset.toString())
        .withNumRows(_numRowsConsumed).withInstanceId(_instanceId).withReason(_stopReason);
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }
    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(params);
    if (!segmentCommitStartResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      _segmentLogger.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return false;
    }
    return true;
  }

  @VisibleForTesting
  protected StreamPartitionMsgOffset extractOffset(SegmentCompletionProtocol.Response response) {
    return _streamPartitionMsgOffsetFactory.create(response.getStreamPartitionMsgOffset());
  }

  // Side effect: Modifies _segmentBuildDescriptor if we do not have a valid built segment file and we
  // built the segment successfully.
  protected void buildSegmentForCommit(long buildTimeLeaseMs)
      throws SegmentBuildFailureException {
    try {
      if (_segmentBuildDescriptor != null && _segmentBuildDescriptor.getOffset().compareTo(_currentOffset) == 0) {
        // Double-check that we have the file, just in case.
        File segmentTarFile = _segmentBuildDescriptor.getSegmentTarFile();
        if (segmentTarFile != null && segmentTarFile.exists()) {
          return;
        }
      }
      removeSegmentFile();
      if (buildTimeLeaseMs <= 0) {
        if (_segBuildSemaphore == null) {
          buildTimeLeaseMs = SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds() * 1000L;
        } else {
          // We know we are going to use a semaphore to limit number of segment builds, and could be
          // blocked for a long time. The controller has not provided a lease time, so set one to
          // some reasonable guess here.
          buildTimeLeaseMs = BUILD_TIME_LEASE_SECONDS * 1000;
        }
      }
      _leaseExtender.addSegment(_segmentNameStr, buildTimeLeaseMs, _currentOffset);
      _segmentBuildDescriptor = buildSegmentInternal(true);
    } finally {
      _leaseExtender.removeSegment(_segmentNameStr);
    }
  }

  /**
   * Returns the current offset for the partition group.
   */
  public Map<String, String> getPartitionToCurrentOffset() {
    return Collections.singletonMap(String.valueOf(_partitionGroupId), _currentOffset.toString());
  }

  /**
   * Returns the state of the consumer.
   */
  public ConsumerState getConsumerState() {
    return _state == State.ERROR ? ConsumerState.NOT_CONSUMING : ConsumerState.CONSUMING;
  }

  /**
   * Returns the timestamp of the last consumed message.
   */
  public long getLastConsumedTimestamp() {
    return _lastConsumedTimestampMs;
  }

  /**
   * Returns the {@link ConsumerPartitionState} for the partition group.
   */
  public Map<String, ConsumerPartitionState> getConsumerPartitionState() {
    String partitionGroupId = String.valueOf(_partitionGroupId);
    return Collections.singletonMap(partitionGroupId, new ConsumerPartitionState(partitionGroupId, getCurrentOffset(),
        getLastConsumedTimestamp(), fetchLatestStreamOffset(5_000), _lastRowMetadata));
  }

  /**
   * Returns the {@link PartitionLagState} for the partition group.
   */
  public Map<String, PartitionLagState> getPartitionToLagState(
      Map<String, ConsumerPartitionState> consumerPartitionStateMap) {
    if (_partitionMetadataProvider == null) {
      createPartitionMetadataProvider("Get Partition Lag State");
    }
    return _partitionMetadataProvider.getCurrentPartitionLagState(consumerPartitionStateMap);
  }

  /**
   * Checks and reports if the consumer is going through data loss.
   *
   * @param messageBatch Message batch to validate
   */
  private void reportDataLoss(MessageBatch messageBatch) {
    if (messageBatch.hasDataLoss()) {
      _serverMetrics.setValueOfTableGauge(_tableStreamName, ServerGauge.STREAM_DATA_LOSS, 1L);
      String message = "Message loss detected in stream partition: " + _partitionGroupId + " for table: "
          + _tableNameWithType + " startOffset: " + _currentOffset + " batchFirstOffset: "
          + messageBatch.getFirstMessageOffset();
      _segmentLogger.error(message);
      _realtimeTableDataManager.addSegmentError(_segmentNameStr, new SegmentErrorInfo(now(), message, null));
    }
  }

  public StreamPartitionMsgOffset getCurrentOffset() {
    return _currentOffset;
  }

  @Nullable
  public StreamPartitionMsgOffset getLatestStreamOffsetAtStartupTime() {
    return _latestStreamOffsetAtStartupTime;
  }

  @VisibleForTesting
  @Nullable
  SegmentBuildDescriptor getSegmentBuildDescriptor() {
    return _segmentBuildDescriptor;
  }

  @VisibleForTesting
  @Nullable
  protected SegmentBuildDescriptor buildSegmentInternal(boolean forCommit)
      throws SegmentBuildFailureException {
    if (_parallelSegmentConsumptionPolicy.isAllowedDuringBuild()) {
      closeStreamConsumer();
    }
    // Do not allow building segment when table data manager is already shut down
    if (_realtimeTableDataManager.isShutDown()) {
      _segmentLogger.warn("Table data manager is already shut down");
      return null;
    }
    final long startTimeMillis = now();
    try {
      if (_segBuildSemaphore != null) {
        _segmentLogger.info("Trying to acquire semaphore for building segment");
        Instant acquireStart = Instant.now();
        int timeoutSeconds = 5;
        while (!_segBuildSemaphore.tryAcquire(timeoutSeconds, TimeUnit.SECONDS)) {
          _segmentLogger.warn("Could not acquire semaphore for building segment in {}",
              Duration.between(acquireStart, Instant.now()));
          timeoutSeconds = Math.min(timeoutSeconds * 2, 300);
        }
        _segmentLogger.info("Acquired semaphore for building segment");
      }
    } catch (InterruptedException e) {
      String errorMessage = "Interrupted while waiting for semaphore";
      reportSegmentBuildFailure(errorMessage, e);
      throw new SegmentBuildFailureException(errorMessage, e);
    }
    try {
      // Increment llc simultaneous segment builds.
      _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, 1L);

      final long lockAcquireTimeMillis = now();
      // Build a segment from in-memory rows.
      // If build compressed archive is true, then build the tar.compressed file as well
      // TODO Use an auto-closeable object to delete temp resources.
      File tempSegmentFolder = new File(_resourceTmpDir, "tmp-" + _segmentNameStr + "-" + now());

      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset(_segmentZKMetadata.getStartOffset());
      segmentZKPropsConfig.setEndOffset(_currentOffset.toString());
      // let's convert the segment now
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(_realtimeSegment, segmentZKPropsConfig, tempSegmentFolder.getAbsolutePath(),
              _schema, _tableNameWithType, _tableConfig, _segmentZKMetadata.getSegmentName(),
              _defaultNullHandlingEnabled);
      _segmentLogger.info("Trying to build segment");
      try {
        converter.build(_segmentVersion, _serverMetrics);
      } catch (Exception e) {
        String errorMessage = "Could not build segment";
        FileUtils.deleteQuietly(tempSegmentFolder);
        if (e instanceof IllegalStateException) {
          // Precondition checks fail, the segment build would fail consistently
          _segmentBuildFailedWithDeterministicError = true;
        }
        reportSegmentBuildFailure(errorMessage, e);
        throw new SegmentBuildFailureException(errorMessage, e);
      }
      final long buildTimeMillis = now() - lockAcquireTimeMillis;
      final long waitTimeMillis = lockAcquireTimeMillis - startTimeMillis;
      _segmentLogger.info("Successfully built segment (Column Mode: {}) in {} ms, after lockWaitTime {} ms",
          converter.isColumnMajorEnabled(), buildTimeMillis, waitTimeMillis);

      File dataDir = new File(_resourceDataDir);
      File indexDir = new File(dataDir, _segmentNameStr);
      FileUtils.deleteQuietly(indexDir);

      File[] tempFiles = tempSegmentFolder.listFiles();
      assert tempFiles != null;
      File tempIndexDir = tempFiles[0];
      try {
        FileUtils.moveDirectory(tempIndexDir, indexDir);
      } catch (IOException e) {
        String errorMessage = "Caught exception while moving index directory from: " + tempIndexDir + " to: "
            + indexDir;
        reportSegmentBuildFailure(errorMessage, e);
        throw new SegmentBuildFailureException(errorMessage, e);
      } finally {
        FileUtils.deleteQuietly(tempSegmentFolder);
      }

      long segmentSizeBytes = FileUtils.sizeOfDirectory(indexDir);
      _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
          TimeUnit.MILLISECONDS.toSeconds(buildTimeMillis));
      _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
          TimeUnit.MILLISECONDS.toSeconds(waitTimeMillis));

      if (forCommit) {
        File segmentTarFile = new File(dataDir, _segmentNameStr + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION);
        try {
          TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
        } catch (IOException e) {
          String errorMessage = "Caught exception while taring index directory from: " + indexDir + " to: "
              + segmentTarFile;
          reportSegmentBuildFailure(errorMessage, e);
          throw new SegmentBuildFailureException(errorMessage, e);
        }

        File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
        if (metadataFile == null) {
          String errorMessage = "Failed to find file: " + V1Constants.MetadataKeys.METADATA_FILE_NAME
              + " under index directory: " + indexDir;
          reportSegmentBuildFailure(errorMessage, null);
          throw new SegmentBuildFailureException(errorMessage);
        }
        File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
        if (creationMetaFile == null) {
          String errorMessage = "Failed to find file: " + V1Constants.SEGMENT_CREATION_META + " under index directory: "
              + indexDir;
          reportSegmentBuildFailure(errorMessage, null);
          throw new SegmentBuildFailureException(errorMessage);
        }
        Map<String, File> metadataFiles = new HashMap<>();
        metadataFiles.put(V1Constants.MetadataKeys.METADATA_FILE_NAME, metadataFile);
        metadataFiles.put(V1Constants.SEGMENT_CREATION_META, creationMetaFile);

        return new SegmentBuildDescriptor(segmentTarFile, metadataFiles, _currentOffset, buildTimeMillis,
            waitTimeMillis, segmentSizeBytes);
      } else {
        return new SegmentBuildDescriptor(null, null, _currentOffset, buildTimeMillis, waitTimeMillis,
            segmentSizeBytes);
      }
    } finally {
      if (_segBuildSemaphore != null) {
        _segmentLogger.info("Releasing semaphore for building segment");
        _segBuildSemaphore.release();
      }
      // Decrement llc simultaneous segment builds.
      _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, -1L);
    }
  }

  private void reportSegmentBuildFailure(String errorMessage, @Nullable Exception e) {
    _segmentLogger.error(errorMessage, e);
    _realtimeTableDataManager.addSegmentError(_segmentNameStr, new SegmentErrorInfo(now(), errorMessage, e));
    _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.SEGMENT_BUILD_FAILURE, 1);
  }

  @VisibleForTesting
  boolean commitSegment(String controllerVipUrl)
      throws Exception {
    File segmentTarFile = _segmentBuildDescriptor.getSegmentTarFile();
    Preconditions.checkState(segmentTarFile != null && segmentTarFile.exists(), "Segment tar file: %s does not exist",
        segmentTarFile);
    SegmentCompletionProtocol.Response commitResponse = commit(controllerVipUrl);
    if (commitResponse.getStatus() != SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS) {
      _segmentLogger.warn("Controller response was {} and not {}", commitResponse.getStatus(),
          SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
      return false;
    }
    _realtimeTableDataManager.replaceConsumingSegment(_segmentNameStr, _segmentZKMetadata);
    removeSegmentFile();
    return true;
  }

  @VisibleForTesting
  void notifySegmentBuildFailedWithDeterministicError() {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params.withSegmentName(_segmentNameStr).withStreamPartitionMsgOffset(_currentOffset.toString())
        .withNumRows(_numRowsConsumed).withInstanceId(_instanceId);
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }

    _protocolHandler.segmentCannotBuild(params);
  }

  @VisibleForTesting
  SegmentCompletionProtocol.Response commit(String controllerVipUrl) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params.withSegmentName(_segmentNameStr).withStreamPartitionMsgOffset(_currentOffset.toString())
        .withNumRows(_numRowsConsumed).withInstanceId(_instanceId).withReason(_stopReason)
        .withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }

    SegmentCommitter segmentCommitter;
    try {
      segmentCommitter = _segmentCommitterFactory.createSegmentCommitter(params, controllerVipUrl);
    } catch (URISyntaxException e) {
      _segmentLogger.error("Failed to create a segment committer: ", e);
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return segmentCommitter.commit(_segmentBuildDescriptor);
  }

  protected boolean buildSegmentAndReplace()
      throws Exception {
    SegmentBuildDescriptor descriptor;
    try {
      descriptor = buildSegmentInternal(false);
    } catch (Exception e) {
      return false;
    }
    if (descriptor == null) {
      return false;
    }
    _realtimeTableDataManager.replaceConsumingSegment(_segmentNameStr, _segmentZKMetadata);
    return true;
  }

  private void closeStreamConsumer() {
    if (_streamConsumerClosed.compareAndSet(false, true)) {
      closePartitionGroupConsumer();
      closePartitionMetadataProvider();
      releaseConsumerSemaphore();
      _transformPipeline.reportStats();
    }
  }

  private void closePartitionGroupConsumer() {
    try {
      _partitionGroupConsumer.close();
    } catch (Exception e) {
      _segmentLogger.warn("Could not close stream consumer", e);
    }
  }

  private void closePartitionMetadataProvider() {
    if (_partitionMetadataProvider != null) {
      try {
        _partitionMetadataProvider.close();
      } catch (Exception e) {
        _segmentLogger.warn("Could not close stream metadata provider", e);
      }
    }
  }

  private void releaseConsumerSemaphore() {
    if (_consumerSemaphoreAcquired.compareAndSet(true, false)) {
      _segmentLogger.info("Releasing consumer semaphore");
      _consumerCoordinator.release();
    }
  }

  /**
   * Cleans up the metrics that reflects the state of the realtime segment.
   * This step is essential as the instance may not be the target location for some of the partitions.
   * E.g. if the number of partitions increases, or a host swap is needed, the target location for some partitions
   * may change,
   * and the current host remains to run. In this case, the current server would still keep the state of the old
   * partitions,
   * which no longer resides in this host any more, thus causes false positive information to the metric system.
   */
  private void cleanupMetrics() {
    _serverMetrics.removeTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING);
    _serverMetrics.removeTableGauge(_clientId, ServerGauge.STREAM_DATA_LOSS);
    _serverMetrics.removeTableGauge(_clientId, ServerGauge.PAUSELESS_CONSUMPTION_ENABLED);
    _serverMetrics.removeTableMeter(_clientId, ServerMeter.SEGMENT_BUILD_FAILURE);
  }

  protected void hold()
      throws InterruptedException {
    Thread.sleep(SegmentCompletionProtocol.MAX_HOLD_TIME_MS);
  }

  private static class ConsumptionStopIndicator {
    final StreamPartitionMsgOffset _offset;
    final String _segmentName;
    final String _instanceId;
    final Logger _logger;
    final ServerSegmentCompletionProtocolHandler _protocolHandler;
    final String _reason;
    private ConsumptionStopIndicator(StreamPartitionMsgOffset offset, String segmentName, String instanceId,
        ServerSegmentCompletionProtocolHandler protocolHandler, String reason, Logger logger) {
      _offset = offset;
      _segmentName = segmentName;
      _instanceId = instanceId;
      _protocolHandler = protocolHandler;
      _logger = logger;
      _reason = reason;
    }

    SegmentCompletionProtocol.Response postSegmentStoppedConsuming() {
      SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
      params.withStreamPartitionMsgOffset(_offset.toString()).withReason(_reason).withSegmentName(_segmentName)
          .withInstanceId(_instanceId);

      SegmentCompletionProtocol.Response response = _protocolHandler.segmentStoppedConsuming(params);
      _logger.info("Got response {}", response.toJsonString());
      return response;
    }
  }

  // Inform the controller that the server had to stop consuming due to an error.
  protected void postStopConsumedMsg(String reason) {
    ConsumptionStopIndicator indicator = new ConsumptionStopIndicator(_currentOffset,
        _segmentNameStr, _instanceId, _protocolHandler, reason, _segmentLogger);
    do {
      SegmentCompletionProtocol.Response response = indicator.postSegmentStoppedConsuming();
      if (response.getStatus() == SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED) {
        break;
      }
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
      _segmentLogger.info("Retrying after response {}", response.toJsonString());
    } while (!_shouldStop);
  }

  protected SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
    // Post segmentConsumed to current leader.
    // Retry maybe once if leader is not found.
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withStreamPartitionMsgOffset(_currentOffset.toString()).withSegmentName(_segmentNameStr)
        .withReason(_stopReason).withNumRows(_numRowsConsumed).withInstanceId(_instanceId);
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }
    return _protocolHandler.segmentConsumed(params);
  }

  private void removeSegmentFile() {
    if (_segmentBuildDescriptor != null) {
      _segmentBuildDescriptor.deleteSegmentFile();
      _segmentBuildDescriptor = null;
    }
  }

  public void goOnlineFromConsuming(SegmentZKMetadata segmentZKMetadata)
      throws InterruptedException {
    _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    try {
      // Remove the segment file before we do anything else.
      removeSegmentFile();
      _leaseExtender.removeSegment(_segmentNameStr);
      StreamPartitionMsgOffset endOffset = _streamPartitionMsgOffsetFactory.create(segmentZKMetadata.getEndOffset());
      _segmentLogger.info("State: {}, transitioning from CONSUMING to ONLINE (startOffset: {}, endOffset: {})", _state,
          _startOffset, endOffset);
      stop();
      _segmentLogger.info("Consumer thread stopped in state {}", _state);

      switch (_state) {
        case COMMITTED:
        case RETAINED:
          // Nothing to do. we already built local segment and swapped it with in-memory data.
          _segmentLogger.info("State {}. Nothing to do", _state.toString());
          break;
        case DISCARDED:
        case ERROR:
          _segmentLogger.info("State {}. Downloading to replace", _state.toString());
          downloadSegmentAndReplace(segmentZKMetadata);
          break;
        case CATCHING_UP:
        case HOLDING:
        case INITIAL_CONSUMING:
          switch (_segmentCompletionMode) {
            case DOWNLOAD:
              _segmentLogger.info("State {}. CompletionMode {}. Downloading to replace", _state.toString(),
                  _segmentCompletionMode);
              downloadSegmentAndReplace(segmentZKMetadata);
              break;
            case DEFAULT:
              // Allow to catch up upto final offset, and then replace.
              if (_currentOffset.compareTo(endOffset) > 0) {
                // We moved ahead of the offset that is committed in ZK.
                _segmentLogger
                    .warn("Current offset {} ahead of the offset in zk {}. Downloading to replace", _currentOffset,
                        endOffset);
                downloadSegmentAndReplace(segmentZKMetadata);
              } else if (_currentOffset.compareTo(endOffset) == 0) {
                _segmentLogger
                    .info("Current offset {} matches offset in zk {}. Replacing segment", _currentOffset, endOffset);
                if (!buildSegmentAndReplace()) {
                  _segmentLogger.warn("Failed to build the segment: {} and replace. Downloading to replace",
                      _segmentNameStr);
                  downloadSegmentAndReplace(segmentZKMetadata);
                }
              } else {
                boolean success = false;
                // Since online helix transition for a segment can arrive before segment's consumer acquires the
                // semaphore, check _consumerSemaphoreAcquired before catching up.
                // This is to avoid consuming in parallel to another segment's consumer.
                if (_consumerSemaphoreAcquired.get()) {
                  _segmentLogger.info("Attempting to catch up from offset {} to {} ", _currentOffset, endOffset);
                  success = catchupToFinalOffset(endOffset,
                      TimeUnit.MILLISECONDS.convert(MAX_TIME_FOR_CONSUMING_TO_ONLINE_IN_SECONDS, TimeUnit.SECONDS));
                } else {
                  _segmentLogger.warn("Consumer semaphore was not acquired, Skipping catch up from offset {} to {} ",
                      _currentOffset, endOffset);
                }

                if (success) {
                  _segmentLogger.info("Caught up to offset {}", _currentOffset);
                  if (!buildSegmentAndReplace()) {
                    _segmentLogger.warn("Failed to build the segment: {} after catchup. Downloading to replace",
                        _segmentNameStr);
                    downloadSegmentAndReplace(segmentZKMetadata);
                  }
                } else {
                  _segmentLogger
                      .info("Could not catch up to offset (current = {}). Downloading to replace", _currentOffset);
                  downloadSegmentAndReplace(segmentZKMetadata);
                }
              }
              break;
            default:
              break;
          }
          break;
        default:
          _segmentLogger.info("Downloading to replace segment while in state {}", _state.toString());
          downloadSegmentAndReplace(segmentZKMetadata);
          break;
      }
    } catch (Exception e) {
      Utils.rethrowException(e);
    } finally {
      _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    }
  }

  protected void downloadSegmentAndReplace(SegmentZKMetadata segmentZKMetadata)
      throws Exception {
    if (_parallelSegmentConsumptionPolicy.isAllowedDuringDownload()) {
      closeStreamConsumer();
    }
    _realtimeTableDataManager.downloadAndReplaceConsumingSegment(segmentZKMetadata);
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  private boolean catchupToFinalOffset(StreamPartitionMsgOffset endOffset, long timeoutMs) {
    _finalOffset = endOffset;
    if (_consumeStartTime == -1) {
      _consumeStartTime = now();
    }
    _consumeEndTime = now() + timeoutMs;
    _state = State.CONSUMING_TO_ONLINE;
    _shouldStop = false;
    try {
      consumeLoop();
    } catch (Exception e) {
      // We will end up downloading the segment, so this is not a serious problem
      _segmentLogger.warn("Exception when catching up to final offset", e);
      return false;
    } finally {
      _serverMetrics.setValueOfTableGauge(_clientId, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    }
    if (_currentOffset.compareTo(endOffset) != 0) {
      // Timeout?
      _segmentLogger.warn("Could not consume up to {} (current offset {})", endOffset, _currentOffset);
      return false;
    }

    return true;
  }

  @Override
  public void doOffload() {
    try {
      stop();
    } catch (Exception e) {
      _segmentLogger.error("Caught exception while stopping the consumer thread", e);
    }
    closeStreamConsumer();
    cleanupMetrics();
    _realtimeSegment.offload();
  }

  @Override
  protected void doDestroy() {
    _realtimeSegment.destroy();
  }

  public void startConsumption() {
    _consumerThread = new Thread(new PartitionConsumer(), _segmentNameStr);
    _segmentLogger.info("Created new consumer thread {} for {}", _consumerThread, this);
    _consumerThread.start();
  }

  /**
   * Stop the consuming thread.
   *
   * This method is invoked in 2 places:
   * 1. By the Helix thread when handling the segment state transition from CONSUMING to ONLINE. When this method is
   *    invoked, Helix thread should already hold the segment lock, and the consumer thread is not building the segment.
   *    We can safely interrupt the consumer thread and wait for it to join.
   * 2. By either the Helix thread or consumer thread to offload the segment. In this case, we can also safely interrupt
   *    the consumer thread because there is no need to build the segment.
   */
  public void stop()
      throws InterruptedException {
    _shouldStop = true;
    if (Thread.currentThread() != _consumerThread && _consumerThread.isAlive()) {
      _segmentLogger.info("Interrupting the consumer thread and waiting for it to join");
      long startTimeMs = System.currentTimeMillis();
      _consumerThread.interrupt();
      _consumerThread.join(CONSUMER_THREAD_INTERRUPT_INTERVAL_MS);
      while (_consumerThread.isAlive()) {
        _segmentLogger.warn("Consumer thread is still alive after {}ms, interrupting again",
            System.currentTimeMillis() - startTimeMs);
        _consumerThread.interrupt();
        _consumerThread.join(CONSUMER_THREAD_INTERRUPT_INTERVAL_MS);
      }
      _segmentLogger.info("Consumer thread has been terminated after {}ms", System.currentTimeMillis() - startTimeMs);
    }
  }

  // Assume that this is called only on OFFLINE to CONSUMING transition.
  // If the transition is OFFLINE to ONLINE, the caller should have downloaded the segment and we don't reach here.
  public RealtimeSegmentDataManager(SegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
      RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, IndexLoadingConfig indexLoadingConfig,
      Schema schema, LLCSegmentName llcSegmentName, ConsumerCoordinator consumerCoordinator,
      ServerMetrics serverMetrics, @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      @Nullable PartitionDedupMetadataManager partitionDedupMetadataManager, BooleanSupplier isReadyToConsumeData)
      throws AttemptsExceededException, RetriableOperationException {
    _segBuildSemaphore = realtimeTableDataManager.getSegmentBuildSemaphore();
    _segmentZKMetadata = segmentZKMetadata;
    _tableConfig = tableConfig;
    _tableNameWithType = _tableConfig.getTableName();
    _realtimeTableDataManager = realtimeTableDataManager;
    _resourceDataDir = resourceDataDir;
    _schema = schema;
    _llcSegmentName = llcSegmentName;
    _consumerCoordinator = consumerCoordinator;
    _serverMetrics = serverMetrics;
    _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
    _partitionDedupMetadataManager = partitionDedupMetadataManager;
    _isReadyToConsumeData = isReadyToConsumeData;
    _segmentVersion = indexLoadingConfig.getSegmentVersion();
    _instanceId = _realtimeTableDataManager.getInstanceId();
    _leaseExtender = SegmentBuildTimeLeaseExtender.getLeaseExtender(_tableNameWithType);
    _protocolHandler = new ServerSegmentCompletionProtocolHandler(_serverMetrics, _tableNameWithType);
    CompletionConfig completionConfig = _tableConfig.getValidationConfig().getCompletionConfig();
    _segmentCompletionMode = completionConfig != null
        && CompletionMode.DOWNLOAD.toString().equalsIgnoreCase(completionConfig.getCompletionMode())
        ? CompletionMode.DOWNLOAD : CompletionMode.DEFAULT;
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    _trackFilteredMessageOffsets = ingestionConfig != null && ingestionConfig.getStreamIngestionConfig() != null
        && ingestionConfig.getStreamIngestionConfig().isTrackFilteredMessageOffsets();
    _parallelSegmentConsumptionPolicy = getParallelConsumptionPolicy();

    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    // TODO Validate configs
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    _partitionGroupId = llcSegmentName.getPartitionGroupId();
    List<Map<String, String>> streamConfigMaps = IngestionConfigUtils.getStreamConfigMaps(_tableConfig);
    int numStreams = streamConfigMaps.size();
    if (numStreams == 1) {
      // Single stream
      // NOTE: We skip partition id translation logic to handle cases where custom stream might return partition id
      // larger than 10000.
      _streamPartitionId = _partitionGroupId;
      _streamConfig = new StreamConfig(_tableNameWithType, streamConfigMaps.get(0));
    } else {
      // Multiple streams
      _streamPartitionId = IngestionConfigUtils.getStreamPartitionIdFromPinotPartitionId(_partitionGroupId);
      int index = IngestionConfigUtils.getStreamConfigIndexFromPinotPartitionId(_partitionGroupId);
      Preconditions.checkState(numStreams > index, "Cannot find stream config of index: %s for table: %s", index,
          _tableNameWithType);
      _streamConfig = new StreamConfig(_tableNameWithType, streamConfigMaps.get(index));
    }
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(_streamConfig);
    _streamPartitionMsgOffsetFactory = _streamConsumerFactory.createStreamMsgOffsetFactory();
    String streamTopic = _streamConfig.getTopicName();
    _segmentNameStr = _segmentZKMetadata.getSegmentName();
    _partitionGroupConsumptionStatus =
        new PartitionGroupConsumptionStatus(_partitionGroupId, llcSegmentName.getSequenceNumber(),
            _streamPartitionMsgOffsetFactory.create(_segmentZKMetadata.getStartOffset()),
            _segmentZKMetadata.getEndOffset() == null ? null
                : _streamPartitionMsgOffsetFactory.create(_segmentZKMetadata.getEndOffset()),
            _segmentZKMetadata.getStatus().toString());
    InstanceDataManagerConfig instanceDataManagerConfig = indexLoadingConfig.getInstanceDataManagerConfig();
    String clientIdSuffix =
        instanceDataManagerConfig != null ? instanceDataManagerConfig.getConsumerClientIdSuffix() : null;
    if (StringUtils.isNotBlank(clientIdSuffix)) {
      _clientId = _tableNameWithType + "-" + streamTopic + "-" + _streamPartitionId + "-" + clientIdSuffix;
    } else {
      _clientId = _tableNameWithType + "-" + streamTopic + "-" + _streamPartitionId;
    }
    _segmentLogger = LoggerFactory.getLogger(RealtimeSegmentDataManager.class.getName() + "_" + _segmentNameStr);
    _tableStreamName = _tableNameWithType + "_" + streamTopic;
    if (indexLoadingConfig.isRealtimeOffHeapAllocation() && !indexLoadingConfig.isDirectRealtimeOffHeapAllocation()) {
      _memoryManager =
          new MmapMemoryManager(_realtimeTableDataManager.getConsumerDir(), _segmentNameStr, _serverMetrics);
    } else {
      // For on-heap allocation, we still need a memory manager for forward index.
      // Dictionary will be allocated on heap.
      _memoryManager = new DirectMemoryManager(_segmentNameStr, _serverMetrics);
    }

    _partitionRateLimiter = RealtimeConsumptionRateManager.getInstance()
        .createRateLimiter(_streamConfig, _tableNameWithType, _serverMetrics, _clientId);
    _serverRateLimiter = RealtimeConsumptionRateManager.getInstance().getServerRateLimiter();

    // Read the max number of rows
    int segmentMaxRowCount = segmentZKMetadata.getSizeThresholdToFlushSegment();
    if (segmentMaxRowCount <= 0) {
      segmentMaxRowCount = _streamConfig.getFlushThresholdRows();
    }
    if (segmentMaxRowCount <= 0) {
      segmentMaxRowCount = StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS;
    }
    _segmentMaxRowCount = segmentMaxRowCount;

    _isOffHeap = indexLoadingConfig.isRealtimeOffHeapAllocation();
    _defaultNullHandlingEnabled = indexingConfig.isNullHandlingEnabled();

    // Start new realtime segment
    String consumerDir = realtimeTableDataManager.getConsumerDir();
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder = new RealtimeSegmentConfig.Builder(indexLoadingConfig)
        .setTableNameWithType(_tableNameWithType)
        .setSegmentName(_segmentNameStr)
        .setStreamName(streamTopic)
        .setSchema(_schema)
        .setTimeColumnName(timeColumnName)
        .setCapacity(_segmentMaxRowCount)
        .setAvgNumMultiValues(indexLoadingConfig.getRealtimeAvgMultiValueCount())
        .setSegmentZKMetadata(segmentZKMetadata)
        .setOffHeap(_isOffHeap)
        .setMemoryManager(_memoryManager)
        .setStatsHistory(realtimeTableDataManager.getStatsHistory())
        .setAggregateMetrics(indexingConfig.isAggregateMetrics())
        .setIngestionAggregationConfigs(IngestionConfigUtils.getAggregationConfigs(tableConfig))
        .setDefaultNullHandlingEnabled(_defaultNullHandlingEnabled)
        .setPartitionUpsertMetadataManager(partitionUpsertMetadataManager)
        .setPartitionDedupMetadataManager(partitionDedupMetadataManager)
        .setConsumerDir(consumerDir)
        .setTextIndexConfig(_tableConfig.getIndexingConfig().getMultiColumnTextIndexConfig());

    // Create message decoder
    Set<String> fieldsToRead = IngestionUtils.getFieldsForRecordExtractor(_tableConfig, _schema);
    RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f);
    AtomicReference<StreamDataDecoder> localStreamDataDecoder = new AtomicReference<>();
    try {
      retryPolicy.attempt(() -> {
        try {
          StreamMessageDecoder streamMessageDecoder = createMessageDecoder(fieldsToRead);
          localStreamDataDecoder.set(new StreamDataDecoderImpl(streamMessageDecoder));
          return true;
        } catch (Exception e) {
          _segmentLogger.warn("Failed to initialize the StreamMessageDecoder: ", e);
          return false;
        }
      });
    } catch (Exception e) {
      _realtimeTableDataManager.addSegmentError(_segmentNameStr,
          new SegmentErrorInfo(now(), "Failed to initialize the StreamMessageDecoder", e));
      throw e;
    }
    _streamDataDecoder = localStreamDataDecoder.get();

    try {
      _transformPipeline = new TransformPipeline(tableConfig, schema);
    } catch (Exception e) {
      _realtimeTableDataManager.addSegmentError(_segmentNameStr,
          new SegmentErrorInfo(now(), "Failed to initialize the TransformPipeline", e));
      throw e;
    }

    try {
      _startOffset = _partitionGroupConsumptionStatus.getStartOffset();
      _currentOffset = _streamPartitionMsgOffsetFactory.create(_startOffset);
      makeStreamConsumer("Starting");
      createPartitionMetadataProvider("Starting");
      setPartitionParameters(realtimeSegmentConfigBuilder, indexingConfig.getSegmentPartitionConfig());
      _realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), serverMetrics);
      _resourceTmpDir = new File(resourceDataDir, RESOURCE_TEMP_DIR_NAME);
      if (!_resourceTmpDir.exists()) {
        _resourceTmpDir.mkdirs();
      }
      _state = State.INITIAL_CONSUMING;
      _latestStreamOffsetAtStartupTime = fetchLatestStreamOffset(5000);
      setConsumeEndTime(segmentZKMetadata, now());
      _segmentCommitterFactory =
          new SegmentCommitterFactory(_segmentLogger, _protocolHandler, tableConfig, indexLoadingConfig, serverMetrics);
    } catch (Throwable t) {
      _realtimeTableDataManager.addSegmentError(_segmentNameStr, new SegmentErrorInfo(now(),
          "Failed to initialize segment data manager", t));
      _segmentLogger.warn(
          "Scheduling task to call controller to mark the segment as OFFLINE in Ideal State due"
           + " to initialization error: '{}'",
          t.getMessage());
      // Since we are going to throw exception from this thread (helix execution thread), the externalview
      // entry for this segment will be ERROR. We allow time for Helix to make this transition, and then
      // invoke controller API mark it OFFLINE in the idealstate.
      new Thread(() -> {
        // Allow 30s for Helix to mark currentstate and externalview to ERROR, because
        // we are about to receive an ERROR->OFFLINE state transition once we call
        // postSegmentStoppedConsuming() method.
        Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
        postStopConsumedMsg("Consuming segment initialization error");
      }).start();
      throw t;
    }
  }

  private ParallelSegmentConsumptionPolicy getParallelConsumptionPolicy() {
    IngestionConfig ingestionConfig = _tableConfig.getIngestionConfig();
    ParallelSegmentConsumptionPolicy parallelSegmentConsumptionPolicy = null;
    boolean pauseless = false;
    if (ingestionConfig != null && ingestionConfig.getStreamIngestionConfig() != null) {
      parallelSegmentConsumptionPolicy =
          ingestionConfig.getStreamIngestionConfig().getParallelSegmentConsumptionPolicy();
      pauseless = ingestionConfig.getStreamIngestionConfig().isPauselessConsumptionEnabled();
    }
    if (parallelSegmentConsumptionPolicy != null) {
      return parallelSegmentConsumptionPolicy;
    }
    // When the policy is not set:
    // - Without dedup or partial upsert, allow consumption during build and download
    // - With dedup or partial upsert, in order to get consistent behavior we need to ensure the metadata contains all
    //   previous records before starting consumption:
    //   - If the table config allows consumption during commit, allow consumption during build and download
    //   - For pauseless tables, allow consumption during build, but disallow consumption during download
    //   - For non-pauseless tables, disallow consumption during build and download
    //     TODO: Revisit the non-pauseless handling
    if (_partitionUpsertMetadataManager != null) {
      UpsertContext upsertContext = _partitionUpsertMetadataManager.getContext();
      if (upsertContext.isAllowPartialUpsertConsumptionDuringCommit()
          || upsertContext.getUpsertMode() != UpsertConfig.Mode.PARTIAL) {
        return ParallelSegmentConsumptionPolicy.ALLOW_ALWAYS;
      }
      return pauseless ? ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY
          : ParallelSegmentConsumptionPolicy.DISALLOW_ALWAYS;
    }
    if (_partitionDedupMetadataManager != null) {
      DedupContext dedupContext = _partitionDedupMetadataManager.getContext();
      if (dedupContext.isAllowDedupConsumptionDuringCommit()) {
        return ParallelSegmentConsumptionPolicy.ALLOW_ALWAYS;
      }
      return pauseless ? ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY
          : ParallelSegmentConsumptionPolicy.DISALLOW_ALWAYS;
    }
    return ParallelSegmentConsumptionPolicy.ALLOW_ALWAYS;
  }

  private void setConsumeEndTime(SegmentZKMetadata segmentZKMetadata, long now) {
    long maxConsumeTimeMillis = _streamConfig.getFlushThresholdTimeMillis();
    _consumeEndTime = segmentZKMetadata.getCreationTime() + maxConsumeTimeMillis;

    // When we restart a server, the consuming segments retain their creationTime (derived from segment
    // metadata), but a couple of corner cases can happen:
    // (1) The server was down for a very long time, and the consuming segment is not yet completed.
    // (2) The consuming segment was just about to be completed, but the server went down.
    // In either of these two cases, if a different replica could not complete the segment, it is possible
    // that we get a value for _consumeEndTime that is in the very near future, or even in the past. In such
    // cases, we let some minimum consumption happen before we attempt to complete the segment (unless, of course
    // the max consumption time has been configured to be less than the minimum time we use in this class).
    long minConsumeTimeMillis =
        Math.min(maxConsumeTimeMillis, TimeUnit.MILLISECONDS.convert(MINIMUM_CONSUME_TIME_MINUTES, TimeUnit.MINUTES));
    if (_consumeEndTime - now < minConsumeTimeMillis) {
      _consumeEndTime = now + minConsumeTimeMillis;
    }
  }

  public long getTimeSinceEventLastConsumedMs() {
    return _idleTimer.getTimeSinceEventLastConsumedMs();
  }

  @Nullable
  public StreamPartitionMsgOffset fetchLatestStreamOffset(long maxWaitTimeMs, boolean useDebugLog) {
    return fetchStreamOffset(OffsetCriteria.LARGEST_OFFSET_CRITERIA, maxWaitTimeMs, useDebugLog);
  }

  @Nullable
  public StreamPartitionMsgOffset fetchLatestStreamOffset(long maxWaitTimeMs) {
    return fetchLatestStreamOffset(maxWaitTimeMs, false);
  }

  @Nullable
  public StreamPartitionMsgOffset fetchEarliestStreamOffset(long maxWaitTimeMs, boolean useDebugLog) {
    return fetchStreamOffset(OffsetCriteria.SMALLEST_OFFSET_CRITERIA, maxWaitTimeMs, useDebugLog);
  }

  @Nullable
  public StreamPartitionMsgOffset fetchEarliestStreamOffset(long maxWaitTimeMs) {
    return fetchEarliestStreamOffset(maxWaitTimeMs, false);
  }

  @Nullable
  private StreamPartitionMsgOffset fetchStreamOffset(OffsetCriteria offsetCriteria, long maxWaitTimeMs,
      boolean useDebugLog) {
    if (_partitionMetadataProvider == null) {
      createPartitionMetadataProvider("Fetch latest stream offset");
    }
    try {
      return _partitionMetadataProvider.fetchStreamPartitionOffset(offsetCriteria, maxWaitTimeMs);
    } catch (Exception e) {
      String logMessage = "Cannot fetch stream offset with criteria " + offsetCriteria + " for clientId " + _clientId
          + " and partitionGroupId " + _partitionGroupId + " with maxWaitTime " + maxWaitTimeMs;
      if (!useDebugLog) {
        _segmentLogger.warn(logMessage, e);
      } else {
        _segmentLogger.debug(logMessage, e);
      }
    }
    return null;
  }

  /*
   * set the following partition parameters in RT segment config builder:
   *  - partition column
   *  - partition function
   *  - partition group id
   */
  private void setPartitionParameters(RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder,
      SegmentPartitionConfig segmentPartitionConfig) {
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      if (columnPartitionMap.size() == 1) {
        Map.Entry<String, ColumnPartitionConfig> entry = columnPartitionMap.entrySet().iterator().next();
        String partitionColumn = entry.getKey();
        ColumnPartitionConfig columnPartitionConfig = entry.getValue();
        String partitionFunctionName = columnPartitionConfig.getFunctionName();

        // NOTE: Here we compare the number of partitions from the config and the stream, and log a warning and emit a
        //       metric when they don't match, but use the one from the stream. The mismatch could happen when the
        //       stream partitions are changed, but the table config has not been updated to reflect the change.
        //       In such case, picking the number of partitions from the stream can keep the segment properly
        //       partitioned as long as the partition function is not changed.
        int numPartitions = columnPartitionConfig.getNumPartitions();
        try {
          // TODO: currentPartitionGroupConsumptionStatus should be fetched from idealState + segmentZkMetadata,
          //  so that we get back accurate partitionGroups info
          //  However this is not an issue for Kafka, since partitionGroups never expire and every partitionGroup has
          //  a single partition
          //  Fix this before opening support for partitioning in Kinesis
          int numPartitionGroups = _partitionMetadataProvider.computePartitionGroupMetadata(_clientId, _streamConfig,
              Collections.emptyList(), /*maxWaitTimeMs=*/15000).size();

          if (numPartitionGroups != numPartitions) {
            _segmentLogger.info(
                "Number of stream partitions: {} does not match number of partitions in the partition config: {}, "
                    + "using number of stream " + "partitions", numPartitionGroups, numPartitions);
            numPartitions = numPartitionGroups;
          }
        } catch (Exception e) {
          _segmentLogger.warn("Failed to get number of stream partitions in 5s, "
              + "using number of partitions in the partition config: {}", numPartitions, e);
          createPartitionMetadataProvider("Timeout getting number of stream partitions");
        }

        realtimeSegmentConfigBuilder.setPartitionColumn(partitionColumn);
        realtimeSegmentConfigBuilder.setPartitionFunction(
            PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions, null));
        realtimeSegmentConfigBuilder.setPartitionId(_partitionGroupId);
      } else {
        _segmentLogger.warn("Cannot partition on multiple columns: {}", columnPartitionMap.keySet());
      }
    }
  }

  /**
   * Creates a new stream consumer
   */
  private void makeStreamConsumer(String reason) {
    if (_partitionGroupConsumer != null) {
      closePartitionGroupConsumer();
    }
    _segmentLogger.info("Creating new stream consumer for topic partition {} , reason: {}", _clientId, reason);
    try {
      _partitionGroupConsumer =
          _streamConsumerFactory.createPartitionGroupConsumer(_clientId, _partitionGroupConsumptionStatus);
      _partitionGroupConsumer.start(_currentOffset);
    } catch (Exception e) {
      _segmentLogger.error("Faced exception while trying to recreate stream consumer for topic partition {} reason {}",
          _clientId, reason, e);
      _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, 1L);
      throw e;
    }
  }

  /**
   * Checkpoints existing consumer before creating a new consumer instance
   * Assumes there is a valid instance of {@link PartitionGroupConsumer}
   */
  private void recreateStreamConsumer(String reason) {
    _segmentLogger.info("Recreating stream consumer for topic partition {}, reason: {}", _clientId, reason);
    _currentOffset = _partitionGroupConsumer.checkpoint(_currentOffset);
    closePartitionGroupConsumer();
    try {
      _partitionGroupConsumer =
          _streamConsumerFactory.createPartitionGroupConsumer(_clientId, _partitionGroupConsumptionStatus);
      _partitionGroupConsumer.start(_currentOffset);
    } catch (Exception e) {
      _segmentLogger.error("Faced exception while trying to recreate stream consumer for topic partition {}", _clientId,
          e);
      _serverMetrics.addMeteredTableValue(_clientId, ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, 1L);
      throw e;
    }
  }

  /**
   * Creates a new stream metadata provider
   */
  private void createPartitionMetadataProvider(String reason) {
    closePartitionMetadataProvider();
    _segmentLogger.info("Creating new partition metadata provider, reason: {}", reason);
    _partitionMetadataProvider =
        _streamConsumerFactory.createPartitionMetadataProvider(_clientId, _streamPartitionId);
  }

  private void updateIngestionMetrics(StreamMessageMetadata metadata) {
    if (metadata != null) {
      try {
        StreamPartitionMsgOffset latestOffset = fetchLatestStreamOffset(5000, true);
        _realtimeTableDataManager.updateIngestionMetrics(_segmentNameStr, _partitionGroupId,
            metadata.getRecordIngestionTimeMs(), metadata.getFirstStreamRecordIngestionTimeMs(), metadata.getOffset(),
            latestOffset);
      } catch (Exception e) {
        _segmentLogger.warn("Failed to fetch latest offset for updating ingestion delay", e);
      }
    }
  }

  /**
   * Sets ingestion delay to zero in situations where we are caught up processing events.
   * TODO: Revisit if we should preserve the offset info.
   */
  private void setIngestionDelayToZero() {
    long currentTimeMs = System.currentTimeMillis();
    _realtimeTableDataManager.updateIngestionMetrics(_segmentNameStr, _partitionGroupId, currentTimeMs, currentTimeMs,
        null, null);
  }

  // This should be done during commit? We may not always commit when we build a segment....
  // TODO Call this method when we are loading the segment, which we do from table datamanager afaik
  private void updateCurrentDocumentCountMetrics() {

    // When updating of metrics is enabled, numRowsIndexed can be <= numRowsConsumed. This is because when in this
    // case when a new row with existing dimension combination comes in, we find the existing row and update metrics.

    // Number of rows indexed should be used for DOCUMENT_COUNT metric, and also for segment flush. Whereas,
    // Number of rows consumed should be used for consumption metric.
    long rowsIndexed = _numRowsIndexed - _lastUpdatedRowsIndexed.get();
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT, rowsIndexed);
    _lastUpdatedRowsIndexed.set(_numRowsIndexed);
    final long now = now();
    final int rowsConsumed = _numRowsConsumed - _lastConsumedCount;
    final long prevTime = _lastLogTime == 0 ? _consumeStartTime : _lastLogTime;
    // Log every minute or 100k events
    if (now - prevTime > TimeUnit.MINUTES.toMillis(TIME_THRESHOLD_FOR_LOG_MINUTES)
        || rowsConsumed >= MSG_COUNT_THRESHOLD_FOR_LOG) {
      // multiply by 1000 to get events/sec. now and prevTime are in milliseconds.
      float consumedRate = ((float) rowsConsumed) * 1000 / (now - prevTime);
      _segmentLogger.info(
          "Consumed {} events from (rate:{}/s), currentOffset={}, numRowsConsumedSoFar={}, numRowsIndexedSoFar={}",
          rowsConsumed, consumedRate, _currentOffset, _numRowsConsumed, _numRowsIndexed);
      if (_filteredMessageOffsets.size() > 0) {
        if (_trackFilteredMessageOffsets) {
          _segmentLogger.info("Filtered events with offsets: {}", _filteredMessageOffsets);
        }
        _filteredMessageOffsets.clear();
      }
      _lastConsumedCount = _numRowsConsumed;
      _lastLogTime = now;
    }
  }

  /**
   * Creates a {@link StreamMessageDecoder} using properties in {@link StreamConfig}.
   *
   * @param fieldsToRead The fields to read from the source stream
   * @return The initialized StreamMessageDecoder
   */
  private StreamMessageDecoder createMessageDecoder(Set<String> fieldsToRead) {
    String decoderClass = _streamConfig.getDecoderClass();
    try {
      StreamMessageDecoder decoder = PluginManager.get().createInstance(decoderClass);
      decoder.init(fieldsToRead, _streamConfig, _tableConfig, _schema);
      return decoder;
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while creating StreamMessageDecoder from stream config: " + _streamConfig, e);
    }
  }

  @Override
  public MutableSegment getSegment() {
    return _realtimeSegment;
  }

  @Override
  public String getSegmentName() {
    return _segmentNameStr;
  }

  public void forceCommit() {
    _forceCommitMessageReceived = true;
  }

  @VisibleForTesting
  AtomicBoolean getConsumerSemaphoreAcquired() {
    return _consumerSemaphoreAcquired;
  }
}
