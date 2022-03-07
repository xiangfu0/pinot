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
package org.apache.pinot.segment.local.segment.creator.impl.inv;

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.segment.spi.index.reader.TimestampIndexGranularity;
import org.apache.pinot.spi.data.FieldSpec;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
import static org.apache.pinot.segment.spi.V1Constants.Indexes.TIMESTAMP_INDEX_FILE_EXTENSION;


/**
 * Timestamp index creator that uses off-heap memory.
 * <p>We reuse range index creator for timestamp index.
 * <ul>
 *   <li>
 *     In the first pass (adding values phase), when add() method is called, store the raw values into the value buffer
 *     (for multi-valued column we flatten the values). We also store the corresponding docId in docIdBuffer which will
 *     be sorted in the next phase based on the value in valueBuffer.
 *   </li>
 *   <li>
 *     In the second pass (processing values phase), when seal() method is called, we sort the docIdBuffer based on the
 *     value in valueBuffer. We then iterate over the sorted docIdBuffer and create ranges such that each range
 *     comprises of _numDocsPerRange.
 *   </li>
 * </ul>
 */
public class TimestampIndexCreator implements org.apache.pinot.segment.spi.index.creator.TimestampIndexCreator {
  public static final int VERSION = 1;

  private final File _indexDir;
  private final String _columnName;
  private final File _timestampIndexFile;
  private final int _numDocs;
  private final Set<TimestampIndexGranularity> _granularities;
  private final Map<TimestampIndexGranularity, RangeIndexCreator> _rangeIndexCreatorMap = new HashMap<>();

  public TimestampIndexCreator(File indexDir, FieldSpec fieldSpec, Set<TimestampIndexGranularity> granularities,
      int numDocs)
      throws IOException {
    Preconditions.checkArgument(fieldSpec.getDataType() == FieldSpec.DataType.TIMESTAMP);
    _indexDir = indexDir;
    _columnName = fieldSpec.getName();
    _timestampIndexFile = new File(indexDir, _columnName + TIMESTAMP_INDEX_FILE_EXTENSION);
    _numDocs = numDocs;
    _granularities = granularities;
    for (TimestampIndexGranularity granularity : _granularities) {
      _rangeIndexCreatorMap.put(granularity,
          new RangeIndexCreator(indexDir, _columnName + "." + granularity, true, FieldSpec.DataType.LONG, -1, -1,
              _numDocs, _numDocs));
    }
  }

  @Override
  public void add(Long value)
      throws IOException {
    for (TimestampIndexGranularity granularity : _granularities) {
      _rangeIndexCreatorMap.get(granularity).add(DateTimeFunctions.dateTrunc(granularity.toString(), value));
    }
  }

  @Override
  public void seal()
      throws IOException {
    for (TimestampIndexGranularity granularity : _granularities) {
      _rangeIndexCreatorMap.get(granularity).seal();
    }
    // TIMESTAMP INDEX FILE LAYOUT
    //HEADER
    //   # VERSION (INT)
    //   # Number OF GRANULARITIES (INT)
    //   # N GRANULARITIES SIZE
    //   # N RANGE INDEX FILE OFFSET
    //BODY
    //   # N RANGE INDEX
    long bytesWritten = 0;
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(_timestampIndexFile));
        DataOutputStream header = new DataOutputStream(bos);
        FileOutputStream fos = new FileOutputStream(_timestampIndexFile);
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(fos))) {
      int numGranularities = _rangeIndexCreatorMap.size();
      // Version
      header.writeInt(VERSION);
      bytesWritten += Integer.BYTES;
      // Number of granularities
      header.writeInt(numGranularities);
      bytesWritten += Integer.BYTES;
      List<TimestampIndexGranularity> granularityList = new ArrayList<>(_rangeIndexCreatorMap.keySet());
      // Granularity enum list
      for (TimestampIndexGranularity granularity : granularityList) {
        header.writeInt(TimestampIndexGranularity.indexOf(granularity));
      }
      bytesWritten += Integer.BYTES * numGranularities;
      // Range index file length
      for (TimestampIndexGranularity granularity : granularityList) {
        File rangeIndexFile = new File(_indexDir, _columnName + "." + granularity + BITMAP_RANGE_INDEX_FILE_EXTENSION);
        header.writeLong(rangeIndexFile.length());
      }
      bytesWritten += Long.BYTES * numGranularities;
      fos.getChannel().position(bytesWritten);
      // Range index file length
      for (TimestampIndexGranularity granularity : granularityList) {
        File rangeIndexFile = new File(_indexDir, _columnName + "." + granularity + BITMAP_RANGE_INDEX_FILE_EXTENSION);
        long bytesCopied = IOUtils.copyLarge(new FileInputStream(rangeIndexFile), fos);
        bytesWritten += bytesCopied;
      }
    } catch (IOException e) {
      FileUtils.deleteQuietly(_timestampIndexFile);
      throw e;
    }
    Preconditions.checkState(bytesWritten == _timestampIndexFile.length(),
        "Length of timestamp index file: " + _timestampIndexFile.length()
            + " does not match the number of bytes written: " + bytesWritten);
  }

  @Override
  public void close()
      throws IOException {
    for (TimestampIndexGranularity granularity : _granularities) {
      _rangeIndexCreatorMap.get(granularity).close();
    }
  }
}
