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
package org.apache.pinot.segment.local.segment.creator.impl.inv.json;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.GenericHeader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;


/**
 * Base implementation of the json index creator.
 * <p>Header format:
 * <ul>
 *   <li>Version (int) 4</li>
 *   <li>header size (int) 4</li>
 *   <li>Max value length (int) 4</li>
 *   <li>Dictionary file length (long) 8</li>
 *   <li>Inverted index file length (long) 8</li>
 *   <li>Range index dictionary file length (long) 8</li>
 * </ul>
 */
public abstract class BaseJsonIndexCreator implements JsonIndexCreator {
  // NOTE: V1 is deprecated because it does not support top-level value, top-level array and nested array
  public static final int VERSION_1 = 1;
  public static final int VERSION_2 = 2;
  public static final int VERSION_3 = 3;
  public static final int HEADER_LENGTH = 36;

  static final String TEMP_DIR_SUFFIX = ".json.idx.tmp";
  static final String DICTIONARY_FILE_NAME = "dictionary.buf";
  static final String INVERTED_INDEX_FILE_NAME = "inverted.index.buf";

  final JsonIndexConfig _jsonIndexConfig;
  final File _indexFile;
  final File _tempDir;
  final File _dictionaryFile;
  final File _invertedIndexFile;
  final IntList _numFlattenedRecordsList = new IntArrayList();
  final Map<String, RoaringBitmapWriter<RoaringBitmap>> _postingListMap = new TreeMap<>();
  final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard = RoaringBitmapWriter.writer();

  int _nextFlattenedDocId;
  int _maxValueLength;
  private Map<String, DataFileWriterReader> _rangeIndexValueWriterReaderMap;

  BaseJsonIndexCreator(File indexDir, String columnName, JsonIndexConfig jsonIndexConfig)
      throws IOException {
    _jsonIndexConfig = jsonIndexConfig;
    _indexFile = new File(indexDir, columnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    _tempDir = new File(indexDir, columnName + TEMP_DIR_SUFFIX);
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    } else {
      FileUtils.forceMkdir(_tempDir);
    }
    _dictionaryFile = new File(_tempDir, DICTIONARY_FILE_NAME);
    _invertedIndexFile = new File(_tempDir, INVERTED_INDEX_FILE_NAME);
    List<Map<String, String>> rangeIndexConfigs = jsonIndexConfig.getRangeIndexConfigs();
    if (rangeIndexConfigs != null && !rangeIndexConfigs.isEmpty()) {
      _rangeIndexValueWriterReaderMap = new HashMap<>();
      for (Map<String, String> rangeIndexConfig : rangeIndexConfigs) {
        String path = rangeIndexConfig.get("path");
        String name = rangeIndexConfig.get("name");
        FieldSpec.DataType dataType = FieldSpec.DataType.valueOf(rangeIndexConfig.get("dataType").toUpperCase());
        boolean useDictionary = Boolean.valueOf(rangeIndexConfig.get("createDictionary"));
        String filePath = _tempDir + "/" + name + ".raw";
        _rangeIndexValueWriterReaderMap.put(path.replace("$[*]", "."),
            new DataFileWriterReader(filePath, dataType, useDictionary));
      }
    }
  }

  @Override
  public void add(String jsonString)
      throws IOException {
    addFlattenedRecords(JsonUtils.flatten(jsonString, _jsonIndexConfig));
  }

  /**
   * Adds the flattened records for the next document.
   */
  void addFlattenedRecords(List<Map<String, String>> records)
      throws IOException {
    int numRecords = records.size();
    Preconditions.checkState(_nextFlattenedDocId + numRecords >= 0, "Got more than %s flattened records",
        Integer.MAX_VALUE);
    _numFlattenedRecordsList.add(numRecords);
    for (Map<String, String> record : records) {
      for (Map.Entry<String, String> entry : record.entrySet()) {
        // Put both key and key-value into the posting list. Key is useful for checking if a key exists in the json.
        String key = entry.getKey();
        addToPostingList(key);
        String keyValue = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + entry.getValue();
        addToPostingList(keyValue);
      }
      //add range index
      if (_rangeIndexValueWriterReaderMap != null) {
        for (Map.Entry<String, DataFileWriterReader> entry : _rangeIndexValueWriterReaderMap.entrySet()) {
          String key = entry.getKey();
          if (record.containsKey(key)) {
            entry.getValue().write(record.get(key));
          } else {
            entry.getValue().write(0L);
          }
        }
      }
      _nextFlattenedDocId++;
    }
  }

  /**
   * Adds the given value to the posting list.
   */
  void addToPostingList(String value) {
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _postingListMap.get(value);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _postingListMap.put(value, bitmapWriter);
    }
    bitmapWriter.add(_nextFlattenedDocId);
  }

  /**
   * Generates the index file based on _maxValueLength, _dictionaryFile, _invertedIndexFile, _numFlattenedRecordsList,
   * _nextFlattenedDocId.
   */
  void generateIndexFile()
      throws IOException {

    GenericHeader rangeIndexHeader = new GenericHeader();
    List<File> files = new ArrayList<>();

    //generate range index
    if (_jsonIndexConfig.getRangeIndexConfigs() != null) {
      for (Map<String, String> rangeIndexConfig : _jsonIndexConfig.getRangeIndexConfigs()) {
        String path = rangeIndexConfig.get("path");
        String name = rangeIndexConfig.get("name");
        //this is what the jsonutil.flatten returns
        String jsonKey = path.replace("$[*]", ".");
        DataFileWriterReader writerReader = _rangeIndexValueWriterReaderMap.get(jsonKey);
        if (writerReader != null) {
          FieldSpec.DataType dataType = FieldSpec.DataType.valueOf(rangeIndexConfig.get("dataType"));
          boolean useDictionary = Boolean.valueOf(rangeIndexConfig.get("createDictionary"));
          if (useDictionary) {
            writerReader.closeAndPrepareForRead();
            DimensionFieldSpec fieldSpec = new DimensionFieldSpec(name, dataType, true);
            if (dataType == FieldSpec.DataType.LONG) {
              File rangeDictFile = new File(_tempDir, name + DICTIONARY_FILE_NAME);
              File rangeIndexFile = new File(_tempDir, name + BITMAP_RANGE_INDEX_FILE_EXTENSION);
              SegmentDictionaryCreator dictionaryCreator =
                  new SegmentDictionaryCreator(name, dataType, rangeDictFile, false);
              BitSlicedRangeIndexCreator creator =
                  new BitSlicedRangeIndexCreator(_tempDir, fieldSpec, writerReader._dictionarySet.size());

              long[] values = new long[writerReader._dictionarySet.size()];
              int index = 0;
              for (Object val : writerReader._dictionarySet) {
                values[index] = ((Number) val).longValue();
                index = index + 1;
              }
              Arrays.sort(values);
              dictionaryCreator.build(values);
              dictionaryCreator.close();
              Long2IntOpenHashMap valueToDictIdMap = new Long2IntOpenHashMap(values.length);
              for (int i = 0; i < values.length; i++) {
                long value = values[i];
                valueToDictIdMap.put(value, i);
              }
              while (writerReader.hasNext()) {
                long k = writerReader.readLong();
                creator.add(valueToDictIdMap.get(k));
              }
              creator.seal();
              creator.close();

              GenericHeader.SubHeader subHeader = new GenericHeader.SubHeader();
              subHeader.addString("name", name);
              subHeader.addString("path", path);
              subHeader.addString("dataType", dataType.toString());
              files.add(rangeDictFile);
              subHeader.addLong("cardinality", values.length);
              subHeader.addLong("dictionaryFileLength", rangeDictFile.length());
              files.add(rangeIndexFile);
              subHeader.addLong("rangeIndexFileLength", rangeIndexFile.length());
              rangeIndexHeader.addSubHeader(subHeader);
            }
          } else {
            //TODO:
          }
        }
      }
    }

    int version = VERSION_3;

    long dictionaryFileLength = _dictionaryFile.length();
    long invertedIndexFileLength = _invertedIndexFile.length();
    int numDocs = _numFlattenedRecordsList.size();

    // Write the doc id mapping to the index file
    File docIdMappingFile = new File(_tempDir, "docIdMapping.bin");
    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(docIdMappingFile, true)));
    if (version == VERSION_3) {
      int currentFlattenedDocId = 0;
      for (int i = 0; i < numDocs; i++) {
        int numRecords = _numFlattenedRecordsList.getInt(i);
        dos.writeInt(currentFlattenedDocId);
        currentFlattenedDocId = currentFlattenedDocId + numRecords;
      }
      // Put last end offset
      dos.writeInt(currentFlattenedDocId);
    } else {
      for (int i = 0; i < numDocs; i++) {
        int numRecords = _numFlattenedRecordsList.getInt(i);
        for (int j = 0; j < numRecords; j++) {
          dos.writeInt(i);
        }
      }
    }
    dos.close();
    long docIdMappingFileLength = docIdMappingFile.length();
    int headerCapacity = HEADER_LENGTH + 4 + rangeIndexHeader.getEstimatedSize();
    ByteBuffer headerBuffer = ByteBuffer.allocate(headerCapacity);
    headerBuffer.putInt(version);
    headerBuffer.putInt(headerCapacity);
    headerBuffer.putInt(_maxValueLength);
    headerBuffer.putLong(dictionaryFileLength);
    headerBuffer.putLong(invertedIndexFileLength);
    headerBuffer.putLong(docIdMappingFileLength);
    byte[] rangeIndexHeaderBuffer = rangeIndexHeader.toBuffer();
    headerBuffer.putInt(rangeIndexHeaderBuffer.length);
    headerBuffer.put(rangeIndexHeaderBuffer);
    headerBuffer.position(0);

    try (FileChannel indexFileChannel = new RandomAccessFile(_indexFile, "rw").getChannel();
        FileChannel dictionaryFileChannel = new RandomAccessFile(_dictionaryFile, "r").getChannel();
        FileChannel invertedIndexFileChannel = new RandomAccessFile(_invertedIndexFile, "r").getChannel();
        FileChannel docIdMappingFileChannel = new RandomAccessFile(docIdMappingFile, "r").getChannel()) {
      indexFileChannel.write(headerBuffer);
      org.apache.pinot.common.utils.FileUtils.transferBytes(dictionaryFileChannel, 0, dictionaryFileLength,
          indexFileChannel);

      org.apache.pinot.common.utils.FileUtils.transferBytes(invertedIndexFileChannel, 0, invertedIndexFileLength,
          indexFileChannel);

      org.apache.pinot.common.utils.FileUtils.transferBytes(docIdMappingFileChannel, 0, docIdMappingFileLength,
          indexFileChannel);
      for (File file : files) {
        try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();) {
          org.apache.pinot.common.utils.FileUtils.transferBytes(fileChannel, 0, file.length(),
              indexFileChannel);
        }
      }
      indexFileChannel.force(true);
    }
  }

  @Override
  public void close()
      throws IOException {

    FileUtils.deleteDirectory(_tempDir);
  }

  /**
   *  TODO: use existing forward index reader instead
   */
  public static class DataFileWriterReader implements AutoCloseable {
    private final RandomAccessFile _raf;
    private final boolean _dictionary;
    private final FileChannel _fileChannel;
    private final BufferedWriter _writer;
    private BufferedReader _reader;
    private final ByteBuffer _buffer;
    private final FieldSpec.DataType _dataType;
    Set _dictionarySet = new HashSet<>();

    /**
     * Constructor for reading/writing
     */
    public DataFileWriterReader(String filePath, FieldSpec.DataType dataType, boolean dictionary)
        throws IOException {
      _dataType = dataType;
      _raf = new RandomAccessFile(filePath, "rw");
      _dictionary = dictionary;
      _fileChannel = _raf.getChannel();
      _buffer =
          (dataType != FieldSpec.DataType.STRING) ? ByteBuffer.allocate(dataType.getStoredType().size()) : null;
      _writer = new BufferedWriter(new FileWriter(filePath, true));
      _reader = new BufferedReader(new FileReader(filePath));
    }

    /**
     * Append an integer
     */
    public void write(int value)
        throws IOException {
      _buffer.clear();
      _buffer.putInt(value);
      _buffer.flip();
      _fileChannel.write(_buffer);
    }

    /**
     * Append a long
     */
    public void write(long value)
        throws IOException {
      if (_dictionary && !_dictionarySet.contains(value)) {
        _dictionarySet.add(value);
      }
      _buffer.clear();
      _buffer.putLong(value);
      _buffer.flip();
      _fileChannel.write(_buffer);
    }

    /**
     * Append a float
     */
    public void write(float value)
        throws IOException {
      if (_dictionary && !_dictionarySet.contains(value)) {
        _dictionarySet.add(value);
      }
      _buffer.clear();
      _buffer.putFloat(value);
      _buffer.flip();
      _fileChannel.write(_buffer);
    }

    /**
     * Append a double
     */
    public void write(double value)
        throws IOException {
      if (_dictionary && !_dictionarySet.contains(value)) {
        _dictionarySet.add(value);
      }
      _buffer.clear();
      _buffer.putDouble(value);
      _buffer.flip();
      _fileChannel.write(_buffer);
    }

    /**
     * Append a string
     */
    public void write(String value)
        throws IOException {
      if (_dataType == FieldSpec.DataType.STRING) {
        if (_dictionary && !_dictionarySet.contains(value)) {
          _dictionarySet.add(value);
        }
        _writer.write(value);
        _writer.newLine();
        _writer.flush();
      } else {
        if (_dataType == FieldSpec.DataType.LONG) {
          write(Long.parseLong(value));
        }
      }
    }

    /**
     * Reset file pointer for reading
     */
    public void closeAndPrepareForRead()
        throws IOException {
      _writer.close();
      _fileChannel.position(0);
      if (_reader != null) {
        _reader.close();
      }
      _reader = new BufferedReader(new FileReader(_raf.getFD())); // Reset buffered reader
    }

    /**
     * Check if there is more data to read
     */
    public boolean hasNext()
        throws IOException {
      if (_dataType == FieldSpec.DataType.STRING) {
        return _reader.ready();
      } else {
        return _fileChannel.position() < _fileChannel.size();
      }
    }

    /**
     * Read the next integer
     */
    public int readInt()
        throws IOException {
      _buffer.clear();
      _fileChannel.read(_buffer);
      _buffer.flip();
      return _buffer.getInt();
    }

    /**
     * Read the next long
     */
    public long readLong()
        throws IOException {
      _buffer.clear();
      _fileChannel.read(_buffer);
      _buffer.flip();
      return _buffer.getLong();
    }

    /**
     * Read the next float
     */
    public float readFloat()
        throws IOException {
      _buffer.clear();
      _fileChannel.read(_buffer);
      _buffer.flip();
      return _buffer.getFloat();
    }

    /**
     * Read the next double
     */
    public double readDouble()
        throws IOException {
      _buffer.clear();
      _fileChannel.read(_buffer);
      _buffer.flip();
      return _buffer.getDouble();
    }

    /**
     * Read the next string
     */
    public String readString()
        throws IOException {
      return _reader.readLine();
    }

    /**
     * Close all file resources
     */
    @Override
    public void close()
        throws IOException {
      if (_writer != null) {
        _writer.close();
      }
      if (_reader != null) {
        _reader.close();
      }
      if (_fileChannel != null) {
        _fileChannel.close();
      }
      if (_raf != null) {
        _raf.close();
      }
    }
  }
}
