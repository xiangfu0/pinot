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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.LUCENE_V912_FST_INDEX_FILE_EXTENSION;
import static org.apache.pinot.segment.spi.V1Constants.Indexes.LUCENE_V912_IFST_INDEX_FILE_EXTENSION;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Verifies FST and IFST index handlers can build their indexes during a reload pass on a column whose reloaded
/// table config opts into the "explicit dictionary on a `RAW`-encoded forward index" shape. The handlers walk the
/// dictionary directly (one entry per dictId), so they are agnostic to the segment's actual forward-index encoding
/// and only require `columnMetadata.hasDictionary()` to be true. This means the [DictionaryBasedIndexBuilder]
/// helper (used by inverted/range to translate raw forward values to dict ids per-doc) does not need to be
/// invoked from FST/IFST: those creators consume dictionary entries directly and the handler code stays unchanged.
public class FstIndexHandlerRawForwardTest {

  private static final String COLUMN = "col";
  private static final String TABLE = "fstRawDictTable";

  @Test
  public void testFstIndexAddedOnReloadWithRawForwardExplicitDictConfig()
      throws Exception {
    File tempDir = Files.createTempDirectory("fst-raw-dict-reload").toFile();
    try {
      File segmentDir = buildBaseSegmentWithDictionary(tempDir);
      File fstFile = new File(segmentDir, COLUMN + LUCENE_V912_FST_INDEX_FILE_EXTENSION);
      assertFalse(fstFile.exists(), "Pre-condition: segment must not yet have an FST index");

      // Reload table config opts into the new "RAW + explicit dictionary + FST" shape. The dictionary is already
      // on disk from segment build, so FSTIndexHandler walks it and emits the FST file.
      TableConfig reloadConfig = new TableConfigBuilder(TableType.OFFLINE)
          .setTableName(TABLE)
          .setFieldConfigList(List.of(rawWithExplicitDictAndIndex(COLUMN, "fst")))
          .build();
      runPreprocessor(segmentDir, reloadConfig);

      assertTrue(fstFile.exists(), "FST index file must exist after preprocessor reload: " + fstFile);
      assertTrue(fstFile.length() > 0, "FST index file must be non-empty");
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testIFstIndexAddedOnReloadWithRawForwardExplicitDictConfig()
      throws Exception {
    File tempDir = Files.createTempDirectory("ifst-raw-dict-reload").toFile();
    try {
      File segmentDir = buildBaseSegmentWithDictionary(tempDir);
      File ifstFile = new File(segmentDir, COLUMN + LUCENE_V912_IFST_INDEX_FILE_EXTENSION);
      assertFalse(ifstFile.exists(), "Pre-condition: segment must not yet have an IFST index");

      TableConfig reloadConfig = new TableConfigBuilder(TableType.OFFLINE)
          .setTableName(TABLE)
          .setFieldConfigList(List.of(rawWithExplicitDictAndIndex(COLUMN, "ifst")))
          .build();
      runPreprocessor(segmentDir, reloadConfig);

      assertTrue(ifstFile.exists(), "IFST index file must exist after preprocessor reload: " + ifstFile);
      assertTrue(ifstFile.length() > 0, "IFST index file must be non-empty");
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  /// Build a segment that has a dictionary on the column. Uses default `DICTIONARY` forward-index encoding because
  /// the segment-build-time path for "RAW forward + explicit shared dictionary" is owned by a follow-up change;
  /// this test covers the reload-time handler behavior, which only depends on the dictionary being present.
  private static File buildBaseSegmentWithDictionary(File parentDir)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension(COLUMN, FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(parentDir.getAbsolutePath());
    config.setSegmentName("fstRawDictSegment");
    config.setSegmentVersion(SegmentVersion.v1);

    GenericRow r1 = new GenericRow();
    r1.putValue(COLUMN, "alpha");
    GenericRow r2 = new GenericRow();
    r2.putValue(COLUMN, "beta");
    GenericRow r3 = new GenericRow();
    r3.putValue(COLUMN, "alpha");
    GenericRow r4 = new GenericRow();
    r4.putValue(COLUMN, "gamma");
    GenericRow r5 = new GenericRow();
    r5.putValue(COLUMN, "delta");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(r1, r2, r3, r4, r5)));
    driver.build();
    return driver.getOutputDirectory();
  }

  private static FieldConfig rawWithExplicitDictAndIndex(String column, String indexName) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    indexes.set(indexName, JsonUtils.newObjectNode());
    return new FieldConfig(column, FieldConfig.EncodingType.RAW, null, null, null, null, indexes, null, null);
  }

  private static void runPreprocessor(File segmentDir, TableConfig reloadConfig)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension(COLUMN, FieldSpec.DataType.STRING).build();
    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(reloadConfig, schema);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentPreProcessor preProcessor = new SegmentPreProcessor(segmentDirectory, loadingConfig)) {
      preProcessor.process();
    }
  }
}
