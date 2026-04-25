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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for {@link FieldConfig} serialization and deserialization.
 */
public class FieldConfigTest {

  /**
   * Verifies that a {@code FieldConfig} with a {@code codecSpec} survives a full JSON round-trip.
   * This guards against the {@code @JsonCreator} being placed on the wrong constructor, which
   * would cause {@code codecSpec} to be silently dropped when reading from ZooKeeper or HTTP.
   */
  @Test
  public void testCodecSpecRoundTrip()
      throws JsonProcessingException {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCodecSpec("CODEC(DELTA,ZSTD(3))")
        .build();

    assertEquals(original.getCodecSpec(), "CODEC(DELTA,ZSTD(3))");
    assertNull(original.getCompressionCodec());

    String json = JsonUtils.objectToString(original);
    FieldConfig deserialized = JsonUtils.stringToObject(json, FieldConfig.class);

    assertEquals(deserialized.getCodecSpec(), "CODEC(DELTA,ZSTD(3))",
        "codecSpec must survive JSON serialization round-trip");
    assertNull(deserialized.getCompressionCodec(),
        "compressionCodec must remain null when codecSpec is set");
    assertEquals(deserialized.getName(), "col");
    assertEquals(deserialized.getEncodingType(), FieldConfig.EncodingType.RAW);
  }

  /**
   * Verifies that {@code compressionCodec} still deserializes correctly (backward compat).
   */
  @Test
  public void testCompressionCodecRoundTrip()
      throws JsonProcessingException {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD)
        .build();

    String json = JsonUtils.objectToString(original);
    FieldConfig deserialized = JsonUtils.stringToObject(json, FieldConfig.class);

    assertEquals(deserialized.getCompressionCodec(), FieldConfig.CompressionCodec.ZSTANDARD,
        "compressionCodec must survive JSON round-trip");
    assertNull(deserialized.getCodecSpec());
  }

  /**
   * Verifies that a JSON payload that includes {@code codecSpec} and not {@code compressionCodec}
   * is deserialized correctly (simulates ZooKeeper read after write by a newer server).
   */
  @Test
  public void testDeserializeFromJsonWithCodecSpec()
      throws JsonProcessingException {
    String json = "{\"name\":\"myCol\",\"encodingType\":\"RAW\",\"codecSpec\":\"ZSTD(8)\"}";
    FieldConfig config = JsonUtils.stringToObject(json, FieldConfig.class);

    assertEquals(config.getCodecSpec(), "ZSTD(8)");
    assertNull(config.getCompressionCodec());
  }

  /**
   * Verifies that constructing a {@code FieldConfig} with both {@code compressionCodec} and
   * {@code codecSpec} throws an {@link IllegalArgumentException}.
   */
  @Test
  public void testWithCodecSpecClearsCompressionCodec() {
    // withCodecSpec() must clear compressionCodec so callers don't need to null it out explicitly
    FieldConfig config = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD)
        .withCodecSpec("ZSTD(3)")
        .build();
    assertEquals(config.getCodecSpec(), "ZSTD(3)");
    assertNull(config.getCompressionCodec(), "compressionCodec must be cleared when codecSpec is set via Builder");
  }

  /**
   * Verifies that a JSON payload with both fields causes an {@link IllegalArgumentException}
   * at construction time (via the {@code @JsonCreator} validation), wrapped by Jackson.
   */
  @Test
  public void testDeserializeWithBothFieldsThrows() {
    String json =
        "{\"name\":\"col\",\"encodingType\":\"RAW\",\"compressionCodec\":\"ZSTANDARD\",\"codecSpec\":\"ZSTD(3)\"}";
    try {
      JsonUtils.stringToObject(json, FieldConfig.class);
      fail("Expected JsonProcessingException");
    } catch (JsonProcessingException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException,
          "Expected IllegalArgumentException as cause, got: " + e.getCause());
    }
  }
}
