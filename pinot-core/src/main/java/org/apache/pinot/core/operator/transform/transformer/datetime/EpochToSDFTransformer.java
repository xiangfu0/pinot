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
package org.apache.pinot.core.operator.transform.transformer.datetime;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.joda.time.DateTimeZone;


/**
 * Date time transformer to transform and bucket date time values from epoch format to simple date format.
 */
public class EpochToSDFTransformer extends BaseDateTimeTransformer<long[], String[]> {

  public EpochToSDFTransformer(DateTimeFormatSpec inputFormat, DateTimeFormatSpec outputFormat,
      DateTimeGranularitySpec outputGranularity, @Nullable DateTimeZone bucketingTimeZone) {
    super(inputFormat, outputFormat, outputGranularity, bucketingTimeZone);
  }

  @Override
  public void transform(long[] input, String[] output, int length) {
    // NOTE: No need to bucket time because it's implicit in the output simple date format
    if (useCustomBucketingTimeZone()) {
      for (int i = 0; i < length; i++) {
        output[i] = truncateDateToSDF(toDateWithTZ(transformEpochToMillis(input[i])));
      }
    } else {
      for (int i = 0; i < length; i++) {
        output[i] = transformMillisToSDF(transformEpochToMillis(input[i]));
      }
    }
  }
}
