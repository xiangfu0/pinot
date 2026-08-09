<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Pinot Batch Ingestion for Spark 3

Runs Pinot segment generation and segment push as a Spark job, built for **Apache Spark 3.5.x**.

## ⚠️ Deprecated — migrate to `pinot-batch-ingestion-spark-4`

This module is **deprecated** and slated for removal in the next minor Pinot release. New users
should adopt [`pinot-batch-ingestion-spark-4`](../pinot-batch-ingestion-spark-4); existing users
should plan their migration during this release cycle.

## Runtime requirements (this release)

Pinot's master branch raised the Java baseline to JDK 25, which means the
`pinot-batch-ingestion-spark-3` jar (and the `pinot-core` / `pinot-common` classes it bundles
transitively) is now compiled to class file 69 (JDK 25 bytecode). To run it you need:

- **JDK 25 or newer** on your Spark application JVM (driver and executors).
- **Apache Spark 3.5.5 or newer** — Spark 3.5.5+ added official JDK 21 support; earlier Spark
  3.5.x patch releases and Spark 3.4.x will refuse to start on modern JDKs with
  `IllegalArgumentException: Unsupported class file major version 69`. Note that Spark 3.5.x
  predates official JDK 25 support, so you may additionally need Spark JVM flags
  (`--add-opens`/`--add-exports`) to launch it on JDK 25; consider migrating to
  `pinot-batch-ingestion-spark-4` for a fully supported JDK 25 stack.

If your deployment is stuck on Spark 3.5.x with JDK 17 or JDK 21, **stay on the previous Pinot
release (`1.5.x`) for this jar** until you can either upgrade to JDK 25 / Spark 3.5.5+ or
migrate to `pinot-batch-ingestion-spark-4`. Mixing a 1.6+ jar with a JDK 17 or JDK 21 Spark
runtime will fail with `UnsupportedClassVersionError` at first class resolution.

## Migration path to Spark 4

`pinot-batch-ingestion-spark-4` is a faithful port of this module: the runner classes
(`SparkSegmentGenerationJobRunner`, `SparkSegmentMetadataPushJobRunner`,
`SparkSegmentTarPushJobRunner`, `SparkSegmentUriPushJobRunner`) keep the same FQN structure
under the `org.apache.pinot.plugin.ingestion.batch.spark4` package, and the
`SegmentGenerationJobSpec` YAML format is identical. To migrate:

1. Replace `pinot-batch-ingestion-spark-3-*-shaded.jar` with `pinot-batch-ingestion-spark-4-*-shaded.jar`.
2. Update any explicit class references in your job spec from
   `org.apache.pinot.plugin.ingestion.batch.spark3.*` to
   `org.apache.pinot.plugin.ingestion.batch.spark4.*`.
3. Switch your Spark cluster from Spark 3.5.x to Spark 4.1.x (the `pinot-batch-ingestion-spark-4`
   jar is JDK 25 bytecode, so run it on JDK 25+).
