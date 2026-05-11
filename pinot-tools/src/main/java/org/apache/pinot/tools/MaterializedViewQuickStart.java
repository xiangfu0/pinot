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
package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.minion.MinionClient;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * Quickstart that demonstrates the Materialized View (MV) query rewrite feature.
 *
 * <p>This quickstart:
 * <ol>
 *   <li>Loads the {@code airlineStats} base table (31 days of flight data, Jan 2014).</li>
 *   <li>Creates an empty {@code airlineStatsMv} table configured with a
 *       {@code MaterializedViewTask} that pre-aggregates daily carrier metrics.</li>
 *   <li>Triggers the minion task to generate MV segments and waits for completion.</li>
 *   <li>Demonstrates query rewrite: queries against {@code airlineStats} are transparently
 *       served from the pre-aggregated {@code airlineStatsMv} table.</li>
 * </ol>
 *
 * <p>The MV definition is:
 * <pre>
 *   SELECT DaysSinceEpoch, Carrier,
 *          SUM(ArrDelay) AS sum_ArrDelay,
 *          COUNT(*) AS flight_count,
 *          MIN(ArrDelay) AS min_ArrDelay,
 *          MAX(ArrDelay) AS max_ArrDelay,
 *          DISTINCTCOUNTRAWHLL(FlightNum) AS raw_hll_FlightNum,
 *          DISTINCTCOUNTRAWHLLPLUS(FlightNum) AS raw_hllplus_FlightNum
 *   FROM airlineStats
 *   GROUP BY DaysSinceEpoch, Carrier
 * </pre>
 *
 * <p>The {@code definedSQL} omits an explicit {@code LIMIT}; the generator auto-injects
 * {@code DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT} (1M) so the broker's small default cannot truncate the window.
 *
 * <p>The example table config sets {@code maxTasksPerBatch=31} to backfill all 31 days of
 * the airlineStats fixture in a single scheduling cycle. Production deployments typically
 * leave the default of 1; raise it only when intentionally back-filling and after sizing
 * the minion pool to absorb the resulting concurrent task load.
 *
 * <p>Run via: {@code bin/pinot-admin.sh QuickStart -type MATERIALIZED_VIEW}
 */
public class MaterializedViewQuickStart extends Quickstart {

  private static final String BASE_TABLE = "airlineStats";
  private static final String MATERIALIZED_VIEW_TABLE = "airlineStatsMv";
  private static final int FIXTURE_COVERAGE_UPPER_DAY = 16102;
  private static final long TASK_POLL_INTERVAL_MS = 5_000L;
  private static final long TASK_TIMEOUT_MS = 300_000L;

  @Override
  public List<String> types() {
    return Arrays.asList("MATERIALIZED_VIEW", "MATERIALIZED-VIEW", "BATCH_MV");
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    overrides.put(Broker.CONFIG_OF_BROKER_QUERY_ENABLE_MATERIALIZED_VIEW_REWRITE, true);
    overrides.putIfAbsent("controller.task.scheduler.enabled", true);
    return overrides;
  }

  @Override
  protected String[] getDefaultBatchTableDirectories() {
    return new String[]{
        "examples/batch/airlineStats",
        "examples/batch/airlineStatsMv"
    };
  }

  @Override
  protected String getValidationTypesToSkip() {
    return "TASK";
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.CYAN, "***** Step 1: Verify airlineStats base table is loaded *****");

    String q1 = "SELECT COUNT(*) FROM " + BASE_TABLE + " LIMIT 1";
    runQuery(runner, "Count all flights in airlineStats", q1);

    String q2 = "SELECT Carrier, SUM(ArrDelay) AS total_delay, COUNT(*) AS flights "
        + "FROM " + BASE_TABLE + " GROUP BY Carrier ORDER BY total_delay DESC LIMIT 10";
    runQuery(runner, "Top 10 carriers by total arrival delay (direct base table query)", q2);

    MinionClient minionClient = new MinionClient(
        "http://localhost:" + QuickstartRunner.DEFAULT_CONTROLLER_PORT, null);

    printStatus(Color.CYAN, "***** Step 2: Trigger MaterializedViewTask to generate MV segments *****");
    printStatus(Color.GREEN,
        "airlineStatsMv stores SUM, COUNT, MIN, MAX and raw HLL/HLLPlus sketches by day and carrier.");
    triggerMaterializedViewTask(minionClient);

    printStatus(Color.CYAN, "***** Step 3: Wait for MV segments to be generated and served *****");
    waitForMaterializedViewSegments(runner, minionClient);

    printStatus(Color.CYAN, "***** Step 4: Demonstrate MV query rewrite *****");
    printStatus(Color.GREEN,
        "The following queries hit airlineStats but will be transparently rewritten to airlineStatsMv.");
    printStatus(Color.GREEN, "Add SET useMaterializedView=true to opt into rewrite.");

    String q3 = ""
        + "SELECT Carrier, SUM(ArrDelay) AS total_delay, COUNT(*) AS flights "
        + "FROM " + BASE_TABLE + " WHERE DaysSinceEpoch < " + FIXTURE_COVERAGE_UPPER_DAY + " "
        + "GROUP BY Carrier ORDER BY total_delay DESC LIMIT 10";
    runMaterializedViewRewriteQuery(runner, "SUM and COUNT: top 10 carriers by total arrival delay", q3);

    String q4 = ""
        + "SELECT DaysSinceEpoch, Carrier, SUM(ArrDelay) AS total_delay "
        + "FROM " + BASE_TABLE + " "
        + "WHERE DaysSinceEpoch BETWEEN 16071 AND 16080 "
        + "GROUP BY DaysSinceEpoch, Carrier ORDER BY DaysSinceEpoch, total_delay DESC LIMIT 20";
    runMaterializedViewRewriteQuery(runner, "SUM: per-day carrier delays for first 10 days of Jan 2014", q4);

    String q5 = ""
        + "SELECT Carrier, MIN(ArrDelay) AS min_delay, MAX(ArrDelay) AS max_delay "
        + "FROM " + BASE_TABLE + " GROUP BY Carrier ORDER BY max_delay DESC LIMIT 10";
    runMaterializedViewRewriteQuery(runner, "MIN and MAX: arrival delay range by carrier", q5);

    String q6 = ""
        + "SELECT Carrier, DISTINCTCOUNTHLL(FlightNum) AS approx_flight_nums "
        + "FROM " + BASE_TABLE + " GROUP BY Carrier ORDER BY approx_flight_nums DESC LIMIT 10";
    runMaterializedViewRewriteQuery(runner, "DISTINCTCOUNTHLL: approximate distinct flight numbers by carrier", q6);

    String q7 = ""
        + "SELECT Carrier, DISTINCTCOUNTHLLPLUS(FlightNum) AS approx_flight_nums_hllplus "
        + "FROM " + BASE_TABLE + " GROUP BY Carrier ORDER BY approx_flight_nums_hllplus DESC LIMIT 10";
    runMaterializedViewRewriteQuery(runner, "DISTINCTCOUNTHLLPLUS: approximate distinct flight numbers by carrier", q7);

    printStatus(Color.GREEN, String.format(
        "You can always go to http://localhost:%d to play around in the query console",
        QuickstartRunner.DEFAULT_CONTROLLER_PORT));
    printStatus(Color.GREEN,
        "Try: SELECT Carrier, MIN(ArrDelay), MAX(ArrDelay) "
            + "FROM airlineStats GROUP BY Carrier");
  }

  private JsonNode runQuery(QuickstartRunner runner, String description, String query)
      throws Exception {
    printStatus(Color.YELLOW, description);
    printStatus(Color.CYAN, "Query : " + query);
    JsonNode response = runner.runQuery(query);
    printStatus(Color.YELLOW, prettyPrintResponse(response));
    printStatus(Color.GREEN, "***************************************************");
    return response;
  }

  private void runMaterializedViewRewriteQuery(QuickstartRunner runner, String description, String query)
      throws Exception {
    printStatus(Color.YELLOW, description + " (MV rewrite)");
    printStatus(Color.CYAN, "Query : " + query);
    JsonNode response = runner.runQuery(query);
    printStatus(Color.YELLOW, prettyPrintResponse(response));
    String materializedViewQueried = getMaterializedViewQueriedFromResponse(response);
    if (materializedViewQueried != null) {
      printStatus(Color.GREEN, "*** Query was rewritten to MV table: " + materializedViewQueried + " ***");
    } else {
      printStatus(Color.YELLOW,
          "WARNING: MV rewrite was NOT applied. Check that the MV is populated and broker rewrite is enabled.");
    }
    printStatus(Color.GREEN, "***************************************************");
  }

  private void triggerMaterializedViewTask(MinionClient minionClient) {
    try {
      Map<String, String> scheduled = minionClient.scheduleMinionTasks(
          MinionConstants.MaterializedViewTask.TASK_TYPE,
          MATERIALIZED_VIEW_TABLE + "_OFFLINE");
      if (scheduled.isEmpty()) {
        printStatus(Color.YELLOW,
            "No tasks scheduled — MV may already be up-to-date or minion is still starting up");
      } else {
        printStatus(Color.GREEN, "Scheduled MV tasks: " + scheduled);
      }
    } catch (Exception e) {
      printStatus(Color.YELLOW, "Could not schedule MV task (will retry): " + e.getMessage());
    }
  }

  private void waitForMaterializedViewSegments(QuickstartRunner runner, MinionClient minionClient)
      throws Exception {
    long expectedRows = getExpectedMaterializedViewRowCount(runner);
    if (expectedRows > 0) {
      printStatus(Color.CYAN,
          "Waiting up to 5 minutes for all " + expectedRows + " MV pre-aggregated rows to be generated...");
    } else {
      printStatus(Color.CYAN, "Waiting up to 5 minutes for MV segments to be generated...");
    }
    long deadline = System.currentTimeMillis() + TASK_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      try {
        JsonNode result = runner.runQuery("SELECT COUNT(*) FROM " + MATERIALIZED_VIEW_TABLE + " LIMIT 1");
        JsonNode rows = result.path("resultTable").path("rows");
        if (rows.isArray() && rows.size() > 0) {
          long count = rows.get(0).get(0).asLong();
          if (expectedRows > 0) {
            if (count >= expectedRows) {
              printStatus(Color.GREEN,
                  "MV table " + MATERIALIZED_VIEW_TABLE + " is ready with " + count + " pre-aggregated rows.");
              return;
            }
            printStatus(Color.CYAN,
                "MV table " + MATERIALIZED_VIEW_TABLE + " has " + count + " of " + expectedRows
                    + " pre-aggregated rows, retrying...");
          } else if (count > 0) {
            printStatus(Color.GREEN,
                "MV table " + MATERIALIZED_VIEW_TABLE + " is ready with " + count + " pre-aggregated rows.");
            return;
          }
        }
      } catch (Exception e) {
        printStatus(Color.YELLOW, "MV not ready yet (" + e.getMessage() + "), retrying...");
      }
      printStatus(Color.CYAN,
          "MV not ready yet, retrying in " + (TASK_POLL_INTERVAL_MS / 1000) + "s...");

      // Re-trigger in case the scheduler hasn't picked it up yet
      triggerMaterializedViewTask(minionClient);

      Thread.sleep(TASK_POLL_INTERVAL_MS);
    }
    printStatus(Color.YELLOW,
        "Timed out waiting for MV segments. Query rewrite demo may show unoptimized results.");
  }

  private long getExpectedMaterializedViewRowCount(QuickstartRunner runner) {
    try {
      JsonNode result = runner.runQuery("SELECT DaysSinceEpoch, Carrier, COUNT(*) "
          + "FROM " + BASE_TABLE + " GROUP BY DaysSinceEpoch, Carrier LIMIT 10000");
      JsonNode rows = result.path("resultTable").path("rows");
      if (rows.isArray()) {
        return rows.size();
      }
    } catch (Exception e) {
      printStatus(Color.YELLOW,
          "Could not compute expected MV row count from base table (" + e.getMessage()
              + "); falling back to first served MV segment.");
    }
    return -1;
  }

  private static String getMaterializedViewQueriedFromResponse(JsonNode response) {
    if (response == null || !response.has("materializedViewQueried")) {
      return null;
    }
    JsonNode materializedViewQueried = response.get("materializedViewQueried");
    return materializedViewQueried.isNull() ? null : materializedViewQueried.asText();
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MATERIALIZED_VIEW"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
