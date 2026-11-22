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
package org.apache.pinot.query.runtime.queries;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.MultiStageStatsTreeBuilder;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class BroadcastJoinEvaluation extends QueryRunnerTestBase {
  static final int SEGMENTS = 16, ROWS_PER_SEGMENT = 32_768, CUSTOMERS = 4096, PRODUCTS = 1024;
  static final int SERVERS = Integer.getInteger("eval.servers", 1);
  static final int WARM_BLOCKS = Integer.getInteger("eval.warm", 8);
  static final int BLOCKS = Integer.getInteger("eval.blocks", 24);
  final long[][][] expected = {new long[16][2], new long[128][2]};
  final OperatingSystemMXBean os = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
  final Map<String, List<String>> segments = new HashMap<>();

  void setup() throws Exception {
    assertTrue(SERVERS == 1 || SERVERS == 2);
    MockInstanceDataManagerFactory facts = new MockInstanceDataManagerFactory("facts");
    MockInstanceDataManagerFactory dimensions = SERVERS == 1 ? facts : new MockInstanceDataManagerFactory("dims");
    for (String name : List.of("fact", "customerDim", "productDim")) {
      (name.equals("fact") ? facts : dimensions).registerTable(new Schema.SchemaBuilder().setSchemaName(name).setEnableColumnBasedNullHandling(true)
          .addSingleValueDimension("k1", DataType.INT).addSingleValueDimension("k2", DataType.INT)
          .addMetric("amount", DataType.INT).addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
          .build(), name + "_OFFLINE");
    }
    for (int s = 0; s < SEGMENTS; s++) {
      List<GenericRow> rows = new ArrayList<>();
      for (int j = 0; j < ROWS_PER_SEGMENT; j++) {
        int i = s * ROWS_PER_SEGMENT + j, customer = i % CUSTOMERS;
        int product = (i * 17 + i / CUSTOMERS) % PRODUCTS, amount = 1 + i % 101;
        rows.add(row(customer, product, amount));
        int region = customer % 16, category = product % 8;
        for (int chain = 0; chain < 2; chain++) {
          long[] totals = expected[chain][chain == 0 ? region : region * 8 + category];
          totals[0]++;
          totals[1] += amount;
        }
      }
      facts.addSegment("fact_OFFLINE", rows);
    }
    for (boolean product : List.of(false, true)) {
      List<GenericRow> rows = new ArrayList<>();
      for (int i = 0; i < (product ? PRODUCTS : CUSTOMERS); i++) {
        rows.add(row(i, i % (product ? 8 : 16), 0));
      }
      dimensions.addSegment(product ? "productDim_OFFLINE" : "customerDim_OFFLINE", rows);
    }
    segments.putAll(facts.buildTableSegmentNameMap());
    segments.putAll(dimensions.buildTableSegmentNameMap());
    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    _mailboxService = new MailboxService(_reducerHostname, _reducerPort, InstanceType.BROKER,
        new PinotConfiguration(Map.of(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname,
            MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort)), null, null, "Broker_localhost");
    _mailboxService.start();
    int factPort = startServer(facts);
    int dimPort = SERVERS == 1 ? QueryTestUtils.getAvailablePort() : startServer(dimensions);
    var schemas = new HashMap<>(facts.getRegisteredSchemaMap());
    schemas.putAll(dimensions.getRegisteredSchemaMap());
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, factPort, dimPort, schemas,
        facts.buildTableSegmentNameMap(), SERVERS == 1 ? Map.of() : dimensions.buildTableSegmentNameMap(), null);
  }

  int startServer(MockInstanceDataManagerFactory factory) {
    QueryServerEnclosure server = new QueryServerEnclosure(factory);
    server.start();
    int port = server.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port, "localhost", port, port), server);
    return port;
  }

  static GenericRow row(int k1, int k2, int amount) {
    GenericRow row = new GenericRow();
    row.putValue("k1", k1);
    row.putValue("k2", k2);
    row.putValue("amount", amount);
    row.putValue("ts", 1700000000000L);
    return row;
  }

  record Arm(boolean chain, int readers, boolean fuse) {
    String sql() {
      return "SET useMultistageEngine=true; SET usePhysicalOptimizer=false; SET timeoutMs=60000; "
          + "SET useLeafServerForIntermediateStage=true; SET stageParallelism=" + (4 / SERVERS) + "; "
          + (readers == 1 ? "" : "SET leafStageWorkersPerServer=" + readers + "; ")
          + (fuse ? "SET fuseBroadcastJoins=true; " : "")
          + "SELECT /*+ joinOptions(left_distribution_type='random',right_distribution_type='broadcast') */ "
          + "c.k2" + (chain ? ", p.k2" : "") + ", COUNT(*), SUM(f.amount) "
          + "FROM fact f JOIN customerDim c ON f.k1=c.k1 "
          + (chain ? "JOIN productDim p ON f.k2=p.k1 " : "")
          + "GROUP BY c.k2" + (chain ? ", p.k2" : "") + " LIMIT 1000";
    }
  }

  static int countJoins(PlanNode n) {
    return (n instanceof JoinNode ? 1 : 0) + n.getInputs().stream().mapToInt(BroadcastJoinEvaluation::countJoins).sum();
  }

  void checkPlan(Arm arm) {
    var plan = planQuery(arm.sql()).getQueryPlan();
    List<Integer> joinCounts = new ArrayList<>();
    int random = 0, builds = 0, leaves = 0;
    for (var stage : plan.getQueryStages()) {
      var root = stage.getPlanFragment().getFragmentRoot();
      if (root instanceof MailboxSendNode send) {
        if (send.getDistributionType() == RelDistribution.Type.RANDOM_DISTRIBUTED) {
          random++;
        }
        if (send.getDistributionType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
          builds++;
        }
      }
      int joins = countJoins(root);
      if (joins > 0) {
        joinCounts.add(joins);
        assertEquals(stage.getWorkerMetadataList().size(), 4, "intermediate workers");
        assertEquals(stage.getServerInstanceToWorkerIdMap().size(), SERVERS);
        stage.getServerInstanceToWorkerIdMap().values().forEach(ids -> assertEquals(ids.size(), 4 / SERVERS));
      }
      var workers = stage.getWorkerIdToSegmentsMap();
      if (workers != null && !workers.isEmpty()) {
        String name = stage.getTableName().replace("_OFFLINE", "");
        leaves++;
        var scanned = workers.values().stream().flatMap(m -> m.get("OFFLINE").stream()).toList();
        var required = segments.get(name + "_OFFLINE");
        assertEquals(new HashSet<>(scanned), new HashSet<>(required));
        assertEquals(scanned.size(), required.size());
        assertEquals(workers.size(), name.equals("fact") ? arm.readers : 1, name);
      }
    }
    assertEquals(leaves, arm.chain ? 3 : 2);
    assertEquals(joinCounts, !arm.chain ? List.of(1) : arm.fuse ? List.of(2) : List.of(1, 1));
    assertEquals(random, arm.chain && !arm.fuse ? 2 : 1);
    assertEquals(builds, arm.chain ? 3 : 2);
    System.out.printf("EVALPLAN,%s,%d,%b,%s,%d,%d,%d%n", arm.chain ? "chain" : "single", arm.readers,
        arm.fuse, joinCounts, random, builds, plan.getQueryStages().size());
  }

  void execute(Arm arm, int block, int slot) {
    long cpu = os.getProcessCpuTime();
    assertTrue(cpu >= 0, "process CPU timing unavailable");
    long start = System.nanoTime();
    var result = queryRunner(arm.sql(), false);
    long wall = System.nanoTime() - start;
    cpu = os.getProcessCpuTime() - cpu;
    assertTrue(cpu > 0, "process CPU did not advance");
    assertNull(result.getProcessingException(), String.valueOf(result.getProcessingException()));
    var rows = result.getResultTable().getRows();
    var totals = expected[arm.chain ? 1 : 0];
    assertEquals(rows.size(), totals.length);
    boolean[] seen = new boolean[totals.length];
    for (Object[] row : rows) {
      int region = ((Number) row[0]).intValue();
      int key = arm.chain ? region * 8 + ((Number) row[1]).intValue() : region;
      assertFalse(seen[key]);
      seen[key] = true;
      assertEquals(((Number) row[row.length - 2]).longValue(), totals[key][0]);
      assertEquals(((Number) row[row.length - 1]).doubleValue(), (double) totals[key][1]);
    }
    System.out.printf(Locale.ROOT, "EVAL,%d,%d,%s,%d,%b,%.3f,%.3f%n", block, slot, arm.chain ? "chain" : "single",
        arm.readers, arm.fuse, wall / 1e6, cpu / 1e6);
  }

  @Test
  public void evaluate() throws Exception {
    try {
      setup();
      List<Arm> arms = new ArrayList<>();
      for (boolean chain : List.of(false, true)) {
        for (int readers : List.of(1, 4)) {
          for (boolean fuse : List.of(false, true)) {
            if (Boolean.getBoolean("eval.baseline") && (readers != 1 || fuse)) {
              continue;
            }
            Arm arm = new Arm(chain, readers, fuse);
            checkPlan(arm);
            arms.add(arm);
          }
        }
      }
      for (Arm arm : arms) {
        var traced = queryRunner(arm.sql(), true);
        assertNull(traced.getProcessingException());
        var plan = planQuery(arm.sql()).getQueryPlan();
        var tree = new MultiStageStatsTreeBuilder(plan.getQueryStageMap(), traced.getQueryStats()).jsonStatsByStage(1);
        System.out.printf("EVALTRACE,%s,%d,%b,%s%n", arm.chain ? "chain" : "single", arm.readers, arm.fuse, tree);
      }
      List<Arm> order = new ArrayList<>(arms);
      Random rng = new Random(Long.getLong("eval.seed", 1042L));
      for (int block = -WARM_BLOCKS; block < BLOCKS; block++) {
        if (Math.floorMod(block, order.size()) == 0) {
          Collections.shuffle(order, rng);
        }
        for (int slot = 0; slot < order.size(); slot++) {
          execute(order.get(Math.floorMod(slot + block, order.size())), block, slot);
        }
      }
    } finally {
      for (QueryServerEnclosure server : _servers.values()) {
        server.shutDown();
      }
      if (_mailboxService != null) {
        _mailboxService.shutdown();
      }
    }
  }

  public static void main(String[] args) {
    try {
      new BroadcastJoinEvaluation().evaluate();
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
    System.exit(0);
  }
}
