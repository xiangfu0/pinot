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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConcurrentBroadcastJoin extends BroadcastJoinEvaluation {
  static final int CLIENTS = 4;
  static final int QUERIES_PER_CLIENT = 4;

  void runBlock(Arm arm, int block, ExecutorService pool) throws Exception {
    var ready = new CountDownLatch(CLIENTS);
    var start = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();
    for (int client = 0; client < CLIENTS; client++) {
      int id = client;
      futures.add(pool.submit(() -> {
        ready.countDown();
        start.await();
        for (int query = 0; query < QUERIES_PER_CLIENT; query++) {
          execute(arm, block, id * QUERIES_PER_CLIENT + query);
        }
        return null;
      }));
    }
    ready.await();
    long cpu = os.getProcessCpuTime();
    long wall = System.nanoTime();
    start.countDown();
    for (Future<?> future : futures) {
      future.get();
    }
    wall = System.nanoTime() - wall;
    cpu = os.getProcessCpuTime() - cpu;
    System.out.printf(Locale.ROOT, "CONCURRENT,%d,%d,%b,%d,%.3f,%.3f%n", block, arm.readers(), arm.fuse(),
        CLIENTS * QUERIES_PER_CLIENT, wall / 1e6, cpu / 1e6);
  }

  void measure() throws Exception {
    try (var pool = Executors.newFixedThreadPool(CLIENTS)) {
      setup();
      var arms = new ArrayList<Arm>();
      for (int readers : List.of(1, 4)) {
        for (boolean fuse : List.of(false, true)) {
          var arm = new Arm(true, readers, fuse);
          checkPlan(arm);
          arms.add(arm);
        }
      }
      var random = new Random(Long.getLong("eval.seed", 1042L));
      for (int block = -4; block < 8; block++) {
        if (Math.floorMod(block, arms.size()) == 0) {
          Collections.shuffle(arms, random);
        }
        for (int slot = 0; slot < arms.size(); slot++) {
          runBlock(arms.get(Math.floorMod(slot + block, arms.size())), block, pool);
        }
      }
    } finally {
      _servers.values().forEach(server -> server.shutDown());
      if (_mailboxService != null) {
        _mailboxService.shutdown();
      }
    }
  }

  public static void main(String[] args) {
    try {
      new ConcurrentBroadcastJoin().measure();
    } catch (Throwable failure) {
      failure.printStackTrace();
      System.exit(1);
    }
    System.exit(0);
  }
}
