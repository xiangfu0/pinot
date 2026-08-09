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
package org.apache.pinot.verifier;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Smoke tests for the verifier. We don't have a built distribution at unit-test time, so
/// these only confirm the binary is wired together correctly — argument parsing, exit codes, and
/// dispatch — by driving the real {@link PluginVerifier#execute(String[])} entry point. The full
/// end-to-end run (realm isolation with a production classpath layout) happens via
/// {@code bin/verify-plugins.sh} against an assembled tarball; it cannot run here because surefire
/// puts every plugin on the system classloader, which defeats realm isolation.
public class PluginVerifierSmokeTest {

  @Test
  public void helpFlagPrintsUsageAndExitsZero() {
    Result r = invokeMain(new String[]{"--help"});
    assertEquals(r.rc, 0, "expected --help to exit 0, got stderr:\n" + r.stderr);
    assertTrue(r.stdout.contains("Usage:"), "expected usage text on stdout, got:\n" + r.stdout);
  }

  @Test
  public void unknownFlagExitsTwoWithSingleDiagnostic() {
    Result r = invokeMain(new String[]{"--no-such-flag"});
    assertEquals(r.rc, 2, "expected exit 2 for unknown flag, got stdout:\n" + r.stdout
        + "\n--- stderr:\n" + r.stderr);
    assertTrue(r.stderr.contains("Unknown flag: --no-such-flag"),
        "expected 'Unknown flag: --no-such-flag' diagnostic, got:\n" + r.stderr);
    // Regression guard: the diagnostic must appear exactly once, not doubled
    // ("Unknown flag: Unknown flag: ...") as an earlier reflective wrapper produced.
    assertEquals(countOccurrences(r.stderr, "Unknown flag"), 1,
        "'Unknown flag' should appear exactly once, got:\n" + r.stderr);
  }

  @Test
  public void unknownCheckTypeExitsTwo() {
    Result r = invokeMain(new String[]{"--check", "no-such-check"});
    assertEquals(r.rc, 2, "expected exit 2 for unknown check type, got stdout:\n" + r.stdout);
    assertTrue(r.stderr.contains("Unknown check type"),
        "expected 'Unknown check type' diagnostic, got:\n" + r.stderr);
  }

  @Test
  public void runWithNoPluginsDirReportsCleanlyAndDoesNotThrow() {
    // No plugins.dir is set; PluginManager will skip the scan, every check loads zero
    // candidates from realms (and may load some via the system classloader if those classes
    // are on the test classpath). The point of this test is to confirm we don't NPE or throw
    // anywhere; the actual pass/fail outcome depends on what's on the test classpath.
    Result r = invokeMain(new String[]{"--check", "metrics"});
    assertTrue(r.stdout.contains("Metrics factory plugins"),
        "expected the metrics check section header, got:\n" + r.stdout);
  }

  private record Result(int rc, String stdout, String stderr) { }

  private Result invokeMain(String[] args) {
    PrintStream origOut = System.out;
    PrintStream origErr = System.err;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out, true));
    System.setErr(new PrintStream(err, true));
    int rc;
    try {
      // execute() is the real main() path minus System.exit, so this drives the actual argument
      // parser and dispatcher rather than a test-only reimplementation.
      rc = PluginVerifier.execute(args);
    } finally {
      System.setOut(origOut);
      System.setErr(origErr);
    }
    return new Result(rc, out.toString(), err.toString());
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    for (int idx = haystack.indexOf(needle); idx >= 0; idx = haystack.indexOf(needle, idx + needle.length())) {
      count++;
    }
    return count;
  }
}
