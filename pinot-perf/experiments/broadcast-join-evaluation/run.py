# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Compile and run the review fixture using the checkout's Surefire test classpath."""
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from xml.etree import ElementTree


def run(checkout, destination, extra_jvm_options):
    reports = checkout / "pinot-query-runtime/target/surefire-reports"
    xml = next(iter(sorted(reports.glob("TEST-*.xml"))), None)
    if xml is None:
        sys.exit("Run a focused pinot-query-runtime test with ./mvnw -pl pinot-query-runtime -am first.")
    properties = {
        node.get("name"): node.get("value") for node in ElementTree.parse(xml).findall(".//property")
    }
    classpath = properties["java.class.path"]
    jdk = Path(properties["java.home"]) / "bin"
    mockito = next(path for path in classpath.split(os.pathsep) if Path(path).name.startswith("mockito-core-"))
    main = os.environ.get("EVAL_MAIN", "BroadcastJoinEvaluation")
    sources = ["BroadcastJoinEvaluation"]
    if main == "ConcurrentBroadcastJoin":
        sources.append(main)
    elif main != sources[0]:
        sys.exit("EVAL_MAIN must be BroadcastJoinEvaluation or ConcurrentBroadcastJoin.")

    output = Path(destination).resolve()
    temporary_files = output / "javatmp"
    temporary_files.mkdir(parents=True, exist_ok=True)
    source_dir = Path(__file__).resolve().parent
    subprocess.run([jdk / "javac", "-cp", classpath, "-d", output,
                    *(source_dir / (name + ".java") for name in sources)], check=True)
    packages = ("java.nio", "sun.nio.ch", "java.lang", "java.util", "java.lang.reflect", "jdk.internal.misc")
    options = [f"--add-opens=java.base/{package}=ALL-UNNAMED" for package in packages]
    options.extend([
        "--add-exports=java.base/jdk.internal.util.random=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true", "-Dio.grpc.netty.shaded.io.netty.tryReflectionSetAccessible=true",
        "-Dnet.bytebuddy.experimental=true", "-javaagent:" + mockito, "-Xshare:off", "-XX:+EnableDynamicAgentLoading",
        "-Xms3g", "-Xmx3g", "-XX:ActiveProcessorCount=8", "-Djava.io.tmpdir=" + str(temporary_files),
    ])
    subprocess.run([jdk / "java", *options, *extra_jvm_options, "-cp", str(output) + os.pathsep + classpath,
                    "org.apache.pinot.query.runtime.queries." + main], cwd=checkout, check=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python3 run.py PINOT_CHECKOUT [--] [-Deval.seed=1042 ...]")
    checkout = Path(sys.argv[1]).resolve()
    jvm_options = sys.argv[2:]
    if jvm_options[:1] == ["--"]:
        jvm_options = jvm_options[1:]
    if "EVAL_TMP" in os.environ:
        run(checkout, os.environ["EVAL_TMP"], jvm_options)
    else:
        with TemporaryDirectory(prefix="pinot-broadcast-eval-") as directory:
            run(checkout, directory, jvm_options)
