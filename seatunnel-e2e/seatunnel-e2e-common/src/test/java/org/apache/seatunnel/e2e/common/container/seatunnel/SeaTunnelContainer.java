/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.common.container.seatunnel;

import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import groovy.lang.Tuple2;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@NoArgsConstructor
@Slf4j
@AutoService(TestContainer.class)
public class SeaTunnelContainer extends AbstractTestContainer {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final String JDK_DOCKER_IMAGE = "openjdk:8";
    private static final String CLIENT_SHELL = "seatunnel.sh";
    protected static final String SERVER_SHELL = "seatunnel-cluster.sh";
    protected GenericContainer<?> server;
    private final AtomicInteger runningCount = new AtomicInteger();

    @Override
    public void startUp() throws Exception {
        server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(
                                ContainerUtil.adaptPathForWin(
                                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()))
                        .withNetworkAliases("server")
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server);
        server.setPortBindings(Collections.singletonList("5801:5801"));
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());
        server.start();
        // execute extra commands
        executeExtraCommands(server);
    }

    @Override
    public void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Override
    protected String getDockerImage() {
        return JDK_DOCKER_IMAGE;
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-starter";
    }

    @Override
    protected String getStartShellName() {
        return CLIENT_SHELL;
    }

    @Override
    protected String getConnectorModulePath() {
        return "seatunnel-connectors-v2";
    }

    @Override
    protected String getConnectorType() {
        return "seatunnel";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "connector-";
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Collections.emptyList();
    }

    @Override
    public TestContainerId identifier() {
        return TestContainerId.SEATUNNEL;
    }

    @Override
    protected String getSavePointCommand() {
        return "-s";
    }

    @Override
    protected String getRestoreCommand() {
        return "-r";
    }

    @Override
    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(server);
    }

    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, null);
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        List<String> beforeThreads = ContainerUtil.getJVMThreadNames(server);
        runningCount.incrementAndGet();
        Container.ExecResult result = executeJob(server, confFile, variables);
        if (runningCount.decrementAndGet() > 0) {
            // only check thread when job all finished.
            return result;
        }
        List<String> afterThreads = ContainerUtil.getJVMThreadNames(server);
        afterThreads = removeSystemThread(beforeThreads, afterThreads);
        if (afterThreads.isEmpty()) {
            //            classLoaderObjectCheck(1);
            return result;
        } else {
            // Waiting 10s for release thread
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                List<String> threads = ContainerUtil.getJVMThreadNames(server);
                                threads = removeSystemThread(beforeThreads, threads);
                                List<String> finalAfterThreads = threads;
                                Assertions.assertTrue(
                                        threads.isEmpty(),
                                        "There are still threads running in the container: \n"
                                                + ContainerUtil.getJVMThreads(server).stream()
                                                        .filter(
                                                                tuple2 ->
                                                                        finalAfterThreads.contains(
                                                                                tuple2.getV1()))
                                                        .map(Tuple2::getV2)
                                                        .map(str -> str + "\n")
                                                        .collect(Collectors.joining()));
                            });
        }
        //        classLoaderObjectCheck(1);
        return result;
    }

    private List<String> removeSystemThread(List<String> beforeThreads, List<String> afterThreads)
            throws IOException {
        afterThreads.removeIf(SeaTunnelContainer::isSystemThread);
        afterThreads.removeIf(beforeThreads::contains);
        Map<String, String> threadAndClassLoader = getThreadClassLoader();
        List<String> notSystemClassLoaderThread =
                threadAndClassLoader.entrySet().stream()
                        .filter(
                                tc -> {
                                    // system thread, ttl 60s
                                    if (tc.getKey().contains("process reaper")) {
                                        return false;
                                    }
                                    String classLoader = tc.getValue();
                                    return !classLoader.contains("AppClassLoader")
                                            && !classLoader.equals("null");
                                })
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        notSystemClassLoaderThread.addAll(afterThreads);
        notSystemClassLoaderThread.removeIf(this::isIssueWeAlreadyKnow);
        notSystemClassLoaderThread.removeIf(SeaTunnelContainer::isSystemThread);
        return notSystemClassLoaderThread;
    }

    private static boolean isSystemThread(String s) {
        Pattern aqsThread = Pattern.compile("pool-[0-9]-thread-[0-9]");
        return s.startsWith("hz.main")
                || s.startsWith("seatunnel-coordinator-service")
                || s.startsWith("GC task thread")
                || s.contains("CompilerThread")
                || s.contains("NioNetworking-closeListenerExecutor")
                || s.contains("ForkJoinPool.commonPool")
                || s.contains("DestroyJavaVM")
                || s.contains("main-query-state-checker")
                || s.contains("Keep-Alive-SocketCleaner")
                || s.contains("process reaper")
                || s.startsWith("Timer-")
                || s.contains("InterruptTimer")
                || s.contains("Java2D Disposer")
                || s.contains(
                        "org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner")
                || s.startsWith("Log4j2-TF-")
                || aqsThread.matcher(s).matches();
    }

    private void classLoaderObjectCheck(Integer maxSize) throws IOException, InterruptedException {
        Map<String, Integer> objects = ContainerUtil.getJVMLiveObject(server);
        String className =
                "org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader";
        if (objects.containsKey(className) && objects.get(className) > maxSize) {
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Map<String, Integer> newObjects =
                                        ContainerUtil.getJVMLiveObject(server);
                                if (newObjects.containsKey(className)) {
                                    Assertions.assertTrue(
                                            newObjects.get(className) <= maxSize,
                                            "There are still SeaTunnelChildFirstClassLoader objects in the seatunnel server");
                                }
                            });
        }
    }

    private Map<String, String> getThreadClassLoader() throws IOException {
        HttpGet get = new HttpGet("http://localhost:5801/hazelcast/rest/maps/running-threads");
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            CloseableHttpResponse response = client.execute(get);
            String threads = EntityUtils.toString(response.getEntity());
            List<Map<String, String>> value =
                    OBJECT_MAPPER.readValue(
                            threads, new TypeReference<List<Map<String, String>>>() {});
            return value.stream()
                    .collect(
                            Collectors.toMap(
                                    map -> map.get("threadName"),
                                    map -> map.get("classLoader"),
                                    (a, b) -> a + " && " + b));
        }
    }

    /** The thread should be recycled but not, we should fix it in the future. */
    private boolean isIssueWeAlreadyKnow(String threadName) {
        // ClickHouse com.clickhouse.client.ClickHouseClientBuilder
        return threadName.startsWith("ClickHouseClientWorker")
                // InfluxDB okio.AsyncTimeout$Watchdog
                || threadName.startsWith("Okio Watchdog")
                // InfluxDB okhttp3.internal.concurrent.TaskRunner.RealBackend
                || threadName.startsWith("OkHttp TaskRunner")
                // IOTDB org.apache.iotdb.session.Session
                || threadName.startsWith("SessionExecutor")
                // Iceberg org.apache.iceberg.util.ThreadPools.WORKER_POOL
                || threadName.startsWith("iceberg-worker-pool")
                // Oracle Driver
                // oracle.jdbc.driver.BlockSource.ThreadedCachingBlockSource.BlockReleaser
                || threadName.contains(
                        "oracle.jdbc.driver.BlockSource.ThreadedCachingBlockSource.BlockReleaser")
                // RocketMQ
                // org.apache.rocketmq.logging.inner.LoggingBuilder$AsyncAppender$Dispatcher
                || threadName.startsWith("AsyncAppender-Dispatcher-Thread")
                // MongoDB
                || threadName.startsWith("BufferPoolPruner")
                || threadName.startsWith("MaintenanceTimer")
                || threadName.startsWith("cluster-")
                // Iceberg
                || threadName.startsWith("iceberg");
    }

    @Override
    public Container.ExecResult savepointJob(String jobId)
            throws IOException, InterruptedException {
        return savepointJob(server, jobId);
    }

    @Override
    public Container.ExecResult restoreJob(String confFile, String jobId)
            throws IOException, InterruptedException {
        runningCount.incrementAndGet();
        Container.ExecResult result = restoreJob(server, confFile, jobId);
        runningCount.decrementAndGet();
        return result;
    }

    @Override
    public String getServerLogs() {
        return server.getLogs();
    }
}
