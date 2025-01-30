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

package org.apache.seatunnel.e2e.connector.maxcompute;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.MaxcomputeSource;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.MaxcomputeSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class MaxComputeIT extends TestSuiteBase implements TestResource {

    private GenericContainer<?> maxcompute;

    private static final String IMAGE = "maxcompute/maxcompute-emulator:v0.0.7";

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        this.maxcompute =
                new GenericContainer<>(IMAGE)
                        .withExposedPorts(8080)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("maxcompute")
                        .waitingFor(
                                Wait.forLogMessage(
                                        ".*Started MaxcomputeEmulatorApplication.*\\n", 1))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
        Startables.deepStart(Stream.of(this.maxcompute)).join();
        log.info("MaxCompute container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(360L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        initTable();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (this.maxcompute != null) {
            this.maxcompute.stop();
        }
    }

    public Odps getTestOdps() {
        Account account = new AliyunAccount("ak", "sk");
        Odps odps = new Odps(account);
        odps.setEndpoint(getEndpoint());
        odps.setDefaultProject("project");
        odps.setTunnelEndpoint(getEndpoint());
        return odps;
    }

    private void initConnection() throws OdpsException {
        Odps odps = getTestOdps();
        Assertions.assertFalse(odps.tables().exists("test_table"));
    }

    private void initTable() throws OdpsException, IOException {
        Odps odps = getTestOdps();
        createTableWithData(odps, "test_table");
        createTableWithData(odps, "test_table_2");
    }

    private static void createTableWithData(Odps odps, String tableName)
            throws OdpsException, IOException {
        Instance instance =
                SQLTask.run(odps, "create table " + tableName + " (id INT, name STRING, age INT);");
        instance.waitForSuccess();
        Assertions.assertTrue(odps.tables().exists(tableName));
        Instance insert =
                SQLTask.run(
                        odps,
                        "insert into "
                                + tableName
                                + " values (1, 'test', 20), (2, 'test2', 30), (3, 'test3', 40);");
        insert.waitForSuccess();
        Assertions.assertEquals(3, queryTable(odps, tableName).size());
    }

    private static List<Record> queryTable(Odps odps, String tableName) throws OdpsException {
        Instance instance = SQLTask.run(odps, "select * from " + tableName + ";");
        instance.waitForSuccess();
        return SQLTask.getResult(instance);
    }

    private String getEndpoint() {
        String ip;
        if (maxcompute.getHost().equals("localhost")) {
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                ip = "127.0.0.1";
            }
        } else {
            ip = maxcompute.getHost();
        }
        return "http://" + ip + ":" + maxcompute.getFirstMappedPort();
    }

    @TestTemplate
    public void testMaxCompute(TestContainer container)
            throws IOException, InterruptedException, OdpsException {
        Odps odps = getTestOdps();
        odps.tables().delete("project", "test_table_sink", true);
        Container.ExecResult execResult = container.executeJob("/maxcompute_to_maxcompute.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(3, odps.tables().get("test_table_sink").getRecordNum());
        List<Record> records = queryTable(odps, "test_table_sink");
        Assertions.assertEquals(3, records.size());
        Assertions.assertEquals(1, records.get(0).get("id"));
        Assertions.assertEquals("test", records.get(0).get("name"));
        Assertions.assertEquals(20, records.get(0).get("age"));
        Assertions.assertEquals(2, records.get(1).get("id"));
        Assertions.assertEquals("test2", records.get(1).get("name"));
        Assertions.assertEquals(30, records.get(1).get("age"));
        Assertions.assertEquals(3, records.get(2).get("id"));
        Assertions.assertEquals("test3", records.get(2).get("name"));
        Assertions.assertEquals(40, records.get(2).get("age"));
    }

    @TestTemplate
    public void testMaxComputeMultiTable(TestContainer container)
            throws OdpsException, IOException, InterruptedException {
        Odps odps = getTestOdps();
        odps.tables().delete("project", "test_table_sink", true);
        odps.tables().delete("project", "test_table_2_sink", true);
        Container.ExecResult execResult =
                container.executeJob("/maxcompute_to_maxcompute_multi_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(3, queryTable(odps, "test_table_sink").size());
        Assertions.assertEquals(3, queryTable(odps, "test_table_2_sink").size());
    }

    @Test
    public void testReadColumn() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "ak");
        config.put("accesskey", "sk");
        config.put("endpoint", "http://maxcompute:8080/api");
        config.put("project", "project");
        config.put("table_name", "test_table");
        config.put("read_columns", Arrays.asList("id", "name"));
        MaxcomputeSource source =
                (MaxcomputeSource)
                        new MaxcomputeSourceFactory()
                                .createSource(
                                        new TableSourceFactoryContext(
                                                ReadonlyConfig.fromMap(config),
                                                Thread.currentThread().getContextClassLoader()));
        CatalogTable table = source.getProducedCatalogTables().get(0);
        Assertions.assertArrayEquals(
                new String[] {"id", "name"}, table.getTableSchema().getFieldNames());
    }
}
