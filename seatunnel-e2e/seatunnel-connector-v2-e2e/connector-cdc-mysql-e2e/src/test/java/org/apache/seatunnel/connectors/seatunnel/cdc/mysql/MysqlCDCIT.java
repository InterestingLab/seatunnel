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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class MysqlCDCIT extends TestSuiteBase implements TestResource {

    // mysql
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String MYSQL_DATABASE2 = "mysql_cdc2";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);
    private final String QUERY_SQL = "select * from %s.%s";

    // mysql source table query sql
    private static final String SOURCE_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_longblob as char) as f_longblob, cast(f_tinyblob as char) as f_tinyblob, cast(f_varbinary as char) as f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                    + " f_enum, cast(f_mediumblob as char) as f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, f_year from %s.%s";
    // mysql sink table query sql
    private static final String SINK_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_longblob as char) as f_longblob, cast(f_tinyblob as char) as f_tinyblob, cast(f_varbinary as char) as f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                    + " f_enum, cast(f_mediumblob as char) as f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, cast(f_year as year) from %s.%s";

    private static final String SOURCE_TABLE_1 = "mysql_cdc_e2e_source_table";
    private static final String SOURCE_TABLE_2 = "mysql_cdc_e2e_source_table2";
    private static final String SOURCE_TABLE_NO_PRIMARY_KEY =
            "mysql_cdc_e2e_source_table_no_primary_key";

    private static final String SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY =
            "mysql_cdc_e2e_source_table_1_custom_primary_key";
    private static final String SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY =
            "mysql_cdc_e2e_source_table_2_custom_primary_key";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table";

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @TestTemplate
    public void testMysqlCdcCheckDataE2e(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "This case requires obtaining the task health status and manually canceling the canceled task, which is currently only supported by the zeta engine.")
    public void testMysqlCdcMetadataTrans(TestContainer container) throws InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SINK_TABLE);
        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_metadata_trans.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        TimeUnit.SECONDS.sleep(10);
        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        TimeUnit.SECONDS.sleep(10);
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String jobStatus = container.getJobStatus(String.valueOf(jobId));
                            Assertions.assertEquals("RUNNING", jobStatus);
                        });
        try {
            Container.ExecResult cancelJobResult = container.cancelJob(String.valueOf(jobId));
            Assertions.assertEquals(0, cancelJobResult.getExitCode(), cancelJobResult.getStderr());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    public void testMysqlCdcCheckDataWithDisableExactlyonce(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_disable_exactly_once.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // insert update delete
        executeSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE_1);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @TestTemplate
    public void testMysqlCdcCheckDataWithNoPrimaryKey(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_no_primary_key.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(
                                            getSourceQuerySQL(
                                                    MYSQL_DATABASE, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // insert update delete
        executeSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE_NO_PRIMARY_KEY);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_NO_PRIMARY_KEY);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            getSourceQuerySQL(
                                                    MYSQL_DATABASE, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testMysqlCdcMultiTableE2e(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_2);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_2);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_two_table.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_2);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_1)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_2)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_2)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_2);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_2);

        // init
        initSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_one_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // wait for data written to sink
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        query(getSourceQuerySQL(MYSQL_DATABASE2, SOURCE_TABLE_1))
                                                        .size()
                                                > 1));

        // Restore job with snapshot read phase
        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_one_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // insert update delete
        changeSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(300000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                        query(getSourceQuerySQL(MYSQL_DATABASE2, SOURCE_TABLE_1))));
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_source").size() == 1);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_sink").size() == 1);

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // Restore job with add a new table
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_two_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_2);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_1)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_2)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_2)))));

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_source").size() == 1);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_sink").size() == 1);

        log.info("****************** container logs start ******************");
        String containerLogs = container.getServerLogs();
        log.info(containerLogs);
        Assertions.assertFalse(containerLogs.contains("ERROR"));
        log.info("****************** container logs end ******************");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testMysqlCdcMultiTableWithCustomPrimaryKey(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY);
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_custom_primary_key.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testMysqlCdcByWildcardsConfig(TestContainer container)
            throws IOException, InterruptedException {
        inventoryDatabase.setTemplateName("wildcards").createAndInitialize();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_wildcards_to_mysql.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        TimeUnit.SECONDS.sleep(5);
        inventoryDatabase.setTemplateName("wildcards_dml").createAndInitialize();
        given().pollDelay(20, TimeUnit.SECONDS)
                .pollInterval(2000, TimeUnit.MILLISECONDS)
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertAll(
                                    () -> {
                                        log.info(
                                                query(getQuerySQL("sink", "source_products"))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(getQuerySQL("source", "products")),
                                                query(getQuerySQL("sink", "source_products")));
                                    },
                                    () -> {
                                        log.info(
                                                query(getQuerySQL("sink", "source_customers"))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(getQuerySQL("source", "customers")),
                                                query(getQuerySQL("sink", "source_customers")));
                                    },
                                    () -> {
                                        log.info(
                                                query(getQuerySQL("sink", "source1_orders"))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(getQuerySQL("source1", "orders")),
                                                query(getQuerySQL("sink", "source1_orders")));
                                    });
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private List<List<Object>> getConnectionStatus(String user) {
        return query(
                "select USER,HOST,DB,COMMAND,TIME,STATE from information_schema.processlist where USER = '"
                        + user
                        + "'");
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                log.debug(String.format("Print MySQL-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void initSourceTable(String database, String tableName) {
        for (int i = 1; i < 100; i++) {
            executeSql(
                    "INSERT INTO "
                            + database
                            + "."
                            + tableName
                            + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                            + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                            + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                            + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                            + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                            + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                            + "VALUES ( "
                            + i
                            + ", 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                            + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                            + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                            + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                            + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                            + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                            + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                            + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        }
    }

    private void changeSourceTable(String database, String tableName) {
        for (int i = 100; i < 110; i++) {
            executeSql(
                    "INSERT INTO "
                            + database
                            + "."
                            + tableName
                            + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                            + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                            + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                            + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                            + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                            + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                            + "VALUES ( "
                            + i
                            + ", 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                            + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                            + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                            + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                            + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                            + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                            + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                            + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        }

        executeSql("DELETE FROM " + database + "." + tableName + " where id > 100");

        executeSql("UPDATE " + database + "." + tableName + " SET f_bigint = 10000 where id < 10");
    }

    private void upsertDeleteSourceTable(String database, String tableName) {

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 5, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 6, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1999 )");
        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2");

        executeSql("UPDATE " + database + "." + tableName + " SET f_bigint = 10000 where id = 3");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMysqlCdcEarliestOffset(TestContainer container)
            throws IOException, InterruptedException {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mysqlcdc_earliest_offset.conf";
        // Purge binary log at first
        purgeBinaryLogs();
        // Insert data
        executeSql(
                String.format(
                        "INSERT INTO %s.%s ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                                + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                                + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                                + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                                + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                                + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                                + "VALUES ( 11, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                                + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                                + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                                + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                                + "         'This is a text field', 'This is a tiny text field', '测试字段4', '2022-04-27', '2022-04-27 14:30:00',\n"
                                + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                                + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                                + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value4\" }', 2022 )",
                        MYSQL_DATABASE, SOURCE_TABLE_1));
        executeSql(
                String.format(
                        "INSERT INTO %s.%s ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                                + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                                + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                                + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                                + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                                + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                                + "VALUES ( 12, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                                + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,\n"
                                + "         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321,\n"
                                + "         123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',\n"
                                + "         'This is a tiny text field', '测试字段5', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',\n"
                                + "         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                                + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                                + "         112.345, '14:30:00', -128, 22, '{ \"key\": \"value5\" }', 2013 )",
                        MYSQL_DATABASE, SOURCE_TABLE_1));

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // verify data
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // Take a savepoint
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
        // Make some changes after the savepoint
        executeSql(
                String.format(
                        "UPDATE %s.%s SET f_year = '2025' WHERE id = 12",
                        MYSQL_DATABASE, SOURCE_TABLE_1));

        // Restart the job from savepoint
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // Make some changes after the restore
        executeSql(
                String.format(
                        "UPDATE %s.%s SET f_tinyint_unsigned = '88' WHERE id = 12",
                        MYSQL_DATABASE, SOURCE_TABLE_1));

        // verify data
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMysqlCdcSpecificOffset(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mysqlcdc_specific_offset.conf";
        String source_sql_where_id_template =
                "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                        + " cast(f_longblob as char) as f_longblob, cast(f_tinyblob as char) as f_tinyblob, cast(f_varbinary as char) as f_varbinary,"
                        + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                        + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                        + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                        + " f_enum, cast(f_mediumblob as char) as f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                        + " f_json, f_year from %s.%s where id in (%s)";

        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        // Purge binary log at first
        purgeBinaryLogs();
        // Record current binlog offset
        BinlogOffset currentBinlogOffset = getCurrentBinlogOffset();

        List<String> variables = new ArrayList<>();
        variables.add("specific_offset_file=" + currentBinlogOffset.getFilename());
        variables.add("specific_offset_pos=" + currentBinlogOffset.getPosition());

        // Insert data
        executeSql(
                String.format(
                        "INSERT INTO %s.%s ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                                + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                                + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                                + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                                + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                                + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                                + "VALUES ( 14, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                                + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                                + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                                + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                                + "         'This is a text field', 'This is a tiny text field', '测试字段4', '2022-04-27', '2022-04-27 14:30:00',\n"
                                + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                                + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                                + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value4\" }', 2022 )",
                        MYSQL_DATABASE, SOURCE_TABLE_1));
        executeSql(
                String.format(
                        "INSERT INTO %s.%s ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                                + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                                + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                                + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                                + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                                + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                                + "VALUES ( 15, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                                + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,\n"
                                + "         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321,\n"
                                + "         123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',\n"
                                + "         'This is a tiny text field', '测试字段5', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',\n"
                                + "         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                                + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                                + "         112.345, '14:30:00', -128, 22, '{ \"key\": \"value5\" }', 2013 )",
                        MYSQL_DATABASE, SOURCE_TABLE_1));

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId, variables);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // validate results
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            String.format(
                                                    source_sql_where_id_template,
                                                    MYSQL_DATABASE,
                                                    SOURCE_TABLE_1,
                                                    "14,15")),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // Take a savepoint
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
        // Make some changes after the savepoint
        executeSql(
                String.format(
                        "UPDATE %s.%s SET f_year = '2025' WHERE id = 15",
                        MYSQL_DATABASE, SOURCE_TABLE_1));

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId, variables);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // Make some changes after the restore
        executeSql(
                String.format(
                        "UPDATE %s.%s SET f_tinyint_unsigned = '77' WHERE id = 15",
                        MYSQL_DATABASE, SOURCE_TABLE_1));

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            String.format(
                                                    source_sql_where_id_template,
                                                    MYSQL_DATABASE,
                                                    SOURCE_TABLE_1,
                                                    "14,15")),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @Override
    @AfterAll
    public void tearDown() {
        // close Container
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    private String getSourceQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    private String getSinkQuerySQL(String database, String tableName) {
        return String.format(SINK_SQL_TEMPLATE, database, tableName);
    }

    private String getQuerySQL(String database, String tableName) {
        return String.format(QUERY_SQL, database, tableName);
    }

    private BinlogOffset getCurrentBinlogOffset() {
        JdbcSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .username(MYSQL_CONTAINER.getUsername())
                        .password(MYSQL_CONTAINER.getPassword())
                        .databaseList(MYSQL_CONTAINER.getDatabaseName());
        MySqlDialect mySqlDialect =
                new MySqlDialect((MySqlSourceConfigFactory) configFactory, Collections.emptyList());
        JdbcConnection jdbcConnection = mySqlDialect.openJdbcConnection(configFactory.create(0));
        return MySqlConnectionUtils.currentBinlogOffset(jdbcConnection);
    }

    private void purgeBinaryLogs() {
        executeSql(
                String.format("PURGE BINARY LOGS TO '%s'", getCurrentBinlogOffset().getFilename()));
    }
}
