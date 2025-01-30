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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
public class TestSQLSchemaChangeIT extends TestSuiteBase implements TestResource {
    private static final String DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String DOCKER_IMAGE = "starrocks/allin1-ubuntu:3.3.4";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String HOST = "starrocks_cdc_e2e";
    private static final int SR_PROXY_PORT = 8080;
    private static final int QUERY_PORT = 9030;
    private static final int HTTP_PORT = 8030;
    private static final int BE_HTTP_PORT = 8040;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String SINK_TABLE = "products";
    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS " + DATABASE;
    private static final String SR_DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private Connection starRocksConnection;
    private Connection mysqlConnection;
    private GenericContainer<?> starRocksServer;

    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String QUERY = "select * from %s.%s order by id";
    private static final String QUERY_COLUMNS =
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER by COLUMN_NAME;";
    private static final String PROJECTION_QUERY =
            "select id,name,description,weight,add_column1,add_column2,add_column3 from %s.%s order by id;";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase shopDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, DATABASE, "mysqluser", "mysqlpw", DATABASE);

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + SR_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private void initializeJdbcConnection() throws Exception {
        URLClassLoader urlClassLoader =
                new URLClassLoader(
                        new URL[] {new URL(SR_DRIVER_JAR)},
                        TestSQLSchemaChangeIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        starRocksConnection =
                driver.connect(
                        String.format("jdbc:mysql://%s:%s", starRocksServer.getHost(), QUERY_PORT),
                        props);
    }

    private void initializeStarRocksServer() {
        starRocksServer =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        starRocksServer.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", QUERY_PORT, QUERY_PORT),
                        String.format("%s:%s", HTTP_PORT, HTTP_PORT),
                        String.format("%s:%s", BE_HTTP_PORT, BE_HTTP_PORT)));
        Startables.deepStart(Stream.of(starRocksServer)).join();
        log.info("StarRocks container started");
        // wait for starrocks fully start
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
    }

    @TestTemplate
    public void testStarRocksSinkWithSchemaEvolutionCase(TestContainer container)
            throws InterruptedException, IOException, SQLException {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/sql_transform/mysqlcdc_to_sql_transform_with_schema_change.conf";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        TimeUnit.SECONDS.sleep(20);

        // verify multi table sink
        verifyDataConsistency("orders");
        verifyDataConsistency("customers");

        // waiting for case1 completed
        assertSchemaEvolutionForAddColumns(
                DATABASE, SOURCE_TABLE, SINK_TABLE, mysqlConnection, starRocksConnection);

        assertSchemaEvolutionForDropColumns(
                DATABASE, SOURCE_TABLE, SINK_TABLE, mysqlConnection, starRocksConnection);

        insertNewDataIntoMySQL();
        insertNewDataIntoMySQL();
        // verify incremental
        verifyDataConsistency("orders");

        // savepoint 1
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
        insertNewDataIntoMySQL();
        // case2 drop columns with cdc data at same time
        shopDatabase.setTemplateName("drop_columns").createAndInitialize();

        // restore 1
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

        // waiting for case2 completed
        assertTableStructureAndData(
                DATABASE, SOURCE_TABLE, SINK_TABLE, mysqlConnection, starRocksConnection);

        // savepoint 2
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

        // case3 change column name with cdc data at same time
        shopDatabase.setTemplateName("change_columns").createAndInitialize();

        // case4 modify column data type with cdc data at same time
        shopDatabase.setTemplateName("modify_columns").createAndInitialize();

        // restore 2
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

        // waiting for case3/case4 completed
        assertTableStructureAndData(
                DATABASE, SOURCE_TABLE, SINK_TABLE, mysqlConnection, starRocksConnection);
        insertNewDataIntoMySQL();
        // verify restore
        verifyDataConsistency("orders");
    }

    private void insertNewDataIntoMySQL() throws SQLException {
        mysqlConnection
                .createStatement()
                .execute(
                        "INSERT INTO orders (id, customer_id, order_date, total_amount, status) "
                                + "VALUES (null, 1, '2025-01-04 13:00:00', 498.99, 'pending')");
    }

    private void verifyDataConsistency(String tableName) {
        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(QUERY, DATABASE, tableName),
                                                mysqlConnection),
                                        query(
                                                String.format(QUERY, DATABASE, tableName),
                                                starRocksConnection)));
    }

    private void assertSchemaEvolutionForAddColumns(
            String database,
            String sourceTable,
            String sinkTable,
            Connection sourceConnection,
            Connection sinkConnection) {
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(QUERY, database, sourceTable),
                                                sourceConnection),
                                        query(
                                                String.format(QUERY, database, sinkTable),
                                                sinkConnection)));

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(QUERY_COLUMNS, database, sourceTable),
                                                sourceConnection),
                                        query(
                                                String.format(QUERY_COLUMNS, database, sinkTable),
                                                sinkConnection)));
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            String.format(
                                                    QUERY.replaceAll(
                                                            "order by id",
                                                            "where id >= 128 order by id"),
                                                    database,
                                                    sourceTable),
                                            sourceConnection),
                                    query(
                                            String.format(
                                                    QUERY.replaceAll(
                                                            "order by id",
                                                            "where id >= 128 order by id"),
                                                    database,
                                                    sinkTable),
                                            sinkConnection));
                        });

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            String.format(PROJECTION_QUERY, database, sourceTable),
                                            sourceConnection),
                                    query(
                                            String.format(PROJECTION_QUERY, database, sinkTable),
                                            sinkConnection));
                        });
    }

    private void assertSchemaEvolutionForDropColumns(
            String database,
            String sourceTable,
            String sinkTable,
            Connection sourceConnection,
            Connection sinkConnection) {

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("drop_columns_validate_schema").createAndInitialize();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(QUERY_COLUMNS, database, sourceTable),
                                                sourceConnection),
                                        query(
                                                String.format(QUERY_COLUMNS, database, sinkTable),
                                                sinkConnection)));
    }

    private void assertTableStructureAndData(
            String database,
            String sourceTable,
            String sinkTable,
            Connection sourceConnection,
            Connection sinkConnection) {
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(QUERY_COLUMNS, database, sourceTable),
                                                sourceConnection),
                                        query(
                                                String.format(QUERY_COLUMNS, database, sinkTable),
                                                sinkConnection)));
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(QUERY, database, sourceTable),
                                                sourceConnection),
                                        query(
                                                String.format(QUERY, database, sinkTable),
                                                sinkConnection)));
    }

    private Connection getMysqlJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    @BeforeAll
    @Override
    public void startUp() throws SQLException {
        initializeStarRocksServer();
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        shopDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
        initializeJdbcTable();
        mysqlConnection = getMysqlJdbcConnection();
    }

    @AfterAll
    @Override
    public void tearDown() throws SQLException {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
        if (starRocksServer != null) {
            starRocksServer.close();
        }
        if (starRocksConnection != null) {
            starRocksConnection.close();
        }
        if (mysqlConnection != null) {
            mysqlConnection.close();
        }
    }

    private void initializeJdbcTable() {
        try (Statement statement = starRocksConnection.createStatement()) {
            // create databases
            statement.execute(CREATE_DATABASE);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    private List<List<Object>> query(String sql, Connection connection) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    if (resultSet.getObject(i) instanceof Timestamp) {
                        Timestamp timestamp = resultSet.getTimestamp(i);
                        objects.add(timestamp.toLocalDateTime().format(DATE_TIME_FORMATTER));
                        break;
                    }
                    if (resultSet.getObject(i) instanceof LocalDateTime) {
                        LocalDateTime localDateTime = resultSet.getObject(i, LocalDateTime.class);
                        objects.add(localDateTime.format(DATE_TIME_FORMATTER));
                        break;
                    }
                    objects.add(resultSet.getObject(i));
                }
                log.debug(String.format("Print query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
