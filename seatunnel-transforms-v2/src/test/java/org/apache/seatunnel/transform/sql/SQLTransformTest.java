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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SQLTransformTest {

    private static final String TEST_NAME = "test";
    private static final String TIMESTAMP_FILEDNAME = "create_time";
    private static final String[] FILED_NAMES =
            new String[] {"id", "name", "age", TIMESTAMP_FILEDNAME};
    private static final String GENERATE_PARTITION_KEY = "dt";
    private static final ReadonlyConfig READONLY_CONFIG =
            ReadonlyConfig.fromMap(
                    new HashMap<String, Object>() {
                        {
                            put(
                                    "query",
                                    "select *,FORMATDATETIME(create_time,'yyyy-MM-dd HH:mm') as dt from dual");
                        }
                    });

    @Test
    public void testScaleSupport() {
        SQLTransform sqlTransform = new SQLTransform(READONLY_CONFIG, getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (column.getName().equals(TIMESTAMP_FILEDNAME)) {
                                Assertions.assertEquals(9, column.getScale());
                            } else if (column.getName().equals(GENERATE_PARTITION_KEY)) {
                                Assertions.assertTrue(Objects.isNull(column.getScale()));
                            } else {
                                Assertions.assertEquals(3, column.getColumnLength());
                            }
                        });
    }

    @Test
    public void testQueryWithAnyTable() {
        SQLTransform sqlTransform =
                new SQLTransform(
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("query", "select * from dual");
                                    }
                                }),
                        getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        Assertions.assertEquals(4, tableSchema.getColumns().size());
    }

    @Test
    public void testNotLoseSourceTypeAndOptions() {
        SQLTransform sqlTransform = new SQLTransform(READONLY_CONFIG, getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (!column.getName().equals(GENERATE_PARTITION_KEY)) {
                                Assertions.assertEquals(
                                        "source_" + column.getDataType(), column.getSourceType());
                                Assertions.assertEquals(
                                        "testInSQL", column.getOptions().get("context"));
                            }
                        });
    }

    private CatalogTable getCatalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        FILED_NAMES,
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Integer scale = null;
            Long columnLength = null;
            if (rowType.getFieldName(i).equals(TIMESTAMP_FILEDNAME)) {
                scale = 9;
            } else {
                columnLength = 3L;
            }
            PhysicalColumn column =
                    new PhysicalColumn(
                            rowType.getFieldName(i),
                            rowType.getFieldType(i),
                            columnLength,
                            scale,
                            true,
                            null,
                            null,
                            "source_" + rowType.getFieldType(i),
                            new HashMap<String, Object>() {
                                {
                                    put("context", "testInSQL");
                                }
                            });
            schemaBuilder.column(column);
        }
        return CatalogTable.of(
                TableIdentifier.of(TEST_NAME, TEST_NAME, null, TEST_NAME),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "It has column information.");
    }

    @Test
    public void testEscapeIdentifier() {
        String tableName = "test";
        String[] fields = new String[] {"id", "apply"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select `id`, trim(`apply`) as `apply` from dual where `apply` = 'a'"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), String.valueOf("a")}));
        Assertions.assertEquals("id", tableSchema.getFieldNames()[0]);
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals("a", result.get(0).getField(1));
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), String.valueOf("b")}));
        Assertions.assertNull(result);

        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, IFNULL(`apply`, '1') as `apply` from dual  where `apply` = 'a'"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), String.valueOf("a")}));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals("a", result.get(0).getField(1));

        table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.LONG_TYPE}));
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, `apply` + 1 as `apply` from dual where `apply` > 0"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), Long.valueOf(1)}));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(BasicType.LONG_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals(Long.valueOf(2), result.get(0).getField(1));
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), Long.valueOf(0)}));
        Assertions.assertNull(result);

        table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    new MapType<String, String>(
                                            BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                                }));
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, `apply`.k1 as `apply` from dual where `apply`.k1 = 'a'"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1), Collections.singletonMap("k1", "a")
                                }));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals("a", result.get(0).getField(1));
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1), Collections.singletonMap("k1", "b")
                                }));
        Assertions.assertNull(result);

        table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                new String[] {"id", "map"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    new MapType<String, String>(
                                            BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                                }));
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, map.`apply` as `apply` from dual where map.`apply` = 'a'"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1), Collections.singletonMap("apply", "a")
                                }));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals("a", result.get(0).getField(1));
    }

    @Test
    public void testSchemaChange() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(TEST_NAME, TEST_NAME, null, TEST_NAME),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.LONG_TYPE,
                                                null,
                                                null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "name",
                                                BasicType.STRING_TYPE,
                                                null,
                                                null,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "age",
                                                BasicType.LONG_TYPE,
                                                null,
                                                null,
                                                true,
                                                null,
                                                null))
                                .primaryKey(PrimaryKey.of("pk1", Arrays.asList("id")))
                                .constraintKey(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                "uk1",
                                                Arrays.asList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name",
                                                                ConstraintKey.ColumnSortType.ASC),
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "age",
                                                                ConstraintKey.ColumnSortType.ASC))))
                                .build(),
                        Collections.emptyMap(),
                        Collections.singletonList("name"),
                        null);

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put("query", "select * from dual");
                            }
                        });
        List<SeaTunnelRow> result;
        SQLTransform transform = new SQLTransform(config, catalogTable);
        result =
                transform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {Integer.valueOf(1), "Cosmos", Integer.valueOf(30)}));
        List<String> columnNames;
        List<String> columnType;
        List<String> assertNames;
        List<String> assertTypes;
        Object[] columnValues;
        Object[] assertValue;

        columnNames =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        columnType =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map((e) -> e.getDataType().getSqlType().name())
                        .collect(Collectors.toList());
        assertNames = Lists.newArrayList("id", "name", "age");
        assertTypes = Lists.newArrayList("BIGINT", "STRING", "BIGINT");

        columnValues = result.get(0).getFields();
        assertValue = new Object[] {Integer.valueOf(1), "Cosmos", Integer.valueOf(30)};
        Assertions.assertIterableEquals(columnNames, assertNames);
        Assertions.assertIterableEquals(columnType, assertTypes);
        Assertions.assertArrayEquals(columnValues, assertValue);

        // test add column
        AlterTableAddColumnEvent addColumnEvent =
                AlterTableAddColumnEvent.add(
                        catalogTable.getTableId(),
                        PhysicalColumn.of("f4", BasicType.LONG_TYPE, null, null, true, null, null));
        transform.mapSchemaChangeEvent(addColumnEvent);

        result =
                transform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1),
                                    "Cosmos",
                                    Integer.valueOf(30),
                                    Integer.valueOf(14)
                                }));
        columnNames =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        columnType =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map((e) -> e.getDataType().getSqlType().name())
                        .collect(Collectors.toList());
        assertNames = Lists.newArrayList("id", "name", "age", "f4");
        assertTypes = Lists.newArrayList("BIGINT", "STRING", "BIGINT", "BIGINT");

        columnValues = result.get(0).getFields();
        assertValue =
                new Object[] {
                    Integer.valueOf(1), "Cosmos", Integer.valueOf(30), Integer.valueOf(14)
                };
        Assertions.assertIterableEquals(columnNames, assertNames);
        Assertions.assertIterableEquals(columnType, assertTypes);
        Assertions.assertArrayEquals(columnValues, assertValue);

        // test modify column
        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        catalogTable.getTableId(),
                        PhysicalColumn.of(
                                "f4", BasicType.STRING_TYPE, null, null, true, null, null));
        transform.mapSchemaChangeEvent(modifyColumnEvent);
        result =
                transform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1), "Cosmos", Integer.valueOf(30), "Cosmos"
                                }));
        columnNames =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        columnType =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map((e) -> e.getDataType().getSqlType().name())
                        .collect(Collectors.toList());
        assertNames = Lists.newArrayList("id", "name", "age", "f4");
        assertTypes = Lists.newArrayList("BIGINT", "STRING", "BIGINT", "STRING");

        columnValues = result.get(0).getFields();
        assertValue = new Object[] {Integer.valueOf(1), "Cosmos", Integer.valueOf(30), "Cosmos"};
        Assertions.assertIterableEquals(columnNames, assertNames);
        Assertions.assertIterableEquals(columnType, assertTypes);
        Assertions.assertArrayEquals(columnValues, assertValue);

        // test change column
        AlterTableChangeColumnEvent changeColumnEvent =
                AlterTableChangeColumnEvent.change(
                        catalogTable.getTableId(),
                        "f4",
                        PhysicalColumn.of("f5", BasicType.INT_TYPE, null, null, true, null, null));
        transform.mapSchemaChangeEvent(changeColumnEvent);
        result =
                transform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1),
                                    "Cosmos",
                                    Integer.valueOf(30),
                                    Integer.valueOf(14)
                                }));
        columnNames =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        columnType =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map((e) -> e.getDataType().getSqlType().name())
                        .collect(Collectors.toList());
        assertNames = Lists.newArrayList("id", "name", "age", "f5");
        assertTypes = Lists.newArrayList("BIGINT", "STRING", "BIGINT", "INT");

        columnValues = result.get(0).getFields();
        assertValue =
                new Object[] {
                    Integer.valueOf(1), "Cosmos", Integer.valueOf(30), Integer.valueOf(14)
                };
        Assertions.assertIterableEquals(columnNames, assertNames);
        Assertions.assertIterableEquals(columnType, assertTypes);
        Assertions.assertArrayEquals(columnValues, assertValue);

        // test drop column
        AlterTableDropColumnEvent dropColumnEvent =
                new AlterTableDropColumnEvent(catalogTable.getTableId(), "f5");
        transform.mapSchemaChangeEvent(dropColumnEvent);
        result =
                transform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {Integer.valueOf(1), "Cosmos", Integer.valueOf(30)}));
        columnNames =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        columnType =
                transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                        .map((e) -> e.getDataType().getSqlType().name())
                        .collect(Collectors.toList());
        assertNames = Lists.newArrayList("id", "name", "age");
        assertTypes = Lists.newArrayList("BIGINT", "STRING", "BIGINT");

        columnValues = result.get(0).getFields();
        assertValue = new Object[] {Integer.valueOf(1), "Cosmos", Integer.valueOf(30)};
        Assertions.assertIterableEquals(columnNames, assertNames);
        Assertions.assertIterableEquals(columnType, assertTypes);
        Assertions.assertArrayEquals(columnValues, assertValue);
    }
}
