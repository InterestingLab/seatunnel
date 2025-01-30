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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;
import org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType.ZETA;

@Slf4j
public class SQLTransform extends AbstractCatalogSupportFlatMapTransform {
    public static final String PLUGIN_NAME = "Sql";
    private final TableSchemaChangeEventHandler tableSchemaChangeEventHandler;

    public static final Option<String> KEY_QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("The query SQL");

    public static final Option<String> KEY_ENGINE =
            Options.key("engine")
                    .stringType()
                    .defaultValue(ZETA.name())
                    .withDescription("The SQL engine type");

    private final String query;

    private final EngineType engineType;

    private SeaTunnelRowType outRowType;

    private transient SQLEngine sqlEngine;

    private final String inputTableName;

    public SQLTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.tableSchemaChangeEventHandler = new TableSchemaChangeEventDispatcher();
        this.query = config.get(KEY_QUERY);
        if (config.getOptional(KEY_ENGINE).isPresent()) {
            this.engineType = EngineType.valueOf(config.get(KEY_ENGINE).toUpperCase());
        } else {
            this.engineType = ZETA;
        }

        List<String> pluginInputIdentifiers = config.get(CommonOptions.PLUGIN_INPUT);
        if (pluginInputIdentifiers != null && !pluginInputIdentifiers.isEmpty()) {
            this.inputTableName = pluginInputIdentifiers.get(0);
        } else {
            this.inputTableName = catalogTable.getTableId().getTableName();
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        sqlEngine = SQLEngineFactory.getSQLEngine(engineType);
        sqlEngine.init(
                inputTableName,
                inputCatalogTable.getTableId().getTableName(),
                inputCatalogTable.getSeaTunnelRowType(),
                query);
    }

    private void tryOpen() {
        if (sqlEngine == null) {
            open();
        }
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return sqlEngine.transformBySQL(inputRow, outRowType);
    }

    @Override
    protected TableSchema transformTableSchema() {
        return convertTableSchema(inputCatalogTable.getTableSchema());
    }

    protected TableSchema convertTableSchema(TableSchema tableSchema) {
        tryOpen();
        List<String> inputColumnsMapping = new ArrayList<>();
        outRowType = sqlEngine.typeMapping(inputColumnsMapping);
        List<String> outputColumns = Arrays.asList(outRowType.getFieldNames());

        TableSchema.Builder builder = TableSchema.builder();
        if (tableSchema.getPrimaryKey() != null
                && outputColumns.containsAll(tableSchema.getPrimaryKey().getColumnNames())) {
            builder.primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey().copy());
        }

        List<ConstraintKey> outputConstraintKeys =
                tableSchema.getConstraintKeys().stream()
                        .filter(
                                key -> {
                                    List<String> constraintColumnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .collect(Collectors.toList());
                                    return outputColumns.containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        builder.constraintKey(outputConstraintKeys);

        String[] fieldNames = outRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = outRowType.getFieldTypes();
        List<Column> columns = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Column simpleColumn = null;
            String inputColumnName = inputColumnsMapping.get(i);
            if (inputColumnName != null) {
                for (Column inputColumn : tableSchema.getColumns()) {
                    if (inputColumnName.equals(inputColumn.getName())) {
                        simpleColumn = inputColumn;
                        break;
                    }
                }
            }
            Column column;
            if (simpleColumn != null) {
                column =
                        new PhysicalColumn(
                                fieldNames[i],
                                fieldTypes[i],
                                simpleColumn.getColumnLength(),
                                simpleColumn.getScale(),
                                simpleColumn.isNullable(),
                                simpleColumn.getDefaultValue(),
                                simpleColumn.getComment(),
                                simpleColumn.getSourceType(),
                                simpleColumn.getOptions());
            } else {
                column = PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, true, null, null);
            }
            columns.add(column);
        }
        return builder.columns(columns).build();
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {

        TableSchema newTableSchema =
                tableSchemaChangeEventHandler
                        .reset(inputCatalogTable.getTableSchema())
                        .apply(event);
        this.inputCatalogTable =
                CatalogTable.of(
                        inputCatalogTable.getTableId(),
                        newTableSchema,
                        inputCatalogTable.getOptions(),
                        inputCatalogTable.getPartitionKeys(),
                        inputCatalogTable.getComment());
        sqlEngine.init(
                inputTableName,
                inputCatalogTable.getTableId().getTableName(),
                inputCatalogTable.getSeaTunnelRowType(),
                query);
        sqlEngine.resetAllColumnsCount();
        restProducedCatalogTable();

        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent alterTableColumnsEvent = (AlterTableColumnsEvent) event;
            AlterTableColumnsEvent newEvent =
                    new AlterTableColumnsEvent(
                            event.tableIdentifier(),
                            alterTableColumnsEvent.getEvents().stream()
                                    .map(this::convertName)
                                    .collect(Collectors.toList()));

            newEvent.setJobId(event.getJobId());
            newEvent.setStatement(((AlterTableColumnsEvent) event).getStatement());
            newEvent.setSourceDialectName(((AlterTableColumnsEvent) event).getSourceDialectName());
            if (event.getChangeAfter() != null) {
                newEvent.setChangeAfter(
                        CatalogTable.of(
                                event.getChangeAfter().getTableId(), event.getChangeAfter()));
            }
            return newEvent;
        }
        if (event instanceof AlterTableColumnEvent) {
            return convertName((AlterTableColumnEvent) event);
        }
        return event;
    }

    @VisibleForTesting
    public AlterTableColumnEvent convertName(AlterTableColumnEvent event) {
        AlterTableColumnEvent newEvent = event;
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                newEvent =
                        new AlterTableAddColumnEvent(
                                event.tableIdentifier(),
                                addColumnEvent.getColumn(),
                                addColumnEvent.isFirst(),
                                addColumnEvent.getAfterColumn());
                break;
            case SCHEMA_CHANGE_DROP_COLUMN:
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                newEvent =
                        new AlterTableDropColumnEvent(
                                event.tableIdentifier(), convertName(dropColumnEvent.getColumn()));
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                AlterTableModifyColumnEvent modifyColumnEvent = (AlterTableModifyColumnEvent) event;
                newEvent =
                        new AlterTableModifyColumnEvent(
                                event.tableIdentifier(),
                                convertName(modifyColumnEvent.getColumn()),
                                modifyColumnEvent.isFirst(),
                                convertName(modifyColumnEvent.getAfterColumn()));
                break;
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                boolean nameChanged =
                        !changeColumnEvent
                                .getOldColumn()
                                .equals(changeColumnEvent.getColumn().getName());
                if (nameChanged) {
                    log.warn(
                            "FieldRenameTransform does not support changing column name, "
                                    + "old column name: {}, new column name: {}",
                            changeColumnEvent.getOldColumn(),
                            changeColumnEvent.getColumn().getName());
                    return changeColumnEvent;
                }

                newEvent =
                        new AlterTableChangeColumnEvent(
                                event.tableIdentifier(),
                                convertName(changeColumnEvent.getOldColumn()),
                                convertName(changeColumnEvent.getColumn()),
                                changeColumnEvent.isFirst(),
                                convertName(changeColumnEvent.getAfterColumn()));
                break;
            default:
                log.warn("Unsupported event: {}", event);
                return event;
        }

        newEvent.setJobId(event.getJobId());
        newEvent.setStatement(event.getStatement());
        newEvent.setSourceDialectName(event.getSourceDialectName());
        if (event.getChangeAfter() != null) {
            CatalogTable newChangeAfter =
                    CatalogTable.of(
                            event.getChangeAfter().getTableId(),
                            convertTableSchema(event.getChangeAfter().getTableSchema()),
                            event.getChangeAfter().getOptions(),
                            event.getChangeAfter().getPartitionKeys(),
                            event.getChangeAfter().getComment());
            newEvent.setChangeAfter(newChangeAfter);
        }
        return newEvent;
    }

    @VisibleForTesting
    public String convertName(String name) {
        return sqlEngine.getChangeColumnName(name);
    }

    private Column convertName(Column column) {
        return column.rename(convertName(column.getName()));
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public void close() {
        sqlEngine.close();
    }
}
