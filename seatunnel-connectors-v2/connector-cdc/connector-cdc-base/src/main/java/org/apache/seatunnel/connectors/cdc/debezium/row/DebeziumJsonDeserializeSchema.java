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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.debezium.AbstractDebeziumDeserializationSchema;
import org.apache.seatunnel.format.compatible.debezium.json.CompatibleDebeziumJsonDeserializationSchema;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isHeartbeatRecord;

@Slf4j
public class DebeziumJsonDeserializeSchema
        extends AbstractDebeziumDeserializationSchema<SeaTunnelRow> {
    private static final String KEY_SCHEMA_ENABLE = "key.converter.schemas.enable";
    private static final String VALUE_SCHEMA_ENABLE = "value.converter.schemas.enable";

    private final CompatibleDebeziumJsonDeserializationSchema deserializationSchema;

    public DebeziumJsonDeserializeSchema(Map<String, String> debeziumConfig) {
        this(debeziumConfig, new HashMap<>());
    }

    public DebeziumJsonDeserializeSchema(
            Map<String, String> debeziumConfig, Map<TableId, Struct> tableIdTableChangeMap) {
        super(tableIdTableChangeMap);
        boolean keySchemaEnable =
                Boolean.valueOf(debeziumConfig.getOrDefault(KEY_SCHEMA_ENABLE, "true"));
        boolean valueSchemaEnable =
                Boolean.valueOf(debeziumConfig.getOrDefault(VALUE_SCHEMA_ENABLE, "true"));
        this.deserializationSchema =
                new CompatibleDebeziumJsonDeserializationSchema(keySchemaEnable, valueSchemaEnable);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<SeaTunnelRow> out) throws Exception {
        super.deserialize(record, out);
        if (!isHeartbeatRecord(record)) {
            SeaTunnelRow row = deserializationSchema.deserialize(record);
            out.collect(row);
            return;
        }

        log.debug("Unsupported record {}, just skip.", record);
    }

    @Override
    public List<CatalogTable> getProducedType() {
        return CatalogTableUtil.convertDataTypeToCatalogTables(
                deserializationSchema.getProducedType(), "default.default");
    }
}
