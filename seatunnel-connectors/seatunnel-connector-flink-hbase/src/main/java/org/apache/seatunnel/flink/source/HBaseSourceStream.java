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

package org.apache.seatunnel.flink.source;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.factory.SeatunnelHBaseTableFactory;
import org.apache.seatunnel.flink.stream.FlinkStreamSource;
import org.apache.seatunnel.flink.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.seatunnel.flink.factory.SeatunnelHBaseTableFactory.CONNECTOR_PROPERTIES;

public class HBaseSourceStream implements FlinkStreamSource<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSourceStream.class);

    private Config config;
    private String tableName;
    private String zookeeperQuorum;
    // optional: the root dir in Zookeeper for HBase cluster.
    // The default value is "/hbase".
    private String zookeeperZnodeParent;

    private Object schemaInfo;
    private String format;

    private static final String SCHEMA = "schema";
    private static final String SOURCE_FORMAT = "format.type";

    private final Map<String, String> properties = Maps.newHashMap();

    private SeatunnelHBaseTableFactory seatunnelHBaseTableFactory;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.check(config, "zookeeper_quorum", SOURCE_FORMAT, SCHEMA, "table_name");
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        this.seatunnelHBaseTableFactory = new SeatunnelHBaseTableFactory();

        properties.putAll(seatunnelHBaseTableFactory.requiredContext());

        this.zookeeperQuorum = config.getString("zookeeper_quorum");
        properties.put(CONNECTOR_ZK_QUORUM, zookeeperQuorum);

        this.tableName = config.getString("table_name");
        properties.put(CONNECTOR_TABLE_NAME, tableName);

        if (config.hasPath("zookeeper_znode_parent")) {
            this.zookeeperZnodeParent = config.getString("zookeeper_znode_parent");
            if (StringUtils.isNotEmpty(CONNECTOR_ZK_NODE_PARENT)) {
                properties.put(CONNECTOR_ZK_NODE_PARENT, this.zookeeperZnodeParent);
            }
        }

        // config schema information
        String schemaContent = config.getString(SCHEMA);
        schemaInfo = JSONObject.parse(schemaContent, Feature.OrderedField);
        format = config.getString(SOURCE_FORMAT);
        properties.putAll(getSchema().toProperties());

        // get all hbase configuration
        if (config.hasPath("hbase.configuration")) {
            Config hbaseConfiguration = this.config.getConfig("hbase.configuration");
            Map<String, ConfigValue> hbaseProperties = hbaseConfiguration.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
            hbaseProperties.forEach((k, v) -> {
                properties.put(CONNECTOR_PROPERTIES + "." + k, (String) v.unwrapped());
            });
        }
    }

    private Schema getSchema() {
        Schema schema = new Schema();
        SchemaUtil.setSchema(schema, schemaInfo, format);
        return schema;
    }

    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();

        HBaseTableSource tableSource =
                (HBaseTableSource) seatunnelHBaseTableFactory.createStreamTableSource(properties);

        String uniqueTableName = SchemaUtil.getUniqueTableName();
        tableEnvironment.registerTableSource(uniqueTableName, tableSource);

        LOGGER.info("hbase register table name,{}", uniqueTableName);

        return tableSource.getDataStream(env.getStreamExecutionEnvironment());
    }
}
