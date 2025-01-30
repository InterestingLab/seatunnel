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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.DAGUtils;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.operation.CancelJobOperation;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.operation.SavePointJobOperation;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.rest.RestJobExecutionEnvironment;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_OUTPUT_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_BYTES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SINK_WRITE_QPS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TABLE_SOURCE_RECEIVED_QPS;

@Slf4j
public abstract class BaseService {

    protected final NodeEngineImpl nodeEngine;

    public BaseService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
        Map<String, Object> extensionServices =
                nodeEngine.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        if (shouldBeMaster && !seaTunnelServer.isMasterNode()) {
            return null;
        }
        return seaTunnelServer;
    }

    protected JsonObject convertToJson(JobInfo jobInfo, long jobId) {

        JsonObject jobInfoJson = new JsonObject();
        JobImmutableInformation jobImmutableInformation =
                nodeEngine
                        .getSerializationService()
                        .toObject(
                                nodeEngine
                                        .getSerializationService()
                                        .toObject(jobInfo.getJobImmutableInformation()));

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        ClassLoaderService classLoaderService =
                seaTunnelServer == null
                        ? getSeaTunnelServer(false).getClassLoaderService()
                        : seaTunnelServer.getClassLoaderService();
        ClassLoader classLoader =
                classLoaderService.getClassLoader(
                        jobId, jobImmutableInformation.getPluginJarsUrls());
        LogicalDag logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(
                        nodeEngine.getSerializationService(),
                        classLoader,
                        jobImmutableInformation.getLogicalDag());
        classLoaderService.releaseClassLoader(jobId, jobImmutableInformation.getPluginJarsUrls());

        String jobMetrics;
        JobStatus jobStatus;
        if (seaTunnelServer == null) {
            jobMetrics =
                    (String)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            nodeEngine, new GetJobMetricsOperation(jobId))
                                    .join();
            jobStatus =
                    JobStatus.values()[
                            (int)
                                    NodeEngineUtil.sendOperationToMasterNode(
                                                    nodeEngine, new GetJobStatusOperation(jobId))
                                            .join()];
        } else {
            jobMetrics =
                    seaTunnelServer.getCoordinatorService().getJobMetrics(jobId).toJsonString();
            jobStatus = seaTunnelServer.getCoordinatorService().getJobStatus(jobId);
        }

        JobDAGInfo jobDAGInfo =
                DAGUtils.getJobDAGInfo(
                        logicalDag,
                        jobImmutableInformation,
                        getSeaTunnelServer(false).getSeaTunnelConfig().getEngineConfig(),
                        true,
                        new ExecutionAddress(
                                this.nodeEngine.getMasterAddress().getHost(),
                                this.nodeEngine.getMasterAddress().getPort()),
                        new HashSet<>());

        jobInfoJson
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, logicalDag.getJobConfig().getName())
                .add(RestConstant.JOB_STATUS, jobStatus.toString())
                .add(
                        RestConstant.ENV_OPTIONS,
                        JsonUtil.toJsonObject(logicalDag.getJobConfig().getEnvOptions()))
                .add(
                        RestConstant.CREATE_TIME,
                        DateTimeUtils.toString(
                                jobImmutableInformation.getCreateTime(),
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(RestConstant.JOB_DAG, jobDAGInfo.toJsonObject())
                .add(
                        RestConstant.PLUGIN_JARS_URLS,
                        (JsonValue)
                                jobImmutableInformation.getPluginJarsUrls().stream()
                                        .map(
                                                url -> {
                                                    JsonObject jarUrl = new JsonObject();
                                                    jarUrl.add(
                                                            RestConstant.JAR_PATH, url.toString());
                                                    return jarUrl;
                                                })
                                        .collect(JsonArray::new, JsonArray::add, JsonArray::add))
                .add(
                        RestConstant.IS_START_WITH_SAVE_POINT,
                        jobImmutableInformation.isStartWithSavePoint())
                .add(RestConstant.METRICS, metricsToJsonObject(getJobMetrics(jobMetrics)));

        return jobInfoJson;
    }

    protected JsonObject getJobInfoJson(
            JobHistoryService.JobState jobState, String jobMetrics, JobDAGInfo jobDAGInfo) {
        return new JsonObject()
                .add(RestConstant.JOB_ID, String.valueOf(jobState.getJobId()))
                .add(RestConstant.JOB_NAME, jobState.getJobName())
                .add(RestConstant.JOB_STATUS, jobState.getJobStatus().toString())
                .add(RestConstant.ERROR_MSG, jobState.getErrorMessage())
                .add(
                        RestConstant.CREATE_TIME,
                        DateTimeUtils.toString(
                                jobState.getSubmitTime(),
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(
                        RestConstant.FINISH_TIME,
                        DateTimeUtils.toString(
                                jobState.getFinishTime(),
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(RestConstant.JOB_DAG, jobDAGInfo.toJsonObject())
                .add(RestConstant.PLUGIN_JARS_URLS, new JsonArray())
                .add(RestConstant.METRICS, metricsToJsonObject(getJobMetrics(jobMetrics)));
    }

    private Map<String, Object> getJobMetrics(String jobMetrics) {
        Map<String, Object> metricsMap = new HashMap<>();
        // To add metrics, populate the corresponding array,
        String[] countMetricsNames = {
            SOURCE_RECEIVED_COUNT, SINK_WRITE_COUNT, SOURCE_RECEIVED_BYTES, SINK_WRITE_BYTES
        };
        String[] rateMetricsNames = {
            SOURCE_RECEIVED_QPS,
            SINK_WRITE_QPS,
            SOURCE_RECEIVED_BYTES_PER_SECONDS,
            SINK_WRITE_BYTES_PER_SECONDS
        };
        String[] tableCountMetricsNames = {
            TABLE_SOURCE_RECEIVED_COUNT,
            TABLE_SINK_WRITE_COUNT,
            TABLE_SOURCE_RECEIVED_BYTES,
            TABLE_SINK_WRITE_BYTES
        };
        String[] tableRateMetricsNames = {
            TABLE_SOURCE_RECEIVED_QPS,
            TABLE_SINK_WRITE_QPS,
            TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS,
            TABLE_SINK_WRITE_BYTES_PER_SECONDS
        };
        Long[] metricsSums =
                Stream.generate(() -> 0L).limit(countMetricsNames.length).toArray(Long[]::new);
        Double[] metricsRates =
                Stream.generate(() -> 0D).limit(rateMetricsNames.length).toArray(Double[]::new);

        // Used to store various indicators at the table
        Map<String, JsonNode>[] tableMetricsMaps =
                new Map[] {
                    new HashMap<>(), // Source Received Count
                    new HashMap<>(), // Sink Write Count
                    new HashMap<>(), // Source Received Bytes
                    new HashMap<>(), // Sink Write Bytes
                    new HashMap<>(), // Source Received QPS
                    new HashMap<>(), // Sink Write QPS
                    new HashMap<>(), // Source Received Bytes Per Second
                    new HashMap<>() // Sink Write Bytes Per Second
                };
        Map<String, Map<String, Map<String, JsonNode>>> transformMetricsMaps = new HashedMap();
        try {
            JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics);

            jobMetricsStr
                    .fieldNames()
                    .forEachRemaining(
                            metricName -> {
                                if (metricName.contains("Transform")) {
                                    processTransformMetric(
                                            transformMetricsMaps, metricName, jobMetricsStr);
                                    return;
                                }
                                if (metricName.contains("#")) {
                                    String tableName =
                                            TablePath.of(metricName.split("#")[1]).getFullName();
                                    JsonNode metricNode = jobMetricsStr.get(metricName);
                                    processMetric(
                                            metricName, tableName, metricNode, tableMetricsMaps);
                                }
                            });

            // Aggregation summary and rate metrics
            aggregateMetrics(
                    jobMetricsStr,
                    metricsSums,
                    metricsRates,
                    ArrayUtils.addAll(countMetricsNames, rateMetricsNames));

        } catch (JsonProcessingException e) {
            return metricsMap;
        }

        populateMetricsMap(
                metricsMap,
                tableMetricsMaps,
                ArrayUtils.addAll(tableCountMetricsNames, tableRateMetricsNames),
                countMetricsNames.length);
        populateMetricsMap(
                metricsMap,
                Stream.concat(Arrays.stream(metricsSums), Arrays.stream(metricsRates))
                        .toArray(Number[]::new),
                ArrayUtils.addAll(countMetricsNames, rateMetricsNames),
                metricsSums.length);

        transformMetricsMaps.forEach(
                (metricName, metricMap) -> {
                    Map<String, Object> transformMetricsMap = new TreeMap<>();
                    metricMap.forEach(
                            (tableName, pathMap) -> {
                                transformMetricsMap.put(
                                        tableName, aggregateTreeMap(pathMap, false));
                            });

                    metricsMap.put(metricName, transformMetricsMap);
                });

        return metricsMap;
    }

    private void processTransformMetric(
            Map<String, Map<String, Map<String, JsonNode>>> transformMetricsMaps,
            String metricName,
            JsonNode jobMetricsStr) {
        if (metricName.contains(TRANSFORM_OUTPUT_COUNT)) {
            processTransformMetric(
                    transformMetricsMaps, TRANSFORM_OUTPUT_COUNT, metricName, jobMetricsStr);
        }
    }

    private void processTransformMetric(
            Map<String, Map<String, Map<String, JsonNode>>> transformMetricsMaps,
            String key,
            String metricName,
            JsonNode jobMetricsStr) {
        Map<String, Map<String, JsonNode>> transformMetricsMap = transformMetricsMaps.get(key);
        if (MapUtils.isEmpty(transformMetricsMap)) {
            transformMetricsMap = new TreeMap<>();
            transformMetricsMaps.put(key, transformMetricsMap);
        }
        String tableName = TablePath.of(metricName.split("#")[1]).getFullName();
        String path = metricName.split("#")[2];
        Map<String, JsonNode> pathMap;
        if (transformMetricsMap.containsKey(tableName)) {
            pathMap = transformMetricsMap.get(tableName);
        } else {
            pathMap = new TreeMap<>();
        }
        pathMap.put(path, jobMetricsStr.get(metricName));
        transformMetricsMap.put(tableName, pathMap);
    }

    private void processMetric(
            String metricName,
            String tableName,
            JsonNode metricNode,
            Map<String, JsonNode>[] tableMetricsMaps) {
        if (metricNode == null) {
            return;
        }

        // Define index constant
        final int SOURCE_COUNT_IDX = 0,
                SINK_COUNT_IDX = 1,
                SOURCE_BYTES_IDX = 2,
                SINK_BYTES_IDX = 3,
                SOURCE_QPS_IDX = 4,
                SINK_QPS_IDX = 5,
                SOURCE_BYTES_SEC_IDX = 6,
                SINK_BYTES_SEC_IDX = 7;
        if (metricName.startsWith(SOURCE_RECEIVED_COUNT + "#")) {
            tableMetricsMaps[SOURCE_COUNT_IDX].put(tableName, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_COUNT + "#")) {
            tableMetricsMaps[SINK_COUNT_IDX].put(tableName, metricNode);
        } else if (metricName.startsWith(SOURCE_RECEIVED_BYTES + "#")) {
            tableMetricsMaps[SOURCE_BYTES_IDX].put(tableName, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_BYTES + "#")) {
            tableMetricsMaps[SINK_BYTES_IDX].put(tableName, metricNode);
        } else if (metricName.startsWith(SOURCE_RECEIVED_QPS + "#")) {
            tableMetricsMaps[SOURCE_QPS_IDX].put(tableName, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_QPS + "#")) {
            tableMetricsMaps[SINK_QPS_IDX].put(tableName, metricNode);
        } else if (metricName.startsWith(SOURCE_RECEIVED_BYTES_PER_SECONDS + "#")) {
            tableMetricsMaps[SOURCE_BYTES_SEC_IDX].put(tableName, metricNode);
        } else if (metricName.startsWith(SINK_WRITE_BYTES_PER_SECONDS + "#")) {
            tableMetricsMaps[SINK_BYTES_SEC_IDX].put(tableName, metricNode);
        }
    }

    private void aggregateMetrics(
            JsonNode jobMetricsStr,
            Long[] metricsSums,
            Double[] metricsRates,
            String[] metricsNames) {
        for (int i = 0; i < metricsNames.length; i++) {
            JsonNode metricNode = jobMetricsStr.get(metricsNames[i]);
            if (metricNode != null && metricNode.isArray()) {
                for (JsonNode node : metricNode) {
                    // Match Rate Metrics vs. Value Metrics
                    if (i < metricsSums.length) {
                        metricsSums[i] += node.path("value").asLong();
                    } else {
                        metricsRates[i - metricsSums.length] += node.path("value").asDouble();
                    }
                }
            }
        }
    }

    private void populateMetricsMap(
            Map<String, Object> metricsMap,
            Object[] metrics,
            String[] metricNames,
            int countMetricNames) {
        for (int i = 0; i < metrics.length; i++) {
            if (metrics[i] != null) {
                if (metrics[i] instanceof Map) {
                    metricsMap.put(
                            metricNames[i],
                            aggregateMap(
                                    (Map<String, JsonNode>) metrics[i], i >= countMetricNames));
                } else {
                    metricsMap.put(metricNames[i], metrics[i]);
                }
            }
        }
    }

    private Map<String, Object> aggregateMap(Map<String, JsonNode> inputMap, boolean isRate) {
        return isRate
                ? inputMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                StreamSupport.stream(
                                                                entry.getValue().spliterator(),
                                                                false)
                                                        .mapToDouble(
                                                                node ->
                                                                        node.path("value")
                                                                                .asDouble())
                                                        .sum()))
                : inputMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                StreamSupport.stream(
                                                                entry.getValue().spliterator(),
                                                                false)
                                                        .mapToLong(
                                                                node -> node.path("value").asLong())
                                                        .sum()));
    }

    private Map<String, Object> aggregateTreeMap(Map<String, JsonNode> inputMap, boolean isRate) {
        return isRate
                ? inputMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                StreamSupport.stream(
                                                                entry.getValue().spliterator(),
                                                                false)
                                                        .mapToDouble(
                                                                node ->
                                                                        node.path("value")
                                                                                .asDouble())
                                                        .sum(),
                                        (v1, v2) -> v1,
                                        TreeMap::new))
                : inputMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                StreamSupport.stream(
                                                                entry.getValue().spliterator(),
                                                                false)
                                                        .mapToLong(
                                                                node -> node.path("value").asLong())
                                                        .sum(),
                                        (v1, v2) -> v1,
                                        TreeMap::new));
    }

    private JsonObject metricsToJsonObject(Map<String, Object> jobMetrics) {
        JsonObject members = new JsonObject();
        jobMetrics.forEach(
                (key, value) -> {
                    if (value instanceof Map) {
                        members.add(key, metricsToJsonObject((Map<String, Object>) value));
                    } else {
                        members.add(key, value.toString());
                    }
                });
        return members;
    }

    protected JsonNode requestHandle(byte[] requestBody) {
        if (requestBody.length == 0) {
            throw new IllegalArgumentException("Request body is empty.");
        }
        JsonNode requestBodyJsonNode;
        try {
            requestBodyJsonNode = RestUtil.convertByteToJsonNode(requestBody);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON format in request body.");
        }
        return requestBodyJsonNode;
    }

    protected void handleStopJob(
            Map<String, Object> map, SeaTunnelServer seaTunnelServer, Node node) {
        boolean isStopWithSavePoint = false;
        if (map.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("jobId cannot be empty.");
        }
        long jobId = Long.parseLong(map.get(RestConstant.JOB_ID).toString());
        if (map.get(RestConstant.IS_STOP_WITH_SAVE_POINT) != null) {
            isStopWithSavePoint =
                    Boolean.parseBoolean(map.get(RestConstant.IS_STOP_WITH_SAVE_POINT).toString());
        }

        if (!seaTunnelServer.isMasterNode()) {
            if (isStopWithSavePoint) {
                NodeEngineUtil.sendOperationToMasterNode(
                                node.nodeEngine, new SavePointJobOperation(jobId))
                        .join();
            } else {
                NodeEngineUtil.sendOperationToMasterNode(
                                node.nodeEngine, new CancelJobOperation(jobId))
                        .join();
            }

        } else {
            CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();

            if (isStopWithSavePoint) {
                coordinatorService.savePoint(jobId);
            } else {
                coordinatorService.cancelJob(jobId);
            }
        }
    }

    protected String mapToUrlParams(Map<String, String> params) {
        return params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&", "?", ""));
    }

    protected JsonObject submitJobInternal(
            Config config,
            Map<String, String> requestParams,
            SeaTunnelServer seaTunnelServer,
            Node node) {
        ReadonlyConfig envOptions = ReadonlyConfig.fromConfig(config.getConfig("env"));
        String jobName = envOptions.get(EnvCommonOptions.JOB_NAME);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(
                StringUtils.isEmpty(requestParams.get(RestConstant.JOB_NAME))
                        ? jobName
                        : requestParams.get(RestConstant.JOB_NAME));

        boolean startWithSavePoint =
                Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT));
        String jobIdStr = requestParams.get(RestConstant.JOB_ID);
        Long finalJobId = StringUtils.isNotBlank(jobIdStr) ? Long.parseLong(jobIdStr) : null;
        RestJobExecutionEnvironment restJobExecutionEnvironment =
                new RestJobExecutionEnvironment(
                        seaTunnelServer, jobConfig, config, node, startWithSavePoint, finalJobId);
        JobImmutableInformation jobImmutableInformation = restJobExecutionEnvironment.build();
        long jobId = jobImmutableInformation.getJobId();
        if (!seaTunnelServer.isMasterNode()) {

            NodeEngineUtil.sendOperationToMasterNode(
                            node.nodeEngine,
                            new SubmitJobOperation(
                                    jobId,
                                    node.nodeEngine.toData(jobImmutableInformation),
                                    jobImmutableInformation.isStartWithSavePoint()))
                    .join();

        } else {
            submitJob(node, seaTunnelServer, jobImmutableInformation, jobConfig);
        }

        return new JsonObject()
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, jobConfig.getName());
    }

    private void submitJob(
            Node node,
            SeaTunnelServer seaTunnelServer,
            JobImmutableInformation jobImmutableInformation,
            JobConfig jobConfig) {
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        Data data = node.nodeEngine.getSerializationService().toData(jobImmutableInformation);
        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                coordinatorService.submitJob(
                        Long.parseLong(jobConfig.getJobContext().getJobId()),
                        data,
                        jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }

    protected JsonArray getSystemMonitoringInformationJsonValues() {
        Cluster cluster = nodeEngine.getHazelcastInstance().getCluster();

        Set<Member> members = cluster.getMembers();
        JsonArray jsonValues =
                members.stream()
                        .map(
                                member -> {
                                    Address address = member.getAddress();
                                    String input = null;
                                    try {
                                        input =
                                                (String)
                                                        NodeEngineUtil.sendOperationToMemberNode(
                                                                        nodeEngine,
                                                                        new GetClusterHealthMetricsOperation(),
                                                                        address)
                                                                .get();
                                    } catch (InterruptedException | ExecutionException e) {

                                        log.error("Failed to get cluster health metrics", e);
                                    }
                                    String[] parts = input.split(", ");
                                    JsonObject jobInfo = new JsonObject();
                                    Arrays.stream(parts)
                                            .forEach(
                                                    part -> {
                                                        String[] keyValue = part.split("=");
                                                        jobInfo.add(keyValue[0], keyValue[1]);
                                                    });
                                    return jobInfo;
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);
        return jsonValues;
    }
}
