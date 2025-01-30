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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.client.util.ContentFormatUtil;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobStatusData;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelCancelJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobCheckpointCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobDetailStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobInfoCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetRunningJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelListJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelSavePointJobCodec;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.HashedMap;

import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_OUTPUT_COUNT;

public class JobClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final SeaTunnelHazelcastClient hazelcastClient;

    public JobClient(@NonNull SeaTunnelHazelcastClient hazelcastClient) {
        this.hazelcastClient = hazelcastClient;
    }

    public long getNewJobId() {
        return hazelcastClient
                .getHazelcastInstance()
                .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)
                .newId();
    }

    public ClientJobProxy createJobProxy(@NonNull JobImmutableInformation jobImmutableInformation) {
        return new ClientJobProxy(hazelcastClient, jobImmutableInformation);
    }

    public ClientJobProxy getJobProxy(@NonNull Long jobId) {
        return new ClientJobProxy(hazelcastClient, jobId);
    }

    public String getJobDetailStatus(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetJobDetailStatusCodec.encodeRequest(jobId),
                SeaTunnelGetJobDetailStatusCodec::decodeResponse);
    }

    /** list all jobId and job status */
    public String listJobStatus(boolean format) {
        String jobStatusStr =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelListJobStatusCodec.encodeRequest(),
                        SeaTunnelListJobStatusCodec::decodeResponse);
        if (!format) {
            return jobStatusStr;
        } else {
            try {
                List<JobStatusData> statusDataList =
                        OBJECT_MAPPER.readValue(
                                jobStatusStr, new TypeReference<List<JobStatusData>>() {});
                statusDataList.sort(
                        (s1, s2) -> {
                            if (s1.getSubmitTime() == s2.getSubmitTime()) {
                                return 0;
                            }
                            return s1.getSubmitTime() > s2.getSubmitTime() ? -1 : 1;
                        });
                return ContentFormatUtil.format(statusDataList);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * get one job status
     *
     * @param jobId jobId
     */
    public String getJobStatus(Long jobId) {
        int jobStatusOrdinal =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelGetJobStatusCodec.encodeRequest(jobId),
                        SeaTunnelGetJobStatusCodec::decodeResponse);
        return JobStatus.values()[jobStatusOrdinal].toString();
    }

    public String getJobMetrics(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetJobMetricsCodec.encodeRequest(jobId),
                SeaTunnelGetJobMetricsCodec::decodeResponse);
    }

    public String getRunningJobMetrics() {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetRunningJobMetricsCodec.encodeRequest(),
                SeaTunnelGetRunningJobMetricsCodec::decodeResponse);
    }

    public void savePointJob(Long jobId) {
        PassiveCompletableFuture<Void> cancelFuture =
                hazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelSavePointJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    public void cancelJob(Long jobId) {
        PassiveCompletableFuture<Void> cancelFuture =
                hazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelCancelJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    public JobDAGInfo getJobInfo(Long jobId) {
        return hazelcastClient
                .getSerializationService()
                .toObject(
                        hazelcastClient.requestOnMasterAndDecodeResponse(
                                SeaTunnelGetJobInfoCodec.encodeRequest(jobId),
                                SeaTunnelGetJobInfoCodec::decodeResponse));
    }

    public JobMetricsRunner.JobMetricsSummary getJobMetricsSummary(Long jobId) {
        long sourceReadCount = 0L;
        long sinkWriteCount = 0L;
        Map<String, Map<String, Object>> transformCountMap = new HashMap<>();
        String jobMetrics = getJobMetrics(jobId);
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(jobMetrics);
            JsonNode sourceReaders = jsonNode.get("SourceReceivedCount");
            JsonNode sinkWriters = jsonNode.get("SinkWriteCount");
            for (int i = 0; i < sourceReaders.size(); i++) {
                JsonNode sourceReader = sourceReaders.get(i);
                JsonNode sinkWriter = sinkWriters.get(i);
                sourceReadCount += sourceReader.get("value").asLong();
                sinkWriteCount += sinkWriter.get("value").asLong();
            }
            transformCountMap = extractTransformKeys(jsonNode);
            return new JobMetricsRunner.JobMetricsSummary(
                    sourceReadCount, sinkWriteCount, transformCountMap);
            // Add NullPointerException because of metrics information can be empty like {}
        } catch (JsonProcessingException | NullPointerException e) {
            return new JobMetricsRunner.JobMetricsSummary(
                    sourceReadCount, sinkWriteCount, transformCountMap);
        }
    }

    private Map<String, Map<String, Object>> extractTransformKeys(JsonNode rootNode) {
        Map<String, Map<String, Object>> transformCountMap = new TreeMap<>();
        Map<String, Map<String, Map<String, JsonNode>>> transformMetricsMaps = new HashedMap();
        rootNode.fieldNames()
                .forEachRemaining(
                        metricName -> {
                            if (metricName.contains("Transform")) {
                                processTransformMetric(transformMetricsMaps, metricName, rootNode);
                            }
                        });
        transformMetricsMaps.forEach(
                (metricName, metricMap) -> {
                    metricMap.forEach(
                            (tableName, pathMap) -> {
                                transformCountMap.put(tableName, aggregateTreeMap(pathMap, false));
                            });
                });
        return transformCountMap;
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

    public List<JobPipelineCheckpointData> getCheckpointData(Long jobId) {
        return hazelcastClient
                .getSerializationService()
                .toObject(
                        hazelcastClient.requestOnMasterAndDecodeResponse(
                                SeaTunnelGetJobCheckpointCodec.encodeRequest(jobId),
                                SeaTunnelGetJobCheckpointCodec::decodeResponse));
    }
}
