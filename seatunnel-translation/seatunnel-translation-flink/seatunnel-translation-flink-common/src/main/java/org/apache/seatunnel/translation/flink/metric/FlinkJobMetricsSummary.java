/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.metric;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;

import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.JobExecutionResult;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_OUTPUT_COUNT;

public final class FlinkJobMetricsSummary {

    private final JobExecutionResult jobExecutionResult;

    private final LocalDateTime jobStartTime;

    private final LocalDateTime jobEndTime;

    FlinkJobMetricsSummary(
            JobExecutionResult jobExecutionResult,
            LocalDateTime jobStartTime,
            LocalDateTime jobEndTime) {
        this.jobExecutionResult = jobExecutionResult;
        this.jobStartTime = jobStartTime;
        this.jobEndTime = jobEndTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private JobExecutionResult jobExecutionResult;

        private long jobStartTime;

        private long jobEndTime;

        private Builder() {}

        public Builder jobExecutionResult(JobExecutionResult jobExecutionResult) {
            this.jobExecutionResult = jobExecutionResult;
            return this;
        }

        public Builder jobStartTime(long jobStartTime) {
            this.jobStartTime = jobStartTime;
            return this;
        }

        public Builder jobEndTime(long jobEndTime) {
            this.jobEndTime = jobEndTime;
            return this;
        }

        public FlinkJobMetricsSummary build() {
            return new FlinkJobMetricsSummary(
                    jobExecutionResult,
                    DateTimeUtils.parse(jobStartTime),
                    DateTimeUtils.parse(jobEndTime));
        }
    }

    @Override
    public String toString() {
        Map<String, Map<String, Object>> transformCountMap = new TreeMap<>();

        // Transform metrics
        jobExecutionResult.getAllAccumulatorResults().entrySet().stream()
                .filter(entry -> entry.getKey().contains(TRANSFORM_OUTPUT_COUNT))
                .forEach(
                        (metric) -> {
                            String tableName = metric.getKey().split("#")[1];
                            String path = metric.getKey().split("#")[2];
                            if (transformCountMap.containsKey(tableName)) {
                                Map<String, Object> metricMap = transformCountMap.get(tableName);
                                metricMap.put(path, metric.getValue());
                            } else {
                                Map<String, Object> metricMap = new HashMap<>();
                                metricMap.put(path, metric.getValue());
                                transformCountMap.put(tableName, metricMap);
                            }
                        });

        StringBuilder logMessage = new StringBuilder();
        logMessage.append(
                StringFormatUtils.formatTable(
                        "Job Statistic Information",
                        "Start Time",
                        DateTimeUtils.toString(
                                jobStartTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                        "End Time",
                        DateTimeUtils.toString(
                                jobEndTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                        "Total Time(s)",
                        Duration.between(jobStartTime, jobEndTime).getSeconds(),
                        "Total Read Count",
                        jobExecutionResult
                                .getAllAccumulatorResults()
                                .get(MetricNames.SOURCE_RECEIVED_COUNT),
                        "Total Write Count",
                        jobExecutionResult
                                .getAllAccumulatorResults()
                                .get(MetricNames.SINK_WRITE_COUNT),
                        "Total Read Bytes",
                        jobExecutionResult
                                .getAllAccumulatorResults()
                                .get(MetricNames.SOURCE_RECEIVED_BYTES),
                        "Total Write Bytes",
                        jobExecutionResult
                                .getAllAccumulatorResults()
                                .get(MetricNames.SINK_WRITE_BYTES)));

        if (MapUtils.isNotEmpty(transformCountMap)) {
            transformCountMap.forEach(
                    (tableName, metrics) -> {
                        String[] transformInfos = new String[metrics.entrySet().size() * 2 + 2];
                        transformInfos[0] = tableName + " Information";
                        int index = 0;
                        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
                            transformInfos[++index] = entry.getKey();
                            transformInfos[++index] = String.valueOf(entry.getValue());
                        }
                        if (Objects.nonNull(transformInfos)) {
                            logMessage.append(StringFormatUtils.formatTable(transformInfos));
                        }
                    });
        }

        return logMessage.toString();
    }
}
