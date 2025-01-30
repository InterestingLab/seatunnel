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

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;

import org.apache.commons.collections4.MapUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class JobMetricsRunner implements Runnable {
    private final SeaTunnelClient seaTunnelClient;
    private final Long jobId;
    private LocalDateTime lastRunTime = LocalDateTime.now();
    private Long lastReadCount = 0L;
    private Long lastWriteCount = 0L;
    private Map<String, Long> transformCountMap = new HashMap<>();

    public JobMetricsRunner(SeaTunnelClient seaTunnelClient, Long jobId) {
        this.seaTunnelClient = seaTunnelClient;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("job-metrics-runner-" + jobId);
        try {
            JobMetricsSummary jobMetricsSummary = seaTunnelClient.getJobMetricsSummary(jobId);
            LocalDateTime now = LocalDateTime.now();
            long seconds = Duration.between(lastRunTime, now).getSeconds();
            long averageRead = (jobMetricsSummary.getSourceReadCount() - lastReadCount) / seconds;
            long averageWrite = (jobMetricsSummary.getSinkWriteCount() - lastWriteCount) / seconds;
            StringBuilder logMessage = new StringBuilder();
            logMessage.append(
                    StringFormatUtils.formatTable(
                            "Job Progress Information",
                            "Job Id",
                            jobId,
                            "Read Count So Far",
                            jobMetricsSummary.getSourceReadCount(),
                            "Write Count So Far",
                            jobMetricsSummary.getSinkWriteCount(),
                            "Average Read Count",
                            averageRead + "/s",
                            "Average Write Count",
                            averageWrite + "/s",
                            "Last Statistic Time",
                            DateTimeUtils.toString(
                                    lastRunTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                            "Current Statistic Time",
                            DateTimeUtils.toString(
                                    now, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)));

            if (MapUtils.isNotEmpty(jobMetricsSummary.getTransformMetricsMaps())) {
                jobMetricsSummary
                        .getTransformMetricsMaps()
                        .forEach(
                                (tableName, metrics) -> {
                                    String[] transformInfos =
                                            new String[metrics.entrySet().size() * 2 + 1];
                                    transformInfos[0] = tableName + "Transform";
                                    int index = 0;
                                    for (Map.Entry<String, Object> entry : metrics.entrySet()) {
                                        transformInfos[++index] = entry.getKey();
                                        transformInfos[++index] = String.valueOf(entry.getValue());
                                    }
                                    if (Objects.nonNull(transformInfos)) {
                                        logMessage.append(
                                                StringFormatUtils.formatTable(transformInfos));
                                    }
                                });
            }

            log.info("{}", logMessage);
            lastRunTime = now;
            lastReadCount = jobMetricsSummary.getSourceReadCount();
            lastWriteCount = jobMetricsSummary.getSinkWriteCount();
        } catch (Exception e) {
            log.warn("Failed to get job metrics summary, it maybe first-run");
        }
    }

    @Data
    @AllArgsConstructor
    public static class JobMetricsSummary {
        private long sourceReadCount;
        private long sinkWriteCount;
        private Map<String, Map<String, Object>> transformMetricsMaps;
    }
}
