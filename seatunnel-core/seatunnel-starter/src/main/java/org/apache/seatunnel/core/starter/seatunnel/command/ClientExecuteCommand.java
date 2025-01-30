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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner;
import org.apache.seatunnel.engine.client.job.JobStatusRunner;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.util.ConcurrencyUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

/** This command is used to execute the SeaTunnel engine job by SeaTunnel API. */
@Slf4j
public class ClientExecuteCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    private JobStatus jobStatus;
    private SeaTunnelClient engineClient;
    private HazelcastInstance instance;
    private ScheduledExecutorService executorService;

    public ClientExecuteCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        JobMetricsRunner.JobMetricsSummary jobMetricsSummary = null;
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        try {
            String clusterName = clientCommandArgs.getClusterName();
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            //  get running mode
            boolean isLocalMode = clientCommandArgs.getMasterType().equals(MasterType.LOCAL);
            if (isLocalMode) {
                clusterName =
                        creatRandomClusterName(
                                StringUtils.isNotEmpty(clusterName)
                                        ? clusterName
                                        : Constant.DEFAULT_SEATUNNEL_CLUSTER_NAME);
                instance = createServerInLocal(clusterName, seaTunnelConfig);
                int port = instance.getCluster().getLocalMember().getSocketAddress().getPort();
                clientConfig
                        .getNetworkConfig()
                        .setAddresses(Collections.singletonList("localhost:" + port));
            }
            if (StringUtils.isNotEmpty(clusterName)) {
                seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
                clientConfig.setClusterName(clusterName);
            }
            engineClient = new SeaTunnelClient(clientConfig);
            if (clientCommandArgs.isListJob()) {
                String jobStatus = engineClient.getJobClient().listJobStatus(true);
                System.out.println(jobStatus);
            } else if (clientCommandArgs.isGetRunningJobMetrics()) {
                String runningJobMetrics = engineClient.getJobClient().getRunningJobMetrics();
                System.out.println(runningJobMetrics);
            } else if (null != clientCommandArgs.getJobId()) {
                String jobState =
                        engineClient
                                .getJobClient()
                                .getJobDetailStatus(Long.parseLong(clientCommandArgs.getJobId()));
                System.out.println(jobState);
            } else if (null != clientCommandArgs.getCancelJobId()) {
                List<String> cancelJobIds = clientCommandArgs.getCancelJobId();
                for (String cancelJobId : cancelJobIds) {
                    engineClient.getJobClient().cancelJob(Long.parseLong(cancelJobId));
                }
            } else if (null != clientCommandArgs.getMetricsJobId()) {
                String jobMetrics =
                        engineClient
                                .getJobClient()
                                .getJobMetrics(Long.parseLong(clientCommandArgs.getMetricsJobId()));
                System.out.println(jobMetrics);
            } else if (null != clientCommandArgs.getSavePointJobId()) {
                engineClient
                        .getJobClient()
                        .savePointJob(Long.parseLong(clientCommandArgs.getSavePointJobId()));
            } else {
                Path configFile = FileUtils.getConfigPath(clientCommandArgs);
                checkConfigExist(configFile);
                JobConfig jobConfig = new JobConfig();
                ClientJobExecutionEnvironment jobExecutionEnv;
                jobConfig.setName(clientCommandArgs.getJobName());
                if (null != clientCommandArgs.getRestoreJobId()) {
                    jobExecutionEnv =
                            engineClient.restoreExecutionContext(
                                    configFile.toString(),
                                    clientCommandArgs.getVariables(),
                                    jobConfig,
                                    seaTunnelConfig,
                                    Long.parseLong(clientCommandArgs.getRestoreJobId()));
                } else {
                    jobExecutionEnv =
                            engineClient.createExecutionContext(
                                    configFile.toString(),
                                    clientCommandArgs.getVariables(),
                                    jobConfig,
                                    seaTunnelConfig,
                                    clientCommandArgs.getCustomJobId() != null
                                            ? Long.parseLong(clientCommandArgs.getCustomJobId())
                                            : null);
                }

                // get job start time
                startTime = LocalDateTime.now();
                // create job proxy
                ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
                if (clientCommandArgs.isAsync()) {
                    if (isLocalMode) {
                        log.warn("The job is running in local mode, can not use async mode.");
                    } else {
                        return;
                    }
                }
                // register cancelJob hook
                Runtime.getRuntime()
                        .addShutdownHook(
                                new Thread(
                                        () -> {
                                            CompletableFuture<Void> future =
                                                    CompletableFuture.runAsync(
                                                            () -> {
                                                                log.info(
                                                                        "run shutdown hook because get close signal");
                                                                shutdownHook(clientJobProxy);
                                                            });
                                            try {
                                                future.get(15, TimeUnit.SECONDS);
                                            } catch (Exception e) {
                                                log.error("Cancel job failed.", e);
                                            }
                                        }));
                // get job id
                long jobId = clientJobProxy.getJobId();
                JobMetricsRunner jobMetricsRunner = new JobMetricsRunner(engineClient, jobId);
                executorService =
                        Executors.newScheduledThreadPool(
                                2,
                                new ThreadFactoryBuilder()
                                        .setNameFormat("job-metrics-runner-%d")
                                        .setDaemon(true)
                                        .build());
                executorService.scheduleAtFixedRate(
                        jobMetricsRunner,
                        0,
                        seaTunnelConfig.getEngineConfig().getPrintJobMetricsInfoInterval(),
                        TimeUnit.SECONDS);

                if (!isLocalMode) {
                    // LOCAL mode does not require running the job status runner
                    executorService.schedule(
                            new JobStatusRunner(engineClient.getJobClient(), jobId),
                            0,
                            TimeUnit.SECONDS);
                }

                // wait for job complete
                JobResult jobResult = clientJobProxy.waitForJobCompleteV2();
                jobStatus = jobResult.getStatus();
                if (StringUtils.isNotEmpty(jobResult.getError())
                        || jobResult.getStatus().equals(JobStatus.FAILED)) {
                    throw new SeaTunnelEngineException(jobResult.getError());
                }
                // get job end time
                endTime = LocalDateTime.now();
                // get job statistic information when job finished
                jobMetricsSummary = engineClient.getJobMetricsSummary(jobId);
            }
        } catch (Exception e) {
            throw new CommandExecuteException("SeaTunnel job executed failed", e);
        } finally {
            if (jobMetricsSummary != null) {
                // print job statistics information when job finished
                StringBuilder logMessage = new StringBuilder();
                logMessage.append(
                        StringFormatUtils.formatTable(
                                "Job Statistic Information",
                                "Start Time",
                                DateTimeUtils.toString(
                                        startTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                                "End Time",
                                DateTimeUtils.toString(
                                        endTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                                "Total Time(s)",
                                Duration.between(startTime, endTime).getSeconds(),
                                "Total Read Count",
                                jobMetricsSummary.getSourceReadCount(),
                                "Total Write Count",
                                jobMetricsSummary.getSinkWriteCount(),
                                "Total Failed Count",
                                jobMetricsSummary.getSourceReadCount()
                                        - jobMetricsSummary.getSinkWriteCount()));

                if (MapUtils.isNotEmpty(jobMetricsSummary.getTransformMetricsMaps())) {
                    jobMetricsSummary
                            .getTransformMetricsMaps()
                            .forEach(
                                    (tableName, metrics) -> {
                                        String[] transformInfos =
                                                new String[metrics.entrySet().size() * 2 + 2];
                                        transformInfos[0] = tableName + " Information";
                                        int index = 0;
                                        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
                                            transformInfos[++index] = entry.getKey();
                                            transformInfos[++index] =
                                                    String.valueOf(entry.getValue());
                                        }
                                        if (Objects.nonNull(transformInfos)) {
                                            logMessage.append(
                                                    StringFormatUtils.formatTable(transformInfos));
                                        }
                                    });
                }
                log.info("{}", logMessage);
            }
            closeClient();
        }
    }

    private void closeClient() {
        if (engineClient != null) {
            engineClient.close();
            log.info("Closed SeaTunnel client......");
        }
        if (instance != null) {
            instance.shutdown();
            log.info("Closed HazelcastInstance ......");
        }
        if (executorService != null) {
            executorService.shutdownNow();
            log.info("Closed metrics executor service ......");
        }
    }

    private HazelcastInstance createServerInLocal(
            String clusterName, SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        // local mode only support MASTER_AND_WORKER role
        seaTunnelConfig
                .getEngineConfig()
                .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        // set local mode
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);

        // set the default async executor for Hazelcast InvocationFuture
        ConcurrencyUtil.setDefaultAsyncExecutor(CompletableFuture.EXECUTOR);

        return HazelcastInstanceFactory.newHazelcastInstance(
                seaTunnelConfig.getHazelcastConfig(),
                Thread.currentThread().getName(),
                new SeaTunnelNodeContext(seaTunnelConfig));
    }

    private String creatRandomClusterName(String namePrefix) {
        Random random = new Random();
        return namePrefix + "-" + random.nextInt(1000000);
    }

    private void shutdownHook(ClientJobProxy clientJobProxy) {
        if (clientCommandArgs.isCloseJob()) {
            if (clientJobProxy.getJobResultCache() == null
                    && (jobStatus == null || !jobStatus.isEndState())) {
                log.warn("Task will be closed due to client shutdown.");
                clientJobProxy.cancelJob();
            }
        }
    }
}
