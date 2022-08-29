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

package org.apache.seatunnel.engine.e2e.engine;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelClientConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import com.google.common.collect.Lists;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class JobExecutionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionIT.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        HazelcastInstanceFactory.newHazelcastInstance(seaTunnelConfig.getHazelcastConfig(),
            Thread.currentThread().getName(),
            new SeaTunnelNodeContext(ConfigProvider.locateAndGetSeaTunnelConfig()));
    }

    @Test
    public void testSayHello() {
        SeaTunnelClientConfig seaTunnelClientConfig = new SeaTunnelClientConfig();
        seaTunnelClientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        seaTunnelClientConfig.getNetworkConfig().setAddresses(Lists.newArrayList("localhost:5801"));
        SeaTunnelClient engineClient = new SeaTunnelClient(seaTunnelClientConfig);

        String msg = "Hello world";
        String s = engineClient.printMessageToMaster(msg);
        Assert.assertEquals(msg, s);
    }

    @Test
    public void testExecuteJob() {
        TestUtils.initPluginDir();
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(filePath, jobConfig);

        try {
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(JobStatus.FINISHED, clientJobProxy.waitForJobComplete()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void cancelJobTest() {
        TestUtils.initPluginDir();
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/streaming_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(filePath, jobConfig);

        try {
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            JobStatus jobStatus1 = clientJobProxy.getJobStatus();
            Assert.assertFalse(jobStatus1.isEndState());
            ClientJobProxy finalClientJobProxy = clientJobProxy;
            CompletableFuture<Object> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
                JobStatus jobStatus = finalClientJobProxy.waitForJobComplete();
                Assert.assertEquals(JobStatus.CANCELED, jobStatus);
                return null;
            });
            Thread.sleep(1000);
            clientJobProxy.cancelJob();

            await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Assert.assertTrue(objectCompletableFuture.isDone());
                });

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
