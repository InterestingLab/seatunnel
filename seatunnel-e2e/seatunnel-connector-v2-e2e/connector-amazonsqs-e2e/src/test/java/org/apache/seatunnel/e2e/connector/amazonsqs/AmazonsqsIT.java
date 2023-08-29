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

package org.apache.seatunnel.e2e.connector.amazonsqs;

import com.google.common.collect.Lists;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import org.testcontainers.containers.localstack.LocalStackContainer;

import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;


@Slf4j
public class AmazonsqsIT extends TestSuiteBase implements TestResource {
    private static final String LOCALSTACK_DOCKER_IMAGE = "localstack/localstack:0.11.3";
    private static final String AMAZONSQS_JOB_CONFIG = "/amazonsqsIT_source_to_sink.conf";
    private static final String AMAZONSQS_CONTAINER_HOST = "sqs-host";
    private static final String AMAZONSQS_CONTAINER_PORT = "4566";
    private static final String SINK_QUEUE = "sink_queue";
    private static final String SOURCE_QUEUE = "source_queue";

    protected SqsClient sqsClient;
    
    private LocalStackContainer localstack;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        // start a localstack docker container
        localstack = new LocalStackContainer(LOCALSTACK_DOCKER_IMAGE)
                .withServices(LocalStackContainer.Service.SQS)
                .withNetwork(NETWORK)
                .withNetworkAliases(AMAZONSQS_CONTAINER_HOST)
                .withEnv("AWS_DEFAULT_REGION", "us-east-1")
                .withEnv("AWS_ACCESS_KEY_ID", "1234")
                .withEnv("AWS_SECRET_ACCESS_KEY", "abcd")
                .withLogConsumer(
                    new Slf4jLogConsumer(
                        DockerLoggerFactory.getLogger(
                                LOCALSTACK_DOCKER_IMAGE)));

        localstack.setPortBindings(Lists.newArrayList(
                String.format(
                        "%s:%s",
                        AMAZONSQS_CONTAINER_PORT, AMAZONSQS_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(localstack)).join();


        log.info("Access Key: {}", localstack.getAccessKey());
        log.info("Secret Key: {}", localstack.getSecretKey());

        log.info("localstack container started");
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(this::initializeSqsClient);
    }

    private void initializeSqsClient() throws ConnectException {
        // create a sqs client
        sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                ))
                .build();

        // create source and sink queue
        sqsClient.createQueue(r -> r.queueName(SOURCE_QUEUE));
        sqsClient.createQueue(r -> r.queueName(SINK_QUEUE));

        // insert message to source queue
        sqsClient.sendMessage(r -> r.queueUrl(sqsClient.listQueues().queueUrls().get(0)).messageBody("test message"));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (localstack != null) {
            localstack.close();
        }
    }

    @TestTemplate
    public void testAmazonSqs(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(AMAZONSQS_JOB_CONFIG);
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData();
        compareResult();
    }

    private void assertHasData() {
        // check if there is message in sink queue
        Assertions.assertEquals(1, sqsClient.receiveMessage(r -> r.queueUrl(sqsClient.listQueues().queueUrls().get(1))).messages().size());
    }

    private void compareResult() {
        // compare the message in source queue and sink queue
        String sourceQueueMessage = sqsClient.receiveMessage(r -> r.queueUrl(sqsClient.listQueues().queueUrls().get(0))).messages().get(0).body();
        String sinkQueueMessage = sqsClient.receiveMessage(r -> r.queueUrl(sqsClient.listQueues().queueUrls().get(1))).messages().get(0).body();
        Assertions.assertEquals(sourceQueueMessage, sinkQueueMessage);
    }
}
