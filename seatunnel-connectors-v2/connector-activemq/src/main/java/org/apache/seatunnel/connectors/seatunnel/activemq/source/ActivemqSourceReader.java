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

package org.apache.seatunnel.connectors.seatunnel.activemq.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.activemq.client.ActivemqClient;
import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig;
import org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.activemq.split.ActivemqSplit;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorErrorCode.HANDLE_SHUTDOWN_SIGNAL_FAILED;

@Slf4j
public class ActivemqSourceReader<T> implements SourceReader<T, ActivemqSplit> {
    protected final Handover<Message> handover;
    protected final Context context;
    protected final MessageConsumer consumer;
    private final boolean usesCorrelationId;
    protected transient Set<String> correlationIdsProcessedButNotAcknowledged;
    protected transient List<String> massageIdsProcessedForCurrentSnapshot;
    protected final SortedMap<Long, List<String>> pendingMassageIdsToCommit;
    protected final SortedMap<Long, Set<String>> pendingCorrelationIdsToCommit;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private ActivemqClient activemqClient;
    private final ActivemqConfig config;

    public ActivemqSourceReader(
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            Context context,
            ActivemqConfig config) {
        this.handover = new Handover<>();
        this.pendingMassageIdsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCorrelationIdsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.context = context;
        this.deserializationSchema = deserializationSchema;
        this.config = config;
        this.activemqClient = new ActivemqClient(config);
        this.consumer = activemqClient.getConsumer();
        this.usesCorrelationId = config.isUsesCorrelationId();
    }

    @Override
    public void open() throws Exception {
        this.correlationIdsProcessedButNotAcknowledged = new HashSet<>();
        this.massageIdsProcessedForCurrentSnapshot = new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CLOSE_SESSION_FAILED,
                    String.format(
                            "Error while closing AMQ session with  %s", config.getQueueName()));
        }
        if (activemqClient != null) {
            activemqClient.close();
        }
    }

    @Override
    public void pollNext(Collector output) throws Exception {
        consumer.setMessageListener(
                new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        try {
                            if (message != null && message instanceof TextMessage) {
                                TextMessage textMessage = (TextMessage) message;
                                String correlationId = textMessage.getJMSCorrelationID();
                                byte[] body = textMessage.getText().getBytes();
                                synchronized (output.getCheckpointLock()) {
                                    boolean newMessage = verifyMessageIdentifier(correlationId);
                                    if (!newMessage) {
                                        return;
                                    }
                                    massageIdsProcessedForCurrentSnapshot.add(
                                            message.getJMSMessageID());
                                    if (deserializationSchema
                                            instanceof JsonDeserializationSchema) {
                                        ((JsonDeserializationSchema) deserializationSchema)
                                                .collect(body, output);
                                    } else {
                                        deserializationSchema.deserialize(body, output);
                                    }
                                }
                            }
                        } catch (JMSException | IOException e) {
                            throw new ActivemqConnectorException(HANDLE_SHUTDOWN_SIGNAL_FAILED, e);
                        }
                    }
                });
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<ActivemqSplit> snapshotState(long checkpointId) throws Exception {

        List<ActivemqSplit> pendingSplit =
                Collections.singletonList(
                        new ActivemqSplit(
                                massageIdsProcessedForCurrentSnapshot,
                                correlationIdsProcessedButNotAcknowledged));
        List<String> deliveryTags =
                pendingMassageIdsToCommit.computeIfAbsent(checkpointId, id -> new ArrayList<>());
        Set<String> correlationIds =
                pendingCorrelationIdsToCommit.computeIfAbsent(checkpointId, id -> new HashSet<>());
        for (ActivemqSplit split : pendingSplit) {
            List<String> currentCheckPointDeliveryTags = split.getMassageIds();
            Set<String> currentCheckPointCorrelationIds = split.getCorrelationIds();
            if (currentCheckPointDeliveryTags != null) {
                deliveryTags.addAll(currentCheckPointDeliveryTags);
            }
            if (currentCheckPointCorrelationIds != null) {
                correlationIds.addAll(currentCheckPointCorrelationIds);
            }
        }
        massageIdsProcessedForCurrentSnapshot.clear();
        return pendingSplit;
    }

    @Override
    public void addSplits(List splits) {
        // do nothing
    }

    @Override
    public void handleNoMoreSplits() {
        // do nothing
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug("Committing cursors for checkpoint " + checkpointId);
        List<String> pendingDeliveryTags = pendingMassageIdsToCommit.remove(checkpointId);
        Set<String> pendingCorrelationIds = pendingCorrelationIdsToCommit.remove(checkpointId);
        if (pendingDeliveryTags == null || pendingCorrelationIds == null) {
            log.debug(
                    "pending delivery tags or correlationIds checkpoint {} either do not exist or have already been committed.",
                    checkpointId);
            return;
        }
        correlationIdsProcessedButNotAcknowledged.removeAll(pendingCorrelationIds);
    }

    public boolean verifyMessageIdentifier(String correlationId) {
        if (usesCorrelationId && correlationId != null) {
            if (!correlationIdsProcessedButNotAcknowledged.add(correlationId)) {
                return false;
            }
        }
        return true;
    }
}
