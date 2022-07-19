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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.client.protocol.codec.EnginePrintMessageCodec;
import org.apache.seatunnel.engine.utils.ExceptionUtil;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import lombok.NonNull;

import java.util.UUID;
import java.util.function.Function;

public class EngineClientImpl implements EngineClientInstance {
    private final HazelcastClientInstanceImpl hazelcastClient;
    private final SerializationService serializationService;

    public EngineClientImpl(@NonNull EngineClientConfig engineClientConfig) {
        Preconditions.checkNotNull(engineClientConfig, "config");
        this.hazelcastClient = ((HazelcastClientProxy) HazelcastClient.newHazelcastClient(engineClientConfig)).client;
        this.serializationService = hazelcastClient.getSerializationService();
        ExceptionUtil.registerJetExceptions(hazelcastClient.getClientExceptionFactory());
    }

    @NonNull
    @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastClient;
    }

    @Override
    public LocalExecutionContext createExecutionContext(String filePath, EngineClientConfig config) {
        // TODO analyze job config file and create LocalExecutionContext
        return null;
    }

    public ILogger getLogger() {
        return hazelcastClient.getLoggingService().getLogger(getClass());
    }

    private <S> S invokeRequestOnMasterAndDecodeResponse(ClientMessage request,
                                                         Function<ClientMessage, Object> decoder) {
        UUID masterUuid = hazelcastClient.getClientClusterService().getMasterMember().getUuid();
        return invokeRequestAndDecodeResponse(masterUuid, request, decoder);
    }

    private <S> S invokeRequestOnAnyMemberAndDecodeResponse(ClientMessage request,
                                                            Function<ClientMessage, Object> decoder) {
        return invokeRequestAndDecodeResponse(null, request, decoder);
    }

    private <S> S invokeRequestAndDecodeResponse(UUID uuid, ClientMessage request,
                                                 Function<ClientMessage, Object> decoder) {
        ClientInvocation invocation = new ClientInvocation(hazelcastClient, request, null, uuid);
        try {
            ClientMessage response = invocation.invoke().get();
            return serializationService.toObject(decoder.apply(response));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public String sayHelloToServer(@NonNull String msg) {
        return invokeRequestOnMasterAndDecodeResponse(
            EnginePrintMessageCodec.encodeRequest(msg),
            response -> EnginePrintMessageCodec.decodeResponse(response)
        );
    }
}
