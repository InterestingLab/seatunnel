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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;
import lombok.NonNull;

import java.util.concurrent.CompletableFuture;

public interface ResourceManager {
    @Deprecated
    Address applyForResource(long jobId, long taskId);

    @NonNull
    @Deprecated
    Address getAppliedResource(long jobId, long taskId);

    CompletableFuture<SlotProfile> applyResource(long jobId, ResourceProfile resourceProfile);

    CompletableFuture<Void>[] releaseResources(long jobId, SlotProfile[] profiles);

    CompletableFuture<Void> releaseResource(long jobId, SlotProfile profile);

    void workerRegister(WorkerProfile workerProfile);

    void heartbeatFromWorker(String workerID);

}
