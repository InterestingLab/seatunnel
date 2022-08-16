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

package org.apache.seatunnel.engine.server.execution;

import lombok.NonNull;

import java.io.IOException;
import java.io.Serializable;

public interface Task extends Serializable {

    default void init() throws Exception {
    }

    @NonNull
    ProgressState call() throws Exception;

    /** The job that the task belongs to. */
    Long getJobId();

    /**
     * The vertex key in the {@link org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex} whose code the task executes.
     * <br> When the user changes the parallelism, the key will not change.
     */
    String getVertexKey();

    /** The task ID of {@link org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex}, It changes due to changes in parallelism. */
    @NonNull
    Long getTaskID();

    default boolean isThreadsShare() {
        return false;
    }

    default void close() throws IOException {
    }

    default void setTaskExecutionContext(TaskExecutionContext taskExecutionContext){
    }

}
