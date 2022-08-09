/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.api;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckPointStorageException;

import java.util.List;
import java.util.Map;

public interface CheckPointStorage {

    /**
     * init storage and create parent directory if not exists
     *
     * @param configuration configuration storage system config params
     * @throws CheckPointStorageException if init failed
     */
    void initStorage(Map<String, String> configuration) throws CheckPointStorageException;

    /**
     * save checkpoint to storage
     *
     * @param state PipelineState
     * @throws CheckPointStorageException if save checkpoint failed
     */
    String storeCheckPoint(PipelineState state) throws CheckPointStorageException;

    /**
     * get all checkpoint from storage
     *
     * @param jobId job id
     * @return All job's checkpoint data from storage
     * @throws CheckPointStorageException if get checkpoint failed
     */
    List<PipelineState> getAllCheckpoints(String jobId);

    /**
     * get latest checkpoint from storage
     *
     * @param jobId job id
     * @return latest checkpoint data from storage
     * @throws CheckPointStorageException if get checkpoint failed
     */
    PipelineState getLatestCheckpoint(String jobId) throws CheckPointStorageException;

    /**
     * get checkpoint from storage, If there are multiple records, one will be returned randomly
     *
     * @param jobId      job id
     * @param pipelineId pipeline id
     * @return checkpoint data from storage
     * @throws CheckPointStorageException if get checkpoint failed or no checkpoint found
     */
    PipelineState getCheckpointByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckPointStorageException;

    /**
     * Delete all checkpoint data under the job
     *
     * @param jobId job id
     * @throws CheckPointStorageException if delete checkpoint failed
     */
    void deleteCheckpoint(String jobId);

}
