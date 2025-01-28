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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * this class need overrides equals methods as it will be added into HashSet. <br>
 * If there is no equals method, a file will be read repeatedly in parallelism.
 */
@Getter
@ToString
@EqualsAndHashCode
public class FileSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String tableId;
    private final String filePath;
    // Left closed right closed
    private Long minRowIndex;
    private Long maxRowIndex;

    public long getRows() {
        return maxRowIndex - minRowIndex;
    }

    public FileSourceSplit(String splitId) {
        this.filePath = splitId;
        this.tableId = null;
    }

    public FileSourceSplit(String splitId, long min, long max) {
        this.filePath = splitId;
        this.tableId = null;
        this.minRowIndex = min;
        this.maxRowIndex = max;
    }

    public FileSourceSplit(String tableId, String filePath) {
        this.tableId = tableId;
        this.filePath = filePath;
    }

    @Override
    public String splitId() {
        // In order to be compatible with the split before the upgrade, when tableId is null,
        // filePath is directly returned
        if (tableId == null) {
            return filePath;
        }
        return tableId + "_" + filePath;
    }
}
