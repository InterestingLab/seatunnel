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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceSplitEnumerator.class);

    private final Context<FileSourceSplit> context;
    private final Set<FileSourceSplit> allSplit =
            new TreeSet<>(Comparator.comparing(FileSourceSplit::splitId));
    private Set<FileSourceSplit> assignedSplit;
    private final List<String> filePaths;
    private final AtomicInteger assignCount = new AtomicInteger(0);
    private ReadStrategy readStrategy;

    public FileSourceSplitEnumerator(
            ReadStrategy readStrategy,
            SourceSplitEnumerator.Context<FileSourceSplit> context,
            List<String> filePaths) {
        this.context = context;
        this.filePaths = filePaths;
        this.assignedSplit = new HashSet<>();
        this.readStrategy = readStrategy;
    }

    public FileSourceSplitEnumerator(
            ReadStrategy readStrategy,
            SourceSplitEnumerator.Context<FileSourceSplit> context,
            List<String> filePaths,
            FileSourceState sourceState) {
        this(readStrategy, context, filePaths);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.allSplit.addAll(discoverySplits());
    }

    @Override
    public void run() {
        for (int i = 0; i < context.currentParallelism(); i++) {
            LOGGER.info("Assigned splits to reader [{}]", i);
            assignSplit(i);
        }
    }

    private Set<FileSourceSplit> discoverySplits() {
        Set<FileSourceSplit> fileSourceSplits = new HashSet<>();
        for (String filePath : filePaths) {
            if (readStrategy != null) {
                Set<FileSourceSplit> set = readStrategy.getFileSourceSplits(filePath);
                fileSourceSplits.addAll(set);
            } else {
                fileSourceSplits.add(new FileSourceSplit(filePath));
            }
        }
        return fileSourceSplits;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            allSplit.addAll(splits);
            assignSplit(subtaskId);
        }
    }

    private void assignSplit(int taskId) {
        ArrayList<FileSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(allSplit);
        } else {
            // if parallelism > 1, according to polling strategy to determine whether to
            // allocate the current task
            assignCount.set(0);
            for (FileSourceSplit fileSourceSplit : allSplit) {
                int splitOwner =
                        getSplitOwner(assignCount.getAndIncrement(), context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(fileSourceSplit);
                }
            }
        }
        // assign splits
        context.assignSplit(taskId, currentTaskSplits);
        // save the state of assigned splits
        assignedSplit.addAll(currentTaskSplits);

        LOGGER.info(
                "SubTask {} is assigned to [{}]",
                taskId,
                currentTaskSplits.stream()
                        .map(FileSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    private static int getSplitOwner(int assignCount, int numReaders) {
        return assignCount % numReaders;
    }

    @Override
    public int currentUnassignedSplitSize() {
        return allSplit.size() - assignedSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        // do nothing
    }

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        return new FileSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {}
}
