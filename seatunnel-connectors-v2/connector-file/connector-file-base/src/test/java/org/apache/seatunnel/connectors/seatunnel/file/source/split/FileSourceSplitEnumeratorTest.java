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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.TextReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.writer.OrcReadStrategyTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class FileSourceSplitEnumeratorTest {

    public TextReadStrategy getTextReadStrategy() throws IOException {
        TextReadStrategy textReadStrategy = new TextReadStrategy();
        OrcReadStrategyTest.LocalConf localConf =
                new OrcReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        textReadStrategy.init(localConf);
        Config config = ConfigFactory.empty();
        config =
                config.withValue(
                        BaseSourceConfigOptions.ROW_COUNT_PER_SPLIT.key(),
                        ConfigValueFactory.fromAnyRef(1500000));
        config =
                config.withValue(
                        BaseSourceConfigOptions.WHETHER_SPLIT_FILE.key(),
                        ConfigValueFactory.fromAnyRef(true));
        textReadStrategy.setPluginConfig(config);
        return textReadStrategy;
    }

    @Test
    void assignSplitRoundTest() throws IOException {
        List<String> filePaths = new ArrayList<>();
        int fileSize = 10;
        int parallelism = 4;

        for (int i = 0; i < fileSize; i++) {
            filePaths.add("file" + i + ".txt");
        }

        Map<Integer, List<FileSourceSplit>> assignSplitMap = new HashMap<>();

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                new SourceSplitEnumerator.Context<FileSourceSplit>() {
                    @Override
                    public int currentParallelism() {
                        return parallelism;
                    }

                    @Override
                    public Set<Integer> registeredReaders() {
                        return null;
                    }

                    @Override
                    public void assignSplit(int subtaskId, List<FileSourceSplit> splits) {
                        assignSplitMap.put(subtaskId, splits);
                    }

                    @Override
                    public void signalNoMoreSplits(int subtask) {}

                    @Override
                    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };

        FileSourceSplitEnumerator fileSourceSplitEnumerator =
                new FileSourceSplitEnumerator(getTextReadStrategy(), context, filePaths);
        fileSourceSplitEnumerator.open();

        fileSourceSplitEnumerator.run();

        // check all files are assigned
        Assertions.assertEquals(fileSourceSplitEnumerator.currentUnassignedSplitSize(), 0);

        Set<FileSourceSplit> valueSet =
                assignSplitMap.values().stream().flatMap(List::stream).collect(Collectors.toSet());

        // check no duplicated assigned split
        Assertions.assertEquals(valueSet.size(), fileSize);

        // check file allocation balance
        for (int i = 1; i < parallelism; i++) {
            Assertions.assertTrue(
                    Math.abs(assignSplitMap.get(i).size() - assignSplitMap.get(i - 1).size()) <= 1,
                    "The number of files assigned to adjacent subtasks is more than 1.");
        }
    }
}
