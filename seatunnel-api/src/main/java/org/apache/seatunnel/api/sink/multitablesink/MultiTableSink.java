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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class MultiTableSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        MultiTableState,
                        MultiTableCommitInfo,
                        MultiTableAggregatedCommitInfo>,
                SupportSchemaEvolutionSink {

    @Getter private final Map<TablePath, SeaTunnelSink> sinks;
    private final int replicaNum;
    private final int multiTableWriterTtl;

    public MultiTableSink(MultiTableFactoryContext context) {
        this.sinks = context.getSinks();
        this.replicaNum = context.getOptions().get(SinkCommonOptions.MULTI_TABLE_SINK_REPLICA);
        this.multiTableWriterTtl =
                context.getOptions().get(SinkCommonOptions.MULTI_TABLE_SINK_TTL_SEC);
    }

    @Override
    public String getPluginName() {
        return "MultiTableSink";
    }

    @Override
    public SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> createWriter(
            SinkWriter.Context context) throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        for (int i = 0; i < replicaNum; i++) {
            for (TablePath tablePath : sinks.keySet()) {
                SeaTunnelSink sink = sinks.get(tablePath);
                int index = context.getIndexOfSubtask() * replicaNum + i;
                String tableIdentifier = tablePath.toString();
                if (multiTableWriterTtl < 0) {
                    writers.put(
                            SinkIdentifier.of(tableIdentifier, index),
                            sink.createWriter(new SinkContextProxy(index, replicaNum, context)));
                } else {
                    writers.put(
                            SinkIdentifier.of(tableIdentifier, index),
                            new MultiTableTtlWriter(
                                    writers,
                                    tableIdentifier,
                                    index,
                                    replicaNum,
                                    sink,
                                    context,
                                    multiTableWriterTtl));
                }
                sinkWritersContext.put(SinkIdentifier.of(tableIdentifier, index), context);
            }
        }
        return new MultiTableSinkWriter(writers, replicaNum, sinkWritersContext);
    }

    @Override
    public SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> restoreWriter(
            SinkWriter.Context context, List<MultiTableState> states) throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();

        for (int i = 0; i < replicaNum; i++) {
            for (TablePath tablePath : sinks.keySet()) {
                SeaTunnelSink sink = sinks.get(tablePath);
                int index = context.getIndexOfSubtask() * replicaNum + i;
                SinkIdentifier sinkIdentifier = SinkIdentifier.of(tablePath.toString(), index);
                List<?> state =
                        states.stream()
                                .map(
                                        multiTableState ->
                                                multiTableState.getStates().get(sinkIdentifier))
                                .filter(Objects::nonNull)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                if (multiTableWriterTtl < 0) {
                    if (state.isEmpty()) {
                        writers.put(
                                sinkIdentifier,
                                sink.createWriter(
                                        new SinkContextProxy(index, replicaNum, context)));
                    } else {
                        writers.put(
                                sinkIdentifier,
                                sink.restoreWriter(
                                        new SinkContextProxy(index, replicaNum, context), state));
                    }
                } else {
                    if (state.isEmpty()) {
                        writers.put(
                                SinkIdentifier.of(tablePath.toString(), index),
                                new MultiTableTtlWriter(
                                        writers,
                                        tablePath.toString(),
                                        index,
                                        replicaNum,
                                        sink,
                                        context,
                                        multiTableWriterTtl));
                    } else {
                        writers.put(
                                SinkIdentifier.of(tablePath.toString(), index),
                                new MultiTableTtlWriter(
                                        writers,
                                        tablePath.toString(),
                                        index,
                                        replicaNum,
                                        sink,
                                        context,
                                        multiTableWriterTtl,
                                        state));
                    }
                }
                sinkWritersContext.put(sinkIdentifier, context);
            }
        }
        return new MultiTableSinkWriter(writers, replicaNum, sinkWritersContext);
    }

    @Override
    public Optional<Serializer<MultiTableState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<MultiTableCommitInfo>> createCommitter() throws IOException {
        Map<String, SinkCommitter<?>> committers = new HashMap<>();
        for (TablePath tablePath : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tablePath);
            sink.createCommitter()
                    .ifPresent(
                            committer ->
                                    committers.put(
                                            tablePath.toString(), (SinkCommitter<?>) committer));
        }
        if (committers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MultiTableSinkCommitter(committers));
    }

    @Override
    public Optional<Serializer<MultiTableCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<MultiTableCommitInfo, MultiTableAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        for (TablePath tablePath : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tablePath);
            if (multiTableWriterTtl < 0) {
                Optional<SinkAggregatedCommitter<?, ?>> sinkOptional =
                        sink.createAggregatedCommitter();
                sinkOptional.ifPresent(
                        sinkAggregatedCommitter ->
                                aggCommitters.put(tablePath.toString(), sinkAggregatedCommitter));
            } else {
                aggCommitters.put(
                        tablePath.toString(), new MultiTablePreparedSinkAggregatedCommitter(sink));
            }
        }
        if (aggCommitters.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MultiTableSinkAggregatedCommitter(aggCommitters));
    }

    public List<TablePath> getSinkTables() {

        List<TablePath> tablePaths = new ArrayList<>();
        List<SeaTunnelSink> values = new ArrayList<>(sinks.values());
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i).getWriteCatalogTable().isPresent()) {
                tablePaths.add(
                        ((CatalogTable) values.get(i).getWriteCatalogTable().get()).getTablePath());
            } else {
                tablePaths.add(sinks.keySet().toArray(new TablePath[0])[i]);
            }
        }
        return tablePaths;
    }

    @Override
    public Optional<Serializer<MultiTableAggregatedCommitInfo>>
            getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        sinks.values().forEach(sink -> sink.setJobContext(jobContext));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return SeaTunnelSink.super.getWriteCatalogTable();
    }

    @Override
    public List<SchemaChangeType> supports() {
        SeaTunnelSink firstSink = sinks.entrySet().iterator().next().getValue();
        if (firstSink instanceof SupportSchemaEvolutionSink) {
            return ((SupportSchemaEvolutionSink) firstSink).supports();
        }
        return Collections.emptyList();
    }
}
