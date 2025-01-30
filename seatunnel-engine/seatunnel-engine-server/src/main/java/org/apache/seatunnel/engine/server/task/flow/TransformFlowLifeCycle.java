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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class TransformFlowLifeCycle<T> extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>> {

    private final TransformChainAction<T> action;

    private final List<SeaTunnelTransform<T>> transform;

    private final List<String> pluginOutputList;

    private final List<String> transformNames;

    private final Collector<Record<?>> collector;

    private MetricsContext metricsContext;

    public TransformFlowLifeCycle(
            TransformChainAction<T> action,
            SeaTunnelTask runningTask,
            Collector<Record<?>> collector,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext) {
        super(action, runningTask, completableFuture);
        this.action = action;
        this.transform = action.getTransforms();
        this.pluginOutputList = action.getPluginOutputs();
        this.transformNames = action.getTransformNames();
        this.collector = collector;
        this.metricsContext = metricsContext;
    }

    @Override
    public void open() throws Exception {
        super.open();
        for (SeaTunnelTransform<T> t : transform) {
            try {
                t.open();
            } catch (Exception e) {
                log.error(
                        "Open transform: {} failed, cause: {}",
                        t.getPluginName(),
                        e.getMessage(),
                        e);
            }
        }
    }

    @Override
    public void received(Record<?> record) {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            if (barrier.prepareClose(this.runningTask.getTaskLocation())) {
                prepareClose = true;
            }
            if (barrier.snapshot()) {
                runningTask.addState(barrier, ActionStateKey.of(action), Collections.emptyList());
            }
            // ack after #addState
            runningTask.ack(barrier);
            collector.collect(record);
        } else if (record.getData() instanceof SchemaChangeEvent) {
            if (prepareClose) {
                return;
            }
            SchemaChangeEvent event = (SchemaChangeEvent) record.getData();
            for (SeaTunnelTransform<T> t : transform) {
                SchemaChangeEvent eventBefore = event;
                event = t.mapSchemaChangeEvent(eventBefore);
                if (event == null) {
                    log.info(
                            "Transform[{}] filtered schema change event {}",
                            t.getPluginName(),
                            eventBefore);
                    break;
                }
                log.info(
                        "Transform[{}] input schema change event {} and output schema change event {}",
                        t.getPluginName(),
                        eventBefore,
                        event);
            }
            if (event != null) {
                collector.collect(new Record<>(event));
            }
        } else {
            if (prepareClose) {
                return;
            }
            T inputData = (T) record.getData();
            List<T> outputDataList = transform(inputData);
            if (!outputDataList.isEmpty()) {
                // todo log metrics
                for (T outputData : outputDataList) {
                    collector.collect(new Record<>(outputData));
                }
            }
        }
    }

    public List<T> transform(T inputData) {
        if (transform.isEmpty()) {
            return Collections.singletonList(inputData);
        }

        List<T> dataList = new ArrayList<>();
        dataList.add(inputData);
        int index = 0;
        for (SeaTunnelTransform<T> transformer : transform) {
            String pluginOutput = pluginOutputList.get(index);
            String metricName = transformNames.get(index);
            index++;
            List<T> nextInputDataList = new ArrayList<>();
            if (transformer instanceof SeaTunnelFlatMapTransform) {
                SeaTunnelFlatMapTransform<T> transformDecorator =
                        (SeaTunnelFlatMapTransform<T>) transformer;
                for (T data : dataList) {
                    List<T> outputDataArray = transformDecorator.flatMap(data);
                    log.debug(
                            "Transform[{}] input row {} and output row {}",
                            transformer,
                            data,
                            outputDataArray);
                    if (CollectionUtils.isNotEmpty(outputDataArray)) {
                        nextInputDataList.addAll(outputDataArray);
                        for (T outputData : outputDataArray) {
                            if (outputData instanceof SeaTunnelRow) {
                                String tableId =
                                        pluginOutput == null
                                                ? ((SeaTunnelRow) outputData).getTableId()
                                                : pluginOutput;
                                updateMetric(metricName, tableId);
                            }
                        }
                    }
                }
            } else if (transformer instanceof SeaTunnelMapTransform) {
                for (T data : dataList) {
                    SeaTunnelMapTransform<T> transformDecorator =
                            (SeaTunnelMapTransform<T>) transformer;
                    T outputData = transformDecorator.map(data);
                    log.debug(
                            "Transform[{}] input row {} and output row {}",
                            transformer,
                            data,
                            outputData);
                    if (outputData == null) {
                        log.trace("Transform[{}] filtered data row {}", transformer, data);
                        continue;
                    }
                    nextInputDataList.add(outputData);
                    if (outputData instanceof SeaTunnelRow) {
                        String tableId =
                                pluginOutput == null
                                        ? ((SeaTunnelRow) outputData).getTableId()
                                        : pluginOutput;
                        updateMetric(metricName, tableId);
                    }
                }
            }

            dataList = nextInputDataList;
        }
        return dataList;
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {}

    @Override
    public void close() throws IOException {
        for (SeaTunnelTransform<T> t : transform) {
            try {
                t.close();
            } catch (Exception e) {
                log.error(
                        "Close transform: {} failed, cause: {}",
                        t.getPluginName(),
                        e.getMessage(),
                        e);
            }
        }
        super.close();
    }

    private void updateMetric(String metricName, String tableId) {
        StringBuilder metricNameBuilder = new StringBuilder();
        metricNameBuilder
                .append(MetricNames.TRANSFORM_OUTPUT_COUNT)
                .append("#")
                .append(metricName)
                .append("#")
                .append(tableId);
        if (metricsContext != null) {
            metricsContext.counter(metricNameBuilder.toString()).inc();
        }
    }
}
