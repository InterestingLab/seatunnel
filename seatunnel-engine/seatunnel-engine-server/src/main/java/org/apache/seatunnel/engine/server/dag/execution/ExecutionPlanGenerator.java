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

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleAction;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleConfig;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleMultipleRowStrategy;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleStrategy;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class ExecutionPlanGenerator {
    private final LogicalDag logicalPlan;
    private final JobImmutableInformation jobImmutableInformation;
    private final EngineConfig engineConfig;
    private final IdGenerator idGenerator = new IdGenerator();

    public ExecutionPlanGenerator(
            @NonNull LogicalDag logicalPlan,
            @NonNull JobImmutableInformation jobImmutableInformation,
            @NonNull EngineConfig engineConfig) {
        checkArgument(
                logicalPlan.getEdges().size() > 0, "ExecutionPlan Builder must have LogicalPlan.");
        this.logicalPlan = logicalPlan;
        this.jobImmutableInformation = jobImmutableInformation;
        this.engineConfig = engineConfig;
    }

    public ExecutionPlan generate() {
        log.debug("Generate execution plan using logical plan:");

        Set<ExecutionEdge> executionEdges = generateExecutionEdges(logicalPlan.getEdges());
        log.debug("Phase 1: generate execution edge list {}", executionEdges);

        executionEdges = generateShuffleEdges(executionEdges);
        log.debug("Phase 2: generate shuffle edge list {}", executionEdges);

        executionEdges = generateTransformChainEdges(executionEdges);
        log.debug("Phase 3: generate transform chain edge list {}", executionEdges);

        List<Pipeline> pipelines = generatePipelines(executionEdges);
        log.debug("Phase 4: generate pipeline list {}", pipelines);

        ExecutionPlan executionPlan = new ExecutionPlan(pipelines, jobImmutableInformation);
        log.debug("Phase 5: generate execution plan: {}", executionPlan);

        return executionPlan;
    }

    public static Action recreateAction(Action action, Long id, int parallelism) {
        Action newAction;
        if (action instanceof ShuffleAction) {
            newAction =
                    new ShuffleAction(id, action.getName(), ((ShuffleAction) action).getConfig());
        } else if (action instanceof SinkAction) {
            newAction =
                    new SinkAction<>(
                            id,
                            action.getName(),
                            new ArrayList<>(),
                            ((SinkAction<?, ?, ?, ?>) action).getSink(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers(),
                            (SinkConfig) action.getConfig());
        } else if (action instanceof SourceAction) {
            newAction =
                    new SourceAction<>(
                            id,
                            action.getName(),
                            ((SourceAction<?, ?, ?>) action).getSource(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers());
        } else if (action instanceof TransformAction) {
            newAction =
                    new TransformAction(
                            id,
                            action.getName(),
                            ((TransformAction) action).getTransform(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers(),
                            ((TransformAction) action).getPluginOutput());
        } else if (action instanceof TransformChainAction) {
            newAction =
                    new TransformChainAction(
                            id,
                            action.getName(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers(),
                            ((TransformChainAction<?>) action).getTransforms(),
                            ((TransformChainAction<?>) action).getPluginOutputs(),
                            ((TransformChainAction<?>) action).getTransformNames());
        } else {
            throw new UnknownActionException(action);
        }
        newAction.setParallelism(parallelism);
        return newAction;
    }

    private Set<ExecutionEdge> generateExecutionEdges(Set<LogicalEdge> logicalEdges) {
        Set<ExecutionEdge> executionEdges = new LinkedHashSet<>();

        Map<Long, ExecutionVertex> logicalVertexIdToExecutionVertexMap = new HashMap();

        List<LogicalEdge> sortedLogicalEdges = new ArrayList<>(logicalEdges);
        Collections.sort(
                sortedLogicalEdges,
                (o1, o2) -> {
                    if (!o1.getInputVertexId().equals(o2.getInputVertexId())) {
                        return o1.getInputVertexId() > o2.getInputVertexId() ? 1 : -1;
                    }
                    if (!o1.getTargetVertexId().equals(o2.getTargetVertexId())) {
                        return o1.getTargetVertexId() > o2.getTargetVertexId() ? 1 : -1;
                    }
                    return 0;
                });
        for (LogicalEdge logicalEdge : sortedLogicalEdges) {
            LogicalVertex logicalInputVertex = logicalEdge.getInputVertex();
            ExecutionVertex executionInputVertex =
                    logicalVertexIdToExecutionVertexMap.computeIfAbsent(
                            logicalInputVertex.getVertexId(),
                            vertexId -> {
                                long newId = idGenerator.getNextId();
                                Action newLogicalInputAction =
                                        recreateAction(
                                                logicalInputVertex.getAction(),
                                                newId,
                                                logicalInputVertex.getParallelism());
                                return new ExecutionVertex(
                                        newId,
                                        newLogicalInputAction,
                                        logicalInputVertex.getParallelism());
                            });

            LogicalVertex logicalTargetVertex = logicalEdge.getTargetVertex();
            ExecutionVertex executionTargetVertex =
                    logicalVertexIdToExecutionVertexMap.computeIfAbsent(
                            logicalTargetVertex.getVertexId(),
                            vertexId -> {
                                long newId = idGenerator.getNextId();
                                Action newLogicalTargetAction =
                                        recreateAction(
                                                logicalTargetVertex.getAction(),
                                                newId,
                                                logicalTargetVertex.getParallelism());
                                return new ExecutionVertex(
                                        newId,
                                        newLogicalTargetAction,
                                        logicalTargetVertex.getParallelism());
                            });

            ExecutionEdge executionEdge =
                    new ExecutionEdge(executionInputVertex, executionTargetVertex);
            executionEdges.add(executionEdge);
        }
        return executionEdges;
    }

    @SuppressWarnings("MagicNumber")
    private Set<ExecutionEdge> generateShuffleEdges(Set<ExecutionEdge> executionEdges) {
        Map<Long, List<ExecutionVertex>> targetVerticesMap = new LinkedHashMap<>();
        Set<ExecutionVertex> sourceExecutionVertices = new HashSet<>();
        executionEdges.forEach(
                edge -> {
                    ExecutionVertex leftVertex = edge.getLeftVertex();
                    ExecutionVertex rightVertex = edge.getRightVertex();
                    if (leftVertex.getAction() instanceof SourceAction) {
                        sourceExecutionVertices.add(leftVertex);
                    }
                    targetVerticesMap
                            .computeIfAbsent(leftVertex.getVertexId(), id -> new ArrayList<>())
                            .add(rightVertex);
                });
        if (sourceExecutionVertices.size() != 1) {
            return executionEdges;
        }
        ExecutionVertex sourceExecutionVertex = sourceExecutionVertices.stream().findFirst().get();
        Action sourceAction = sourceExecutionVertex.getAction();
        List<CatalogTable> producedCatalogTables = new ArrayList<>();
        if (sourceAction instanceof SourceAction) {
            try {
                producedCatalogTables =
                        ((SourceAction<?, ?, ?>) sourceAction)
                                .getSource()
                                .getProducedCatalogTables();
            } catch (UnsupportedOperationException e) {
            }
        } else if (sourceAction instanceof TransformChainAction) {
            return executionEdges;
        } else {
            throw new SeaTunnelException(
                    "source action must be SourceAction or TransformChainAction");
        }
        if (producedCatalogTables.size() <= 1
                || targetVerticesMap.get(sourceExecutionVertex.getVertexId()).size() <= 1) {
            return executionEdges;
        }

        List<ExecutionVertex> sinkVertices =
                targetVerticesMap.get(sourceExecutionVertex.getVertexId());
        Optional<ExecutionVertex> hasOtherAction =
                sinkVertices.stream()
                        .filter(vertex -> !(vertex.getAction() instanceof SinkAction))
                        .findFirst();
        checkArgument(!hasOtherAction.isPresent());

        Set<ExecutionEdge> newExecutionEdges = new LinkedHashSet<>();
        ShuffleStrategy shuffleStrategy =
                ShuffleMultipleRowStrategy.builder()
                        .jobId(jobImmutableInformation.getJobId())
                        .inputPartitions(sourceAction.getParallelism())
                        .catalogTables(producedCatalogTables)
                        .queueEmptyQueueTtl(
                                (int)
                                        (engineConfig.getCheckpointConfig().getCheckpointInterval()
                                                * 3))
                        .build();
        ShuffleConfig shuffleConfig =
                ShuffleConfig.builder().shuffleStrategy(shuffleStrategy).build();

        long shuffleVertexId = idGenerator.getNextId();
        String shuffleActionName = String.format("Shuffle [%s]", sourceAction.getName());
        ShuffleAction shuffleAction =
                new ShuffleAction(shuffleVertexId, shuffleActionName, shuffleConfig);
        shuffleAction.setParallelism(sourceAction.getParallelism());
        ExecutionVertex shuffleVertex =
                new ExecutionVertex(shuffleVertexId, shuffleAction, shuffleAction.getParallelism());
        ExecutionEdge sourceToShuffleEdge = new ExecutionEdge(sourceExecutionVertex, shuffleVertex);
        newExecutionEdges.add(sourceToShuffleEdge);

        for (ExecutionVertex sinkVertex : sinkVertices) {
            sinkVertex.setParallelism(1);
            sinkVertex.getAction().setParallelism(1);
            ExecutionEdge shuffleToSinkEdge = new ExecutionEdge(shuffleVertex, sinkVertex);
            newExecutionEdges.add(shuffleToSinkEdge);
        }
        return newExecutionEdges;
    }

    private Set<ExecutionEdge> generateTransformChainEdges(Set<ExecutionEdge> executionEdges) {
        Map<Long, List<ExecutionVertex>> inputVerticesMap = new HashMap<>();
        Map<Long, List<ExecutionVertex>> targetVerticesMap = new HashMap<>();
        Set<ExecutionVertex> sourceExecutionVertices = new HashSet<>();
        executionEdges.forEach(
                edge -> {
                    ExecutionVertex leftVertex = edge.getLeftVertex();
                    ExecutionVertex rightVertex = edge.getRightVertex();
                    if (leftVertex.getAction() instanceof SourceAction) {
                        sourceExecutionVertices.add(leftVertex);
                    }
                    inputVerticesMap
                            .computeIfAbsent(rightVertex.getVertexId(), id -> new ArrayList<>())
                            .add(leftVertex);
                    targetVerticesMap
                            .computeIfAbsent(leftVertex.getVertexId(), id -> new ArrayList<>())
                            .add(rightVertex);
                });

        Map<Long, ExecutionVertex> transformChainVertexMap = new HashMap<>();
        Map<Long, Long> chainedTransformVerticesMapping = new HashMap<>();
        for (ExecutionVertex sourceVertex : sourceExecutionVertices) {
            List<ExecutionVertex> vertices = new ArrayList<>();
            vertices.add(sourceVertex);
            for (int index = 0; index < vertices.size(); index++) {
                ExecutionVertex vertex = vertices.get(index);

                fillChainedTransformExecutionVertex(
                        vertex,
                        chainedTransformVerticesMapping,
                        transformChainVertexMap,
                        executionEdges,
                        Collections.unmodifiableMap(inputVerticesMap),
                        Collections.unmodifiableMap(targetVerticesMap));

                if (targetVerticesMap.containsKey(vertex.getVertexId())) {
                    vertices.addAll(targetVerticesMap.get(vertex.getVertexId()));
                }
            }
        }

        Set<ExecutionEdge> transformChainEdges = new LinkedHashSet<>();
        for (ExecutionEdge executionEdge : executionEdges) {
            ExecutionVertex leftVertex = executionEdge.getLeftVertex();
            ExecutionVertex rightVertex = executionEdge.getRightVertex();
            boolean needRebuild = false;
            if (chainedTransformVerticesMapping.containsKey(leftVertex.getVertexId())) {
                needRebuild = true;
                leftVertex =
                        transformChainVertexMap.get(
                                chainedTransformVerticesMapping.get(leftVertex.getVertexId()));
            }
            if (chainedTransformVerticesMapping.containsKey(rightVertex.getVertexId())) {
                needRebuild = true;
                rightVertex =
                        transformChainVertexMap.get(
                                chainedTransformVerticesMapping.get(rightVertex.getVertexId()));
            }
            if (needRebuild) {
                executionEdge = new ExecutionEdge(leftVertex, rightVertex);
            }
            transformChainEdges.add(executionEdge);
        }
        return transformChainEdges;
    }

    private void fillChainedTransformExecutionVertex(
            ExecutionVertex currentVertex,
            Map<Long, Long> chainedTransformVerticesMapping,
            Map<Long, ExecutionVertex> transformChainVertexMap,
            Set<ExecutionEdge> executionEdges,
            Map<Long, List<ExecutionVertex>> inputVerticesMap,
            Map<Long, List<ExecutionVertex>> targetVerticesMap) {
        if (chainedTransformVerticesMapping.containsKey(currentVertex.getVertexId())) {
            return;
        }

        List<ExecutionVertex> transformChainedVertices = new ArrayList<>();
        collectChainedVertices(
                currentVertex,
                transformChainedVertices,
                executionEdges,
                inputVerticesMap,
                targetVerticesMap);
        if (transformChainedVertices.size() > 0) {
            long newVertexId = idGenerator.getNextId();
            List<SeaTunnelTransform> transforms = new ArrayList<>(transformChainedVertices.size());
            List<String> pluginOutputs = new ArrayList<>(transformChainedVertices.size());
            List<String> names = new ArrayList<>(transformChainedVertices.size());
            Set<URL> jars = new HashSet<>();
            Set<ConnectorJarIdentifier> identifiers = new HashSet<>();

            transformChainedVertices.stream()
                    .peek(
                            vertex ->
                                    chainedTransformVerticesMapping.put(
                                            vertex.getVertexId(), newVertexId))
                    .map(ExecutionVertex::getAction)
                    .map(action -> (TransformAction) action)
                    .forEach(
                            action -> {
                                transforms.add(action.getTransform());
                                pluginOutputs.add(action.getPluginOutput());
                                jars.addAll(action.getJarUrls());
                                identifiers.addAll(action.getConnectorJarIdentifiers());
                                names.add(action.getName());
                            });
            String transformChainActionName =
                    String.format("TransformChain[%s]", String.join("->", names));
            List<String> transformNames = names;
            TransformChainAction transformChainAction =
                    new TransformChainAction(
                            newVertexId,
                            transformChainActionName,
                            jars,
                            identifiers,
                            transforms,
                            pluginOutputs,
                            transformNames);
            transformChainAction.setParallelism(currentVertex.getAction().getParallelism());

            ExecutionVertex executionVertex =
                    new ExecutionVertex(
                            newVertexId, transformChainAction, currentVertex.getParallelism());
            transformChainVertexMap.put(newVertexId, executionVertex);
            chainedTransformVerticesMapping.put(
                    currentVertex.getVertexId(), executionVertex.getVertexId());
        }
    }

    private void collectChainedVertices(
            ExecutionVertex currentVertex,
            List<ExecutionVertex> chainedVertices,
            Set<ExecutionEdge> executionEdges,
            Map<Long, List<ExecutionVertex>> inputVerticesMap,
            Map<Long, List<ExecutionVertex>> targetVerticesMap) {
        Action action = currentVertex.getAction();
        // Currently only support Transform action chaining.
        if (action instanceof TransformAction) {
            if (chainedVertices.size() == 0) {
                chainedVertices.add(currentVertex);
            } else if (inputVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
                // It cannot be chained to any input vertex if it has multiple input vertices.
                executionEdges.remove(
                        new ExecutionEdge(
                                chainedVertices.get(chainedVertices.size() - 1), currentVertex));
                chainedVertices.add(currentVertex);
            } else {
                return;
            }
        } else {
            return;
        }

        // It cannot chain to any target vertex if it has multiple target vertices.
        if (targetVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
            collectChainedVertices(
                    targetVerticesMap.get(currentVertex.getVertexId()).get(0),
                    chainedVertices,
                    executionEdges,
                    inputVerticesMap,
                    targetVerticesMap);
        }
    }

    private List<Pipeline> generatePipelines(Set<ExecutionEdge> executionEdges) {
        Set<ExecutionVertex> executionVertices = new LinkedHashSet<>();
        for (ExecutionEdge edge : executionEdges) {
            executionVertices.add(edge.getLeftVertex());
            executionVertices.add(edge.getRightVertex());
        }
        PipelineGenerator pipelineGenerator =
                new PipelineGenerator(executionVertices, new ArrayList<>(executionEdges));
        List<Pipeline> pipelines = pipelineGenerator.generatePipelines();

        Set<String> duplicatedActionNames = new HashSet<>();
        Set<String> actionNames = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            Integer pipelineId = pipeline.getId();
            for (ExecutionVertex vertex : pipeline.getVertexes().values()) {
                Action action = vertex.getAction();
                String actionName = String.format("pipeline-%s [%s]", pipelineId, action.getName());
                action.setName(actionName);
                if (actionNames.contains(actionName)) {
                    duplicatedActionNames.add(actionName);
                }
                actionNames.add(actionName);
            }
        }
        checkArgument(
                duplicatedActionNames.isEmpty(),
                "Action name is duplicated: " + duplicatedActionNames);

        return pipelines;
    }
}
