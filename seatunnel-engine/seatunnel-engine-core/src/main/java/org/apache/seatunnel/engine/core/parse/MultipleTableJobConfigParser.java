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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SaveModeExecuteLocation;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceCheckpoint;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getFactoryId;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getInputIds;

@Slf4j
public class MultipleTableJobConfigParser {

    private final IdGenerator idGenerator;
    private final JobConfig jobConfig;

    private final List<URL> commonPluginJars;
    private final Config seaTunnelJobConfig;

    private final ReadonlyConfig envOptions;

    private final boolean isStartWithSavePoint;
    private final List<JobPipelineCheckpointData> pipelineCheckpoints;

    public MultipleTableJobConfigParser(
            String jobDefineFilePath, IdGenerator idGenerator, JobConfig jobConfig) {
        this(jobDefineFilePath, idGenerator, jobConfig, Collections.emptyList(), false);
    }

    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig, IdGenerator idGenerator, JobConfig jobConfig) {
        this(
                seaTunnelJobConfig,
                idGenerator,
                jobConfig,
                Collections.emptyList(),
                false,
                Collections.emptyList());
    }

    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint) {
        this(
                jobDefineFilePath,
                null,
                idGenerator,
                jobConfig,
                commonPluginJars,
                isStartWithSavePoint,
                Collections.emptyList());
    }

    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            List<String> variables,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint,
            List<JobPipelineCheckpointData> pipelineCheckpoints) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.seaTunnelJobConfig = ConfigBuilder.of(Paths.get(jobDefineFilePath), variables);
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.pipelineCheckpoints = pipelineCheckpoints;
    }

    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint,
            List<JobPipelineCheckpointData> pipelineCheckpoints) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.seaTunnelJobConfig = seaTunnelJobConfig;
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.pipelineCheckpoints = pipelineCheckpoints;
    }

    public ImmutablePair<List<Action>, Set<URL>> parse(ClassLoaderService classLoaderService) {
        this.fillJobConfigAndCommonJars();
        List<? extends Config> sourceConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "source", Collections.emptyList());
        List<? extends Config> transformConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "transform", Collections.emptyList());
        List<? extends Config> sinkConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "sink", Collections.emptyList());

        List<URL> sourceConnectorJars = getConnectorJarList(sourceConfigs, PluginType.SOURCE);
        List<URL> transformConnectorJars =
                getConnectorJarList(transformConfigs, PluginType.TRANSFORM);
        List<URL> sinkConnectorJars = getConnectorJarList(sinkConfigs, PluginType.SINK);
        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();

        // source and transform use the same classloader
        List<URL> sourceJars =
                Stream.of(sourceConnectorJars, transformConnectorJars)
                        .flatMap(Collection::stream)
                        .distinct()
                        .collect(Collectors.toList());
        ClassLoader sourceAndTransformClassLoader =
                getClassLoader(classLoaderService, parentClassLoader, sourceJars);
        ClassLoader sinkClassLoader =
                getClassLoader(classLoaderService, parentClassLoader, sinkConnectorJars);

        try {
            Thread.currentThread().setContextClassLoader(sourceAndTransformClassLoader);
            ConfigParserUtil.checkGraph(sourceConfigs, transformConfigs, sinkConfigs);
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap =
                    new LinkedHashMap<>();

            log.info("start generating all sources.");
            if (isStartWithSavePoint
                    && pipelineCheckpoints != null
                    && !pipelineCheckpoints.isEmpty()) {
                Preconditions.checkState(
                        sourceConfigs.size() == pipelineCheckpoints.size(),
                        "The number of source configurations and pipeline checkpoints must be equal.");
            }
            for (int configIndex = 0; configIndex < sourceConfigs.size(); configIndex++) {
                Config sourceConfig = sourceConfigs.get(configIndex);
                Tuple2<String, List<Tuple2<CatalogTable, Action>>> tuple2 =
                        parseSource(configIndex, sourceConfig, sourceAndTransformClassLoader);
                tableWithActionMap.put(tuple2._1(), tuple2._2());
            }

            log.info("start generating all transforms.");
            parseTransforms(transformConfigs, sourceAndTransformClassLoader, tableWithActionMap);

            Thread.currentThread().setContextClassLoader(sinkClassLoader);
            log.info("start generating all sinks.");
            List<Action> sinkActions = new ArrayList<>();
            for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
                Config sinkConfig = sinkConfigs.get(configIndex);
                sinkActions.addAll(
                        parseSink(configIndex, sinkConfig, sinkClassLoader, tableWithActionMap));
            }
            Set<URL> factoryUrls = getUsedFactoryUrls(sinkActions);
            return new ImmutablePair<>(sinkActions, factoryUrls);
        } finally {
            Thread.currentThread().setContextClassLoader(parentClassLoader);
            if (classLoaderService != null) {
                classLoaderService.releaseClassLoader(
                        Long.parseLong(jobConfig.getJobContext().getJobId()), sourceJars);
                classLoaderService.releaseClassLoader(
                        Long.parseLong(jobConfig.getJobContext().getJobId()), sinkConnectorJars);
            }
        }
    }

    private ClassLoader getClassLoader(
            ClassLoaderService classLoaderService,
            ClassLoader parentClassLoader,
            List<URL> connectorJars) {
        ClassLoader classLoader;
        if (classLoaderService == null) {
            classLoader = new SeaTunnelChildFirstClassLoader(connectorJars, parentClassLoader);
        } else {
            classLoader =
                    classLoaderService.getClassLoader(
                            Long.parseLong(jobConfig.getJobContext().getJobId()), connectorJars);
        }
        return classLoader;
    }

    public Set<URL> getUsedFactoryUrls(List<Action> sinkActions) {
        Set<URL> urls = new HashSet<>();
        fillUsedFactoryUrls(sinkActions, urls);
        return urls;
    }

    private List<URL> getConnectorJarList(List<? extends Config> configs, PluginType type) {
        List<PluginIdentifier> factoryIds =
                configs.stream()
                        .map(ConfigParserUtil::getFactoryId)
                        .map(
                                factory ->
                                        PluginIdentifier.of(
                                                CollectionConstants.SEATUNNEL_PLUGIN,
                                                type.getType(),
                                                factory))
                        .collect(Collectors.toList());
        List<URL> jarPaths = new ArrayList<>();
        jarPaths.addAll(new SeaTunnelSinkPluginDiscovery().getPluginJarPaths(factoryIds));
        jarPaths.addAll(commonPluginJars);
        return jarPaths;
    }

    private void fillUsedFactoryUrls(List<Action> actions, Set<URL> result) {
        actions.forEach(
                action -> {
                    result.addAll(action.getJarUrls());
                    if (!action.getUpstream().isEmpty()) {
                        fillUsedFactoryUrls(action.getUpstream(), result);
                    }
                });
    }

    private void fillJobConfigAndCommonJars() {
        JobMode jobMode = envOptions.get(EnvCommonOptions.JOB_MODE);
        jobConfig
                .getJobContext()
                .setJobMode(jobMode)
                .setEnableCheckpoint(
                        (envOptions.get(EnvCommonOptions.CHECKPOINT_INTERVAL) != null)
                                || jobMode == JobMode.STREAMING);
        if (StringUtils.isEmpty(jobConfig.getName())
                || jobConfig.getName().equals(Constants.LOGO)
                || jobConfig.getName().equals(EnvCommonOptions.JOB_NAME.defaultValue())) {
            jobConfig.setName(envOptions.get(EnvCommonOptions.JOB_NAME));
        }
        jobConfig.getEnvOptions().putAll(envOptions.getSourceMap());
        this.commonPluginJars.addAll(
                new ArrayList<>(
                        Common.getThirdPartyJars(
                                        jobConfig
                                                .getEnvOptions()
                                                .getOrDefault(EnvCommonOptions.JARS.key(), "")
                                                .toString())
                                .stream()
                                .map(Path::toUri)
                                .map(
                                        uri -> {
                                            try {
                                                return uri.toURL();
                                            } catch (MalformedURLException e) {
                                                throw new SeaTunnelEngineException(
                                                        "the uri of jar illegal:" + uri, e);
                                            }
                                        })
                                .collect(Collectors.toList())));
        log.info("add common jar in plugins :{}", commonPluginJars);
    }

    private int getParallelism(ReadonlyConfig config) {
        return Math.max(
                1,
                config.getOptional(CommonOptions.PARALLELISM)
                        .orElse(envOptions.get(CommonOptions.PARALLELISM)));
    }

    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
            int configIndex, Config sourceConfig, ClassLoader classLoader) {
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
        final String factoryId = getFactoryId(readonlyConfig);
        final String tableId =
                readonlyConfig.getOptional(CommonOptions.PLUGIN_OUTPUT).orElse(DEFAULT_ID);

        final int parallelism = getParallelism(readonlyConfig);

        Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource =
                pluginIdentifier -> {
                    SeaTunnelSourcePluginDiscovery sourcePluginDiscovery =
                            new SeaTunnelSourcePluginDiscovery();
                    return sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
                };

        Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> tuple2;
        if (isStartWithSavePoint && pipelineCheckpoints != null && !pipelineCheckpoints.isEmpty()) {
            ChangeStreamTableSourceCheckpoint checkpoint =
                    getSourceCheckpoint(configIndex, factoryId);
            tuple2 =
                    FactoryUtil.restoreAndPrepareSource(
                            readonlyConfig,
                            classLoader,
                            factoryId,
                            checkpoint,
                            fallbackCreateSource,
                            null);
        } else {
            tuple2 =
                    FactoryUtil.createAndPrepareSource(
                            readonlyConfig, classLoader, factoryId, fallbackCreateSource, null);
        }

        Set<URL> factoryUrls = new HashSet<>();
        factoryUrls.addAll(getSourcePluginJarPaths(sourceConfig));

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        long id = idGenerator.getNextId();
        String actionName = JobConfigParser.createSourceActionName(configIndex, factoryId);
        SeaTunnelSource<Object, SourceSplit, Serializable> source = tuple2._1();
        source.setJobContext(jobConfig.getJobContext());
        PluginUtil.ensureJobModeMatch(jobConfig.getJobContext(), source);
        SourceAction<Object, SourceSplit, Serializable> action =
                new SourceAction<>(id, actionName, tuple2._1(), factoryUrls, new HashSet<>());
        action.setParallelism(parallelism);
        for (CatalogTable catalogTable : tuple2._2()) {
            actions.add(new Tuple2<>(catalogTable, action));
        }
        return new Tuple2<>(tableId, actions);
    }

    public void parseTransforms(
            List<? extends Config> transformConfigs,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        if (CollectionUtils.isEmpty(transformConfigs) || transformConfigs.isEmpty()) {
            return;
        }
        Queue<Config> configList = new LinkedList<>(transformConfigs);
        int index = 0;
        while (!configList.isEmpty()) {
            parseTransform(index++, configList, classLoader, tableWithActionMap);
        }
    }

    private void parseTransform(
            int index,
            Queue<Config> transforms,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        Config config = transforms.poll();
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        final String factoryId = getFactoryId(readonlyConfig);
        // get jar urls
        Set<URL> jarUrls = new HashSet<>();
        jarUrls.addAll(getTransformPluginJarPaths(config));
        final List<String> inputIds = getInputIds(readonlyConfig);

        List<Tuple2<CatalogTable, Action>> inputs =
                inputIds.stream()
                        .map(tableWithActionMap::get)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
        if (inputs.isEmpty()) {
            if (transforms.isEmpty()) {
                // Tolerates incorrect configuration of simple graph
                inputs = findLast(tableWithActionMap);
            } else {
                // The previous transform has not been created
                transforms.offer(config);
                return;
            }
        }

        final String tableId =
                readonlyConfig.getOptional(CommonOptions.PLUGIN_OUTPUT).orElse(DEFAULT_ID);

        Set<Action> inputActions =
                inputs.stream()
                        .map(Tuple2::_2)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        LinkedHashSet<CatalogTable> catalogTables =
                inputs.stream()
                        .map(Tuple2::_1)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        checkProducedTypeEquals(inputActions);
        int spareParallelism = inputs.get(0)._2().getParallelism();
        int parallelism =
                readonlyConfig.getOptional(CommonOptions.PARALLELISM).orElse(spareParallelism);
        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareMultiTableTransform(
                        new ArrayList<>(catalogTables), readonlyConfig, classLoader, factoryId);

        transform.setJobContext(jobConfig.getJobContext());
        long id = idGenerator.getNextId();
        String actionName = JobConfigParser.createTransformActionName(index, factoryId);

        TransformAction transformAction =
                new TransformAction(
                        id,
                        actionName,
                        new ArrayList<>(inputActions),
                        transform,
                        jarUrls,
                        new HashSet<>(),
                        readonlyConfig.get(CommonOptions.PLUGIN_OUTPUT));
        transformAction.setParallelism(parallelism);

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        List<CatalogTable> producedCatalogTables = transform.getProducedCatalogTables();

        for (CatalogTable catalogTable : producedCatalogTables) {
            actions.add(new Tuple2<>(catalogTable, transformAction));
        }

        tableWithActionMap.put(tableId, actions);
    }

    public static SeaTunnelDataType<?> getProducedType(Action action) {
        if (action instanceof SourceAction) {
            try {
                return ((SourceAction<?, ?, ?>) action)
                        .getSource()
                        .getProducedCatalogTables()
                        .get(0)
                        .getSeaTunnelRowType();
            } catch (UnsupportedOperationException e) {
                // TODO remove it when all connector use `getProducedCatalogTables`
                return ((SourceAction<?, ?, ?>) action).getSource().getProducedType();
            }
        } else if (action instanceof TransformAction) {
            return ((TransformAction) action)
                    .getTransform()
                    .getProducedCatalogTable()
                    .getSeaTunnelRowType();
        }
        throw new UnsupportedOperationException();
    }

    public static void checkProducedTypeEquals(Set<Action> inputActions) {
        SeaTunnelDataType<?> expectedType = getProducedType(new ArrayList<>(inputActions).get(0));
        for (Action action : inputActions) {
            SeaTunnelDataType<?> producedType = getProducedType(action);
            if (!expectedType.equals(producedType)) {
                throw new JobDefineCheckException(
                        "Transform/Sink don't support processing data with two different structures.");
            }
        }
    }

    @Deprecated
    private static <T> T findLast(LinkedHashMap<?, T> map) {
        int size = map.size();
        int i = 1;
        for (T value : map.values()) {
            if (i == size) {
                return value;
            }
            i++;
        }
        // never execution
        return null;
    }

    public List<SinkAction<?, ?, ?, ?>> parseSink(
            int configIndex,
            Config sinkConfig,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        String factoryId = getFactoryId(readonlyConfig);
        List<String> inputIds = getInputIds(readonlyConfig);

        List<List<Tuple2<CatalogTable, Action>>> inputVertices =
                inputIds.stream()
                        .map(tableWithActionMap::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        if (inputVertices.isEmpty()) {
            // Tolerates incorrect configuration of simple graph
            inputVertices = Collections.singletonList(findLast(tableWithActionMap));
        } else if (inputVertices.size() > 1) {
            for (List<Tuple2<CatalogTable, Action>> inputVertex : inputVertices) {
                if (inputVertex.size() > 1) {
                    throw new JobDefineCheckException(
                            "Sink don't support simultaneous writing of data from multi-table source and other sources.");
                }
            }
        }

        // get jar urls
        Set<URL> jarUrls = new HashSet<>();
        jarUrls.addAll(getSinkPluginJarPaths(sinkConfig));
        List<SinkAction<?, ?, ?, ?>> sinkActions = new ArrayList<>();

        // union
        if (inputVertices.size() > 1) {
            Set<Action> inputActions =
                    inputVertices.stream()
                            .flatMap(Collection::stream)
                            .map(Tuple2::_2)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            checkProducedTypeEquals(inputActions);
            Tuple2<CatalogTable, Action> inputActionSample = inputVertices.get(0).get(0);
            SinkAction<?, ?, ?, ?> sinkAction =
                    createSinkAction(
                            inputActionSample._1(),
                            inputActions,
                            readonlyConfig,
                            classLoader,
                            jarUrls,
                            new HashSet<>(),
                            factoryId,
                            inputActionSample._2().getParallelism(),
                            configIndex);
            sinkActions.add(sinkAction);
            return sinkActions;
        }

        // TODO move it into tryGenerateMultiTableSink when we don't support sink template
        // sink template
        for (Tuple2<CatalogTable, Action> tuple : inputVertices.get(0)) {
            SinkAction<?, ?, ?, ?> sinkAction =
                    createSinkAction(
                            tuple._1(),
                            Collections.singleton(tuple._2()),
                            readonlyConfig,
                            classLoader,
                            jarUrls,
                            new HashSet<>(),
                            factoryId,
                            tuple._2().getParallelism(),
                            configIndex);
            sinkActions.add(sinkAction);
        }
        Optional<SinkAction<?, ?, ?, ?>> multiTableSink =
                tryGenerateMultiTableSink(
                        sinkActions, readonlyConfig, classLoader, factoryId, configIndex);
        return multiTableSink
                .<List<SinkAction<?, ?, ?, ?>>>map(Collections::singletonList)
                .orElse(sinkActions);
    }

    private Optional<SinkAction<?, ?, ?, ?>> tryGenerateMultiTableSink(
            List<SinkAction<?, ?, ?, ?>> sinkActions,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryId,
            int configIndex) {
        if (sinkActions.stream()
                .anyMatch(action -> !(action.getSink() instanceof SupportMultiTableSink))) {
            log.info("Unsupported multi table sink api, rollback to sink template");
            return Optional.empty();
        }
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        Set<URL> jars =
                sinkActions.stream()
                        .flatMap(a -> a.getJarUrls().stream())
                        .collect(Collectors.toSet());
        sinkActions.forEach(
                action -> {
                    SeaTunnelSink sink = action.getSink();
                    TablePath tablePath = action.getConfig().getTablePath();
                    sinks.put(tablePath, sink);
                });
        SeaTunnelSink<?, ?, ?, ?> sink =
                FactoryUtil.createMultiTableSink(sinks, options, classLoader);
        String actionName =
                JobConfigParser.createSinkActionName(configIndex, factoryId, "MultiTableSink");
        SinkAction<?, ?, ?, ?> multiTableAction =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        actionName,
                        sinkActions.get(0).getUpstream(),
                        sink,
                        jars,
                        new HashSet<>());
        multiTableAction.setParallelism(sinkActions.get(0).getParallelism());
        return Optional.of(multiTableAction);
    }

    private SinkAction<?, ?, ?, ?> createSinkAction(
            CatalogTable catalogTable,
            Set<Action> inputActions,
            ReadonlyConfig readonlyConfig,
            ClassLoader classLoader,
            Set<URL> factoryUrls,
            Set<ConnectorJarIdentifier> connectorJarIdentifiers,
            String factoryId,
            int parallelism,
            int configIndex) {

        Function<PluginIdentifier, SeaTunnelSink> fallbackCreateSink =
                pluginIdentifier -> {
                    SeaTunnelSinkPluginDiscovery sinkPluginDiscovery =
                            new SeaTunnelSinkPluginDiscovery();
                    return sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
                };

        SeaTunnelSink<?, ?, ?, ?> sink =
                FactoryUtil.createAndPrepareSink(
                        catalogTable,
                        readonlyConfig,
                        classLoader,
                        factoryId,
                        fallbackCreateSink,
                        null);
        sink.setJobContext(jobConfig.getJobContext());
        SinkConfig actionConfig = new SinkConfig(catalogTable.getTableId().toTablePath());
        long id = idGenerator.getNextId();
        String actionName =
                JobConfigParser.createSinkActionName(
                        configIndex, factoryId, actionConfig.getTablePath().toString());
        SinkAction<?, ?, ?, ?> sinkAction =
                new SinkAction<>(
                        id,
                        actionName,
                        new ArrayList<>(inputActions),
                        sink,
                        factoryUrls,
                        connectorJarIdentifiers,
                        actionConfig);
        if (!isStartWithSavePoint) {
            handleSaveMode(sink);
        }
        sinkAction.setParallelism(parallelism);
        return sinkAction;
    }

    public void handleSaveMode(SeaTunnelSink<?, ?, ?, ?> sink) {
        if (SupportSaveMode.class.isAssignableFrom(sink.getClass())) {
            SupportSaveMode saveModeSink = (SupportSaveMode) sink;
            if (envOptions
                    .get(EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION)
                    .equals(SaveModeExecuteLocation.CLIENT)) {
                log.warn(
                        "SaveMode execute location on CLIENT is deprecated, please use CLUSTER instead.");
                Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
                if (saveModeHandler.isPresent()) {
                    try (SaveModeHandler handler = saveModeHandler.get()) {
                        handler.open();
                        new SaveModeExecuteWrapper(handler).execute();
                    } catch (Exception e) {
                        throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                    }
                }
            }
        }
    }

    private List<URL> getSourcePluginJarPaths(Config sourceConfig) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SOURCE_PLUGIN,
                        sourceConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private List<URL> getTransformPluginJarPaths(Config transformConfig) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.TRANSFORM_PLUGIN,
                        transformConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private List<URL> getSinkPluginJarPaths(Config sinkConfig) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SINK_PLUGIN,
                        sinkConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private ChangeStreamTableSourceCheckpoint getSourceCheckpoint(
            int sourceConfigIndex, String sourceFactoryId) {
        String sourceActionName =
                JobConfigParser.createSourceActionName(sourceConfigIndex, sourceFactoryId);
        JobPipelineCheckpointData pipelineCheckpointData =
                pipelineCheckpoints.get(sourceConfigIndex);
        Preconditions.checkArgument(
                pipelineCheckpointData.getPipelineId() == sourceConfigIndex + 1,
                String.format(
                        "The pipeline id in the checkpoint data is %d, but the config index is %d.",
                        pipelineCheckpointData.getPipelineId(), sourceConfigIndex + 1));

        List<JobPipelineCheckpointData.ActionState> sourceCheckpointData =
                pipelineCheckpointData.getTaskStates().entrySet().stream()
                        .filter(entry -> entry.getKey().contains(sourceActionName))
                        .map(e -> e.getValue())
                        .collect(Collectors.toList());
        Preconditions.checkArgument(
                sourceCheckpointData.size() == 1,
                String.format(
                        "The source action name %s is not found in the checkpoint keys %s.",
                        sourceActionName, pipelineCheckpointData.getTaskStates().keySet()));

        byte[] coordinatorState = sourceCheckpointData.get(0).getCoordinatorState().get(0);
        List<List<byte[]>> subtaskState =
                sourceCheckpointData.get(0).getSubtaskState().stream()
                        .flatMap(
                                (Function<
                                                JobPipelineCheckpointData.ActionSubtaskState,
                                                Stream<List<byte[]>>>)
                                        state ->
                                                state == null
                                                        ? Stream.of(Collections.emptyList())
                                                        : Stream.of(state.getState()))
                        .collect(Collectors.toList());
        return new ChangeStreamTableSourceCheckpoint(coordinatorState, subtaskState);
    }
}
