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

package org.apache.seatunnel.engine.server;

import static org.apache.seatunnel.api.common.metrics.MetricTags.JOB_ID;
import static org.apache.seatunnel.api.common.metrics.MetricTags.PIPELINE_ID;
import static org.apache.seatunnel.api.common.metrics.MetricTags.TASK_GROUP_ID;
import static org.apache.seatunnel.api.common.metrics.MetricTags.TASK_GROUP_LOCATION;
import static org.apache.seatunnel.api.common.metrics.MetricTags.TASK_ID;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

import org.apache.seatunnel.api.common.metrics.MetricTags;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskCallTimer;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.execution.TaskTracker;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.engine.server.task.operation.NotifyTaskStatusOperation;

import com.google.common.collect.Lists;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.properties.HazelcastProperties;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * This class is responsible for the execution of the Task
 */
public class TaskExecutionService implements DynamicMetricsProvider {

    private final String hzInstanceName;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private volatile boolean isRunning = true;
    private final LinkedBlockingDeque<TaskTracker> threadShareTaskQueue = new LinkedBlockingDeque<>();
    private final ExecutorService executorService = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final RunBusWorkSupplier runBusWorkSupplier = new RunBusWorkSupplier(executorService, threadShareTaskQueue);
    // key: TaskID
    private final ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts = new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskGroupLocation, TaskGroupContext> finishedExecutionContexts = new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskGroupLocation, CompletableFuture<Void>> cancellationFutures =
        new ConcurrentHashMap<>();
    private final SeaTunnelConfig seaTunnelConfig;

    private final ScheduledExecutorService scheduledExecutorService;

    public TaskExecutionService(NodeEngineImpl nodeEngine, HazelcastProperties properties) {
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        this.hzInstanceName = nodeEngine.getHazelcastInstance().getName();
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLoggingService().getLogger(TaskExecutionService.class);

        MetricsRegistry registry = nodeEngine.getMetricsRegistry();
        MetricDescriptor descriptor = registry.newMetricDescriptor()
            .withTag(MetricTags.SERVICE, this.getClass().getSimpleName());
        registry.registerStaticMetrics(descriptor, this);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::updateMetricsContextInImap, 0, seaTunnelConfig.getEngineConfig().getJobMetricsBackupInterval(), TimeUnit.SECONDS);
    }

    public void start() {
        runBusWorkSupplier.runNewBusWork(false);
    }

    public void shutdown() {
        isRunning = false;
        executorService.shutdownNow();
        scheduledExecutorService.shutdown();
    }

    public TaskGroupContext getExecutionContext(TaskGroupLocation taskGroupLocation) {
        if (executionContexts.get(taskGroupLocation) == null) {
            return finishedExecutionContexts.get(taskGroupLocation);
        }
        return executionContexts.get(taskGroupLocation);
    }

    private void submitThreadShareTask(TaskGroupExecutionTracker taskGroupExecutionTracker, List<Task> tasks) {
        Stream<TaskTracker> taskTrackerStream = tasks.stream()
            .map(t -> {
                if (!taskGroupExecutionTracker.executionCompletedExceptionally()) {
                    try {
                        TaskTracker taskTracker = new TaskTracker(t, taskGroupExecutionTracker);
                        taskTracker.task.init();
                        return taskTracker;
                    } catch (Exception e) {
                        taskGroupExecutionTracker.exception(e);
                        taskGroupExecutionTracker.taskDone(t);
                    }
                }
                return null;
            });
        if (!taskGroupExecutionTracker.executionCompletedExceptionally()) {
            taskTrackerStream.forEach(threadShareTaskQueue::add);
        }
    }

    private void submitBlockingTask(TaskGroupExecutionTracker taskGroupExecutionTracker, List<Task> tasks) {

        CountDownLatch startedLatch = new CountDownLatch(tasks.size());
        taskGroupExecutionTracker.blockingFutures = tasks
            .stream()
            .map(t -> new BlockingWorker(new TaskTracker(t, taskGroupExecutionTracker), startedLatch))
            .map(executorService::submit)
            .collect(toList());

        // Do not return from this method until all workers have started. Otherwise,
        // on cancellation there is a race where the executor might not have started
        // the worker yet. This would result in taskletDone() never being called for
        // a worker.
        uncheckRun(startedLatch::await);
    }

    public PassiveCompletableFuture<TaskExecutionState> deployTask(@NonNull Data taskImmutableInformation) {
        TaskGroupImmutableInformation taskImmutableInfo =
            nodeEngine.getSerializationService().toObject(taskImmutableInformation);
        return deployTask(taskImmutableInfo);
    }

    public <T extends Task> T getTask(TaskLocation taskLocation) {
        return this.getExecutionContext(taskLocation.getTaskGroupLocation()).getTaskGroup()
            .getTask(taskLocation.getTaskID());
    }

    public PassiveCompletableFuture<TaskExecutionState> deployTask(
        @NonNull TaskGroupImmutableInformation taskImmutableInfo) {
        CompletableFuture<TaskExecutionState> resultFuture = new CompletableFuture<>();
        TaskGroup taskGroup = null;
        try {
            Set<URL> jars = taskImmutableInfo.getJars();
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (!CollectionUtils.isEmpty(jars)) {
                classLoader = new SeaTunnelChildFirstClassLoader(Lists.newArrayList(jars));
                taskGroup =
                    CustomClassLoadedObject.deserializeWithCustomClassLoader(nodeEngine.getSerializationService(),
                        classLoader,
                        taskImmutableInfo.getGroup());
            } else {
                taskGroup = nodeEngine.getSerializationService().toObject(taskImmutableInfo.getGroup());
            }
            logger.info(String.format("deploying task %s", taskGroup.getTaskGroupLocation()));

            synchronized (this) {
                if (executionContexts.containsKey(taskGroup.getTaskGroupLocation())) {
                    throw new RuntimeException(
                        String.format("TaskGroupLocation: %s already exists", taskGroup.getTaskGroupLocation()));
                }
                return deployLocalTask(taskGroup, resultFuture, classLoader);
            }
        } catch (Throwable t) {
            logger.severe(String.format("TaskGroupID : %s  deploy error with Exception: %s",
                taskGroup != null && taskGroup.getTaskGroupLocation() != null ?
                    taskGroup.getTaskGroupLocation().toString() : "taskGroupLocation is null",
                ExceptionUtils.getMessage(t)));
            resultFuture.complete(
                new TaskExecutionState(
                    taskGroup != null && taskGroup.getTaskGroupLocation() != null ? taskGroup.getTaskGroupLocation() :
                        null, ExecutionState.FAILED, t));
        }
        return new PassiveCompletableFuture<>(resultFuture);
    }

    @Deprecated
    public PassiveCompletableFuture<TaskExecutionState> deployLocalTask(
        @NonNull TaskGroup taskGroup,
        @NonNull CompletableFuture<TaskExecutionState> resultFuture) {
        return deployLocalTask(taskGroup, resultFuture, Thread.currentThread().getContextClassLoader());
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public PassiveCompletableFuture<TaskExecutionState> deployLocalTask(
        @NonNull TaskGroup taskGroup,
        @NonNull CompletableFuture<TaskExecutionState> resultFuture,
        @NonNull ClassLoader classLoader) {
        try {
            taskGroup.init();
            Collection<Task> tasks = taskGroup.getTasks();
            CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();
            TaskGroupExecutionTracker executionTracker =
                new TaskGroupExecutionTracker(cancellationFuture, taskGroup, resultFuture);
            ConcurrentMap<Long, TaskExecutionContext> taskExecutionContextMap = new ConcurrentHashMap<>();
            final Map<Boolean, List<Task>> byCooperation =
                tasks.stream()
                    .peek(task -> {
                        TaskExecutionContext taskExecutionContext = new TaskExecutionContext(task, nodeEngine);
                        task.setTaskExecutionContext(taskExecutionContext);
                        taskExecutionContextMap.put(task.getTaskID(), taskExecutionContext);
                    })
                    .collect(partitioningBy(Task::isThreadsShare));
            executionContexts.put(taskGroup.getTaskGroupLocation(), new TaskGroupContext(taskGroup, classLoader));
            cancellationFutures.put(taskGroup.getTaskGroupLocation(), cancellationFuture);
            submitThreadShareTask(executionTracker, byCooperation.get(true));
            submitBlockingTask(executionTracker, byCooperation.get(false));
            taskGroup.setTasksContext(taskExecutionContextMap);
        } catch (Throwable t) {
            logger.severe(ExceptionUtils.getMessage(t));
            resultFuture.completeExceptionally(t);
        }
        resultFuture.whenComplete(withTryCatch(logger, (r, s) -> {
            logger.info(
                String.format("Task %s complete with state %s", r != null ? r.getTaskGroupLocation() : "null",
                    r != null ? r.getExecutionState() : "null"));
            notifyTaskStatusToMaster(taskGroup.getTaskGroupLocation(), r);
        }));
        return new PassiveCompletableFuture<>(resultFuture);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void notifyTaskStatusToMaster(TaskGroupLocation taskGroupLocation, TaskExecutionState taskExecutionState) {
        long sleepTime = 1000;
        boolean notifyStateSuccess = false;
        while (isRunning && !notifyStateSuccess) {
            InvocationFuture<Object> invoke = nodeEngine.getOperationService().createInvocationBuilder(
                SeaTunnelServer.SERVICE_NAME,
                new NotifyTaskStatusOperation(taskGroupLocation, taskExecutionState),
                nodeEngine.getMasterAddress()).invoke();
            try {
                invoke.get();
                notifyStateSuccess = true;
            } catch (InterruptedException e) {
                logger.severe("send notify task status failed", e);
            } catch (ExecutionException e) {
                logger.warning(ExceptionUtils.getMessage(e));
                logger.warning(String.format("notify the job of the task(%s) status failed, retry in %s millis",
                    taskGroupLocation, sleepTime));
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ex) {
                    logger.severe(e);
                }
            }
        }
    }

    /**
     * JobMaster call this method to cancel a task, and then {@link TaskExecutionService} cancel this task and send the
     * {@link TaskExecutionState} to JobMaster.
     *
     * @param taskGroupLocation TaskGroup.getTaskGroupLocation()
     */
    public void cancelTaskGroup(TaskGroupLocation taskGroupLocation) {
        logger.info(String.format("Task (%s) need cancel.", taskGroupLocation));
        if (cancellationFutures.containsKey(taskGroupLocation)) {
            try {
                cancellationFutures.get(taskGroupLocation).cancel(false);
            } catch (CancellationException ignore) {
                // ignore
            }
        } else {
            logger.warning(String.format("need cancel taskId : %s is not exist", taskGroupLocation));
        }

    }

    public void notifyCleanTaskGroupContext(TaskGroupLocation taskGroupLocation) {
        finishedExecutionContexts.remove(taskGroupLocation);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        try {
            MetricDescriptor copy1 = descriptor.copy().withTag(MetricTags.SERVICE, this.getClass().getSimpleName());
            Map<TaskGroupLocation, TaskGroupContext> contextMap = new HashMap<>();
            contextMap.putAll(executionContexts);
            contextMap.putAll(finishedExecutionContexts);
            contextMap.forEach((taskGroupLocation, taskGroupContext) -> {
                MetricDescriptor copy2 = copy1.copy().withTag(TASK_GROUP_LOCATION, taskGroupLocation.toString())
                    .withTag(JOB_ID, String.valueOf(taskGroupLocation.getJobId()))
                    .withTag(PIPELINE_ID, String.valueOf(taskGroupLocation.getPipelineId()))
                    .withTag(TASK_GROUP_ID, String.valueOf(taskGroupLocation.getTaskGroupId()));
                taskGroupContext.getTaskGroup().getTasks().forEach(task -> {
                    Long taskID = task.getTaskID();
                    MetricDescriptor copy3 = copy2.copy().withTag(TASK_ID, String.valueOf(taskID));
                    task.provideDynamicMetrics(copy3, context);
                });
            });
            updateMetricsContextInImap();
        } catch (Throwable t) {
            logger.warning("Dynamic metric collection failed", t);
            throw t;
        }
    }

    private synchronized void updateMetricsContextInImap() {
        Map<TaskGroupLocation, TaskGroupContext> contextMap = new HashMap<>();
        contextMap.putAll(executionContexts);
        contextMap.putAll(finishedExecutionContexts);
        try {
            IMap<TaskLocation, SeaTunnelMetricsContext> map =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
            contextMap.forEach((taskGroupLocation, taskGroupContext) -> {
                taskGroupContext.getTaskGroup().getTasks().forEach(task -> {
                    // MetricsContext only exists in SeaTunnelTask
                    if (task instanceof SeaTunnelTask) {
                        SeaTunnelTask seaTunnelTask = (SeaTunnelTask) task;
                        if (null != seaTunnelTask.getMetricsContext()) {
                            map.put(seaTunnelTask.getTaskLocation(), seaTunnelTask.getMetricsContext());
                        }
                    }
                });
            });
        } catch (WrongTargetException e){
            logger.warning("The Imap acquisition failed due to the hazelcast node being offline or restarted, and will be retried next time", e);
        }
    }

    private final class BlockingWorker implements Runnable {

        private final TaskTracker tracker;
        private final CountDownLatch startedLatch;

        private BlockingWorker(TaskTracker tracker, CountDownLatch startedLatch) {
            this.tracker = tracker;
            this.startedLatch = startedLatch;
        }

        @Override
        public void run() {
            TaskExecutionService.TaskGroupExecutionTracker taskGroupExecutionTracker = tracker.taskGroupExecutionTracker;
            ClassLoader classLoader = executionContexts.get(taskGroupExecutionTracker.taskGroup.getTaskGroupLocation()).getClassLoader();
            ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
            final Task t = tracker.task;
            try {
                startedLatch.countDown();
                t.init();
                ProgressState result;
                do {
                    result = t.call();
                } while (!result.isDone() && isRunning &&
                    !taskGroupExecutionTracker.executionCompletedExceptionally());
            } catch (InterruptedException e) {
                logger.warning(String.format("Interrupted task %d - %s", t.getTaskID(), t));
                if (taskGroupExecutionTracker.executionException.get() == null && !taskGroupExecutionTracker.isCancel.get()) {
                    taskGroupExecutionTracker.exception(e);
                }
            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                taskGroupExecutionTracker.exception(e);
            } finally {
                taskGroupExecutionTracker.taskDone(t);
            }
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    private final class BlockingTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(@NonNull Runnable r) {
            return new Thread(r,
                String.format("hz.%s.seaTunnel.task.thread-%d", hzInstanceName, seq.getAndIncrement()));
        }
    }

    /**
     * CooperativeTaskWorker is used to poll the task call method,
     * When a task times out, a new BusWork will be created to take over the execution of the task
     */
    public final class CooperativeTaskWorker implements Runnable {

        AtomicBoolean keep = new AtomicBoolean(true);
        public AtomicReference<TaskTracker> exclusiveTaskTracker = new AtomicReference<>();
        final TaskCallTimer timer;
        private Thread myThread;
        public LinkedBlockingDeque<TaskTracker> taskqueue;

        @SuppressWarnings("checkstyle:MagicNumber")
        public CooperativeTaskWorker(LinkedBlockingDeque<TaskTracker> taskqueue,
                                     RunBusWorkSupplier runBusWorkSupplier) {
            logger.info(String.format("Created new BusWork : %s", this.hashCode()));
            this.taskqueue = taskqueue;
            this.timer = new TaskCallTimer(50, keep, runBusWorkSupplier, this);
        }

        @SneakyThrows
        @Override
        public void run() {
            myThread = currentThread();
            while (keep.get() && isRunning) {
                TaskTracker taskTracker = null != exclusiveTaskTracker.get() ?
                    exclusiveTaskTracker.get() :
                    taskqueue.takeFirst();
                TaskGroupExecutionTracker taskGroupExecutionTracker = taskTracker.taskGroupExecutionTracker;
                if (taskGroupExecutionTracker.executionCompletedExceptionally()) {
                    taskGroupExecutionTracker.taskDone(taskTracker.task);
                    if (null != exclusiveTaskTracker.get()) {
                        // If it's exclusive need to end the work
                        break;
                    } else {
                        // No action required and don't put back
                        continue;
                    }
                }
                //start timer, if it's exclusive, don't need to start
                if (null == exclusiveTaskTracker.get()) {
                    timer.timerStart(taskTracker);
                }
                ProgressState call = null;
                try {
                    //run task
                    myThread.setContextClassLoader(executionContexts.get(taskGroupExecutionTracker.taskGroup.getTaskGroupLocation()).getClassLoader());
                    call = taskTracker.task.call();
                    synchronized (timer) {
                        timer.timerStop();
                    }
                } catch (Throwable e) {
                    //task Failure and complete
                    taskGroupExecutionTracker.exception(e);
                    taskGroupExecutionTracker.taskDone(taskTracker.task);
                    //If it's exclusive need to end the work
                    logger.warning("Exception in " + taskTracker.task, e);
                    if (null != exclusiveTaskTracker.get()) {
                        break;
                    }
                } finally {
                    //stop timer
                    timer.timerStop();
                }
                //task call finished
                if (null != call) {
                    if (call.isDone()) {
                        //If it's exclusive, you need to end the work
                        taskGroupExecutionTracker.taskDone(taskTracker.task);
                        if (null != exclusiveTaskTracker.get()) {
                            break;
                        }
                    } else {
                        //Task is not completed. Put task to the end of the queue
                        //If the current work has an exclusive tracker, it will not be put back
                        if (null == exclusiveTaskTracker.get()) {
                            taskqueue.offer(taskTracker);
                        }
                    }
                }
            }
        }
    }

    /**
     * Used to create a new BusWork and run
     */
    public final class RunBusWorkSupplier {

        ExecutorService executorService;
        LinkedBlockingDeque<TaskTracker> taskQueue;

        public RunBusWorkSupplier(ExecutorService executorService, LinkedBlockingDeque<TaskTracker> taskqueue) {
            this.executorService = executorService;
            this.taskQueue = taskqueue;
        }

        public boolean runNewBusWork(boolean checkTaskQueue) {
            if (!checkTaskQueue || taskQueue.size() > 0) {
                executorService.submit(new CooperativeTaskWorker(taskQueue, this));
                return true;
            }
            return false;
        }
    }

    /**
     * Internal utility class to track the overall state of tasklet execution.
     * There's one instance of this class per job.
     */
    public final class TaskGroupExecutionTracker {

        private final TaskGroup taskGroup;
        final CompletableFuture<TaskExecutionState> future;
        volatile List<Future<?>> blockingFutures = emptyList();

        private final AtomicInteger completionLatch;
        private final AtomicReference<Throwable> executionException = new AtomicReference<>();

        private final AtomicBoolean isCancel = new AtomicBoolean(false);

        TaskGroupExecutionTracker(@NonNull CompletableFuture<Void> cancellationFuture, @NonNull TaskGroup taskGroup,
                                  @NonNull CompletableFuture<TaskExecutionState> future) {
            this.future = future;
            this.completionLatch = new AtomicInteger(taskGroup.getTasks().size());
            this.taskGroup = taskGroup;
            cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
                isCancel.set(true);
                if (e == null) {
                    e = new IllegalStateException("cancellationFuture should be completed exceptionally");
                }
                exception(e);
                cancelAllTask();
            }));
        }

        void exception(Throwable t) {
            executionException.compareAndSet(null, t);
        }

        private void cancelAllTask() {
            try {
                blockingFutures.forEach(f -> f.cancel(true));
            } catch (CancellationException ignore) {
                // ignore
            }
        }

        void taskDone(Task task) {
            TaskGroupLocation taskGroupLocation = taskGroup.getTaskGroupLocation();
            logger.info(String.format("taskDone, taskId = %d, taskGroup = %s", task.getTaskID(), taskGroupLocation));
            Throwable ex = executionException.get();
            if (completionLatch.decrementAndGet() == 0) {
                finishedExecutionContexts.put(taskGroupLocation, executionContexts.remove(taskGroupLocation));
                cancellationFutures.remove(taskGroupLocation);
                if (ex == null) {
                    future.complete(new TaskExecutionState(taskGroupLocation, ExecutionState.FINISHED, null));
                    return;
                } else if (isCancel.get()) {
                    future.complete(new TaskExecutionState(taskGroupLocation, ExecutionState.CANCELED, null));
                    return;
                } else {
                    future.complete(new TaskExecutionState(taskGroupLocation, ExecutionState.FAILED, ex));
                }
            }
            if (!isCancel.get() && ex != null) {
                cancelAllTask();
            }
        }

        boolean executionCompletedExceptionally() {
            return executionException.get() != null;
        }
    }

}
