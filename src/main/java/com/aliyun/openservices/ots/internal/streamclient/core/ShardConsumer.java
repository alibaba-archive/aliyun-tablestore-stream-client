package com.aliyun.openservices.ots.internal.streamclient.core;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.core.task.*;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class ShardConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

    enum ShardConsumerState {
        WAITING_ON_PARENT_SHARDS, INITIALIZING, PROCESSING, SHUTTING_DOWN, SHUTDOWN_COMPLETE;
    }

    private final ShardInfo shardInfo;
    private final StreamConfig streamConfig;
    private final IRecordProcessor recordProcessor;
    private final ILeaseManager<ShardLease> leaseManager;
    private final long parentShardPollIntervalMillis;
    private final ExecutorService executorService;
    private ICheckpointTracker checkpointTracker;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final DataFetcher dataFetcher;
    private final ShardSyncer shardSyncer;
    private final IRetryStrategy taskRetryStrategy;

    private ITask currentTask;
    private long currentTaskSubmitTime;
    private Future<TaskResult> future;
    private ShardConsumerState currentState = ShardConsumerState.WAITING_ON_PARENT_SHARDS;
    private boolean beginShutdown;
    private ShutdownReason shutdownReason;
    private IShutdownMarker shutdownMarker = new IShutdownMarker() {

        public void markForProcessDone() {
            ShardConsumer.this.markForShutdown(ShutdownReason.PROCESS_DONE);
        }

        public void markForProcessRestart() {
            ShardConsumer.this.markForShutdown(ShutdownReason.PROCESS_RESTART);
        }
    };

    public ShardConsumer(ShardInfo shardInfo,
                         StreamConfig streamConfig,
                         ICheckpointTracker checkpointTracker,
                         IRecordProcessor recordProcessor,
                         ILeaseManager<ShardLease> leaseManager,
                         long parentShardPollIntervalMillis,
                         ExecutorService executorService,
                         ShardSyncer shardSyncer,
                         IRetryStrategy taskRetryStrategy) {
        this.shardInfo = shardInfo;
        this.streamConfig = streamConfig;
        this.checkpointTracker = checkpointTracker;
        this.recordProcessor = recordProcessor;
        this.leaseManager = leaseManager;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.executorService = executorService;
        this.shardSyncer = shardSyncer;
        this.taskRetryStrategy = taskRetryStrategy;
        this.dataFetcher = new DataFetcher(streamConfig.getOTSClient(), shardInfo);
        this.recordProcessorCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpointTracker, dataFetcher);
    }

    public synchronized boolean consumeShard() throws StreamClientException, DependencyException {
        return checkAndSubmitNextTask();
    }

    synchronized boolean checkAndSubmitNextTask() throws StreamClientException, DependencyException {
        if (future != null && !future.isCancelled() && !future.isDone()) {
            return false;
        }

        boolean isPhaseCompleted = false;
        if (future != null && future.isDone()) {
            try {
                TaskResult result = future.get();
                if (result.getException() == null) {
                    isPhaseCompleted = result.isPhaseCompleted();
                    LOG.debug("PreviousTaskDone, ShardId: {}, Task: {}, IsPhaseCompleted: {}.",
                            shardInfo.getShardId(), currentTask.getTaskType(), isPhaseCompleted);
                } else {
                    LOG.error("ShardId: {}, Task: {}, Exception: {}.",
                            shardInfo.getShardId(), currentTask.getTaskType(), result.getException());
                    throw result.getException();
                }
            } catch (DependencyException e) {
                throw e;
            } catch (StreamClientException e) {
                throw e;
            } catch (Exception e) {
                throw new StreamClientException(e.getMessage(), e);
            }
        }

        boolean submittedNewTask = false;
        updateState(isPhaseCompleted);
        ITask nextTask = getNextTask();
        if (nextTask != null) {
            currentTask = nextTask;
            future = executorService.submit(currentTask);
            currentTaskSubmitTime = System.currentTimeMillis();
            submittedNewTask = true;
            LOG.debug("SubmitNewTask, ShardId: {}, Task: {}.", shardInfo.getShardId(), currentTask.getTaskType());
        }
        return submittedNewTask;
    }

    /**
     * Figure out next task to run based on current state, task, and shutdown context.
     *
     * @return Return next task to run
     */
    ITask getNextTask() {
        ITask task = null;
        ITask taskDecorated = null;
        switch (currentState) {
            case WAITING_ON_PARENT_SHARDS:
                task =
                        new BlockOnParentShardTask(shardInfo,
                                leaseManager,
                                checkpointTracker,
                                parentShardPollIntervalMillis);
                taskDecorated =
                        new RetryingTaskDecorator(
                                IRetryStrategy.RetryableAction.TASK_BLOCK_ON_PARENT_SHARD,
                                taskRetryStrategy,
                                task);
                break;
            case INITIALIZING:
                task =
                        new InitializeTask(shardInfo,
                                recordProcessor,
                                checkpointTracker,
                                recordProcessorCheckpointer,
                                dataFetcher,
                                shutdownMarker);
                taskDecorated =
                        new RetryingTaskDecorator(
                                IRetryStrategy.RetryableAction.TASK_INITIALIZE,
                                taskRetryStrategy,
                                task);
                break;
            case PROCESSING:
                task =
                        new ProcessTask(shardInfo,
                                recordProcessor,
                                recordProcessorCheckpointer,
                                dataFetcher,
                                streamConfig,
                                shutdownMarker);
                taskDecorated =
                        new RetryingTaskDecorator(
                                IRetryStrategy.RetryableAction.TASK_PROCESS,
                                taskRetryStrategy,
                                task);
                break;
            case SHUTTING_DOWN:
                task =
                        new ShutdownTask(shardInfo,
                                recordProcessor,
                                recordProcessorCheckpointer,
                                shutdownReason,
                                shardSyncer);
                taskDecorated =
                        new RetryingTaskDecorator(
                                IRetryStrategy.RetryableAction.TASK_SHUTDOWN,
                                taskRetryStrategy,
                                task);
                break;
            case SHUTDOWN_COMPLETE:
                break;
            default:
                break;
        }

        return taskDecorated;
    }

    synchronized boolean beginShutdown(ShutdownReason reason) throws StreamClientException, DependencyException {
        if (!isShutdown()) {
            markForShutdown(reason);
            checkAndSubmitNextTask();
        }
        return isShutdown();
    }

    synchronized void markForShutdown(ShutdownReason reason) {
        beginShutdown = true;
        shutdownReason = reason;
    }

    public boolean isShutdown() {
        return currentState == ShardConsumerState.SHUTDOWN_COMPLETE;
    }

    public ShutdownReason getShutdownReason() {
        return shutdownReason;
    }

    void updateState(boolean isPhaseCompleted) {
        switch (currentState) {
            case WAITING_ON_PARENT_SHARDS:
                if (isPhaseCompleted && TaskType.BLOCK_ON_PARENT_SHARDS.equals(currentTask.getTaskType())) {
                    if (beginShutdown) {
                        currentState = ShardConsumerState.SHUTDOWN_COMPLETE;
                    } else {
                        currentState = ShardConsumerState.INITIALIZING;
                    }
                } else if ((currentTask == null) && beginShutdown) {
                    currentState = ShardConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case INITIALIZING:
                if (isPhaseCompleted && TaskType.INITIALIZE.equals(currentTask.getTaskType())) {
                    if (beginShutdown) {
                        currentState = ShardConsumerState.SHUTTING_DOWN;
                    } else {
                        currentState = ShardConsumerState.PROCESSING;
                    }
                } else if ((currentTask == null) && beginShutdown) {
                    currentState = ShardConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case PROCESSING:
                if (TaskType.PROCESS.equals(currentTask.getTaskType())) {
                    if (beginShutdown) {
                        currentState = ShardConsumerState.SHUTTING_DOWN;
                    } else if (isPhaseCompleted) {
                        markForShutdown(ShutdownReason.TERMINATE);
                        currentState = ShardConsumerState.SHUTTING_DOWN;
                    }
                }
                break;
            case SHUTTING_DOWN:
                if (currentTask == null
                        || (isPhaseCompleted && TaskType.SHUTDOWN.equals(currentTask.getTaskType()))) {
                    currentState = ShardConsumerState.SHUTDOWN_COMPLETE;
                }
                break;
            case SHUTDOWN_COMPLETE:
                break;
            default:
                LOG.error("Unexpected state: " + currentState);
                break;
        }
    }

    ShardConsumerState getCurrentState() {
        return currentState;
    }

}
