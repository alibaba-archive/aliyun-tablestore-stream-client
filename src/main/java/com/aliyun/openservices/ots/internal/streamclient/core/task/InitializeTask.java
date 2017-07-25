package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.aliyun.openservices.ots.internal.streamclient.core.exceptions.ApplicationException;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.core.DataFetcher;
import com.aliyun.openservices.ots.internal.streamclient.core.RecordProcessorCheckpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 初始化DataFetcher、Checkpointer, 执行用户的initialize方法。
 */
public class InitializeTask implements ITask {

    private static final Logger LOG = LoggerFactory.getLogger(InitializeTask.class);

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final DataFetcher dataFetcher;
    private final ICheckpointTracker checkpointTracker;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final IShutdownMarker shutdownMarker;

    public InitializeTask(ShardInfo shardInfo,
                          IRecordProcessor recordProcessor,
                          ICheckpointTracker checkpointTracker,
                          RecordProcessorCheckpointer recordProcessorCheckpointer,
                          DataFetcher dataFetcher,
                          IShutdownMarker shutdownMarker) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.checkpointTracker = checkpointTracker;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.dataFetcher = dataFetcher;
        this.shutdownMarker = shutdownMarker;
    }

    public TaskResult call() {
        LOG.debug("Start, ShardId: {}.", shardInfo.getShardId());
        try {
            String initialCheckpoint = checkpointTracker.getCheckpoint(shardInfo.getShardId());
            if (initialCheckpoint == null || initialCheckpoint.isEmpty()) {
                initialCheckpoint = CheckpointPosition.TRIM_HORIZON;
            }
            LOG.debug("ShardId: {}, InitialCheckpoint: {}.", shardInfo.getShardId(), initialCheckpoint);

            dataFetcher.initialize(initialCheckpoint);
            recordProcessorCheckpointer.setLargestPermittedCheckpointValue(initialCheckpoint);

            InitializationInput initializationInput = new InitializationInput();
            initializationInput.setShardInfo(shardInfo);
            initializationInput.setInitialCheckpoint(initialCheckpoint);
            initializationInput.setCheckpointer(recordProcessorCheckpointer);
            initializationInput.setShutdownMarker(shutdownMarker);

            try {
                recordProcessor.initialize(initializationInput);
            } catch (Exception e) {
                throw new ApplicationException("ApplicationInitializeError", e);
            }
            LOG.debug("Complete, ShardId: {}.", shardInfo.getShardId());
            return new TaskResult(true);
        } catch (Exception e) {
            LOG.warn("ShardId: {}, Exception: {}.", shardInfo.getShardId(), e);
            return new TaskResult(e);
        }
    }

    public TaskType getTaskType() {
        return TaskType.INITIALIZE;
    }
}
