package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.aliyun.openservices.ots.internal.streamclient.core.ShardSyncer;
import com.aliyun.openservices.ots.internal.streamclient.core.exceptions.ApplicationException;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.core.RecordProcessorCheckpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 执行用户的shutdown方法。
 * 如果ShutdownReason为TERMINATE，需要做一次SyncShards。
 */
public class ShutdownTask implements ITask {

    private static final Logger LOG = LoggerFactory.getLogger(ShutdownTask.class);

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownReason shutdownReason;
    private final ShardSyncer shardSyncer;

    public ShutdownTask(ShardInfo shardInfo,
                        IRecordProcessor recordProcessor,
                        RecordProcessorCheckpointer recordProcessorCheckpointer,
                        ShutdownReason shutdownReason,
                        ShardSyncer shardSyncer) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.shutdownReason = shutdownReason;
        this.shardSyncer = shardSyncer;
    }

    public TaskResult call() {
        LOG.debug("Start, ShardId: {}, Reason: {}.", shardInfo.getShardId(), shutdownReason);
        try {
            if (shutdownReason == ShutdownReason.TERMINATE) {
                recordProcessorCheckpointer.setLargestPermittedCheckpointValue(CheckpointPosition.SHARD_END);
            }
            ShutdownInput shutdownInput = new ShutdownInput();
            shutdownInput.setCheckpointer(recordProcessorCheckpointer);
            shutdownInput.setShutdownReason(shutdownReason);
            try {
                recordProcessor.shutdown(shutdownInput);
            } catch (ApplicationException e) {
                throw new ApplicationException("ApplicationShutDownError", e);
            }
            LOG.debug("Complete, ShardId: {}.", shardInfo.getShardId());
            if (shutdownReason == ShutdownReason.TERMINATE) {
                shardSyncer.syncShardAndLeaseInfo(true);
            }
            return new TaskResult(true);
        } catch (Exception e) {
            LOG.warn("ShardId: {}, Exception: {}.", shardInfo.getShardId(), e);
            return new TaskResult(e);
        }
    }

    public TaskType getTaskType() {
        return TaskType.SHUTDOWN;
    }
}
