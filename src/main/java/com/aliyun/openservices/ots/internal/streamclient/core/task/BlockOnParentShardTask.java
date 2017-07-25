package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.ICheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.ShardInfo;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 检查是否全部ParentShard都已处理完(checkpoint＝SHARD_END)。
 * 只有当一个Shard的全部ParentShard都已处理完时，才能处理该Shard，因为ParentShard的数据在时序上是先于该Shard的数据的。
 * Task每次执行只检查一次，若检查不通过，sleep一段时间(pollIntervalMillis)，用于控制轮询间隔。
 */
public class BlockOnParentShardTask implements ITask {

    private static final Logger LOG = LoggerFactory.getLogger(BlockOnParentShardTask.class);

    private final ShardInfo shardInfo;
    private final ILeaseManager<ShardLease> leaseManager;
    private ICheckpointTracker checkpointTracker;
    private final long parentShardPollIntervalMillis;

    public BlockOnParentShardTask(ShardInfo shardInfo,
                                  ILeaseManager<ShardLease> leaseManager,
                                  ICheckpointTracker checkpointTracker,
                                  long parentShardPollIntervalMillis) {
        this.shardInfo = shardInfo;
        this.leaseManager = leaseManager;
        this.checkpointTracker = checkpointTracker;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
    }

    public TaskResult call() {
        LOG.debug("Start, ShardId: {}, ParentIds: {}.", shardInfo.getShardId(), shardInfo.getParentShardIds());
        try {
            for (String shardId : shardInfo.getParentShardIds()) {
                /**
                 * parentShard读完了的条件：
                 * 1. 对应ShardLease不存在，可能已经被系统回收或者处理完毕被清理。
                 * 2. 对应shardLease存在，通过checkpointTracker拿到的checkpoint必须是SHARD_END.
                 */
                ShardLease lease = leaseManager.getLease(shardId);
                if (lease != null) {
                    String checkpoint = checkpointTracker.getCheckpoint(shardId);
                    if ((checkpoint == null) || (!checkpoint.equals(CheckpointPosition.SHARD_END))) {
                        TimeUtils.sleepMillis(parentShardPollIntervalMillis);
                        LOG.debug("Parent shard not complete, ShardId: {}, ParentId: {}, Checkpoint: {}.",
                                shardInfo.getShardId(), shardId, checkpoint);
                        return new TaskResult(false);
                    }
                } else {
                    LOG.info("No lease found for parent shard: {}.", shardId);
                }
            }
            LOG.info("Parent shard complete, ShardId: {}.", shardInfo.getShardId());
            return new TaskResult(true);
        } catch (Exception e) {
            LOG.warn("ShardId: {}, Exception: {}.", shardInfo.getShardId(), e);
            return new TaskResult(e);
        }
    }

    public TaskType getTaskType() {
        return TaskType.BLOCK_ON_PARENT_SHARDS;
    }

}
