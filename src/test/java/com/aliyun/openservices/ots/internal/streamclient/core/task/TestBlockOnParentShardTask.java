package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.mock.SimpleMockCheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.ICheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.ShardInfo;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestBlockOnParentShardTask {

    /**
     * CaseList：
     * 1. 没有parentshards。
     * 2. 有parentShards，全部处理完成。
     * 3. 有parentShards，有一个没有处理完成。
     * 4. 有parentShards，存在parentShard没有对应的Lease，代表该parentShard已经被回收
     */

    @Test
    public void testNoParentShards() {
        ILeaseManager<ShardLease> mockLeaseManager = new MemoryLeaseManager();
        ICheckpointTracker mockCheckpointTracker = new SimpleMockCheckpointTracker();
        long pollTime = 100;
        ShardInfo shardInfo = new ShardInfo("TestShard", "", new HashSet<String>(), "");

        BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, mockLeaseManager, mockCheckpointTracker, pollTime);
        TaskResult result = task.call();
        assertEquals(TaskType.BLOCK_ON_PARENT_SHARDS, task.getTaskType());
        assertEquals(null, result.getException());
        assertEquals(true, result.isPhaseCompleted());
    }

    @Test
    public void testAllParentShardsEnd() throws StreamClientException, DependencyException, ShutdownException {
        ILeaseManager<ShardLease> mockLeaseManager = new MemoryLeaseManager();
        ICheckpointTracker mockCheckpointTracker = new SimpleMockCheckpointTracker();
        long pollTime = 100;
        List<String> parentShards = Arrays.asList("ParentShard", "ParentSiblingShard");
        for (String parentShard : parentShards) {
            ShardLease shardLease = new ShardLease(parentShard);
            mockLeaseManager.createLease(shardLease);
            mockCheckpointTracker.setCheckpoint(parentShard, CheckpointPosition.SHARD_END, "");
        }
        ShardInfo shardInfo = new ShardInfo("TestShard", "", new HashSet<String>(parentShards), "");

        BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, mockLeaseManager, mockCheckpointTracker, pollTime);
        TaskResult result = task.call();
        assertEquals(null, result.getException());
        assertEquals(true, result.isPhaseCompleted());
    }

    @Test
    public void testHasParentShardNotEnd() throws StreamClientException, DependencyException, ShutdownException {
        ILeaseManager<ShardLease> mockLeaseManager = new MemoryLeaseManager();
        ICheckpointTracker mockCheckpointTracker = new SimpleMockCheckpointTracker();
        long pollTime = 100;
        List<String> parentShards = Arrays.asList("ParentShard", "ParentSiblingShard");
        for (String parentShard : parentShards) {
            ShardLease shardLease = new ShardLease(parentShard);
            mockLeaseManager.createLease(shardLease);
        }
        mockCheckpointTracker.setCheckpoint(parentShards.get(0), CheckpointPosition.SHARD_END, "");
        ShardInfo shardInfo = new ShardInfo("TestShard", "", new HashSet<String>(parentShards), "");
        {
            BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, mockLeaseManager, mockCheckpointTracker, pollTime);
            TaskResult result = task.call();
            assertEquals(null, result.getException());
            assertEquals(false, result.isPhaseCompleted());
        }
        {
            mockCheckpointTracker.setCheckpoint(parentShards.get(1), "dasdasdas", "");
            BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, mockLeaseManager, mockCheckpointTracker, pollTime);
            TaskResult result = task.call();
            assertEquals(null, result.getException());
            assertEquals(false, result.isPhaseCompleted());
        }
    }

    @Test
    public void testHasParentShardExpired() throws StreamClientException, DependencyException, ShutdownException {
        ILeaseManager<ShardLease> mockLeaseManager = new MemoryLeaseManager();
        ICheckpointTracker mockCheckpointTracker = new SimpleMockCheckpointTracker();
        long pollTime = 100;
        List<String> parentShards = Arrays.asList("ParentShard", "ParentSiblingShard");
        ShardLease shardLease = new ShardLease(parentShards.get(0));
        mockLeaseManager.createLease(shardLease);
        ShardInfo shardInfo = new ShardInfo("TestShard", "", new HashSet<String>(parentShards), "");
        {
            mockCheckpointTracker.setCheckpoint(parentShards.get(0), CheckpointPosition.SHARD_END, "");
            BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, mockLeaseManager, mockCheckpointTracker, pollTime);
            TaskResult result = task.call();
            assertEquals(null, result.getException());
            assertEquals(true, result.isPhaseCompleted());
        }
        {
            mockCheckpointTracker.setCheckpoint(parentShards.get(0), "dasdasdas", "");
            BlockOnParentShardTask task = new BlockOnParentShardTask(shardInfo, mockLeaseManager, mockCheckpointTracker, pollTime);
            TaskResult result = task.call();
            assertEquals(null, result.getException());
            assertEquals(false, result.isPhaseCompleted());
        }
    }
}
