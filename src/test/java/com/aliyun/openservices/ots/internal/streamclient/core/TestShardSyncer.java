package com.aliyun.openservices.ots.internal.streamclient.core;

import com.alicloud.openservices.tablestore.model.ListStreamRequest;
import com.alicloud.openservices.tablestore.model.ListStreamResponse;
import com.alicloud.openservices.tablestore.model.StreamShard;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.core.task.ShardSyncTask;
import com.aliyun.openservices.ots.internal.streamclient.core.task.TaskRetryStrategy;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseCoordinator;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class TestShardSyncer {

    @Test
    public void testShardSyncer() throws StreamClientException, DependencyException {
        MockOTS ots = new MockOTS();

        String streamId = ots.listStream(new ListStreamRequest("testTable")).getStreams().get(0).getStreamId();

        ShardInfo shardInfo = new ShardInfo("TestShard", streamId, new HashSet<String>(), "");

        ots.createShard(shardInfo.getShardId(), null, null);

        StreamConfig config = new StreamConfig();
        config.setDataTableName("testTable");
        config.setStatusTableName("statusTable");
        config.setOTSClient(ots);

        MemoryLeaseManager leaseManager = new MemoryLeaseManager();

        ExecutorService executorService = Executors.newCachedThreadPool();

        IRetryStrategy retryStrategy = new TaskRetryStrategy();

        ShardSyncer shardSyncer = new ShardSyncer(config, leaseManager, executorService, retryStrategy);

        shardSyncer.syncShardAndLeaseInfo(true);

        assertEquals(shardInfo.getShardId(), leaseManager.getLease(shardInfo.getShardId()).getLeaseKey());
        assertEquals(shardInfo.getStreamId(), leaseManager.getLease(shardInfo.getShardId()).getStreamId());
    }
}
