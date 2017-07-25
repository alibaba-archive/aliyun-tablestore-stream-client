package com.aliyun.openservices.ots.internal.streamclient.core;

import com.aliyun.openservices.ots.internal.streamclient.ClientConfig;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseCoordinator;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TestCheckpointTracker {

    @Test
    public void testCheckpointTracker() throws StreamClientException, DependencyException, ShutdownException {
        MemoryLeaseManager leaseManager = new MemoryLeaseManager();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setTakerIntervalMillis(100);
        clientConfig.setRenewerIntervalMillis(100);
        LeaseCoordinator<ShardLease> leaseCoordinator = new LeaseCoordinator<ShardLease>(leaseManager, "worker", clientConfig);

        CheckpointTracker tracker = new CheckpointTracker(leaseManager, leaseCoordinator);

        leaseCoordinator.initialize();
        leaseCoordinator.start();

        ShardLease lease = new ShardLease("shard");
        leaseManager.createLease(lease);

        assertEquals(CheckpointPosition.TRIM_HORIZON, tracker.getCheckpoint("shard"));
        assertEquals(null, tracker.getCheckpoint("notExistShard"));

        TimeUtils.sleepMillis(1000);

        Collection<ShardLease> leases = leaseCoordinator.getCurrentlyHeldLeases();
        assertEquals(1, leases.size());
        String leaseIdentifier = null;
        for (ShardLease shardLease : leases) {
            leaseIdentifier = shardLease.getLeaseIdentifier();
        }

        tracker.setCheckpoint("shard", "checkpoint", leaseIdentifier);

        assertEquals("checkpoint", tracker.getCheckpoint("shard"));
    }
}
