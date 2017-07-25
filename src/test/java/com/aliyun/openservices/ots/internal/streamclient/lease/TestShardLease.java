package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestShardLease {

    @Test
    public void testInitialize() {
        ShardLease lease = new ShardLease("leaseKey");

        assertEquals(lease.getLeaseKey(), "leaseKey");
        assertEquals(lease.getStreamId(), "");
        assertEquals(lease.getLastCounterIncrementMillis(), 0);
        assertEquals(lease.getParentShardIds(), new HashSet<String>());
        assertEquals(lease.getLeaseIdentifier(), "");
        assertEquals(lease.getCheckpoint(), CheckpointPosition.TRIM_HORIZON);
        assertEquals(lease.getLeaseStealer(), "");
        assertEquals(lease.getLeaseCounter(), 0);
    }

    @Test
    public void testCopy() {
        ShardLease lease = new ShardLease("leaseKey");
        lease.setCheckpoint("check point");
        lease.setLeaseOwner("lease owner");
        lease.setLeaseStealer("stealer");
        lease.setLastCounterIncrementMillis(101);
        lease.setLeaseIdentifier("lease identifier");
        lease.setParentShardIds(new HashSet<String>(Arrays.asList("shardId1", "shardId2")));
        lease.setStreamId("stream id");
        lease.setLeaseCounter(10);

        ShardLease copy = new ShardLease(lease);
        assertEquals(copy.getCheckpoint(), lease.getCheckpoint());
        assertEquals(copy.getParentShardIds(), lease.getParentShardIds());
        assertEquals(copy.getStreamId(), lease.getStreamId());
        assertEquals(copy.getLeaseCounter(), lease.getLeaseCounter());
        assertEquals(copy.getLeaseIdentifier(), lease.getLeaseIdentifier());
        assertEquals(copy.getLastCounterIncrementMillis(), lease.getLastCounterIncrementMillis());
        assertEquals(copy.getLeaseKey(), lease.getLeaseKey());
        assertEquals(copy.getLeaseOwner(), lease.getLeaseOwner());

        lease.getParentShardIds().add("shardId3");
        assertTrue(!copy.getParentShardIds().equals(lease.getParentShardIds()));
    }

    @Test
    public void testHashcode() {
        ShardLease lease = new ShardLease("leaseKey");
        lease.setCheckpoint("check point");
        lease.setLeaseOwner("lease owner");
        lease.setLeaseStealer("stealer");
        lease.setLastCounterIncrementMillis(101);
        lease.setLeaseIdentifier("lease identifier");
        lease.setParentShardIds(new HashSet<String>(Arrays.asList("shardId1", "shardId2")));
        lease.setStreamId("stream id");
        lease.setLeaseCounter(10);

        ShardLease lease2 = new ShardLease(lease);
        assertEquals(lease.hashCode(), lease2.hashCode());

        lease2.setCheckpoint("check point 2");
        assertTrue(lease.hashCode() != lease2.hashCode());

        lease2 = new ShardLease(lease);
        lease2.getParentShardIds().add("shardId3");
        assertTrue(lease.hashCode() != lease2.hashCode());

        lease2 = new ShardLease(lease);
        lease2.setStreamId("stream id 2");
        assertTrue(lease.hashCode() != lease2.hashCode());
    }

    @Test
    public void testEquals() {
        ShardLease lease = new ShardLease("leaseKey");
        lease.setCheckpoint("check point");
        lease.setLeaseOwner("lease owner");
        lease.setLeaseStealer("stealer");
        lease.setLastCounterIncrementMillis(101);
        lease.setLeaseIdentifier("lease identifier");
        lease.setParentShardIds(new HashSet<String>(Arrays.asList("shardId1", "shardId2")));
        lease.setStreamId("stream id");
        lease.setLeaseCounter(10);

        ShardLease lease2 = new ShardLease(lease);

        assertTrue(lease.equals(lease2));

        lease2.setCheckpoint("check point 2");
        assertTrue(!lease.equals(lease2));

        lease2 = new ShardLease(lease);
        lease2.getParentShardIds().add("shardId3");
        assertTrue(!lease.equals(lease2));

        lease2 = new ShardLease(lease);
        lease2.setStreamId("stream id 2");
        assertTrue(!lease.equals(lease2));
    }
}
