package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestShardSyncTask {

    private ShardSyncTask createShardSyncTask(SyncClientInterface ots, ILeaseManager<ShardLease> leaseManager) {
        StreamConfig config = new StreamConfig();
        config.setOTSClient(ots);
        config.setDataTableName("testTable");
        ShardSyncTask task = new ShardSyncTask(config, leaseManager);
        return task;
    }

    @Test
    public void testGetStreamId() throws DependencyException {
        MockOTS ots = new MockOTS();
        ShardSyncTask task = createShardSyncTask(ots, new MemoryLeaseManager());
        String streamId = ots.listStream(new ListStreamRequest("testTable")).getStreams().get(0).getStreamId();
        assertEquals(streamId, task.getStreamId());
    }

    @Test
    public void testCleanupGarbageLeases() throws StreamClientException, DependencyException {
        MockOTS ots = new MockOTS();
        MemoryLeaseManager leaseManager = new MemoryLeaseManager();
        List<ShardLease> currentLeases = new ArrayList<ShardLease>();
        for (int i = 0; i < 4; i++) {
            ShardLease lease = new ShardLease("shard" + i);
            currentLeases.add(lease);
            leaseManager.createLease(lease);
        }
        assertEquals(4, leaseManager.listLeases().size());

        List<StreamShard> shards = new ArrayList<StreamShard>();
        for (int i = 2; i < 6; i++) {
            StreamShard shard = new StreamShard("shard" + i);
            shards.add(shard);
        }

        ShardSyncTask task = createShardSyncTask(ots, leaseManager);
        task.cleanupGarbageLeases(shards, currentLeases);

        List<ShardLease> list = leaseManager.listLeases();
        assertEquals(2, list.size());
        assertEquals(list.get(0).getLeaseKey(), "shard2");
        assertEquals(list.get(1).getLeaseKey(), "shard3");
    }

    @Test
    public void testDetermineNewLeasesToCreate() throws Exception {
        List<StreamShard> shards = new ArrayList<StreamShard>();
        StreamShard a = new StreamShard("a");
        StreamShard b = new StreamShard("b");
        StreamShard c = new StreamShard("c");
        StreamShard d = new StreamShard("d");
        StreamShard e = new StreamShard("e");
        StreamShard f = new StreamShard("f");
        StreamShard g = new StreamShard("g");
        StreamShard h = new StreamShard("h");
        StreamShard i = new StreamShard("i");
        StreamShard j = new StreamShard("j");
        shards.add(a);
        shards.add(b);
        shards.add(c);
        shards.add(d);
        shards.add(e);
        shards.add(f);
        shards.add(g);
        shards.add(h);
        shards.add(i);
        shards.add(j);

        d.setParentId("a");
        e.setParentId("a");

        f.setParentId("e");
        f.setParentSiblingId("b");

        g.setParentId("f");
        h.setParentId("f");

        i.setParentId("d");
        i.setParentSiblingId("g");

        j.setParentId("i");
        j.setParentSiblingId("h");

        MockOTS ots = new MockOTS();
        ILeaseManager<ShardLease> leaseManager = new MemoryLeaseManager();
        ShardSyncTask sst = createShardSyncTask(ots, leaseManager);
        List<ShardLease> leaseToCreate = sst.determineNewLeasesToCreate(shards, new ArrayList<ShardLease>());
        checkLeaseOrder(leaseToCreate, shards, new ArrayList<String>());

        leaseToCreate = sst.determineNewLeasesToCreate(shards, Arrays.asList(new ShardLease("a"), new ShardLease("b"), new ShardLease("f")));
        checkLeaseOrder(leaseToCreate, shards, Arrays.asList("a", "b", "f"));
    }

    private void checkLeaseOrder(List<ShardLease> leaseToCreate, List<StreamShard> shards, List<String> createdShards) {
        Map<String, Integer> leaseToId =  new HashMap<String, Integer>();
        for (int id = 0; id < leaseToCreate.size(); id++) {
            ShardLease lease = leaseToCreate.get(id);
            leaseToId.put(lease.getLeaseKey(), id);
        }

        assertEquals(leaseToId.size(), shards.size() - createdShards.size());
        for (StreamShard shard : shards) {
            if (createdShards.contains(shard.getShardId())) {
                continue;
            }

            assertTrue(leaseToId.containsKey(shard.getShardId()));
            int id = leaseToId.get(shard.getShardId());

            if (shard.getParentId() != null) {
                Integer pid = leaseToId.get(shard.getParentId());
                assertTrue(pid == null || pid < id);
            }

            if (shard.getParentSiblingId() != null) {
                Integer pid = leaseToId.get(shard.getParentSiblingId());
                assertTrue(pid == null || pid < id);
            }
        }
    }
}
