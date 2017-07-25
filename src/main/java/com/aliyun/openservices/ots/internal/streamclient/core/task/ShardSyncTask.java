package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.utils.OTSHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 同步Shard列表，为新增的Shard创建Lease，同时清除已经过期的Shard的lease。
 * 新增Lease时，必须先为parent shard添加，后为child shard添加。
 * 当前我们支持的初始化Checkpoint只有TRIM_HORIZON，即oldest record。
 */
public class ShardSyncTask implements ITask {

    private static final Logger LOG = LoggerFactory.getLogger(ShardSyncTask.class);

    private final SyncClientInterface ots;
    private final String tableName;
    private final ILeaseManager<ShardLease> leaseManager;

    private String streamId;

    public ShardSyncTask(StreamConfig streamConfig,
                         ILeaseManager<ShardLease> leaseManager) {
        this.ots = streamConfig.getOTSClient();
        this.tableName = streamConfig.getDataTableName();
        this.leaseManager = leaseManager;
    }

    public TaskResult call() {
        LOG.debug("Start shard sync task.");
        try {
            checkAndCreateLeasesForNewShards();
            LOG.debug("Shard Sync task completed.");
            return new TaskResult(true);
        } catch (Exception e) {
            LOG.warn("Exception encountered in shard sync task: {}", e);
            return new TaskResult(e);
        }
    }

    public TaskType getTaskType() {
        return TaskType.SHARDSYNC;
    }

    void checkAndCreateLeasesForNewShards() throws StreamClientException, DependencyException {
        List<StreamShard> shardList = OTSHelper.listShard(ots, getStreamId());
        List<ShardLease> currentLeases = leaseManager.listLeases();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Current leases, count: {}." + currentLeases.size());
            for (ShardLease lease : currentLeases) {
                LOG.debug("ShardLease: {}", lease);
            }
        }

        List<ShardLease> newLeasesToCreate = determineNewLeasesToCreate(shardList, currentLeases);
        for (ShardLease lease : newLeasesToCreate) {
            leaseManager.createLease(lease);
            LOG.info("New lease created, Lease: {}.", lease);
        }
        cleanupGarbageLeases(shardList, currentLeases);
    }

    List<ShardLease> determineNewLeasesToCreate(List<StreamShard> shardList, List<ShardLease> currentLeases) throws DependencyException {
        Set<String> leaseKeySet = new HashSet<String>();
        for (ShardLease lease : currentLeases) {
            leaseKeySet.add(lease.getLeaseKey());
        }

        Map<String, ShardLease> shardIdToNewShard = new HashMap<String, ShardLease>();
        for (StreamShard shard : shardList) {
            if (!leaseKeySet.contains(shard.getShardId())) {
                ShardLease newShard = createNewShardLease(getStreamId(), shard);
                shardIdToNewShard.put(newShard.getLeaseKey(), newShard);
            }
        }

        LinkedHashMap<String, ShardLease> sortedLeasesToCreate = new LinkedHashMap<String, ShardLease>();
        for (ShardLease lease : shardIdToNewShard.values()) {
            sortLeaseInInherit(lease, shardIdToNewShard, sortedLeasesToCreate);
        }
        return new ArrayList<ShardLease>(sortedLeasesToCreate.values());
    }

    private void sortLeaseInInherit(ShardLease lease, Map<String, ShardLease> shardIdToNewShard, LinkedHashMap<String, ShardLease> sortedLeasesToCreate) {
        // add parent first
        for (String shardId : lease.getParentShardIds()) {
            ShardLease parentLease = shardIdToNewShard.get(shardId);
            if (parentLease != null && !sortedLeasesToCreate.containsKey(parentLease.getLeaseKey())) {
                sortLeaseInInherit(parentLease, shardIdToNewShard, sortedLeasesToCreate);
            }
        }

        // add itself
        if (!sortedLeasesToCreate.containsKey(lease.getLeaseKey())) {
            sortedLeasesToCreate.put(lease.getLeaseKey(), lease);
        }
    }

    ShardLease createNewShardLease(String streamId, StreamShard shard) {
        ShardLease shardLease = new ShardLease(shard.getShardId());
        shardLease.setStreamId(streamId);
        Set<String> parentShardIds = new HashSet<String>();
        if (shard.getParentId() != null) {
            parentShardIds.add(shard.getParentId());
        }
        if (shard.getParentSiblingId() != null) {
            parentShardIds.add(shard.getParentSiblingId());
        }
        shardLease.setParentShardIds(parentShardIds);

        // currently only TRIM_HORIZON is supported
        shardLease.setCheckpoint(CheckpointPosition.TRIM_HORIZON);
        return shardLease;
    }

    String getStreamId() throws DependencyException {
        if (streamId != null) {
            return streamId;
        } else {
            List<Stream> streams = OTSHelper.listStream(ots, tableName);
            if (streams.isEmpty()) {
                throw new DependencyException("Can't get streamId. Please check whether to enable Stream.");
            }

            if (streams.size() != 1) {
                LOG.error("Expect there is only one stream, tableName: {}.", tableName);
                for (Stream stream : streams) {
                    LOG.error("Stream: {}", stream);
                }
                throw new DependencyException("Expect there is only one stream.");
            }

            streamId = streams.get(0).getStreamId();
            return streamId;
        }
    }

    void cleanupGarbageLeases(List<StreamShard> shardList, List<ShardLease> currentLeases)
            throws StreamClientException, DependencyException {
        Set<String> leaseKeySet = new HashSet<String>();
        for (ShardLease lease : currentLeases) {
            leaseKeySet.add(lease.getLeaseKey());
        }
        for (StreamShard shard : shardList) {
            if (leaseKeySet.contains(shard.getShardId())) {
                leaseKeySet.remove(shard.getShardId());
            }
        }
        for (String leaseKey : leaseKeySet) {
            leaseManager.deleteLease(leaseKey);
            LOG.info("Delete expired lease, LeaseKey: {}.", leaseKey);
        }
    }
}
