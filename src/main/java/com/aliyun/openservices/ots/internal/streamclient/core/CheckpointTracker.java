package com.aliyun.openservices.ots.internal.streamclient.core;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseCoordinator;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.ICheckpointTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ICheckpointTracker的实现类，用于记录和获取Checkpoint。
 * Checkpoint作为ShardLease的一个属性，持久化在StatusTable中，通过UpdateLease和GetLease进行更新和获取。
 */
class CheckpointTracker implements ICheckpointTracker {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointTracker.class);

    private final ILeaseManager<ShardLease> leaseManager;
    private final LeaseCoordinator<ShardLease> leaseCoordinator;

    CheckpointTracker(ILeaseManager<ShardLease> leaseManager,
                      LeaseCoordinator<ShardLease> leaseCoordinator) {
        this.leaseManager = leaseManager;
        this.leaseCoordinator = leaseCoordinator;
    }

    public void setCheckpoint(String shardId, String checkpointValue, String leaseIdentifier) throws ShutdownException, StreamClientException, DependencyException {
        if (!setCheckpointByUpdateLease(shardId, checkpointValue, leaseIdentifier)) {
            LOG.warn("Can't update checkpoint because worker doesn't hold the lease for this shard.");
            throw new ShutdownException("Can't update checkpoint because worker doesn't hold the lease for this shard.");
        }
    }

    private boolean setCheckpointByUpdateLease(String shardId, String checkpointValue, String leaseIdentifier) throws StreamClientException, DependencyException {
        ShardLease lease = leaseCoordinator.getCurrentlyHeldLease(shardId);
        if (lease == null) {
            return false;
        }
        lease.setCheckpoint(checkpointValue);
        boolean result = leaseCoordinator.updateLease(lease, leaseIdentifier);
        LOG.debug("Set, ShardId: {}, CheckpointValue: {}", shardId, checkpointValue);
        return result;
    }

    public String getCheckpoint(String shardId) throws StreamClientException, DependencyException {
        if (leaseManager.getLease(shardId) == null) {
            return null;
        }
        return leaseManager.getLease(shardId).getCheckpoint();
    }
}
