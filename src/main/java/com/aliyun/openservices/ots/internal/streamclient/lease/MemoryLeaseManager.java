package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryLeaseManager implements ILeaseManager<ShardLease> {

    private NavigableMap<String, ShardLease> table = new ConcurrentSkipListMap<String, ShardLease>();

    public boolean createLeaseTableIfNotExists(int readCU, int writeCU, int ttl) {
        return true;
    }

    public boolean waitUntilTableReady(long maxWaitTimeMillis) {
        return true;
    }

    public List<ShardLease> listLeases() {
        List<ShardLease> shardLeases = new ArrayList<ShardLease>();
        for (String leaseKey : table.keySet()) {
            shardLeases.add(table.get(leaseKey).<ShardLease>copy());
        }
        return shardLeases;
    }

    public void createLease(ShardLease lease) {
        ShardLease copy = lease.copy();
        copy.setLeaseIdentifier("");
        copy.setLastCounterIncrementMillis(0);
        table.put(lease.getLeaseKey(), copy);
    }

    public ShardLease getLease(String leaseKey) {
        if (table.get(leaseKey) == null) {
            return null;
        }
        return table.get(leaseKey).copy();
    }

    public boolean renewLease(ShardLease lease) {
        synchronized (table) {
            if (table.get(lease.getLeaseKey()).getLeaseCounter() != lease.getLeaseCounter()) {
                return false;
            }
            lease.setLeaseCounter(lease.getLeaseCounter() + 1);
            ShardLease newLease = table.get(lease.getLeaseKey());
            newLease.setLeaseCounter(newLease.getLeaseCounter() + 1);
            table.put(newLease.getLeaseKey(), newLease);
            return true;
        }
    }

    public boolean takeLease(ShardLease lease, String owner) {
        synchronized (table) {
            if (table.get(lease.getLeaseKey()).getLeaseCounter() != lease.getLeaseCounter()) {
                return false;
            }
            lease.setLeaseCounter(lease.getLeaseCounter() + 1);
            lease.setLeaseOwner(owner);
            lease.setLeaseStealer("");
            ShardLease newLease = table.get(lease.getLeaseKey());
            newLease.setLeaseCounter(newLease.getLeaseCounter() + 1);
            newLease.setLeaseOwner(owner);
            newLease.setLeaseStealer("");
            table.put(newLease.getLeaseKey(), newLease);
            return true;
        }
    }

    public boolean stealLease(ShardLease lease, String stealer) {
        synchronized (table) {
            if (!table.get(lease.getLeaseKey()).getLeaseStealer().isEmpty()) {
                return false;
            }
            if (!table.get(lease.getLeaseKey()).getLeaseOwner().equals(lease.getLeaseOwner())) {
                return false;
            }
            lease.setLeaseStealer(stealer);
            ShardLease newLease = table.get(lease.getLeaseKey());
            newLease.setLeaseStealer(stealer);
            table.put(newLease.getLeaseKey(), newLease);
            return true;
        }
    }

    public boolean transferLease(ShardLease lease) {
        synchronized (table) {
            if (table.get(lease.getLeaseKey()).getLeaseCounter() != lease.getLeaseCounter()) {
                return false;
            }
            lease.setLeaseOwner(lease.getLeaseStealer());
            lease.setLeaseCounter(lease.getLeaseCounter() + 1);

            ShardLease newLease = table.get(lease.getLeaseKey());
            newLease.setLeaseCounter(newLease.getLeaseCounter() + 1);
            newLease.setLeaseOwner(lease.getLeaseStealer());
            table.put(newLease.getLeaseKey(), newLease);
            return true;
        }
    }

    public void deleteLease(String leaseKey) {
        synchronized (table) {
            table.remove(leaseKey);
        }
    }

    public boolean updateLease(ShardLease lease) {
        synchronized (table) {
            if (table.get(lease.getLeaseKey()).getLeaseCounter() != lease.getLeaseCounter()) {
                return false;
            }
            lease.setLeaseCounter(lease.getLeaseCounter() + 1);
            ShardLease newLease = table.get(lease.getLeaseKey());
            newLease.setLeaseCounter(newLease.getLeaseCounter() + 1);
            newLease.setCheckpoint(lease.getCheckpoint());
            table.put(newLease.getLeaseKey(), newLease);
            return true;
        }
    }

    public void clear() {
        table.clear();
    }
}
