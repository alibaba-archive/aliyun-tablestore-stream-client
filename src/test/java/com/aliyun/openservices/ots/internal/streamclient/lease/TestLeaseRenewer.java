package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLeaseRenewer {

    private ILeaseManager<ShardLease> getLeaseManager() {
        MemoryLeaseManager leaseManager = new MemoryLeaseManager();
        return leaseManager;
    }

    @Test
    public void testFindStealer() throws StreamClientException, DependencyException {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDurationMillis = 20 * 1000;
        LeaseRenewer<ShardLease> leaseRenewer = new LeaseRenewer<ShardLease>(
                leaseManager, workerIdentifier, leaseDurationMillis, Executors.newFixedThreadPool(10));

        List<ShardLease> leases = new ArrayList<ShardLease>();
        for (int i = 0; i < 10; i++) {
            ShardLease lease = new ShardLease("lease" + i);
            if (i < 5) {
                lease.setLeaseOwner(workerIdentifier);
            }
            leaseManager.createLease(lease);
            leases.add(lease);
        }
        leaseRenewer.initialize();
        Map<String, ShardLease> heldLeases = leaseRenewer.getCurrentlyHeldLeases();
        assertEquals(5, heldLeases.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("lease" + i, heldLeases.get("lease" + i).getLeaseKey());
            assertEquals(workerIdentifier, heldLeases.get("lease" + i).getLeaseOwner());
            assertEquals("", heldLeases.get("lease" + i).getLeaseStealer());
        }

        leaseManager.stealLease(leases.get(2), "stealer");
        leaseManager.stealLease(leases.get(4), "stealer");
        leaseManager.stealLease(leases.get(6), "stealer");

        leaseRenewer.findStealer();
        heldLeases = leaseRenewer.getCurrentlyHeldLeases();
        for (int i = 0; i < 5; i++) {
            assertEquals("lease" + i, heldLeases.get("lease" + i).getLeaseKey());
            assertEquals(workerIdentifier, heldLeases.get("lease" + i).getLeaseOwner());
            if (i == 2 || i == 4) {
                assertEquals("stealer", heldLeases.get("lease" + i).getLeaseStealer());
            } else {
                assertEquals("", heldLeases.get("lease" + i).getLeaseStealer());
            }
        }
    }

    @Test
    public void testAddLeasesToRenew() throws StreamClientException, DependencyException {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDurationMillis = 20 * 1000;
        LeaseRenewer<ShardLease> leaseRenewer = new LeaseRenewer<ShardLease>(
                leaseManager, workerIdentifier, leaseDurationMillis, Executors.newFixedThreadPool(10));

        List<ShardLease> leases = new ArrayList<ShardLease>();
        for (int i = 0; i < 10; i++) {
            ShardLease lease = new ShardLease("lease" + i);
            leaseManager.createLease(lease);
            lease.setLastCounterIncrementMillis(System.currentTimeMillis());
            if (i % 3 == 0) {
                leases.add(lease);
            }
        }
        leaseRenewer.initialize();
        Map<String, ShardLease> heldLeases = leaseRenewer.getCurrentlyHeldLeases();
        assertEquals(0, heldLeases.size());

        leaseRenewer.addLeasesToRenew(leases);
        heldLeases = leaseRenewer.getCurrentlyHeldLeases();
        assertEquals(leases.size(), heldLeases.size());
        for (ShardLease lease : leases) {
            assertEquals(lease.getLeaseKey(), heldLeases.get(lease.getLeaseKey()).getLeaseKey());
            assertEquals(lease.getLeaseOwner(), heldLeases.get(lease.getLeaseKey()).getLeaseOwner());
            assertEquals("", heldLeases.get(lease.getLeaseKey()).getLeaseStealer());
        }
    }

    @Test
    public void testUpdateLease() throws StreamClientException, DependencyException {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDurationMillis = 20 * 1000;
        LeaseRenewer<ShardLease> leaseRenewer = new LeaseRenewer<ShardLease>(
                leaseManager, workerIdentifier, leaseDurationMillis, Executors.newFixedThreadPool(10));

        List<ShardLease> leases = new ArrayList<ShardLease>();
        for (int i = 0; i < 10; i++) {
            ShardLease lease = new ShardLease("lease" + i);
            lease.setLastCounterIncrementMillis(System.currentTimeMillis());
            if (i % 3 == 0) {
                lease.setLeaseOwner(workerIdentifier);
                leases.add(lease);
            }
            leaseManager.createLease(lease);
        }

        leaseRenewer.initialize();
        Map<String, ShardLease> heldLeases = leaseRenewer.getCurrentlyHeldLeases();
        assertEquals(4, heldLeases.size());
        for (ShardLease lease : leases) {
            assertEquals(lease.getLeaseKey(), heldLeases.get(lease.getLeaseKey()).getLeaseKey());
            assertEquals(lease.getLeaseOwner(), heldLeases.get(lease.getLeaseKey()).getLeaseOwner());
            assertEquals("", heldLeases.get(lease.getLeaseKey()).getLeaseStealer());
            assertEquals(CheckpointPosition.TRIM_HORIZON, heldLeases.get(lease.getLeaseKey()).getCheckpoint());
        }

        ShardLease lease = heldLeases.get(leases.get(0).getLeaseKey());
        lease.setCheckpoint("testCheckpoint");
        assertTrue(leaseRenewer.updateLease(lease, lease.getLeaseIdentifier()));

        assertEquals("testCheckpoint", leaseManager.getLease(lease.getLeaseKey()).getCheckpoint());
        assertEquals("testCheckpoint", leaseRenewer.getCurrentlyHeldLease(lease.getLeaseKey()).getCheckpoint());

        assertFalse(leaseRenewer.updateLease(lease, "otherIdentifier"));
    }

    @Test
    public void testTransferLease() throws StreamClientException, DependencyException {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDurationMillis = 20 * 1000;
        LeaseRenewer<ShardLease> leaseRenewer = new LeaseRenewer<ShardLease>(
                leaseManager, workerIdentifier, leaseDurationMillis, Executors.newFixedThreadPool(10));

        List<ShardLease> leases = new ArrayList<ShardLease>();
        for (int i = 0; i < 10; i++) {
            ShardLease lease = new ShardLease("lease" + i);
            lease.setLastCounterIncrementMillis(System.currentTimeMillis());
            if (i % 3 == 0) {
                lease.setLeaseOwner(workerIdentifier);
                leases.add(lease);
            }
            leaseManager.createLease(lease);
        }

        leaseRenewer.initialize();
        Map<String, ShardLease> heldLeases = leaseRenewer.getCurrentlyHeldLeases();
        assertEquals(4, heldLeases.size());
        for (ShardLease lease : leases) {
            assertEquals(lease.getLeaseKey(), heldLeases.get(lease.getLeaseKey()).getLeaseKey());
            assertEquals(lease.getLeaseOwner(), heldLeases.get(lease.getLeaseKey()).getLeaseOwner());
            assertEquals("", heldLeases.get(lease.getLeaseKey()).getLeaseStealer());
            assertEquals(CheckpointPosition.TRIM_HORIZON, heldLeases.get(lease.getLeaseKey()).getCheckpoint());
        }

        ShardLease lease = heldLeases.get(leases.get(0).getLeaseKey());
        leaseManager.stealLease(lease, "otherWorker");
        leaseRenewer.findStealer();

        assertEquals("otherWorker", leaseManager.getLease(lease.getLeaseKey()).getLeaseStealer());
        assertEquals("otherWorker", leaseRenewer.getCurrentlyHeldLease(lease.getLeaseKey()).getLeaseStealer());

        assertTrue(leaseRenewer.transferLease(lease.getLeaseKey(), lease.getLeaseIdentifier()));
        assertEquals("otherWorker", leaseManager.getLease(lease.getLeaseKey()).getLeaseOwner());
        assertEquals("otherWorker", leaseManager.getLease(lease.getLeaseKey()).getLeaseStealer());

        assertFalse(leaseRenewer.transferLease(lease.getLeaseKey(), lease.getLeaseIdentifier()));

        lease = heldLeases.get(leases.get(1).getLeaseKey());
        assertFalse(leaseRenewer.transferLease(lease.getLeaseKey(), "otherIdentifier"));
    }

}
