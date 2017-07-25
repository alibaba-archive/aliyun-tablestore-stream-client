package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestLeaseTaker {


    private ILeaseManager<ShardLease> getLeaseManager() {
        return new MemoryLeaseManager();
    }

    @Test
    public void testUpdateLeases() throws Exception {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();

        {
            // lease has no owner
            ShardLease lease = new ShardLease("lease_1");
            lease.setLeaseCounter(1);
            lease.setLeaseOwner("");
            lease.setLeaseStealer("");

            leaseManager.createLease(lease);
        }

        {
            // lease has owner
            ShardLease lease = new ShardLease("lease_2");
            lease.setLeaseCounter(1);
            lease.setLeaseOwner("worker_1");
            lease.setLeaseStealer("");

            leaseManager.createLease(lease);
        }

        {
            // lease has owner
            ShardLease lease = new ShardLease("lease_3");
            lease.setLeaseCounter(1);
            lease.setLeaseOwner("worker_2");
            lease.setLeaseStealer("");

            leaseManager.createLease(lease);
        }

        {
            // lease has owner
            ShardLease lease = new ShardLease("lease_4");
            lease.setLeaseCounter(1);
            lease.setLeaseOwner("worker_3");
            lease.setLeaseStealer("");

            leaseManager.createLease(lease);
        }

        {
            // lease has owner
            ShardLease lease = new ShardLease("lease_5");
            lease.setLeaseCounter(1);
            lease.setLeaseOwner("worker_4");
            lease.setLeaseStealer("");

            leaseManager.createLease(lease);
        }

        String workerIdentifier = "worker";
        long leaseDuration = 3 * 1000;
        LeaseTaker<ShardLease> leaseTaker = new LeaseTaker<ShardLease>(leaseManager, workerIdentifier,
                leaseDuration, true);

        List<ShardLease> leaseStealComplete = new ArrayList<ShardLease>();
        long timeBeforeUpdate = System.currentTimeMillis();
        leaseTaker.updateAllLeases(leaseStealComplete);
        Map<String, ShardLease> allLeases = leaseTaker.getAllLeases();
        assertEquals(allLeases.size(), 5);

        // lease with no owner
        assertEquals(allLeases.get("lease_1").getLastCounterIncrementMillis(), 0);

        // lease with owner
        assertTrue(allLeases.get("lease_2").getLastCounterIncrementMillis() >= timeBeforeUpdate);
        assertTrue(allLeases.get("lease_3").getLastCounterIncrementMillis() >= timeBeforeUpdate);
        assertTrue(allLeases.get("lease_4").getLastCounterIncrementMillis() >= timeBeforeUpdate);
        assertTrue(allLeases.get("lease_5").getLastCounterIncrementMillis() >= timeBeforeUpdate);

        // renew lease
        boolean succeed = leaseManager.renewLease(allLeases.get("lease_2").<ShardLease>copy());
        assertTrue(succeed);
        succeed = leaseManager.renewLease(allLeases.get("lease_4").<ShardLease>copy());
        assertTrue(succeed);

        // steal lease
        ShardLease leaseToTransfer = allLeases.get("lease_3").<ShardLease>copy();
        succeed = leaseManager.stealLease(leaseToTransfer, workerIdentifier);
        assertTrue(succeed);
        leaseToTransfer.setLeaseStealer(workerIdentifier);
        succeed = leaseManager.transferLease(leaseToTransfer);
        assertTrue(succeed);

        timeBeforeUpdate = System.currentTimeMillis();
        leaseTaker.updateAllLeases(leaseStealComplete);
        Map<String, ShardLease> allLeases2 = leaseTaker.getAllLeases();
        assertEquals(allLeases.size(), 5);

        // lease not update
        assertEquals(allLeases2.get("lease_1").getLastCounterIncrementMillis(), allLeases.get("lease_1").getLastCounterIncrementMillis());
        // lease update
        assertTrue(allLeases2.get("lease_2").getLastCounterIncrementMillis() >= timeBeforeUpdate);
        // lease transfer
        assertTrue(allLeases2.get("lease_3").getLastCounterIncrementMillis() >= timeBeforeUpdate);
        assertEquals(leaseStealComplete.size(), 1);
        assertEquals(leaseStealComplete.get(0).getLeaseOwner(), workerIdentifier);
        // lease update
        assertTrue(allLeases2.get("lease_4").getLastCounterIncrementMillis() >= timeBeforeUpdate);
        // lease not update
        assertEquals(allLeases2.get("lease_5").getLastCounterIncrementMillis(), allLeases.get("lease_5").getLastCounterIncrementMillis());
    }

    private void createLease(String owner, int start, int end, ILeaseManager<ShardLease> leaseManager) throws Exception {
        for (int i = start; i < end; i++) {
            ShardLease lease = new ShardLease("lease_" + i);
            lease.setLeaseCounter(1);
            lease.setLeaseOwner(owner);
            lease.setLeaseStealer("");

            leaseManager.createLease(lease);
        }
    }

    @Test
    public void testComputeLeaseToTake() throws Exception {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDuration = 3 * 1000;
        LeaseTaker<ShardLease> leaseTaker = new LeaseTaker<ShardLease>(leaseManager, workerIdentifier,
                leaseDuration, true);

        createLease("worker1", 0, 9, leaseManager);
        createLease("worker2", 9, 17, leaseManager);
        createLease("worker3", 17, 24, leaseManager);
        createLease("worker4", 24, 30, leaseManager);
        createLease("worker5", 30, 35, leaseManager);
        leaseTaker.updateAllLeases(new ArrayList<ShardLease>());

        {
            Map<String, Integer> leaseCounts = leaseTaker.computeLeaseCounts(new ArrayList<ShardLease>());
            assertEquals(leaseCounts.size(), 6);
            assertEquals(leaseCounts.get("worker1"), new Integer(9));
            assertEquals(leaseCounts.get("worker2"), new Integer(8));
            assertEquals(leaseCounts.get("worker3"), new Integer(7));
            assertEquals(leaseCounts.get("worker4"), new Integer(6));
            assertEquals(leaseCounts.get("worker5"), new Integer(5));
            assertEquals(leaseCounts.get("worker"), new Integer(0));
        }

        {
            List<ShardLease> expiredLeases = new ArrayList<ShardLease>();
            Map<String, ShardLease> allLeases = leaseTaker.getAllLeases();

            // lease from worker1
            expiredLeases.add(allLeases.get("lease_1"));
            expiredLeases.add(allLeases.get("lease_3"));
            expiredLeases.add(allLeases.get("lease_5"));
            expiredLeases.add(allLeases.get("lease_8"));

            // lease from worker2
            expiredLeases.add(allLeases.get("lease_9"));
            expiredLeases.add(allLeases.get("lease_11"));
            expiredLeases.add(allLeases.get("lease_15"));

            // lease form worker4
            expiredLeases.add(allLeases.get("lease_25"));
            expiredLeases.add(allLeases.get("lease_26"));
            expiredLeases.add(allLeases.get("lease_28"));

            Map<String, Integer> leaseCounts = leaseTaker.computeLeaseCounts(expiredLeases);
            assertEquals(leaseCounts.size(), 6);
            assertEquals(leaseCounts.get("worker1"), new Integer(5));
            assertEquals(leaseCounts.get("worker2"), new Integer(5));
            assertEquals(leaseCounts.get("worker3"), new Integer(7));
            assertEquals(leaseCounts.get("worker4"), new Integer(3));
            assertEquals(leaseCounts.get("worker5"), new Integer(5));
            assertEquals(leaseCounts.get("worker"), new Integer(0));
        }
    }

    @Test
    public void testChooseLeaseToSteal() throws Exception {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDuration = 3 * 1000;
        LeaseTaker<ShardLease> leaseTaker = new LeaseTaker<ShardLease>(leaseManager, workerIdentifier,
                leaseDuration, true);

        createLease("worker1", 0, 9, leaseManager);
        createLease("worker2", 9, 17, leaseManager);
        createLease("worker3", 17, 24, leaseManager);
        createLease("worker4", 24, 30, leaseManager);
        createLease("worker5", 30, 35, leaseManager);
        leaseTaker.updateAllLeases(new ArrayList<ShardLease>());

        // take from empty worker
        {
            Map<String, Integer> leaseCounts = new HashMap<String, Integer>();
            ShardLease lease = leaseTaker.chooseLeaseToSteal(leaseCounts, 2, 10);
            assertNull(lease);
        }

        // all worker's count is less than target
        {
            Map<String, Integer> leaseCounts = leaseTaker.computeLeaseCounts(new ArrayList<ShardLease>());

            ShardLease lease = leaseTaker.chooseLeaseToSteal(leaseCounts, 1, 9);
            assertNull(lease);

            lease = leaseTaker.chooseLeaseToSteal(leaseCounts, 2, 9);
            assertTrue(lease != null);

            lease = leaseTaker.chooseLeaseToSteal(leaseCounts, 2, 10);
            assertNull(lease);
        }

        // test take lease from the random worker who has the largest count
        {
            Map<String, Integer> leaseCounts = leaseTaker.computeLeaseCounts(new ArrayList<ShardLease>());

            ShardLease lease = leaseTaker.chooseLeaseToSteal(leaseCounts, 3, 8);
            assertEquals(lease.getLeaseOwner(), "worker1");

            lease = leaseTaker.chooseLeaseToSteal(leaseCounts, 3, 6);
            assertEquals(lease.getLeaseOwner(), "worker1");
        }
    }

    /**
     * 新增一批lease，新增一个worker，尝试take lease成功。
     * @throws Exception
     */
    @Test
    public void testTakeLeases() throws Exception {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDuration = 3 * 1000;
        LeaseTaker<ShardLease> leaseTaker = new LeaseTaker<ShardLease>(leaseManager, workerIdentifier,
                leaseDuration, true);

        createLease("worker1", 0, 9, leaseManager);
        createLease("worker2", 9, 17, leaseManager);
        createLease("worker3", 17, 24, leaseManager);
        createLease("worker4", 24, 30, leaseManager);
        createLease("worker5", 30, 35, leaseManager);
        createLease("", 35, 42, leaseManager);

        Map<String, ShardLease> takenLeases = leaseTaker.takeLeases();
        assertEquals(takenLeases.size(), 7);
        for (Map.Entry<String, ShardLease> entry : takenLeases.entrySet()) {
            System.out.println("Owner:" + entry.getValue().getLeaseOwner() + ".");
            assertEquals(entry.getValue().getLeaseOwner(), workerIdentifier);
            String leaseKey = entry.getKey();
            long id = Long.parseLong(leaseKey.substring(leaseKey.indexOf('_') + 1));
            assertTrue(id >= 35 && id < 42);
        }
    }

    /**
     * 部分原先被其他worker own的lease expired，尝试take成功。
     * take的数量未达到预期，尝试steal，transfer成功后steal成功。
     *
     * @throws Exception
     */
    @Test
    public void testTakeLeasesAndSteal() throws Exception {
        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDuration = 3 * 1000;
        LeaseTaker<ShardLease> leaseTaker = new LeaseTaker<ShardLease>(leaseManager, workerIdentifier,
                leaseDuration, true);

        createLease("worker1", 0, 9, leaseManager);
        createLease("worker2", 9, 17, leaseManager);
        createLease("worker3", 17, 24, leaseManager);
        createLease("worker4", 24, 30, leaseManager);
        createLease("worker5", 30, 35, leaseManager);

        leaseTaker.updateAllLeases(new ArrayList<ShardLease>());
        Map<String, ShardLease> allLeases = leaseTaker.getAllLeases();
        Thread.sleep(leaseDuration - 1000);

        renewLease(0, 9, allLeases, leaseManager);
        renewLease(9, 16, allLeases, leaseManager);
        renewLease(17, 23, allLeases, leaseManager);
        renewLease(24, 29, allLeases, leaseManager);
        renewLease(30, 35, allLeases, leaseManager);

        Thread.sleep(2000);
        Map<String, ShardLease> takenLeases = leaseTaker.takeLeases();

        // only take 3 lease and should steal 1 lease
        assertEquals(takenLeases.size(), 3);

        List<ShardLease> leaseList = leaseManager.listLeases();
        List<ShardLease> leaseSteal = new ArrayList<ShardLease>();
        for (ShardLease lease : leaseList) {
            if (lease.getLeaseStealer().equals(workerIdentifier)) {
                leaseSteal.add(lease);
            }
        }

        assertEquals(leaseSteal.size(), 1);

        // transfer lease and take succeed
        leaseManager.transferLease(leaseSteal.get(0));
        takenLeases = leaseTaker.takeLeases();
        assertEquals(takenLeases.size(), 1);
        String leaseKey = leaseSteal.get(0).getLeaseKey();
        assertTrue(takenLeases.containsKey(leaseKey));
    }

    private void renewLease(int start, int end, Map<String, ShardLease> allLeases, ILeaseManager<ShardLease> leaseManager) throws Exception {
        for (int i = start; i < end; i++) {
            ShardLease lease = allLeases.get("lease_" + i);
            boolean succeed = leaseManager.renewLease(lease);
            assertTrue(succeed);
        }
    }

    @Test
    public void testComputeLeasesToTake() {
        List<ShardLease> leaseSteal = new ArrayList<ShardLease>();
        List<ShardLease> leaseExpired = new ArrayList<ShardLease>();

        ILeaseManager<ShardLease> leaseManager = getLeaseManager();
        String workerIdentifier = "worker";
        long leaseDuration = 3 * 1000;
        LeaseTaker<ShardLease> leaseTaker = new LeaseTaker<ShardLease>(leaseManager, workerIdentifier,
                leaseDuration, true);

        Map<String, ShardLease> leases = leaseTaker.computeLeasesToTake(leaseSteal, leaseExpired, 10);
        assertEquals(leases.size(), 0);

        leaseSteal.add(new ShardLease("lease_1"));
        leaseSteal.add(new ShardLease("lease_2"));
        leaseSteal.add(new ShardLease("lease_3"));
        leaseSteal.add(new ShardLease("lease_4"));

        leaseExpired.add(new ShardLease("lease_3"));
        leaseExpired.add(new ShardLease("lease_4"));
        leaseExpired.add(new ShardLease("lease_5"));
        leaseExpired.add(new ShardLease("lease_6"));

        leases = leaseTaker.computeLeasesToTake(leaseSteal, leaseExpired, 3);
        assertEquals(leases.size(), 3);
        assertTrue(leases.containsKey("lease_1"));
        assertTrue(leases.containsKey("lease_2"));
        assertTrue(leases.containsKey("lease_3"));

        leases = leaseTaker.computeLeasesToTake(leaseSteal, leaseExpired, 4);
        assertEquals(leases.size(), 4);

        leases = leaseTaker.computeLeasesToTake(leaseSteal, leaseExpired, 5);
        assertEquals(leases.size(), 5);

        leases = leaseTaker.computeLeasesToTake(leaseSteal, leaseExpired, 6);
        assertEquals(leases.size(), 6);

        leases = leaseTaker.computeLeasesToTake(leaseSteal, leaseExpired, 7);
        assertEquals(leases.size(), 6);

        leases = leaseTaker.computeLeasesToTake(leaseSteal, leaseExpired, 8);
        assertEquals(leases.size(), 6);
    }
}
