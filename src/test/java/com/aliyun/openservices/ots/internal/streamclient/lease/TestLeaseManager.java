package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.ListTableResponse;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.IRetryStrategy;
import com.aliyun.openservices.ots.internal.utils.ServiceSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLeaseManager {

    private static final String OBJECT_NOT_EXIST = "OTSObjectNotExist";

    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static final SyncClientInterface ots = new SyncClient(
            serviceSettings.getOTSEndpoint(),
            serviceSettings.getOTSAccessKeyId(),
            serviceSettings.getOTSAccessKeySecret(),
            serviceSettings.getOTSInstanceName());

    private static final String tableName = "StatusTable";

    private static final String streamId = "streamId";

    private static final ShardLeaseSerializer serializer = new ShardLeaseSerializer(tableName, streamId);

    private static final IRetryStrategy leaseManagerRetryStrategy = new IRetryStrategy() {
        public boolean shouldRetry(RetryableAction actionName, Exception ex, int retries) {
            return false;
        }

        public long getBackoffTimeMillis(RetryableAction actionName, Exception ex, int retries) {
            return 0;
        }
    };

    @Before
    public void setUp() {
        clearData(null);
    }

    public void clearData(ILeaseManager<ShardLease> leaseManager) {
        if (leaseManager != null && leaseManager instanceof MemoryLeaseManager) {
            MemoryLeaseManager mlm = (MemoryLeaseManager)leaseManager;
            mlm.clear();
        } else {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(tableName);
            try {
                ots.deleteTable(deleteTableRequest);
            } catch (TableStoreException ex) {
                if (!ex.getErrorCode().equals(OBJECT_NOT_EXIST)) {
                    throw ex;
                }
            }
        }
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testLeaseManager() throws StreamClientException, DependencyException {
        ILeaseManager<ShardLease> leaseManager =
                new LeaseManager<ShardLease>(ots, tableName, serializer, leaseManagerRetryStrategy, 100);
        testCreateTable(leaseManager);
        testWaitTableReady(leaseManager);
        testCreateAndGet(leaseManager);
        testListLeases(leaseManager);
        testRenewLease(leaseManager);
        testTakeLease(leaseManager);
        testStealLease(leaseManager);
        testUpdateLease(leaseManager);
        testDeleteLease(leaseManager);
        testTransferLease(leaseManager);
    }

    @Test
    public void testMemoryLeaseManager() throws StreamClientException, DependencyException {
        ILeaseManager<ShardLease> leaseManager = new MemoryLeaseManager();
        testCreateAndGet(leaseManager);
        testListLeases(leaseManager);
        testRenewLease(leaseManager);
        testTakeLease(leaseManager);
        testStealLease(leaseManager);
        testUpdateLease(leaseManager);
        testDeleteLease(leaseManager);
        testTransferLease(leaseManager);
    }

    public void testCreateTable(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        clearData(leaseManager);
        boolean created = leaseManager.createLeaseTableIfNotExists(100, 100, -1);
        assertEquals(true, created);

        ListTableResponse listTableResult = ots.listTable();
        assertEquals(true, listTableResult.getTableNames().contains(tableName));

        created = leaseManager.createLeaseTableIfNotExists(100, 100, -1);
        assertEquals(false, created);
    }

    private void createTable(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        leaseManager.createLeaseTableIfNotExists(100, 100, -1);
        leaseManager.waitUntilTableReady(100 * 1000);
    }

    public void testWaitTableReady(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        clearData(leaseManager);
        boolean created = leaseManager.createLeaseTableIfNotExists(100, 100, -1);
        assertEquals(true, created);
        leaseManager.waitUntilTableReady(100 * 1000);

        ShardLease lease = new ShardLease("testLeaseKey");
        lease.setCheckpoint("");
        lease.setParentShardIds(new HashSet<String>());
        lease.setStreamId(streamId);
        PutRowRequest putRowRequest = serializer.getPutRowRequest(lease);
        ots.putRow(putRowRequest);
    }

    public void testCreateAndGet(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        clearData(leaseManager);
        createTable(leaseManager);

        ShardLease leaseToCreate = new ShardLease("LeaseKey");
        leaseToCreate.setLeaseOwner("LeaseOwner");
        leaseToCreate.setCheckpoint("checkpoint");
        leaseToCreate.setLeaseCounter(101);
        leaseToCreate.setStreamId(streamId);
        leaseToCreate.setLeaseStealer("leaseStealer");

        List<String> parentShardIds = new ArrayList<String>();
        parentShardIds.add("parentShard1");
        parentShardIds.add("parentShard2");
        leaseManager.createLease(leaseToCreate);

        ShardLease lease = leaseManager.getLease(leaseToCreate.getLeaseKey());
        assertTrue(leaseToCreate.equals(lease));
    }

    public void testListLeases(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        clearData(leaseManager);
        createTable(leaseManager);

        List<ShardLease> leases = new ArrayList<ShardLease>();
        for (int i = 0; i < 123; i++) {
            ShardLease lease = new ShardLease("lease" + String.format("%05d", i));
            lease.setLeaseOwner("owner" + i);
            lease.setCheckpoint("checkpoint" + i);
            lease.setLeaseStealer("stealer" + i);
            lease.setStreamId(streamId);
            lease.setParentShardIds(new HashSet(Arrays.asList("parent" + i, "parentSibling" + i)));
            leaseManager.createLease(lease);
            leases.add(lease);
        }

        List<ShardLease> list = leaseManager.listLeases();
        assertEquals(123, list.size());
        for (int i = 0; i < 123; i++) {
            assertEquals(leases.get(i), list.get(i));
        }
    }

    public void testRenewLease(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        createTable(leaseManager);

        ShardLease lease = new ShardLease("lease");
        lease.setLeaseOwner("owner");
        lease.setLeaseCounter(10);
        lease.setLeaseStealer("stealer");
        lease.setStreamId(streamId);
        lease.setCheckpoint("checkpoint");
        lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
        lease.setLastCounterIncrementMillis(1234);
        lease.setLeaseIdentifier("leaseIdentifier");

        leaseManager.createLease(lease);

        for (int i = 0; i < 10; i++) {
            long oldCounter = lease.getLeaseCounter();
            boolean succeed = leaseManager.renewLease(lease);
            assertTrue(succeed);

            assertEquals("lease", lease.getLeaseKey());
            assertEquals("owner", lease.getLeaseOwner());
            assertEquals(oldCounter + 1, lease.getLeaseCounter());
            assertEquals("stealer", lease.getLeaseStealer());
            assertEquals(streamId, lease.getStreamId());
            assertEquals("checkpoint", lease.getCheckpoint());
            assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
            assertEquals(1234, lease.getLastCounterIncrementMillis());
            assertEquals("leaseIdentifier", lease.getLeaseIdentifier());

            ShardLease leaseGot = leaseManager.getLease(lease.getLeaseKey());
            assertEquals("lease", leaseGot.getLeaseKey());
            assertEquals("owner", leaseGot.getLeaseOwner());
            assertEquals(oldCounter + 1, leaseGot.getLeaseCounter());
            assertEquals("stealer", leaseGot.getLeaseStealer());
            assertEquals(streamId, leaseGot.getStreamId());
            assertEquals("checkpoint", leaseGot.getCheckpoint());
            assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), leaseGot.getParentShardIds());
            assertEquals(0, leaseGot.getLastCounterIncrementMillis());
            assertEquals("", leaseGot.getLeaseIdentifier());
        }

        lease.setLeaseCounter(lease.getLeaseCounter() - 1);
        boolean succeed = leaseManager.renewLease(lease);
        assertTrue(!succeed);
    }

    public void testTakeLease(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        createTable(leaseManager);

        ShardLease lease = new ShardLease("lease");
        lease.setLeaseOwner("owner");
        lease.setLeaseCounter(10);
        lease.setLeaseStealer("stealer");
        lease.setStreamId(streamId);
        lease.setCheckpoint("checkpoint");
        lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
        lease.setLastCounterIncrementMillis(1234);
        lease.setLeaseIdentifier("leaseIdentifier");

        leaseManager.createLease(lease);
        for (int i = 0; i < 10; i++) {
            long oldCounter = lease.getLeaseCounter();
            boolean succeed = leaseManager.takeLease(lease, "newOwner");
            assertTrue(succeed);

            assertEquals("lease", lease.getLeaseKey());
            assertEquals("newOwner", lease.getLeaseOwner());
            assertEquals(oldCounter + 1, lease.getLeaseCounter());
            assertEquals("", lease.getLeaseStealer());
            assertEquals(streamId, lease.getStreamId());
            assertEquals("checkpoint", lease.getCheckpoint());
            assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
            assertEquals(1234, lease.getLastCounterIncrementMillis());
            assertEquals("leaseIdentifier", lease.getLeaseIdentifier());

            ShardLease leaseGot = leaseManager.getLease(lease.getLeaseKey());

            assertEquals("lease", leaseGot.getLeaseKey());
            assertEquals("newOwner", leaseGot.getLeaseOwner());
            assertEquals(oldCounter + 1, leaseGot.getLeaseCounter());
            assertEquals("", leaseGot.getLeaseStealer());
            assertEquals(streamId, leaseGot.getStreamId());
            assertEquals("checkpoint", leaseGot.getCheckpoint());
            assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), leaseGot.getParentShardIds());
            assertEquals(0, leaseGot.getLastCounterIncrementMillis());
            assertEquals("", leaseGot.getLeaseIdentifier());
        }

        lease.setLeaseCounter(lease.getLeaseCounter() - 1);
        boolean succeed = leaseManager.takeLease(lease, "newOwner");
        assertTrue(!succeed);
    }

    public void testStealLease(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        createTable(leaseManager);

        {
            ShardLease lease = new ShardLease("lease");
            lease.setLeaseOwner("owner");
            lease.setLeaseCounter(0);
            lease.setLeaseStealer("");
            lease.setStreamId(streamId);
            lease.setCheckpoint("checkpoint");
            lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
            lease.setLastCounterIncrementMillis(1234);
            lease.setLeaseIdentifier("leaseIdentifier");

            leaseManager.createLease(lease);
            boolean succeed = leaseManager.stealLease(lease, "stealer");
            assertTrue(succeed);

            assertEquals("lease", lease.getLeaseKey());
            assertEquals("owner", lease.getLeaseOwner());
            assertEquals(0, lease.getLeaseCounter());
            assertEquals("stealer", lease.getLeaseStealer());
            assertEquals(streamId, lease.getStreamId());
            assertEquals("checkpoint", lease.getCheckpoint());
            assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
            assertEquals(1234, lease.getLastCounterIncrementMillis());
            assertEquals("leaseIdentifier", lease.getLeaseIdentifier());

            lease = leaseManager.getLease(lease.getLeaseKey());

            assertEquals("lease", lease.getLeaseKey());
            assertEquals("owner", lease.getLeaseOwner());
            assertEquals(0, lease.getLeaseCounter());
            assertEquals("stealer", lease.getLeaseStealer());
            assertEquals(streamId, lease.getStreamId());
            assertEquals("checkpoint", lease.getCheckpoint());
            assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
            assertEquals(0, lease.getLastCounterIncrementMillis());
            assertEquals("", lease.getLeaseIdentifier());

            lease.setLeaseCounter(lease.getLeaseCounter() - 1);
            succeed = leaseManager.stealLease(lease, "stealer");
            assertTrue(!succeed);
        }

        // steal lease which has stealer
        {
            ShardLease lease = new ShardLease("lease1");
            lease.setLeaseOwner("owner");
            lease.setLeaseCounter(0);
            lease.setLeaseStealer("");
            lease.setStreamId(streamId);
            lease.setCheckpoint("checkpoint");
            lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
            lease.setLastCounterIncrementMillis(1234);
            lease.setLeaseIdentifier("leaseIdentifier");
            lease.setLeaseStealer("stealer");

            leaseManager.createLease(lease);
            boolean succeed = leaseManager.stealLease(lease, "stealer2");
            assertTrue(!succeed);

            succeed = leaseManager.takeLease(lease, lease.getLeaseOwner());
            assertTrue(succeed);

            succeed = leaseManager.stealLease(lease, "stealer2");
            assertTrue(succeed);
        }

        // steal lease which owner changed
        {
            ShardLease lease = new ShardLease("lease2");
            lease.setLeaseOwner("owner");
            lease.setLeaseCounter(0);
            lease.setLeaseStealer("");
            lease.setStreamId(streamId);
            lease.setCheckpoint("checkpoint");
            lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
            lease.setLastCounterIncrementMillis(1234);
            lease.setLeaseIdentifier("leaseIdentifier");

            leaseManager.createLease(lease);
            lease.setLeaseOwner("owner1");
            boolean succeed = leaseManager.stealLease(lease, "stealer2");
            assertTrue(!succeed);

            lease.setLeaseOwner("owner");
            succeed = leaseManager.stealLease(lease, "stealer2");
            assertTrue(succeed);
        }
    }

    public void testTransferLease(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        createTable(leaseManager);

        ShardLease lease = new ShardLease("lease");
        lease.setLeaseOwner("owner");
        lease.setLeaseCounter(10);
        lease.setLeaseStealer("stealer");
        lease.setStreamId(streamId);
        lease.setCheckpoint("checkpoint");
        lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
        lease.setLastCounterIncrementMillis(1234);
        lease.setLeaseIdentifier("leaseIdentifier");

        leaseManager.createLease(lease);
        boolean succeed = leaseManager.transferLease(lease);
        assertTrue(succeed);

        assertEquals("lease", lease.getLeaseKey());
        assertEquals("stealer", lease.getLeaseOwner());
        assertEquals(11, lease.getLeaseCounter());
        assertEquals("stealer", lease.getLeaseStealer());
        assertEquals(streamId, lease.getStreamId());
        assertEquals("checkpoint", lease.getCheckpoint());
        assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
        assertEquals(1234, lease.getLastCounterIncrementMillis());
        assertEquals("leaseIdentifier", lease.getLeaseIdentifier());

        lease = leaseManager.getLease(lease.getLeaseKey());

        assertEquals("lease", lease.getLeaseKey());
        assertEquals("stealer", lease.getLeaseOwner());
        assertEquals(11, lease.getLeaseCounter());
        assertEquals("stealer", lease.getLeaseStealer());
        assertEquals(streamId, lease.getStreamId());
        assertEquals("checkpoint", lease.getCheckpoint());
        assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
        assertEquals(0, lease.getLastCounterIncrementMillis());
        assertEquals("", lease.getLeaseIdentifier());

        lease.setLeaseCounter(lease.getLeaseCounter() - 1);
        succeed = leaseManager.transferLease(lease);
        assertTrue(!succeed);
    }

    public void testDeleteLease(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        createTable(leaseManager);

        ShardLease lease = new ShardLease("lease");
        lease.setLeaseOwner("owner");
        lease.setLeaseCounter(0);
        lease.setLeaseStealer("stealer");
        lease.setStreamId(streamId);
        lease.setCheckpoint("checkpoint");
        lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
        lease.setLastCounterIncrementMillis(1234);
        lease.setLeaseIdentifier("leaseIdentifier");

        leaseManager.createLease(lease);

        lease = leaseManager.getLease(lease.getLeaseKey());

        assertEquals("lease", lease.getLeaseKey());
        assertEquals("owner", lease.getLeaseOwner());
        assertEquals(0, lease.getLeaseCounter());
        assertEquals("stealer", lease.getLeaseStealer());
        assertEquals(streamId, lease.getStreamId());
        assertEquals("checkpoint", lease.getCheckpoint());
        assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
        assertEquals(0, lease.getLastCounterIncrementMillis());
        assertEquals("", lease.getLeaseIdentifier());

        leaseManager.deleteLease(lease.getLeaseKey());

        lease = leaseManager.getLease(lease.getLeaseKey());

        assertEquals(null, lease);
    }

    public void testUpdateLease(ILeaseManager<ShardLease> leaseManager) throws StreamClientException, DependencyException {
        createTable(leaseManager);

        ShardLease lease = new ShardLease("lease");
        lease.setLeaseOwner("owner");
        lease.setLeaseCounter(0);
        lease.setLeaseStealer("stealer");
        lease.setStreamId(streamId);
        lease.setCheckpoint("checkpoint");
        lease.setParentShardIds(new HashSet(Arrays.asList("parent1", "parent2")));
        lease.setLastCounterIncrementMillis(1234);
        lease.setLeaseIdentifier("leaseIdentifier");

        leaseManager.createLease(lease);

        lease = leaseManager.getLease(lease.getLeaseKey());

        assertEquals("lease", lease.getLeaseKey());
        assertEquals("owner", lease.getLeaseOwner());
        assertEquals(0, lease.getLeaseCounter());
        assertEquals("stealer", lease.getLeaseStealer());
        assertEquals(streamId, lease.getStreamId());
        assertEquals("checkpoint", lease.getCheckpoint());
        assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
        assertEquals(0, lease.getLastCounterIncrementMillis());
        assertEquals("", lease.getLeaseIdentifier());

        lease.setCheckpoint("newCheckpoint");
        boolean succeed = leaseManager.updateLease(lease);
        assertTrue(succeed);

        lease = leaseManager.getLease(lease.getLeaseKey());

        assertEquals("lease", lease.getLeaseKey());
        assertEquals("owner", lease.getLeaseOwner());
        assertEquals(1, lease.getLeaseCounter());
        assertEquals("stealer", lease.getLeaseStealer());
        assertEquals(streamId, lease.getStreamId());
        assertEquals("newCheckpoint", lease.getCheckpoint());
        assertEquals(new HashSet<String>(Arrays.asList("parent1", "parent2")), lease.getParentShardIds());
        assertEquals(0, lease.getLastCounterIncrementMillis());
        assertEquals("", lease.getLeaseIdentifier());

        lease.setLeaseCounter(lease.getLeaseCounter() - 1);
        succeed = leaseManager.updateLease(lease);
        assertTrue(!succeed);
    }

    private static final String SHARD_KEY_PREFIX = "shard_";
    private static final String OWNER_PREFIX = "owner_";

    class ShardStat {
        AtomicLong renewCount = new AtomicLong(0);
        AtomicLong updateCount = new AtomicLong(0);
    }

    class LeaseTask implements Runnable {

        ILeaseManager<ShardLease> leaseManager;
        int leaseNumber;
        volatile boolean stop;
        String owner;
        Random random;

        public Map<String, ShardLease> ownedLeases = new HashMap<String, ShardLease>();
        public Set<String> notOwnedLease = new HashSet<String>();

        public Map<String, ShardStat> allLeases;
        public boolean hasException = false;

        public LeaseTask(ILeaseManager<ShardLease> leaseManager, Map<String, ShardStat> allLeases, int id) {
            this.leaseManager = leaseManager;
            this.leaseNumber = allLeases.size();
            stop = false;
            owner = OWNER_PREFIX + id;
            random = new Random(Thread.currentThread().getId());

            for (String lease : allLeases.keySet()) {
                notOwnedLease.add(lease);
            }

            this.allLeases = allLeases;
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    int opType = random.nextInt() % 3;
                    switch (opType) {
                        case 0:
                            renewLease();
                            break;
                        case 1:
                            updateLease();
                            break;
                        case 2:
                            takeLease();
                            break;
                    }
                    Thread.sleep(10);
                } catch (Exception e) {
                    e.printStackTrace();
                    hasException = true;
                }

            }
        }

        private void renewLease() throws Exception {
            if (ownedLeases.isEmpty()) {
                return;
            }

            int id = random.nextInt(ownedLeases.size());
            ShardLease lease = ownedLeases.values().toArray(new ShardLease[0])[id];

            boolean succeed = leaseManager.updateLease(lease);
            if (!succeed) {
                ownedLeases.remove(lease.getLeaseKey());
                notOwnedLease.add(lease.getLeaseKey());
            } else {
                allLeases.get(lease.getLeaseKey()).renewCount.incrementAndGet();
                System.out.println("Renew succeed: " + owner + ", " + lease.getLeaseKey() + ", " + lease.getLeaseCounter());
            }
        }

        private void updateLease() throws Exception {
            if (ownedLeases.isEmpty()) {
                return;
            }

            int id = random.nextInt(ownedLeases.size());
            ShardLease lease = ownedLeases.values().toArray(new ShardLease[0])[id];

            long checkPoint = Long.parseLong(lease.getCheckpoint());

            lease.setCheckpoint(Long.toString(checkPoint + 1));
            boolean succeed = leaseManager.updateLease(lease);
            if (!succeed) {
                ownedLeases.remove(lease.getLeaseKey());

                lease.setCheckpoint(Long.toString(checkPoint));
                notOwnedLease.add(lease.getLeaseKey());
            } else {
                allLeases.get(lease.getLeaseKey()).updateCount.incrementAndGet();
                allLeases.get(lease.getLeaseKey()).renewCount.incrementAndGet();
                System.out.println("Update succeed: " + owner + ", " + lease.getLeaseKey() + ", " + lease.getLeaseCounter());
            }
        }

        private void takeLease() throws Exception {
            if (notOwnedLease.isEmpty()) {
                return;
            }

            int id = random.nextInt(notOwnedLease.size());
            String leaseKey = notOwnedLease.toArray(new String[0])[id];

            ShardLease lease = getLease(leaseKey);
            boolean succeed = leaseManager.takeLease(lease, owner);
            if (succeed) {
                allLeases.get(lease.getLeaseKey()).renewCount.incrementAndGet();

                notOwnedLease.remove(lease.getLeaseKey());
                ownedLeases.put(lease.getLeaseKey(), lease);

                System.out.println("Take succeed: " + owner + ", " + lease.getLeaseKey() + ", " + lease.getLeaseCounter());
            }
        }

        private ShardLease getLease(String leaseKey) throws Exception {
            return leaseManager.getLease(leaseKey);
        }

        public void stop() {
            stop = true;
        }
    }

    /**
     * 多线程并发的执行lease的各项操作：RenewLease, UpdateLease, TakeLease。
     * 检查每个线程最终的拥有的Lease的状态，以及在状态表中的最终状态，期望达到一致。
     */
    @Test
    public void testConcurrentOperations() throws Exception {
        ILeaseManager<ShardLease> leaseManager =
                new LeaseManager<ShardLease>(ots, tableName, serializer, leaseManagerRetryStrategy, 100);
        createTable(leaseManager);

        int leaseNumber = 100;

        // create leases
        Map<String, ShardStat> allLeases = new HashMap<String, ShardStat>();
        for (int i = 0; i < leaseNumber; i++) {
            ShardLease lease = new ShardLease(SHARD_KEY_PREFIX + i);
            lease.setStreamId(streamId);
            lease.setCheckpoint("0");

            leaseManager.createLease(lease);
            allLeases.put(lease.getLeaseKey(), new ShardStat());
        }

        int leaseTaskNumber = 30;
        List<LeaseTask> tasks = new ArrayList<LeaseTask>();
        for (int i = 0; i < leaseTaskNumber; i++) {
            tasks.add(new LeaseTask(leaseManager, allLeases, i));
        }

        List<Thread> threads = new ArrayList<Thread>();
        for (LeaseTask task : tasks) {
            Thread thread = new Thread(task);
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        Thread.sleep(20000);
        for (LeaseTask task : tasks) {
            task.stop();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Map<String, ShardLease> ownedLease = new HashMap<String, ShardLease>();
        Map<String, ShardLease> allLeasesMap = new HashMap<String, ShardLease>();
        int ownedLeaseCount = 0;

        for (ShardLease lease : leaseManager.listLeases()) {
            if (!lease.getLeaseOwner().equals("")) {
                ownedLeaseCount++;
            }
            allLeasesMap.put(lease.getLeaseKey(), lease);
        }
        assertEquals(allLeasesMap.size(), leaseNumber);

        for (LeaseTask task : tasks) {
            assertTrue(!task.hasException);

            for (ShardLease lease : task.ownedLeases.values()) {
                boolean succeed = leaseManager.renewLease(lease.<ShardLease>copy());
                if (succeed) {
                    assertTrue(!ownedLease.containsKey(lease.getLeaseKey()));
                    ownedLease.put(lease.getLeaseKey(), lease);
                }
            }
        }

        for (ShardLease lease : ownedLease.values()) {
            assertTrue(allLeasesMap.containsKey(lease.getLeaseKey()));

            ShardLease target = allLeasesMap.get(lease.getLeaseKey());

            assertEquals(lease.getStreamId(), streamId);
            assertEquals(lease.getLeaseOwner(), target.getLeaseOwner());
            assertEquals(lease.getLeaseCounter(), target.getLeaseCounter());

            assertEquals(lease.getLeaseCounter(), allLeases.get(lease.getLeaseKey()).renewCount.get());
            assertEquals(Long.parseLong(lease.getCheckpoint()), allLeases.get(lease.getLeaseKey()).updateCount.get());
        }

        assertEquals(ownedLease.size(), ownedLeaseCount);
    }
}
