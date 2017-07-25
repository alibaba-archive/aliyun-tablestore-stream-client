package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.aliyun.openservices.ots.internal.streamclient.ClientConfig;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.IRetryStrategy;
import com.aliyun.openservices.ots.internal.streamclient.model.ShardInfo;
import com.aliyun.openservices.ots.internal.utils.ServiceSettings;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLeaseCoordinator {

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

    private static final String SHARD_KEY_PREFIX = "shard_";

    @Test
    public void testSingClientTakeAllLease() throws Exception {
        ILeaseManager<ShardLease> leaseManager = new MemoryLeaseManager();

        int leaseNumber = 100;

        // create leases
        for (int i = 0; i < leaseNumber; i++) {
            ShardLease lease = new ShardLease(SHARD_KEY_PREFIX + i);
            lease.setStreamId(streamId);
            lease.setCheckpoint("0");

            leaseManager.createLease(lease);
        }

        ClientConfig config = new ClientConfig();
        config.setTakerIntervalMillis(100);
        config.setRenewerIntervalMillis(1000);
        config.setLeaseDurationMillis(2000);
        LeaseCoordinator<ShardLease> leaseCoordinator = new LeaseCoordinator<ShardLease>(leaseManager, "worker", config);

        leaseCoordinator.initialize();
        leaseCoordinator.start();

        Thread.sleep(5000);

        Collection<ShardLease> heldLeases = leaseCoordinator.getCurrentlyHeldLeases();
        assertEquals(heldLeases.size(), 100);

        leaseCoordinator.stop();
    }

    @Test
    public void testSingleClientAddNewLease() throws Exception {
        ILeaseManager<ShardLease> leaseManager = new MemoryLeaseManager();

        int leaseNumber = 100;

        // create leases
        for (int i = 0; i < leaseNumber; i++) {
            ShardLease lease = new ShardLease(SHARD_KEY_PREFIX + i);
            lease.setStreamId(streamId);
            lease.setCheckpoint("0");

            leaseManager.createLease(lease);
        }

        ClientConfig config = new ClientConfig();
        config.setTakerIntervalMillis(100);
        config.setRenewerIntervalMillis(1000);
        config.setLeaseDurationMillis(2000);
        LeaseCoordinator<ShardLease> leaseCoordinator = new LeaseCoordinator<ShardLease>(leaseManager, "worker", config);

        leaseCoordinator.initialize();
        leaseCoordinator.start();

        Thread.sleep(5000);

        Collection<ShardLease> heldLeases = leaseCoordinator.getCurrentlyHeldLeases();
        assertEquals(heldLeases.size(), leaseNumber);

        // add new leases
        for (int i = leaseNumber; i < leaseNumber + 30; i++) {
            ShardLease lease = new ShardLease(SHARD_KEY_PREFIX + i);
            lease.setStreamId(streamId);
            lease.setCheckpoint("0");

            leaseManager.createLease(lease);
        }

        Thread.sleep(5000);
        heldLeases = leaseCoordinator.getCurrentlyHeldLeases();
        assertEquals(heldLeases.size(), leaseNumber + 30);

        leaseCoordinator.stop();
    }

    class TransferTask implements Runnable {
        ILeaseManager<ShardLease> leaseManager;
        LeaseCoordinator<ShardLease> leaseCoordinator;
        volatile boolean stop = false;

        TransferTask(ILeaseManager<ShardLease> leaseManager, LeaseCoordinator<ShardLease> leaseCoordinator) {
            this.leaseManager = leaseManager;
            this.leaseCoordinator = leaseCoordinator;
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    List<ShardInfo> heldShards = new ArrayList<ShardInfo>();
                    List<ShardInfo> stolenShards = new ArrayList<ShardInfo>();
                    getCurrentlyHeldShards(heldShards, stolenShards);

                    for (ShardInfo shard : stolenShards) {
                        leaseCoordinator.transferLease(shard.getShardId(), shard.getLeaseIdentifier());
                    }

                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void getCurrentlyHeldShards(List<ShardInfo> heldShards, List<ShardInfo> stolenShards) {
            heldShards.clear();
            stolenShards.clear();
            Collection<ShardLease> heldLeases = leaseCoordinator.getCurrentlyHeldLeases();
            for (ShardLease shardLease : heldLeases) {
                if (shardLease.getLeaseStealer().isEmpty()) {
                    heldShards.add(shardLease.toShardInfo());
                } else {
                    stolenShards.add(shardLease.toShardInfo());
                }
            }
        }

        public void stop() {
            stop = true;
        }
    }

    @Test
    public void testMultiClient() throws Exception {
        ILeaseManager<ShardLease> leaseManager = new MemoryLeaseManager();

        int leaseNumber = 100;

        // create leases
        for (int i = 0; i < leaseNumber; i++) {
            ShardLease lease = new ShardLease(SHARD_KEY_PREFIX + i);
            lease.setStreamId(streamId);
            lease.setCheckpoint("0");

            leaseManager.createLease(lease);
        }

        ClientConfig config = new ClientConfig();
        config.setTakerIntervalMillis(500);
        config.setRenewerIntervalMillis(1000);
        config.setLeaseDurationMillis(2000);
        config.setRenewThreadPoolSize(1);

        int workerNum = 10;
        List<LeaseCoordinator<ShardLease>> leaseCoordinators = new ArrayList<LeaseCoordinator<ShardLease>>();
        for (int i = 0; i < workerNum; i++) {
            LeaseCoordinator<ShardLease> leaseCoordinator = new LeaseCoordinator<ShardLease>(leaseManager, "worker_" + i, config);
            leaseCoordinator.initialize();
            leaseCoordinators.add(leaseCoordinator);
        }

        List<TransferTask> tts = new ArrayList<TransferTask>();
        List<Thread> ttt = new ArrayList<Thread>();

        for (LeaseCoordinator<ShardLease> leaseCoordinator : leaseCoordinators) {
            leaseCoordinator.start();
            TransferTask tt = new TransferTask(leaseManager, leaseCoordinator);
            Thread t = new Thread(tt);
            t.start();

            tts.add(tt);
            ttt.add(t);
            Thread.sleep(5000);
        }

        Thread.sleep(20000);

        checkLeaseBalance(leaseCoordinators, leaseNumber);

        // create leases
        int addNumber = 55;
        for (int i = leaseNumber; i < leaseNumber + addNumber; i++) {
            ShardLease lease = new ShardLease(SHARD_KEY_PREFIX + i);
            lease.setStreamId(streamId);
            lease.setCheckpoint("0");

            leaseManager.createLease(lease);
        }

        Thread.sleep(20000);

        checkLeaseBalance(leaseCoordinators, leaseNumber + addNumber);

        for (TransferTask tt : tts) {
            tt.stop();
        }

        for (Thread t : ttt) {
            t.join();
        }
    }

    private void checkLeaseBalance(List<LeaseCoordinator<ShardLease>> leaseCoordinators, int leaseNumber) {
        int workerNum = leaseCoordinators.size();
        Set<String> leasesSet = new HashSet<String>();
        for (LeaseCoordinator<ShardLease> leaseCoordinator : leaseCoordinators) {
            Collection<ShardLease> leases = leaseCoordinator.getCurrentlyHeldLeases();
            System.out.println(leases.iterator().next().getLeaseOwner() + ":" + leases.size());
        }

        for (LeaseCoordinator<ShardLease> leaseCoordinator : leaseCoordinators) {
            Collection<ShardLease> leases = leaseCoordinator.getCurrentlyHeldLeases();
            for (ShardLease lease : leases) {
                assertTrue(!leasesSet.contains(lease.getLeaseKey()));
                leasesSet.add(lease.getLeaseKey());
            }

            if (leaseNumber % workerNum == 0) {
                assertEquals(leases.size(), leaseNumber / workerNum);
            } else {
                assertTrue(leases.size() >= leaseNumber / workerNum);
                assertTrue(leases.size() <= leaseNumber / workerNum + 1);
            }
        }

        assertEquals(leaseNumber, leasesSet.size());
    }
}
