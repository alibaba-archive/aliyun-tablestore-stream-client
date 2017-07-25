package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseRenewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;

/**
 * 管理当前已经分配到的Lease的更新。
 * 所有的lease加入LeaseRenewer或者被获取，都必须是经过copy，保证除了LeaseRenewer本身，其他人不会去修改其内部管理的Lease的状态。
 *
 * @param <T>
 */
public class LeaseRenewer<T extends Lease> implements ILeaseRenewer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseRenewer.class);

    private final ILeaseManager<T> leaseManager;
    private final String workerIdentifier;
    private final long leaseDurationMillis;
    private final ConcurrentNavigableMap<String, T> ownedLeases = new ConcurrentSkipListMap<String, T>();
    private final ExecutorService executorService;

    public LeaseRenewer(ILeaseManager<T> leaseManager,
                        String workerIdentifier,
                        long leaseDurationMillis,
                        ExecutorService executorService) {
        this.leaseManager = leaseManager;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationMillis = leaseDurationMillis;
        this.executorService = executorService;
    }

    @Override
    public void renewLeases() throws StreamClientException, DependencyException {
        LOG.debug("Start renew leases.");
        executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                findStealer();
                return null;
            }
        });

        for (final T lease : ownedLeases.descendingMap().values()) {
            executorService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return renewLease(lease, false);
                }
            });
        }
    }

    void findStealer() throws StreamClientException, DependencyException {
        LOG.debug("Start find stealer.");
        List<T> newList = leaseManager.listLeases(); // TODO it's a huge consumption of read capacity, need to optimize
        for (T lease : newList) {
            String leaseKey = lease.getLeaseKey();
            T ownedLease = ownedLeases.get(leaseKey);
            if (ownedLease != null) {
                synchronized (ownedLease) {
                    if (ownedLease.getLeaseStealer().isEmpty() && !lease.getLeaseStealer().isEmpty()
                            && lease.getLeaseCounter() == ownedLease.getLeaseCounter()) {
                        LOG.info("Stealer found. LeaseKey: {}, Stealer: {}.", lease.getLeaseKey(), lease.getLeaseStealer());
                        ownedLease.setLeaseStealer(lease.getLeaseStealer());
                    }
                }
            }
        }
    }

    private boolean renewLease(T lease, boolean renewEvenIfExpired) throws StreamClientException, DependencyException {
        LOG.debug("Start renew lease: {}", lease);
        long now = System.currentTimeMillis();
        boolean renewedLease = false;

        synchronized (lease) {
            /**
             * In startup, lease can be renewed even if it is expired.
             */
            if (renewEvenIfExpired || !lease.isExpired(leaseDurationMillis, now)) {
                renewedLease = leaseManager.renewLease(lease);
                if (!renewedLease) {
                    LOG.info("Lease renew failed: {}.", lease);
                }
            } else {
                LOG.info("Lease is expired: {}, now: {}.", lease, now);
            }
        }

        if (renewedLease) {
            LOG.debug("Renew lease, Lease: {}.", lease);
            lease.setLastCounterIncrementMillis(System.currentTimeMillis());
        } else {
            LOG.error("Lease is lost, Lease: {}.", lease);
            ownedLeases.remove(lease.getLeaseKey());
        }

        return renewedLease;
    }

    @Override
    public Map<String, T> getCurrentlyHeldLeases() {
        Map<String, T> result = new HashMap<String, T>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, T> entry : ownedLeases.entrySet()) {
            T lease = entry.getValue();
            if (lease != null && !lease.isExpired(leaseDurationMillis, now)) {
                T copy = lease.copy();
                result.put(copy.getLeaseKey(), copy);
            }
        }
        return result;
    }

    @Override
    public T getCurrentlyHeldLease(String leaseKey) {
        T lease = ownedLeases.get(leaseKey);
        long now = System.currentTimeMillis();
        if (lease == null || lease.isExpired(leaseDurationMillis, now)) {
            return null;
        } else {
            return lease.copy();
        }
    }

    @Override
    public void addLeasesToRenew(Collection<T> newLeases) {
        for (T lease : newLeases) {
            T copy = lease.copy();

            // We should reset lease identifier for each new added lease.
            // Because we can't identifier lease only by lease key,
            // the identifier should changed each time the same lease is re-acquired.
            copy.setLeaseIdentifier(UUID.randomUUID().toString());

            LOG.info("New lease added: {}", copy);
            ownedLeases.put(copy.getLeaseKey(), copy);
        }
    }

    @Override
    public void clearCurrentlyHeldLeases() {
        ownedLeases.clear();
    }

    @Override
    public boolean updateLease(T lease, String leaseIdentifier) throws StreamClientException, DependencyException {
        LOG.debug("Start update lease. Lease: {}, LeaseIdentifier: {}.", lease, leaseIdentifier);
        String leaseKey = lease.getLeaseKey();
        T ownedLease = ownedLeases.get(leaseKey);
        LOG.debug("Owned lease to update: {}.", ownedLease);
        if (ownedLease == null) {
            return false;
        }

        /**
         * If the lease identifier is different, it means that the lease is lost and regained during its last taken and next update.
         */
        if (!ownedLease.getLeaseIdentifier().equals(leaseIdentifier)) {
            LOG.error("Try update lease but identifier mismatch. OwnedLease: {}, LeaseToUpdate: {}.", ownedLease, lease);
            return false;
        }

        synchronized (ownedLease) {
            ownedLease.update(lease);
            boolean updatedLease = leaseManager.updateLease(ownedLease);
            if (updatedLease) {
                ownedLease.setLastCounterIncrementMillis(System.currentTimeMillis());
            } else {
                LOG.info("UpdateLease: lease is lost. Lease: {}", lease);

                /**
                 * Can't only remove by leaseKey, we should also check the value (exactly check the lease counter).
                 * Because it is possible that lease is lost and regained, plays like:
                 *  1. lease identifier check pass
                 *  2. lost lease; re-acquire lease;
                 *  3. update lease <--- now the counter is different with step 1.
                 *
                 *  So we can't remove it if the counter has changed.
                 */
                ownedLeases.remove(leaseKey, ownedLease);
            }
            return updatedLease;
        }
    }

    @Override
    public boolean transferLease(String leaseKey, String leaseIdentifier) throws StreamClientException, DependencyException {
        LOG.debug("Start transfer lease. LeaseKey: {}, LeaseIdentifier: {}.", leaseKey, leaseIdentifier);
        T ownedLease = ownedLeases.get(leaseKey);
        if (ownedLease == null) {
            return false;
        }

        /**
         * If the lease identifier is different, it means that the lease is lost and regained during its last taken and next update.
         */
        if (!ownedLease.getLeaseIdentifier().equals(leaseIdentifier)) {
            LOG.error("Try transfer lease but identifier mismatch. OwnedLease: {}, LeaseIdentifier: {}.", ownedLease, leaseIdentifier);
            return false;
        }

        synchronized (ownedLease) {
            /**
             * Can't only remove by leaseKey, we should also check the value (exactly check the lease counter).
             * Because it is possible that lease is lost and regained, plays like:
             *  1. lease identifier check pass
             *  2. lost lease; re-acquire lease;
             *  3. update lease <--- now the counter is different with step 1.
             *
             *  So we can't remove it if the counter has changed.
             */
            if (ownedLeases.remove(ownedLease.getLeaseKey(), ownedLease)) {
                LOG.info("Lease is removed from owned leases map and start to transfer. Lease: {}.", ownedLease);
                return leaseManager.transferLease(ownedLease);
            } else {
                return false;
            }
        }
    }

    @Override
    public void initialize() throws StreamClientException, DependencyException {
        Collection<T> leases = leaseManager.listLeases();
        List<T> myLeases = new ArrayList<T>();

        for (T lease : leases) {
            if (workerIdentifier.equals(lease.getLeaseOwner())) {
                LOG.info("Found a lease owned by this worker. Lease: {}, Worker: {}.", lease, workerIdentifier);
                if (renewLease(lease, true)) {
                    LOG.info("Successfully take lease: {}.", lease);
                    myLeases.add(lease);
                }
            }
        }

        addLeasesToRenew(myLeases);
    }

}
