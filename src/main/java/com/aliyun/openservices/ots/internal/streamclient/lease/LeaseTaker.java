package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseTaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * ILeaseTaker的实现类。
 * @param <T>
 */
public class LeaseTaker<T extends Lease> implements ILeaseTaker<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseTaker.class);

    private final ILeaseManager<T> leaseManager;
    private final String workerIdentifier;
    private final long leaseDurationMillis;
    private boolean autoSteal;
    private long lastUpdateLeasesMillis = 0;
    private final Map<String, T> allLeases = new HashMap<String, T>();

    public LeaseTaker(ILeaseManager<T> leaseManager,
                      String workerIdentifier,
                      long leaseDurationMillis,
                      boolean autoSteal) {
        this.leaseManager = leaseManager;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationMillis = leaseDurationMillis;
        this.autoSteal = autoSteal;
    }

    @Override
    public synchronized Map<String, T> takeLeases() throws StreamClientException, DependencyException {
        LOG.debug("Start take leases.");
        Map<String, T> takenLeases = new HashMap<String, T>();

        List<T> leasesStealComplete = new ArrayList<T>();
        updateAllLeases(leasesStealComplete);

        List<T> expiredLeases = getExpiredLeases();
        Map<String, Integer> leaseCounts = computeLeaseCounts(expiredLeases);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Lease after updated.");
            for (T lease : allLeases.values()) {
                LOG.debug("Lease: {}", lease);
            }

            for (T lease : leasesStealComplete) {
                LOG.debug("Steal lease has been transferred to this worker. Lease: {}.", lease.getLeaseKey());
            }

            for (T lease : expiredLeases) {
                LOG.debug("LeaseExpired, LeaseKey: {}.", lease.getLeaseKey());
            }

            for (Map.Entry<String, Integer> leaseCount : leaseCounts.entrySet()) {
                LOG.debug("Owner: {}, Count: {}", leaseCount.getKey(), leaseCount.getValue());
            }
        }

        int numLeases = allLeases.size();
        int numWorkers = leaseCounts.size();
        int target = numLeases / numWorkers + (numLeases % numWorkers == 0 ? 0 : 1);
        int need = target - leaseCounts.get(workerIdentifier);
        LOG.debug("Try to take lease. NumLeases: {}, NumWorkers: {}, Target: {}, Need: {}.",
                numLeases, numWorkers, target, need);

        Map<String, T> leasesToTake = computeLeasesToTake(leasesStealComplete, expiredLeases, need);

        for (T lease : leasesToTake.values()) {
            if (leaseManager.takeLease(lease, workerIdentifier)) {
                lease.setLastCounterIncrementMillis(System.currentTimeMillis());

                LOG.info("Successfully take lease. Lease: {}.", lease);
                takenLeases.put(lease.getLeaseKey(), lease);
            } else {
                LOG.info("Failed to take lease. Lease: {}.", lease);
            }
        }

        LOG.debug("Worker: {}, AutoSteal: {}, TakenLeases: {}, Need: {}", workerIdentifier, autoSteal, takenLeases.size(), need);
        if (autoSteal) {
            if (need > takenLeases.size()) {
                T lease = chooseLeaseToSteal(leaseCounts, need, target); // TODO optimize to take more than one lease each time
                if (lease != null) {
                    LOG.info("Steal lease, Lease: {}.", lease);
                    leaseManager.stealLease(lease, workerIdentifier);
                }
            }
        }
        return takenLeases;
    }

    /**
     * 更新当前Lease的更新时间。
     * 若是新的Lease，且没有owner，则将更新时间设置为0。
     * 若是新的Lease，且有owner，则将更新时间设置为当前时间。
     * 若是旧的Lease，且counter被更新，则将更新时间设置为当前时间。
     * 若是旧的Lease，且counter未更新，则不必更新其更新时间。
     * 若Lease已经不存在，则从当前列表中删除。
     *
     * 若发现当前lease owner与lease stealer均为本Worker，则认为该lease已经被transfer，当前worker steal成功。
     *
     * package level for test purpose
     * @param leasesStealComplete
     * @throws StreamClientException
     * @throws DependencyException
     */
    void updateAllLeases(List<T> leasesStealComplete) throws StreamClientException, DependencyException {
        List<T> newList = leaseManager.listLeases();
        lastUpdateLeasesMillis = System.currentTimeMillis();

        LOG.debug("Update all leases: {}.", lastUpdateLeasesMillis);
        Set<String> notUpdated = new HashSet<String>(allLeases.keySet());
        for (T lease : newList) {
            String leaseKey = lease.getLeaseKey();
            T oldLease = allLeases.get(leaseKey);
            notUpdated.remove(leaseKey);
            if (oldLease != null) {
                if (oldLease.getLeaseCounter() == lease.getLeaseCounter()) {
                    lease.setLastCounterIncrementMillis(oldLease.getLastCounterIncrementMillis());
                } else {
                    lease.setLastCounterIncrementMillis(lastUpdateLeasesMillis);
                }
            } else {
                if (lease.getLeaseOwner().isEmpty()) {
                    lease.setLastCounterIncrementMillis(0);
                } else {
                    lease.setLastCounterIncrementMillis(lastUpdateLeasesMillis);
                }
            }
            if (lease.getLeaseOwner().equals(workerIdentifier) && lease.getLeaseStealer().equals(workerIdentifier)) {
                leasesStealComplete.add(lease);
            }
            allLeases.put(leaseKey, lease);
        }

        for (String key : notUpdated) {
            allLeases.remove(key);
        }
    }

    public Map<String, T> getAllLeases() {
        Map<String, T> copy = new HashMap<String, T>();
        for (Map.Entry<String, T> entry : allLeases.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().<T>copy());
        }
        return copy;
    }

    private List<T> getExpiredLeases() {
        List<T> expiredLeases = new ArrayList<T>();
        for (T lease : allLeases.values()) {
            // new lease is treated as expired because its counter update time is initialized to 0.
            if (lease.isExpired(leaseDurationMillis, lastUpdateLeasesMillis)) {
                expiredLeases.add(lease);
            }
        }
        return expiredLeases;
    }

    /**
     * package level for test purpose
     *
     * @param leasesStealComplete
     * @param expiredLeases
     * @param need
     * @return
     */
    Map<String, T> computeLeasesToTake(List<T> leasesStealComplete, List<T> expiredLeases, int need) {
        Map<String, T> leasesToTake = new HashMap<String, T>();
        for (T lease : leasesStealComplete) {
            leasesToTake.put(lease.getLeaseKey(), lease);
            if (--need <= 0) {
                return leasesToTake;
            }
        }

        List<T> leaseToPick = new LinkedList<T>(expiredLeases);
        Collections.shuffle(leaseToPick);
        while (need > 0 && leaseToPick.size() > 0) {
           T lease = leaseToPick.remove(0);
            if (!leasesToTake.containsKey(lease.getLeaseKey())) {
                leasesToTake.put(lease.getLeaseKey(), lease);
                need--;
            }
        }
        return leasesToTake;
    }

    /**
     * 计算当前每个worker上持有的lease的个数，不包含过期的Lease以及正在被Steal的Lease。
     *
     * @param expiredLeases
     * @return
     */
    Map<String, Integer> computeLeaseCounts(List<T> expiredLeases) {
        Map<String, Integer> leaseCounts = new HashMap<String, Integer>();
        for (T lease : allLeases.values()) {
            if (!expiredLeases.contains(lease) && lease.getLeaseStealer().isEmpty()) {
                String leaseOwner = lease.getLeaseOwner();
                Integer oldCount = leaseCounts.get(leaseOwner);
                if (oldCount == null) {
                    leaseCounts.put(leaseOwner, 1);
                } else {
                    leaseCounts.put(leaseOwner, oldCount + 1);
                }
            }
        }

        if (leaseCounts.get(workerIdentifier) == null) {
            leaseCounts.put(workerIdentifier, 0);
        }
        return leaseCounts;
    }

    /**
     * Steal one lease randomly from worker who has the most count of leases.
     *
     * @param leaseCounts
     * @param target
     * @return
     */
    T chooseLeaseToSteal(Map<String, Integer> leaseCounts, int need, int target) {
        if (leaseCounts.isEmpty()) {
            return null;
        }

        Map.Entry<String, Integer> mostLoadedWorker = null;
        for (Map.Entry<String, Integer> worker : leaseCounts.entrySet()) {
            if (mostLoadedWorker == null || mostLoadedWorker.getValue() < worker.getValue()) {
                mostLoadedWorker = worker;
            }
        }

        if (mostLoadedWorker.getValue() <= target - 1 || (need <= 1 && mostLoadedWorker.getValue() == target)) {
            return null;
        }

        String mostLoadedWorkerIdentifier = mostLoadedWorker.getKey();
        List<T> candidates = new ArrayList<T>();
        for (T lease : allLeases.values()) {
            if (mostLoadedWorkerIdentifier.equals(lease.getLeaseOwner())) {
                candidates.add(lease);
            }
        }

        if (candidates.isEmpty()) {
            return null;
        }

        int randomIndex = new Random().nextInt(candidates.size());
        return candidates.get(randomIndex);
    }
}
