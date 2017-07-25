package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.ClientConfig;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseRenewer;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseTaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lease协调器，职责为：
 * 1. 定期的更新当前hold的lease
 * 2. 定期的尝试获取更多的lease
 * 3. 主动处理lease的转交
 * 4. 提供更新lease的接口
 *
 * @param <T>
 */
public class LeaseCoordinator<T extends Lease> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseCoordinator.class);

    private final ILeaseManager<T> leaseManager;
    private final ILeaseRenewer<T> leaseRenewer;
    private final ILeaseTaker<T> leaseTaker;
    private final long renewerIntervalMillis;
    private final long takerIntervalMillis;
    private final long maxWaitTableReadyTimeMillis;
    private final int statusTableReadCapacity;
    private final int statusTableWriteCapacity;

    private ScheduledExecutorService scheduledExecutorService;
    private static final String THREAD_PREFIX = "LeaseCoordinator-";
    private volatile boolean running = false;
    private Throwable lastTakeException;
    private Throwable lastRenewException;
    private long lastTimeOfSuccessfulTake;
    private long lastTimeOfSuccessfulRenew;

    private Object shutdownLock = new Object();

    public LeaseCoordinator(ILeaseManager<T> leaseManager,
                            String workerIdentifier,
                            ClientConfig config) {
        ThreadPoolExecutor renewPool = new ThreadPoolExecutor(config.getRenewThreadPoolSize(), config.getRenewThreadPoolSize(),
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        renewPool.allowCoreThreadTimeOut(true);

        this.leaseManager = leaseManager;
        this.leaseTaker = new LeaseTaker<T>(leaseManager, workerIdentifier, config.getLeaseDurationMillis(), config.isAutoStealLease());
        this.leaseRenewer = new LeaseRenewer<T>(leaseManager, workerIdentifier, config.getLeaseDurationMillis(), renewPool);
        this.renewerIntervalMillis = config.getRenewerIntervalMillis();
        this.takerIntervalMillis = config.getTakerIntervalMillis();
        this.maxWaitTableReadyTimeMillis = config.getMaxWaitTableReadyTimeMillis();
        this.statusTableReadCapacity = config.getStatusTableReadCapacity();
        this.statusTableWriteCapacity = config.getStatusTableWriteCapacity();
    }

    public Throwable getLastTakeException() {
        return lastTakeException;
    }

    public Throwable getLastRenewException() {
        return lastRenewException;
    }

    public long getLastTimeOfSuccessfulTake() {
        return lastTimeOfSuccessfulTake;
    }

    public long getLastTimeOfSuccessfulRenew() {
        return lastTimeOfSuccessfulRenew;
    }

    private class TakerRunnable implements Runnable {

        public void run() {
            try {
                runTaker();
                lastTimeOfSuccessfulTake = System.currentTimeMillis();
                LOG.debug("Period take lease at {}.", lastTimeOfSuccessfulTake);
            } catch (Throwable t) {
                LOG.error("Failed to take lease: {}.", t);
                lastTakeException = t;
            }
        }

    }

    private class RenewerRunnable implements Runnable {

        public void run() {
            try {
                runRenewer();
                lastTimeOfSuccessfulRenew = System.currentTimeMillis();
                LOG.debug("Period review lease at {}.", lastTimeOfSuccessfulRenew);
            } catch (Throwable t) {
                LOG.error("Failed to renew lease: {}.", t);
                lastRenewException = t;
            }
        }

    }

    public void start() throws StreamClientException, DependencyException {
        leaseRenewer.initialize();
        scheduledExecutorService = Executors.newScheduledThreadPool(2, new ThreadFactory() {
            private AtomicInteger counter = new AtomicInteger(0);

            public Thread newThread(Runnable runnable) {
                Thread thread = Executors.defaultThreadFactory().newThread(runnable);
                thread.setName(THREAD_PREFIX + counter.incrementAndGet());
                return thread;
            }
        });

        scheduledExecutorService.scheduleWithFixedDelay(new TakerRunnable(), 0, takerIntervalMillis, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleWithFixedDelay(new RenewerRunnable(), 0, renewerIntervalMillis, TimeUnit.MILLISECONDS);
        running = true;
    }

    public void initialize() throws StreamClientException, DependencyException {
        final boolean created = leaseManager.createLeaseTableIfNotExists(
                statusTableReadCapacity, statusTableWriteCapacity, -1);
        LOG.debug("Create status table, created: {}.", created);
        leaseManager.waitUntilTableReady(maxWaitTableReadyTimeMillis);
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        LOG.info("Start stop lease coordinator.");
        if (!isRunning()) {
            LOG.info("Lease coordinator is not running.");
            return;
        }

        LOG.debug("Shutdown executor service for taker and renewer.");
        scheduledExecutorService.shutdownNow();
        LOG.debug("Executor service is shutdown.");

        synchronized (shutdownLock) {
            LOG.debug("Clear currently held leases as coordinator is stopped.");
            leaseRenewer.clearCurrentlyHeldLeases();
            running = false;
        }
        LOG.info("Lease coordinator is stopped.");
    }

    private void runTaker() throws StreamClientException, DependencyException {
        Map<String, T> takenLeases = leaseTaker.takeLeases();

        synchronized (shutdownLock) {
            if (isRunning()) {
                leaseRenewer.addLeasesToRenew(takenLeases.values());
            }
        }
    }

    private void runRenewer() throws StreamClientException, DependencyException {
        leaseRenewer.renewLeases();
    }

    public Collection<T> getCurrentlyHeldLeases() {
        return leaseRenewer.getCurrentlyHeldLeases().values();
    }

    public T getCurrentlyHeldLease(String leaseKey) {
        return leaseRenewer.getCurrentlyHeldLease(leaseKey);
    }

    public boolean updateLease(T lease, String leaseIdentifier) throws StreamClientException, DependencyException {
        return leaseRenewer.updateLease(lease, leaseIdentifier);
    }

    public boolean transferLease(String leaseKey, String leaseIdentifier) throws StreamClientException, DependencyException {
        return leaseRenewer.transferLease(leaseKey, leaseIdentifier);
    }

    public void checkRenewerAndTakerStatus(long maxDurationBeforeLastSuccess) throws StreamClientException {
        long now = System.currentTimeMillis();
        if (lastTimeOfSuccessfulRenew == 0) {
            lastTimeOfSuccessfulRenew = now;
        }
        if (lastTimeOfSuccessfulTake == 0) {
            lastTimeOfSuccessfulTake = now;
        }
        if (lastTimeOfSuccessfulRenew + maxDurationBeforeLastSuccess < now) {
            throw new StreamClientException("Too long didn't renew. LastRenewTime: " + lastTimeOfSuccessfulRenew + ".", lastRenewException);
        }
        if (lastTimeOfSuccessfulTake + maxDurationBeforeLastSuccess < now) {
            throw new StreamClientException("Too long didn't take. LastTakeTime: " + lastTimeOfSuccessfulTake + ".", lastTakeException);
        }
    }
}
