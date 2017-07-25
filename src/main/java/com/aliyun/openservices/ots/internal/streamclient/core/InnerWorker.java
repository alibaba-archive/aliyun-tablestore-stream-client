package com.aliyun.openservices.ots.internal.streamclient.core;

import com.alicloud.openservices.tablestore.model.StreamDetails;
import com.aliyun.openservices.ots.internal.streamclient.ClientConfig;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.core.task.*;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.lease.*;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.utils.OTSHelper;
import com.aliyun.openservices.ots.internal.streamclient.utils.Preconditions;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * InnerWorker对象由Worker对象进行创建，执行具体逻辑。
 */
public class InnerWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(InnerWorker.class);

    private final String workerIdentifier;
    private final ClientConfig clientConfig;
    private final StreamConfig streamConfig;
    private final IRecordProcessorFactory recordProcessorFactory;
    private final ILeaseManager<ShardLease> leaseManager;
    private final ExecutorService executorService;
    private final LeaseCoordinator<ShardLease> leaseCoordinator;
    private final ShardSyncer shardSyncer;
    private final ICheckpointTracker checkpointTracker;
    private final IRetryStrategy taskRetryStrategy;
    private final IRetryStrategy leaseManagerRetryStrategy;

    long lastSyncShardTimeMillis;

    private ConcurrentMap<ShardInfo, ShardConsumer> shardConsumerMap =
            new ConcurrentHashMap<ShardInfo, ShardConsumer>();
    private volatile boolean running;
    private volatile boolean shutdown;
    private Exception exception;
    private String streamId;

    public InnerWorker(String workerIdentifier,
                       ClientConfig clientConfig,
                       StreamConfig streamConfig,
                       IRecordProcessorFactory recordProcessorFactory,
                       ExecutorService executorService,
                       ILeaseManager<ShardLease> leaseManager,
                       LeaseCoordinator<ShardLease> shardLeaseCoordinator,
                       ICheckpointTracker checkpointTracker) {
        Preconditions.checkArgument(workerIdentifier != null && !workerIdentifier.isEmpty(),
                "workerIdentifier should not be null or empty");
        Preconditions.checkNotNull(clientConfig);
        Preconditions.checkNotNull(streamConfig);
        Preconditions.checkNotNull(streamConfig.getOTSClient());
        Preconditions.checkNotNull(recordProcessorFactory);
        Preconditions.checkNotNull(executorService);

        LOG.info("Initialize inner worker.");
        LOG.info("ClientConfig: {}", clientConfig);
        LOG.info("StreamConfig: {}", streamConfig);

        this.workerIdentifier = workerIdentifier;
        this.clientConfig = clientConfig;
        this.streamConfig = streamConfig;
        this.recordProcessorFactory = recordProcessorFactory;
        this.executorService = executorService;

        StreamDetails streamDetails = OTSHelper.getStreamDetails(this.streamConfig.getOTSClient(), this.streamConfig.getDataTableName());
        if (!streamDetails.isEnableStream()) {
            throw new IllegalArgumentException("The data table does not enable stream.");
        }
        this.streamId = streamDetails.getStreamId();

        if (clientConfig.getTaskRetryStrategy() != null) {
            this.taskRetryStrategy = clientConfig.getTaskRetryStrategy();
        } else {
            this.taskRetryStrategy = new TaskRetryStrategy();
        }

        if (clientConfig.getLeaseManagerRetryStrategy() != null) {
            this.leaseManagerRetryStrategy = clientConfig.getLeaseManagerRetryStrategy();
        } else {
            this.leaseManagerRetryStrategy = new LeaseManagerRetryStrategy();
        }

        if (leaseManager != null) {
            this.leaseManager = leaseManager;
        } else {
            this.leaseManager = new LeaseManager<ShardLease>(
                    this.streamConfig.getOTSClient(), this.streamConfig.getStatusTableName(),
                    new ShardLeaseSerializer(this.streamConfig.getStatusTableName(), this.streamId),
                    leaseManagerRetryStrategy, this.clientConfig.getCheckTableReadyIntervalMillis());
        }

        if (shardLeaseCoordinator != null) {
            this.leaseCoordinator = shardLeaseCoordinator;
        } else {
            this.leaseCoordinator = new LeaseCoordinator<ShardLease>(
                    this.leaseManager, this.workerIdentifier, this.clientConfig);
        }

        if (checkpointTracker != null) {
            this.checkpointTracker = checkpointTracker;
        } else {
            this.checkpointTracker = new CheckpointTracker(this.leaseManager, this.leaseCoordinator);
        }

        this.shardSyncer = new ShardSyncer(
                this.streamConfig,
                this.leaseManager,
                this.executorService,
                this.taskRetryStrategy);
    }

    public void run() {
        try {
            if (running || shutdown) {
                throw new StreamClientException("Can't rerun a worker.");
            }
            running = true;
            initialize();
            leaseCoordinator.start();
            lastSyncShardTimeMillis = System.currentTimeMillis();

            while (!shutdown) {
                runProcessLoop();

                // todo optimize: only sleep when there is no data to consume
                TimeUtils.sleepMillis(clientConfig.getWorkerIdleTimeMillis());
            }
        } catch (Throwable ex) {
            LOG.error("Exception: {}.", ex);
            if (ex instanceof Exception) {
                this.exception = (Exception) ex;
            } else {
                this.exception = new RuntimeException(ex);
            }
            shutdown();
        }
    }

    void runProcessLoop() throws Exception {
        List<ShardInfo> heldShards = new ArrayList<ShardInfo>();
        List<ShardInfo> stolenShards = new ArrayList<ShardInfo>();
        getCurrentlyHeldShards(heldShards, stolenShards);
        for (ShardInfo shardInfo : heldShards) {
            ShardConsumer shardConsumer = createOrGetShardConsumer(shardInfo);
            if (!shardConsumer.isShutdown()) {
                shardConsumer.consumeShard();
            }
        }

        // todo optimize: need to sync only when there is any shard has been consumed to end.
        if (System.currentTimeMillis() - lastSyncShardTimeMillis > clientConfig.getSyncShardIntervalMillis()) {
            if (shardSyncer.syncShardAndLeaseInfo(false)) {
                lastSyncShardTimeMillis = System.currentTimeMillis();
            }
        }

        cleanupShardConsumers(heldShards, stolenShards);
        leaseCoordinator.checkRenewerAndTakerStatus(clientConfig.getMaxDurationBeforeLastSuccessfulRenewOrTakeLease());
    }

    public void shutdown() {
        this.running = false;
        this.shutdown = true;
        leaseCoordinator.stop();
    }

    public WorkerStatus getWorkerStatus() {
        if (this.exception != null) {
            return WorkerStatus.ERROR;
        }
        if (this.running) {
            return WorkerStatus.RUNNING;
        }
        if (this.shutdown) {
            return WorkerStatus.SHUTDOWN;
        }
        return WorkerStatus.NOT_RUNNING;
    }

    public Exception getException() {
        return this.exception;
    }

    private void initialize() throws StreamClientException, DependencyException {
        leaseCoordinator.initialize();
        shardSyncer.syncShardAndLeaseInfo(true);
    }

    private void getCurrentlyHeldShards(List<ShardInfo> heldShards, List<ShardInfo> stolenShards) {
        heldShards.clear();
        stolenShards.clear();
        Collection<ShardLease> heldLeases = leaseCoordinator.getCurrentlyHeldLeases();
        for (ShardLease shardLease : heldLeases) {
            if (shardLease.getLeaseStealer().isEmpty()) {
                LOG.debug("Currently held shard: {}", shardLease);
                heldShards.add(shardLease.toShardInfo());
            } else {
                LOG.debug("Currently stolen shard: {}", shardLease);
                stolenShards.add(shardLease.toShardInfo());
            }
        }
    }

    private ShardConsumer createOrGetShardConsumer(ShardInfo shardInfo) {
        ShardConsumer consumer = shardConsumerMap.get(shardInfo);
        if ((consumer == null) ||
                (consumer.isShutdown() && consumer.getShutdownReason() == ShutdownReason.PROCESS_RESTART)) {
            IRecordProcessor recordProcessor = recordProcessorFactory.createProcessor();
            consumer = new ShardConsumer(
                        shardInfo,
                        streamConfig,
                        checkpointTracker,
                        recordProcessor,
                        leaseManager,
                        clientConfig.getParentShardPollIntervalMillis(),
                        executorService,
                        shardSyncer,
                        taskRetryStrategy);
            shardConsumerMap.put(shardInfo, consumer);
            LOG.info("CreateNewConsumer, ShardInfo: {}.", shardInfo);
        }
        return consumer;
    }

    private void cleanupShardConsumers(List<ShardInfo> heldShards, List<ShardInfo> stolenShards) throws StreamClientException, DependencyException {
        Set<ShardInfo> heldShardSet = new HashSet<ShardInfo>();
        Set<ShardInfo> stolenShardSet = new HashSet<ShardInfo>();
        for (ShardInfo shardInfo : heldShards) {
            heldShardSet.add(shardInfo);
        }
        for (ShardInfo shardInfo : stolenShards) {
            stolenShardSet.add(shardInfo);
        }

        for (ShardInfo shardInfo : shardConsumerMap.keySet()) {
            if (!heldShardSet.contains(shardInfo)) {
                boolean stolen = stolenShardSet.contains(shardInfo);
                ShutdownReason reason = stolen ? ShutdownReason.STOLEN : ShutdownReason.ZOMBIE;
                boolean isShutdown = shardConsumerMap.get(shardInfo).beginShutdown(reason);
                LOG.info("CleanConsumer, ShardInfo: {}, Reason: {}, IsShutDown: {}.", shardInfo, reason, isShutdown);
                if (isShutdown) {
                    shardConsumerMap.remove(shardInfo);
                    if (stolen) {
                        LOG.info("Transfer lease: {}.", shardInfo);
                        leaseCoordinator.transferLease(shardInfo.getShardId(), shardInfo.getLeaseIdentifier());
                    }
                }
            }
        }
    }
}
