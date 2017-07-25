package com.aliyun.openservices.ots.internal.streamclient;

import com.aliyun.openservices.ots.internal.streamclient.core.InnerWorker;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseCoordinator;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.ICheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.IRecordProcessorFactory;
import com.aliyun.openservices.ots.internal.streamclient.model.WorkerStatus;

import java.util.concurrent.ExecutorService;

public class Worker implements Runnable {

    private InnerWorker innerWorker;

    public Worker(String workerIdentifier,
                       ClientConfig clientConfig,
                       StreamConfig streamConfig,
                       IRecordProcessorFactory recordProcessorFactory,
                       ExecutorService executorService) {
        this(workerIdentifier, clientConfig, streamConfig, recordProcessorFactory, executorService, null);
    }

    public Worker(String workerIdentifier,
                       ClientConfig clientConfig,
                       StreamConfig streamConfig,
                       IRecordProcessorFactory recordProcessorFactory,
                       ExecutorService executorService,
                       ILeaseManager<ShardLease> leaseManager) {
        this(workerIdentifier, clientConfig, streamConfig, recordProcessorFactory, executorService, leaseManager, null);
    }

    public Worker(String workerIdentifier,
                       ClientConfig clientConfig,
                       StreamConfig streamConfig,
                       IRecordProcessorFactory recordProcessorFactory,
                       ExecutorService executorService,
                       ILeaseManager<ShardLease> leaseManager,
                       LeaseCoordinator<ShardLease> shardLeaseCoordinator) {
        this(workerIdentifier, clientConfig, streamConfig, recordProcessorFactory,
                executorService, leaseManager, shardLeaseCoordinator, null);
    }

    public Worker(String workerIdentifier,
                  ClientConfig clientConfig,
                  StreamConfig streamConfig,
                  IRecordProcessorFactory recordProcessorFactory,
                  ExecutorService executorService,
                  ILeaseManager<ShardLease> leaseManager,
                  LeaseCoordinator<ShardLease> shardLeaseCoordinator,
                  ICheckpointTracker checkpointTracker) {
        this.innerWorker = new InnerWorker(workerIdentifier, clientConfig, streamConfig, recordProcessorFactory,
                executorService, leaseManager, shardLeaseCoordinator, checkpointTracker);
    }

    public void run() {
        innerWorker.run();
    }

    public void shutdown() {
        innerWorker.shutdown();
    }

    public WorkerStatus getWorkerStatus() {
        return innerWorker.getWorkerStatus();
    }

    public Exception getException() {
        return innerWorker.getException();
    }
}
