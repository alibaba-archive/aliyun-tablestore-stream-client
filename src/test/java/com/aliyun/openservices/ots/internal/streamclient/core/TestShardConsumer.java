package com.aliyun.openservices.ots.internal.streamclient.core;

import com.alicloud.openservices.tablestore.model.ListStreamRequest;
import com.aliyun.openservices.ots.internal.streamclient.ClientConfig;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.core.task.TaskRetryStrategy;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseCoordinator;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class TestShardConsumer {

    @Test
    public void testShardConsumer() throws StreamClientException, DependencyException {
        MockOTS ots = new MockOTS();

        String streamId = ots.listStream(new ListStreamRequest("testTable")).getStreams().get(0).getStreamId();

        ShardInfo shardInfo = new ShardInfo("TestShard", streamId, new HashSet<String>(), "");

        ots.createShard(shardInfo.getShardId(), null, null);

        StreamConfig config = new StreamConfig();
        config.setDataTableName("dataTable");
        config.setStatusTableName("statusTable");
        config.setOTSClient(ots);

        MemoryLeaseManager leaseManager = new MemoryLeaseManager();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setTakerIntervalMillis(1000);
        clientConfig.setRenewerIntervalMillis(1000);

        LeaseCoordinator<ShardLease> leaseCoordinator = new LeaseCoordinator<ShardLease>(leaseManager, "worker", clientConfig);
        leaseCoordinator.initialize();
        leaseCoordinator.start();

        ICheckpointTracker checkpointTracker = new CheckpointTracker(leaseManager, leaseCoordinator);

        final boolean[] initialized = {false};
        final boolean[] processed = {false};
        final boolean[] shutdown = {false};
        IRecordProcessor recordProcessor = new IRecordProcessor() {
            public void initialize(InitializationInput initializationInput) {
                initialized[0] = true;
            }

            public void processRecords(ProcessRecordsInput processRecordsInput) {
                processed[0] = true;
                processRecordsInput.getShutdownMarker().markForProcessDone();
            }

            public void shutdown(ShutdownInput shutdownInput) {
                shutdown[0] = true;
            }
        };

        ExecutorService executorService = Executors.newCachedThreadPool();

        IRetryStrategy retryStrategy = new TaskRetryStrategy();

        ShardSyncer shardSyncer = new ShardSyncer(config, leaseManager, executorService, retryStrategy);

        ShardConsumer consumer = new ShardConsumer(shardInfo, config, checkpointTracker, recordProcessor,
                leaseManager, 10, executorService, shardSyncer, retryStrategy);

        assertEquals(ShardConsumer.ShardConsumerState.WAITING_ON_PARENT_SHARDS, consumer.getCurrentState());
        assertEquals(true, consumer.consumeShard());
        TimeUtils.sleepMillis(1000);

        assertEquals(false, initialized[0]);
        assertEquals(true, consumer.consumeShard());
        assertEquals(ShardConsumer.ShardConsumerState.INITIALIZING, consumer.getCurrentState());
        TimeUtils.sleepMillis(1000);
        assertEquals(true, initialized[0]);

        assertEquals(false, processed[0]);
        assertEquals(true, consumer.consumeShard());
        assertEquals(ShardConsumer.ShardConsumerState.PROCESSING, consumer.getCurrentState());
        TimeUtils.sleepMillis(1000);
        assertEquals(true, processed[0]);

        assertEquals(false, shutdown[0]);
        assertEquals(true, consumer.consumeShard());
        assertEquals(ShardConsumer.ShardConsumerState.SHUTTING_DOWN, consumer.getCurrentState());
        TimeUtils.sleepMillis(1000);
        assertEquals(true, shutdown[0]);

        assertEquals(false, consumer.consumeShard());
        assertEquals(ShardConsumer.ShardConsumerState.SHUTDOWN_COMPLETE, consumer.getCurrentState());
    }

}
