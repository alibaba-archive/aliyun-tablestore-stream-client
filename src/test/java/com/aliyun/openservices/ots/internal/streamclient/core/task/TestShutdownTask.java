package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.core.DataFetcher;
import com.aliyun.openservices.ots.internal.streamclient.core.RecordProcessorCheckpointer;
import com.aliyun.openservices.ots.internal.streamclient.core.ShardSyncer;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.mock.SimpleMockCheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class TestShutdownTask {

    @Test
    public void testShutdownTask() {
        ShardInfo shardInfo = new ShardInfo("TestShard", "", new HashSet<String>(), "");
        final ShutdownInput[] input = {null};

        IRecordProcessor recordProcessor = new IRecordProcessor() {
            public void initialize(InitializationInput initializationInput) {
                System.exit(-1);
            }

            public void processRecords(ProcessRecordsInput processRecordsInput) {
                System.exit(-1);
            }

            public void shutdown(ShutdownInput shutdownInput) {
                input[0] = shutdownInput;
            }
        };

        ICheckpointTracker checkpointTracker = new SimpleMockCheckpointTracker();

        MockOTS ots = new MockOTS();
        DataFetcher dataFetcher = new DataFetcher(ots, shardInfo);
        RecordProcessorCheckpointer checkpointer = new RecordProcessorCheckpointer(shardInfo, checkpointTracker, dataFetcher);

        ShutdownReason reason = ShutdownReason.TERMINATE;

        StreamConfig config = new StreamConfig();
        config.setDataTableName("testTable");
        config.setOTSClient(ots);

        MemoryLeaseManager leaseManager = new MemoryLeaseManager();

        ShardSyncer shardSyncer = new ShardSyncer(config, leaseManager, Executors.newCachedThreadPool(), new TaskRetryStrategy());
        {
            ShutdownTask shutdownTask = new ShutdownTask(shardInfo, recordProcessor, checkpointer, reason, shardSyncer);
            TaskResult result = shutdownTask.call();
            assertEquals(null, result.getException());
            ShutdownInput shutdownInput = input[0];
            assertEquals(checkpointer, shutdownInput.getCheckpointer());
            assertEquals(reason, shutdownInput.getShutdownReason());
        }
    }
}
