package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.core.DataFetcher;
import com.aliyun.openservices.ots.internal.streamclient.core.RecordProcessorCheckpointer;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.mock.SimpleMockCheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestInitializeTask {

    @Test
    public void testInitializeTask() throws StreamClientException, DependencyException, ShutdownException {
        ShardInfo shardInfo = new ShardInfo("TestShard", "", new HashSet<String>(), "");
        final InitializationInput[] input = {null};

        IRecordProcessor recordProcessor = new IRecordProcessor() {
            public void initialize(InitializationInput initializationInput) {
                input[0] = initializationInput;
            }

            public void processRecords(ProcessRecordsInput processRecordsInput) {
                System.exit(-1);
            }

            public void shutdown(ShutdownInput shutdownInput) {
                System.exit(-1);
            }
        };

        ICheckpointTracker checkpointTracker = new SimpleMockCheckpointTracker();


        IShutdownMarker shutdownMarker = new IShutdownMarker() {
            public void markForProcessDone() {

            }

            public void markForProcessRestart() {

            }
        };

        {
            DataFetcher dataFetcher = new DataFetcher(new MockOTS(), shardInfo);

            RecordProcessorCheckpointer checkpointer = new RecordProcessorCheckpointer(shardInfo, checkpointTracker, dataFetcher);

            InitializeTask initializeTask = new InitializeTask(shardInfo, recordProcessor, checkpointTracker,
                    checkpointer, dataFetcher, shutdownMarker);
            TaskResult result = initializeTask.call();
            assertEquals(null, result.getException());
            InitializationInput initializationInput = input[0];
            assertTrue(initializationInput != null);
            assertEquals(shardInfo.getShardId(), initializationInput.getShardInfo().getShardId());
            assertEquals(CheckpointPosition.TRIM_HORIZON, initializationInput.getInitialCheckpoint());
            assertEquals(CheckpointPosition.TRIM_HORIZON, initializationInput.getCheckpointer().getLargestPermittedCheckpointValue());
            assertEquals(shutdownMarker, initializationInput.getShutdownMarker());
        }

        {
            DataFetcher dataFetcher = new DataFetcher(new MockOTS(), shardInfo);

            RecordProcessorCheckpointer checkpointer = new RecordProcessorCheckpointer(shardInfo, checkpointTracker, dataFetcher);

            InitializeTask initializeTask = new InitializeTask(shardInfo, recordProcessor, checkpointTracker,
                    checkpointer, dataFetcher, shutdownMarker);
            String checkpointValue = "tdasdadasdas";
            checkpointTracker.setCheckpoint(shardInfo.getShardId(), checkpointValue, "");
            TaskResult result = initializeTask.call();
            assertEquals(null, result.getException());
            InitializationInput initializationInput = input[0];
            assertTrue(initializationInput != null);
            assertEquals(shardInfo.getShardId(), initializationInput.getShardInfo().getShardId());
            assertEquals(checkpointValue, initializationInput.getInitialCheckpoint());
            assertEquals(checkpointValue, initializationInput.getCheckpointer().getLargestPermittedCheckpointValue());
            assertEquals(shutdownMarker, initializationInput.getShutdownMarker());
        }
    }
}
