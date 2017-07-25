package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.core.DataFetcher;
import com.aliyun.openservices.ots.internal.streamclient.core.RecordProcessorCheckpointer;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.mock.SimpleMockCheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestProcessTask {

    @Test
    public void testProcessTask() throws StreamClientException {
        MockOTS ots = new MockOTS();

        String streamId = ots.listStream(new ListStreamRequest("testTable")).getStreams().get(0).getStreamId();

        ShardInfo shardInfo = new ShardInfo("TestShard", streamId, new HashSet<String>(), "");

        final ProcessRecordsInput[] input = {null};

        IRecordProcessor recordProcessor = new IRecordProcessor() {
            public void initialize(InitializationInput initializationInput) {
                System.exit(-1);
            }

            public void processRecords(ProcessRecordsInput processRecordsInput) {
                input[0] = processRecordsInput;
            }

            public void shutdown(ShutdownInput shutdownInput) {
                System.exit(-1);
            }
        };

        ICheckpointTracker checkpointTracker = new SimpleMockCheckpointTracker();

        DataFetcher dataFetcher = new DataFetcher(ots, shardInfo);

        RecordProcessorCheckpointer checkpointer = new RecordProcessorCheckpointer(shardInfo, checkpointTracker, dataFetcher);

        IShutdownMarker shutdownMarker = new IShutdownMarker() {
            public void markForProcessDone() {

            }

            public void markForProcessRestart() {

            }
        };

        StreamConfig config = new StreamConfig();
        config.setMaxRecords(10);

        {
            ProcessTask processTask = new ProcessTask(shardInfo, recordProcessor, checkpointer, dataFetcher, config, shutdownMarker);
            TaskResult result = processTask.call();
            assertEquals("DataFetcherNotInitialized", result.getException().getMessage());
        }

        dataFetcher.initialize(CheckpointPosition.TRIM_HORIZON);
        ots.createShard(shardInfo.getShardId(), null, null);

        {
            ProcessTask processTask = new ProcessTask(shardInfo, recordProcessor, checkpointer, dataFetcher, config, shutdownMarker);
            TaskResult result = processTask.call();
            assertEquals(null, result.getException());
            ProcessRecordsInput processRecordsInput = input[0];
            assertTrue(processRecordsInput != null);
            String shardIterator = ots.getShardIterator(new GetShardIteratorRequest(streamId, shardInfo.getShardId())).getShardIterator();
            assertEquals(shardIterator, processRecordsInput.getCheckpointer().getLargestPermittedCheckpointValue());
            assertEquals(0, processRecordsInput.getRecords().size());
        }

        List<StreamRecord> list = new ArrayList<StreamRecord>();
        for (int i = 0; i < 100; i++) {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.PUT);
            record.setPrimaryKey(new PrimaryKey(Arrays.asList(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)))));
            record.setColumns(Arrays.asList(new RecordColumn(new Column("col", ColumnValue.fromLong(i), i), RecordColumn.ColumnType.PUT)));
            list.add(record);
        }
        ots.appendRecords(shardInfo.getShardId(), list);

        {
            ProcessTask processTask = new ProcessTask(shardInfo, recordProcessor, checkpointer, dataFetcher, config, shutdownMarker);
            TaskResult result = processTask.call();
            assertEquals(null, result.getException());
            ProcessRecordsInput processRecordsInput = input[0];
            assertTrue(processRecordsInput != null);
            String shardIterator = ots.getShardIterator(new GetShardIteratorRequest(streamId, shardInfo.getShardId())).getShardIterator();
            GetStreamRecordRequest request = new GetStreamRecordRequest(shardIterator);
            request.setLimit(10);
            shardIterator = ots.getStreamRecord(request).getNextShardIterator();
            assertEquals(shardIterator, processRecordsInput.getCheckpointer().getLargestPermittedCheckpointValue());
            assertEquals(10, processRecordsInput.getRecords().size());
        }

        {
            config.setMaxRecords(1000);
            ProcessTask processTask = new ProcessTask(shardInfo, recordProcessor, checkpointer, dataFetcher, config, shutdownMarker);
            TaskResult result = processTask.call();
            assertEquals(null, result.getException());
            ProcessRecordsInput processRecordsInput = input[0];
            assertTrue(processRecordsInput != null);
            String shardIterator = ots.getShardIterator(new GetShardIteratorRequest(streamId, shardInfo.getShardId())).getShardIterator();
            GetStreamRecordRequest request = new GetStreamRecordRequest(shardIterator);
            shardIterator = ots.getStreamRecord(request).getNextShardIterator();
            assertEquals(shardIterator, processRecordsInput.getCheckpointer().getLargestPermittedCheckpointValue());
            assertEquals(100, processRecordsInput.getRecords().size());
        }
    }

    @Test
    public void testRetry() throws StreamClientException {

        class AMockOts extends MockOTS {
            @Override
            public GetStreamRecordResponse getStreamRecord(GetStreamRecordRequest request) throws TableStoreException {
                throw new TableStoreException("test", null, "test", "requestId", 400);
            }
        }

        AMockOts ots = new AMockOts();

        String streamId = ots.listStream(new ListStreamRequest("testTable")).getStreams().get(0).getStreamId();

        ShardInfo shardInfo = new ShardInfo("TestShard", streamId, new HashSet<String>(), "");

        final ProcessRecordsInput[] input = {null};

        IRecordProcessor recordProcessor = new IRecordProcessor() {
            public void initialize(InitializationInput initializationInput) {
                System.exit(-1);
            }

            public void processRecords(ProcessRecordsInput processRecordsInput) {
                input[0] = processRecordsInput;
            }

            public void shutdown(ShutdownInput shutdownInput) {
                System.exit(-1);
            }
        };

        ICheckpointTracker checkpointTracker = new SimpleMockCheckpointTracker();

        DataFetcher dataFetcher = new DataFetcher(ots, shardInfo);

        RecordProcessorCheckpointer checkpointer = new RecordProcessorCheckpointer(shardInfo, checkpointTracker, dataFetcher);

        IShutdownMarker shutdownMarker = new IShutdownMarker() {
            public void markForProcessDone() {

            }

            public void markForProcessRestart() {

            }
        };

        StreamConfig config = new StreamConfig();
        config.setMaxRecords(10);

        dataFetcher.initialize(CheckpointPosition.TRIM_HORIZON);

        ots.createShard(shardInfo.getShardId(), null, null);

        ProcessTask processTask = new ProcessTask(shardInfo, recordProcessor, checkpointer, dataFetcher, config, shutdownMarker);
        RetryingTaskDecorator decorator = new RetryingTaskDecorator(IRetryStrategy.RetryableAction.TASK_PROCESS, new TaskRetryStrategy(), processTask);

        decorator.call();
    }
}
