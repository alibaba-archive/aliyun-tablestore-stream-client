package com.aliyun.openservices.ots.internal.streamclient.core;

import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.ShardInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestDataFetcher {

    @Test
    public void testDataFetcher() throws DependencyException, StreamClientException {
        MockOTS ots = new MockOTS();

        String streamId = ots.listStream(new ListStreamRequest("testTable")).getStreams().get(0).getStreamId();

        ShardInfo shardInfo = new ShardInfo("TestShard", streamId, new HashSet<String>(), "");

        List<StreamRecord> list = new ArrayList<StreamRecord>();
        for (int i = 0; i < 100; i++) {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.PUT);
            record.setPrimaryKey(new PrimaryKey(Arrays.asList(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)))));
            record.setColumns(Arrays.asList(new RecordColumn(new Column("col", ColumnValue.fromLong(i), i), RecordColumn.ColumnType.PUT)));
            list.add(record);
        }
        ots.createShard(shardInfo.getShardId(), null, null);
        ots.appendRecords(shardInfo.getShardId(), list);

        DataFetcher dataFetcher = new DataFetcher(ots, shardInfo);

        try {
            dataFetcher.getRecords(10);
            fail();
        } catch (StreamClientException e) {
            assertEquals("DataFetcherNotInitialized", e.getMessage());
        }

        dataFetcher.initialize(CheckpointPosition.TRIM_HORIZON);

        try {
            dataFetcher.initialize(CheckpointPosition.TRIM_HORIZON);
            fail();
        } catch (StreamClientException e) {
            assertEquals("DataFetcherAlreadyInitialized", e.getMessage());
        }

        String iterator = ots.getShardIterator(new GetShardIteratorRequest(streamId, shardInfo.getShardId())).getShardIterator();
        assertEquals(iterator, dataFetcher.getShardIterator());
        assertEquals(10, dataFetcher.getRecords(10).getRecords().size());
        assertEquals(iterator, dataFetcher.getShardIterator());

        GetStreamRecordRequest request = new GetStreamRecordRequest(iterator);
        request.setLimit(10);
        iterator = ots.getStreamRecord(request).getNextShardIterator();
        dataFetcher.updateCheckpoint(iterator);

        assertEquals(iterator, dataFetcher.getShardIterator());
        assertEquals(10, dataFetcher.getRecords(10).getRecords().size());
        assertEquals(iterator, dataFetcher.getShardIterator());
    }


}
