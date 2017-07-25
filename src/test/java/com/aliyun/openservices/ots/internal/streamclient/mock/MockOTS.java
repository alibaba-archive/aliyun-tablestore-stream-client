package com.aliyun.openservices.ots.internal.streamclient.mock;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.model.OTSModelGenerator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MockOTS implements SyncClientInterface {

    private OTSModelGenerator otsModelGenerator = new OTSModelGenerator();
    private long creationTime = System.currentTimeMillis();

    private NavigableMap<String, StreamShard> shardMap = new ConcurrentSkipListMap<String, StreamShard>();
    private Map<String, List<StreamRecord>> shardRecords = new ConcurrentHashMap<String, List<StreamRecord>>();
    private Map<String, Boolean> isShardClose = new ConcurrentHashMap<String, Boolean>();
    private Map<String, Long> startIteratorIdx = new ConcurrentHashMap<String, Long>();

    public ListStreamResponse listStream(ListStreamRequest listStreamRequest) throws TableStoreException, ClientException {
        return otsModelGenerator.genListStreamResponse(listStreamRequest.getTableName(),
                listStreamRequest.getTableName() + "_Stream", creationTime);
    }

    public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest) throws TableStoreException, ClientException {
        List<StreamShard> shards = new ArrayList<StreamShard>();
        for (String shardId : shardMap.keySet()) {
            shards.add(shardMap.get(shardId));
        }
        return otsModelGenerator.genDescribeStreamResponse(describeStreamRequest.getStreamId().split("_")[0],
                describeStreamRequest.getStreamId(), creationTime, 24 * 60 * 60, shards);
    }

    public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws TableStoreException, ClientException {
        String shardId = getShardIteratorRequest.getShardId();
        if (shardMap.get(shardId) == null) {
            throw new RuntimeException("Illegal state.");
        }
        long idx = startIteratorIdx.get(shardId);
        return otsModelGenerator.genGetShardIteratorResponse(shardId + "\t" + idx);
    }

    public GetStreamRecordResponse getStreamRecord(GetStreamRecordRequest getStreamRecordRequest) throws TableStoreException, ClientException {
        List<StreamRecord> streamRecords = new ArrayList<StreamRecord>();
        int limit = getStreamRecordRequest.getLimit();
        if (limit < 0) {
            limit = 10000000;
        }
        String iterator = getStreamRecordRequest.getShardIterator();

        if (iterator.split("\t").length != 2) {
            throw new TableStoreException("Requested stream data is already trimmed or does not exist.",
                    null, "OTSTrimmedDataAccess", "", 404);
        }

        String shardId = iterator.split("\t")[0];
        int idx = Integer.parseInt(iterator.split("\t")[1]);

        List<StreamRecord> list = shardRecords.get(shardId);
        long minIdx = startIteratorIdx.get(shardId);
        int endIdx = Math.min(idx + limit, list.size());

        if (idx < minIdx || idx > list.size()) {
            throw new TableStoreException("Requested stream data is already trimmed or does not exist.",
                    null, "OTSTrimmedDataAccess", "", 404);
        }

        for (int i = idx; i < endIdx; i++) {
            streamRecords.add(list.get(i));
        }
        String nextIterator = shardId + "\t" + endIdx;
        if (isShardClose.get(shardId) && endIdx == list.size()) {
            nextIterator = null;
        }
        return otsModelGenerator.genGetStreamRecordResponse(streamRecords, nextIterator);
    }

    public void createShard(String shardId, String parentShardId, String parentSiblingShardId) {
        synchronized (shardMap) {
            if (shardMap.get(shardId) != null) {
                throw new RuntimeException("Illegal state.");
            }
            StreamShard streamShard = new StreamShard(shardId);
            streamShard.setParentId(parentShardId);
            streamShard.setParentSiblingId(parentSiblingShardId);
            shardMap.put(shardId, streamShard);
            shardRecords.put(shardId, new ArrayList<StreamRecord>());
            isShardClose.put(shardId, false);
            startIteratorIdx.put(shardId, 0L);
        }
    }

    public void appendRecords(String shardId, List<StreamRecord> records) {
        if (shardMap.get(shardId) == null || isShardClose.get(shardId)) {
            throw new RuntimeException("Illegal state.");
        }
        List<StreamRecord> list = shardRecords.get(shardId);
        synchronized (list) {
            list.addAll(records);
        }
    }

    private void closeShard(String shardId) {
        synchronized (isShardClose) {
            if (shardMap.get(shardId) == null || isShardClose.get(shardId)) {
                throw new RuntimeException("Illegal state.");
            }
            isShardClose.put(shardId, true);
        }
    }

    public void splitShard(String shardId, String resShardId1, String resShardId2) {
        closeShard(shardId);
        createShard(resShardId1, shardId, null);
        createShard(resShardId2, shardId, null);
    }

    public void mergeShard(String shardId1, String shardId2, String resShardId) {
        closeShard(shardId1);
        closeShard(shardId2);
        createShard(resShardId, shardId1, shardId2);
    }

    public void shutdown() {
    }

    public CreateTableResponse createTable(CreateTableRequest createTableRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public UpdateTableResponse updateTable(UpdateTableRequest updateTableRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public DescribeTableResponse describeTable(DescribeTableRequest describeTableRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public ListTableResponse listTable() throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public GetRowResponse getRow(GetRowRequest getRowRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public PutRowResponse putRow(PutRowRequest putRowRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public UpdateRowResponse updateRow(UpdateRowRequest updateRowRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public DeleteRowResponse deleteRow(DeleteRowRequest deleteRowRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public BatchGetRowResponse batchGetRow(BatchGetRowRequest batchGetRowRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public BatchWriteRowResponse batchWriteRow(BatchWriteRowRequest batchWriteRowRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    public GetRangeResponse getRange(GetRangeRequest getRangeRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ComputeSplitsBySizeResponse computeSplitsBySize(ComputeSplitsBySizeRequest computeSplitsBySizeRequest) throws TableStoreException, ClientException {
        return null;
    }

    public Iterator<Row> createRangeIterator(RangeIteratorParameter rangeIteratorParameter) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public WideColumnIterator createWideColumnIterator(GetRowRequest getRowRequest) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("");
    }
}
