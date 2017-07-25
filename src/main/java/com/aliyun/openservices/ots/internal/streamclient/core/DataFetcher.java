package com.aliyun.openservices.ots.internal.streamclient.core;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.core.exceptions.ShardEndReachedException;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.ShardInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataFetcher用于获取StreamRecords数据。
 * DataFetcher内部也会保存一个Checkpoint，每次从这个Checkpoint开始获取数据，
 * 该Checkpoint会在用户更新Checkpoint时同步更新。
 */
public class DataFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(DataFetcher.class);

    private String checkpointValue;
    private SyncClientInterface ots;
    private final String shardId;
    private final String streamId;
    private boolean isInitialized;

    public DataFetcher(SyncClientInterface ots, ShardInfo shardInfo) {
        this.shardId = shardInfo.getShardId();
        this.streamId = shardInfo.getStreamId();
        this.ots = ots;
    }

    public GetStreamRecordResponse getRecords(int maxRecords) throws DependencyException, StreamClientException {
        if (!isInitialized) {
            throw new StreamClientException("DataFetcherNotInitialized");
        }
        GetStreamRecordResponse result = null;
        try {
            GetStreamRecordRequest request = new GetStreamRecordRequest(getShardIterator());
            request.setLimit(maxRecords);
            result = ots.getStreamRecord(request);
            this.updateCheckpoint(result.getNextShardIterator());
        } catch (TableStoreException ex) {
            throw new DependencyException(ex.toString(), ex);
        }
        return result;
    }

    public void initialize(String initialCheckpoint) throws StreamClientException {
        if (isInitialized) {
            throw new StreamClientException("DataFetcherAlreadyInitialized");
        }
        checkpointValue = initialCheckpoint;
        isInitialized = true;
    }

    public void updateCheckpoint(String checkpointValue) throws StreamClientException {
        if (!isInitialized) {
            throw new StreamClientException("DataFetcherNotInitialized");
        }
        this.checkpointValue = checkpointValue;
    }

    String getShardIterator() throws DependencyException, StreamClientException {
        if (!isInitialized) {
            throw new StreamClientException("DataFetcherNotInitialized");
        }
        try {
            if (checkpointValue.equals(CheckpointPosition.SHARD_END)) {
                throw new ShardEndReachedException("CheckpointReachedShardEnd");
            } else if (checkpointValue.equals(CheckpointPosition.TRIM_HORIZON)) {
                GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest(streamId, shardId);
                return ots.getShardIterator(getShardIteratorRequest).getShardIterator();
            } else {
                return checkpointValue;
            }
        } catch (TableStoreException ex) {
            throw new DependencyException(ex.toString(), ex);
        }
    }
}
