package com.aliyun.openservices.ots.internal.streamclient.core;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.model.IRecordProcessorCheckpointer;
import com.aliyun.openservices.ots.internal.streamclient.model.ICheckpointTracker;
import com.aliyun.openservices.ots.internal.streamclient.model.ShardInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在RecordProcessor中，由用户代码调用RecordProcessorCheckpointer进行Checkpoint的更新。
 */
public class RecordProcessorCheckpointer implements IRecordProcessorCheckpointer {

    private static final Logger LOG = LoggerFactory.getLogger(RecordProcessorCheckpointer.class);

    private ShardInfo shardInfo;
    private ICheckpointTracker checkpointTracker;
    private String largestPermittedCheckpointValue;
    private DataFetcher dataFetcher;

    public RecordProcessorCheckpointer(ShardInfo shardInfo,
                                ICheckpointTracker checkpointTracker,
                                DataFetcher dataFetcher) {
        this.shardInfo = shardInfo;
        this.checkpointTracker = checkpointTracker;
        this.dataFetcher = dataFetcher;
    }

    public synchronized void checkpoint() throws ShutdownException, StreamClientException, DependencyException {
        doCheckpoint(largestPermittedCheckpointValue);
    }

    public synchronized void checkpoint(String checkpointValue) throws ShutdownException, StreamClientException, DependencyException {
        doCheckpoint(checkpointValue);
    }

    public String getLargestPermittedCheckpointValue() {
        return largestPermittedCheckpointValue;
    }

    public void setLargestPermittedCheckpointValue(String largestPermittedCheckpointValue) {
        this.largestPermittedCheckpointValue = largestPermittedCheckpointValue;
    }

    private void doCheckpoint(String checkpointValue) throws ShutdownException, StreamClientException, DependencyException {
        LOG.debug("DoCheckpoint, ShardId: {}, Checkpoint: {}.", shardInfo.getShardId(), checkpointValue);
        checkpointTracker.setCheckpoint(shardInfo.getShardId(), checkpointValue, shardInfo.getLeaseIdentifier());
        dataFetcher.updateCheckpoint(checkpointValue);
    }
}
