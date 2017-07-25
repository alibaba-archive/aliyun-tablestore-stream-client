package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.alicloud.openservices.tablestore.model.GetStreamRecordResponse;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.core.exceptions.ApplicationException;
import com.aliyun.openservices.ots.internal.streamclient.core.exceptions.ShardEndReachedException;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.core.DataFetcher;
import com.aliyun.openservices.ots.internal.streamclient.core.RecordProcessorCheckpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 通过DataFetcher获取数据，执行用户的processRecords方法。
 */
public class ProcessTask implements ITask {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessTask.class);

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final DataFetcher dataFetcher;
    private final StreamConfig streamConfig;
    private final IShutdownMarker shutdownMarker;

    public ProcessTask(ShardInfo shardInfo,
                       IRecordProcessor recordProcessor,
                       RecordProcessorCheckpointer recordProcessorCheckpointer,
                       DataFetcher dataFetcher,
                       StreamConfig streamConfig,
                       IShutdownMarker shutdownMarker) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.dataFetcher = dataFetcher;
        this.streamConfig = streamConfig;
        this.shutdownMarker = shutdownMarker;
    }

    public TaskResult call() {
        LOG.debug("Start, ShardId: {}.", shardInfo.getShardId());
        try {
            GetStreamRecordResponse getStreamRecordResult = null;
            try {
                getStreamRecordResult = getRecordResult();
            } catch (ShardEndReachedException ex) {
                LOG.debug("Complete, ShardEndReached, ShardId: {}.", shardInfo.getShardId());
                return new TaskResult(true);
            }

            List<StreamRecord> records = getStreamRecordResult.getRecords();
            LOG.debug("GetRecords, ShardId: {}, Num: {}.", shardInfo.getShardId(), records.size());

            String nextIterator = getStreamRecordResult.getNextShardIterator();
            if (nextIterator != null) {
                recordProcessorCheckpointer.setLargestPermittedCheckpointValue(nextIterator);
            } else {
                recordProcessorCheckpointer.setLargestPermittedCheckpointValue(CheckpointPosition.SHARD_END);
            }
            ProcessRecordsInput processRecordsInput = new ProcessRecordsInput();
            processRecordsInput.setRecords(records);
            processRecordsInput.setCheckpointer(recordProcessorCheckpointer);
            processRecordsInput.setShutdownMarker(shutdownMarker);

            try {
                recordProcessor.processRecords(processRecordsInput);
            } catch (Throwable e) {
                throw new ApplicationException("ApplicationProcessError", e);
            }
            LOG.debug("Complete, ShardId: {}", shardInfo.getShardId());
            return new TaskResult(false);
        } catch (Exception e) {
            LOG.warn("ShardId: {}, Exception: {}", e);
            return new TaskResult(e);
        }
    }

    public TaskType getTaskType() {
        return TaskType.PROCESS;
    }

    private GetStreamRecordResponse getRecordResult() throws DependencyException, StreamClientException {
        return dataFetcher.getRecords(streamConfig.getMaxRecords());
    }
}
