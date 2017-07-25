package com.aliyun.openservices.ots.internal.streamclient.model;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;

public interface IRecordProcessorCheckpointer {

    /**
     * 更新Checkpoint为最大允许的CheckpointValue。
     *
     * 最大允许的CheckpointValue对应已处理(或已获取)的数据的下一条记录。
     *
     * @throws ShutdownException
     *             表示Worker此时已经不再持有该Shard的Lease，无法更新Checkpoint，同时RecordProcessor将会Shutdown。
     *             触发该异常的原因可能有：Worker未及时更新租约、该Shard已经过期被清理等。
     * @throws StreamClientException
     *             StreamClient产生的异常。
     * @throws DependencyException
     *             依赖环境导致的异常，如访问OTS异常等。
     */
    void checkpoint() throws ShutdownException, StreamClientException, DependencyException;

    /**
     * 更新Checkpoint为指定的CheckpointValue。
     *
     * @param checkpointValue
     * @throws ShutdownException
     *             表示Worker此时已经不再持有该Shard的Lease，无法更新Checkpoint，同时RecordProcessor将会Shutdown。
     *             触发该异常的原因可能有：Worker未及时更新租约、该Shard已经过期被清理等。
     * @throws StreamClientException
     *             StreamClient产生的异常。
     * @throws DependencyException
     *             依赖环境导致的异常，如访问OTS异常等。
     */
    void checkpoint(String checkpointValue) throws ShutdownException, StreamClientException, DependencyException;

    /**
     * 获取最大允许更新的CheckpointValue。
     * 最大允许的CheckpointValue对应已处理(或已获取)的数据的下一条记录。
     *
     * @return
     */
    String getLargestPermittedCheckpointValue();
}
