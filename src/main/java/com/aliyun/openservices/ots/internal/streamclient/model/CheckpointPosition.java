package com.aliyun.openservices.ots.internal.streamclient.model;

public interface CheckpointPosition {

    /**
     * Shard目前保存的数据的最开始位置。
     */
    static final String TRIM_HORIZON = "TRIM_HORIZON";

    /**
     * Shard结束，即读完且不会再产生新的数据。
     */
    static final String SHARD_END = "SHARD_END";

}
