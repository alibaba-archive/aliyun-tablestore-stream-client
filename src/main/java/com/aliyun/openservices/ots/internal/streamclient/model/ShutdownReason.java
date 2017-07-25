package com.aliyun.openservices.ots.internal.streamclient.model;

public enum ShutdownReason {

    /**
     * 由StreamClient触发“ZOMBIE”类型的Shutdown，
     * 发生在Worker发现其不再持有一个已经处理的Shard时，可能原因是Lease过期。
     */
    ZOMBIE,

    /**
     * 由StreamClient触发“TERMINATE”类型的Shutdown，
     * 发生在Worker完全读完一个Shard时。
     */
    TERMINATE,

    /**
     * 由StreamClient触发“STOLEN”类型的Shutdown，
     * 发生在Worker发现其持有的Shard被偷时，Shutdown完毕后，Worker将该Shard转交给Stealer。
     */
    STOLEN,

    /**
     * 由用户通过ShutdownMarker触发“PROCESS_DONE”类型的Shutdown，
     * “PROCESS_DONE”的含义是该ShardConsumer已经处理完毕，不管Shard中是否还有新的数据。
     * 值得注意的是，其只是说明目前的ShardConsumer处理完毕，如果该ShardLease过期，该Shard仍会被一个新的ShardConsumer处理。
     */
    PROCESS_DONE,

    /**
     * 由用户通过ShutdownMarker触发“PROCESS_RESTART”类型的Shutdown。
     * Shutdown后，Worker会启动一个新的ShardConsumer处理该Shard。
     */
    PROCESS_RESTART
}

