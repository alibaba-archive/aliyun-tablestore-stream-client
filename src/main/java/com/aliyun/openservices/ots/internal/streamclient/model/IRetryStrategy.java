package com.aliyun.openservices.ots.internal.streamclient.model;

public interface IRetryStrategy {

    static enum RetryableAction {
        TASK_BLOCK_ON_PARENT_SHARD,
        TASK_INITIALIZE,
        TASK_PROCESS,
        TASK_SHUTDOWN,
        TASK_SHARDSYNC,
        LEASE_ACTION_CREATE_TABLE_IF_NOT_EXISTS,
        LEASE_ACTION_WAIT_UNTIL_TABLE_READY,
        LEASE_ACTION_LIST_LEASES,
        LEASE_ACTION_CREATE_LEASE,
        LEASE_ACTION_GET_LEASE,
        LEASE_ACTION_RENEW_LEASE,
        LEASE_ACTION_TAKE_LEASE,
        LEASE_ACTION_STEAL_LEASE,
        LEASE_ACTION_TRANSFER_LEASE,
        LEASE_ACTION_DELETE_LEASE,
        LEASE_ACTION_UPDATE_LEASE,
    }

    boolean shouldRetry(RetryableAction actionName, Exception ex, int retries);

    long getBackoffTimeMillis(RetryableAction actionName, Exception ex, int retries);
}
