package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.model.IRetryStrategy;

/**
 * LeaseManage的重试逻辑。
 */
public class LeaseManagerRetryStrategy implements IRetryStrategy {

    private int maxRetries = 10;
    private long minBackoffTimeMillis = 100;
    private long maxBackoffTimeMillis = 1000;

    public boolean shouldRetry(RetryableAction actionName, Exception ex, int retries) {
        if (retries >= maxRetries) {
            return false;
        }
        if (ex instanceof DependencyException) {
            return true;
        } else {
            return false;
        }
    }

    public long getBackoffTimeMillis(RetryableAction actionName, Exception ex, int retries) {
        long backoffTimeMillis = minBackoffTimeMillis * (retries + 1);
        backoffTimeMillis = Math.min(backoffTimeMillis, maxBackoffTimeMillis);
        return backoffTimeMillis;
    }
}
