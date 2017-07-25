package com.aliyun.openservices.ots.internal.streamclient.model;


import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * 为Callable扩充重试逻辑。
 * @param <T>
 */
public class RetryingCallableDecorator<T> implements Callable<T>{

    private static final Logger LOG = LoggerFactory.getLogger(RetryingCallableDecorator.class);

    private final IRetryStrategy.RetryableAction actionName;
    private final IRetryStrategy retryStrategy;
    private final Callable<T> callable;

    public RetryingCallableDecorator(IRetryStrategy.RetryableAction actionName,
                                     IRetryStrategy retryStrategy,
                                     Callable<T> callable) {
        this.actionName = actionName;
        this.retryStrategy = retryStrategy;
        this.callable = callable;
    }

    public T call() throws DependencyException, StreamClientException {
        int retries = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception ex) {
                if (retryStrategy.shouldRetry(actionName, ex, retries)) {
                    TimeUtils.sleepMillis(retryStrategy.getBackoffTimeMillis(actionName, ex, retries));
                    retries++;
                } else {
                    if (ex instanceof DependencyException) {
                        throw (DependencyException)ex;
                    } else {
                        throw new StreamClientException(ex.getMessage(), ex);
                    }
                }
            }
        }
    }
}
