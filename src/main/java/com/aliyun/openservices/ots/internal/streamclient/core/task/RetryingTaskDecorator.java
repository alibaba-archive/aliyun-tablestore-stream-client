package com.aliyun.openservices.ots.internal.streamclient.core.task;

import com.aliyun.openservices.ots.internal.streamclient.model.IRetryStrategy;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 为Task扩充重试逻辑。
 */
public class RetryingTaskDecorator implements ITask {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingTaskDecorator.class);

    private final IRetryStrategy.RetryableAction actionName;
    private final IRetryStrategy retryStrategy;
    private final ITask task;

    public RetryingTaskDecorator(IRetryStrategy.RetryableAction actionName,
                           IRetryStrategy retryStrategy,
                           ITask task) {
        this.actionName = actionName;
        this.retryStrategy = retryStrategy;
        this.task = task;
    }

    public TaskResult call() {
        int retries = 0;
        while (true) {
            Exception exception = null;
            try {
                TaskResult taskResult = task.call();
                if (taskResult.getException() == null) {
                    return taskResult;
                } else {
                    exception = taskResult.getException();
                }
            } catch (Exception ex) {
                exception = ex;
            }
            if (retryStrategy.shouldRetry(actionName, exception, retries)) {
                LOG.debug("Retry, Action: {}, Exception: {}, Retries: {}", actionName, exception, retries);
                TimeUtils.sleepMillis(retryStrategy.getBackoffTimeMillis(actionName, exception, retries));
                retries++;
            } else {
                LOG.debug("NoRetry, Action: {}, Exception: {}, Retries: {}", actionName, exception, retries);
                return new TaskResult(exception);
            }
        }
    }

    public TaskType getTaskType() {
        return task.getTaskType();
    }
}
