package com.aliyun.openservices.ots.internal.streamclient.core;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.core.task.ITask;
import com.aliyun.openservices.ots.internal.streamclient.core.task.RetryingTaskDecorator;
import com.aliyun.openservices.ots.internal.streamclient.core.task.ShardSyncTask;
import com.aliyun.openservices.ots.internal.streamclient.core.task.TaskResult;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.IRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * 同步Shard信息。
 */
public class ShardSyncer {

    private static final Logger LOG = LoggerFactory.getLogger(ShardSyncer.class);

    private ITask currentTask;
    private Future<TaskResult> future;
    private final StreamConfig streamConfig;
    private final ILeaseManager<ShardLease> leaseManager;
    private final ExecutorService executorService;
    private final IRetryStrategy taskRetryStrategy;

    public ShardSyncer(StreamConfig streamConfig,
                       ILeaseManager<ShardLease> leaseManager,
                       ExecutorService executorService,
                       IRetryStrategy taskRetryStrategy) {
        this.streamConfig = streamConfig;
        this.leaseManager = leaseManager;
        this.executorService = executorService;
        this.taskRetryStrategy = taskRetryStrategy;
    }

    private synchronized boolean checkPreviousTask(boolean synchronous) throws DependencyException, StreamClientException {
        if (future == null) {
            return true;
        }
        if (synchronous || future.isDone()) {
            try {
                TaskResult result = future.get();
                if (result.getException() != null) {
                    throw result.getException();
                }
            } catch (DependencyException e) {
                LOG.error("SyncTask failed", e);
                throw e;
            } catch (StreamClientException e) {
                LOG.error("SyncTask failed", e);
                throw e;
            } catch (Exception e) {
                LOG.error("SyncTask failed", e);
                throw new StreamClientException("ShardSyncError.", e);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取最新的Shard列表并更新Lease，支持同步等待任务完成。
     *
     * @param synchronous
     *          表示是否同步完成任务，若synchronous为true，该方法会等待本次提交的任务完成后返回true，
     *          若synchronous为false，同时上一个任务已经完成，此时会提交一个新的任务并返回true，若上一个任务未完成，返回false。
     *
     * @return 是否提交了新任务。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public synchronized boolean syncShardAndLeaseInfo(boolean synchronous) throws StreamClientException, DependencyException {
        boolean submitted = checkAndSubmitNextTask(synchronous);
        LOG.debug("SyncShard, Synchronous: {}, Submitted: {}.", synchronous, submitted);
        return submitted;
    }

    private synchronized boolean checkAndSubmitNextTask(boolean synchronous) throws StreamClientException, DependencyException {
        LOG.debug("check and submit next task. Synchronous: {}.", synchronous);
        boolean previousTaskDone = checkPreviousTask(synchronous);
        if (previousTaskDone) {
            ITask task = new ShardSyncTask(streamConfig, leaseManager);
            currentTask = new RetryingTaskDecorator(
                    IRetryStrategy.RetryableAction.TASK_SHARDSYNC,
                    taskRetryStrategy,
                    task);
            future = executorService.submit(currentTask);
            if (synchronous) {
                checkPreviousTask(synchronous);
            }
            return true;
        } else {
            return false;
        }
    }
}
