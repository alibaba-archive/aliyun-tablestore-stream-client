package com.aliyun.openservices.ots.internal.streamclient;

import com.aliyun.openservices.ots.internal.streamclient.model.IRetryStrategy;

public class ClientConfig {

    /**
     * Lease的过期时间。
     */
    private long leaseDurationMillis = 20 * 1000;

    /**
     * 定时进行RenewLease操作的间隔时间。
     */
    private long renewerIntervalMillis = 6 * 1000;

    /**
     * 定时进行TakeLease操作的间隔时间。
     */
    private long takerIntervalMillis = 6 * 1000;

    /**
     * 轮询ParentShard状态的间隔时间。
     */
    private long parentShardPollIntervalMillis = 5 * 1000;

    /**
     * Worker主线程轮询的间隔时间。
     */
    private long workerIdleTimeMillis = 20;

    /**
     * 最长等待表Ready的时间。
     */
    private long maxWaitTableReadyTimeMillis = 120 * 1000;

    /**
     * 检查表是否Ready的间隔时间。
     */
    private long checkTableReadyIntervalMillis = 100;

    /**
     * 定时获取最新Shard列表的间隔时间。
     */
    private long syncShardIntervalMillis = 10 * 1000;

    /**
     * 距离上次成功RenewLease或TakeLease的最长间隔时间。
     */
    private long maxDurationBeforeLastSuccessfulRenewOrTakeLease = 60 * 1000;

    /**
     * statusTable的读CU。
     */
    private int statusTableReadCapacity = 0;

    /**
     * statusTable的写CU。
     */
    private int statusTableWriteCapacity = 0;

    /**
     * 是否开启StealLease。
     */
    private boolean autoStealLease = true;

    /**
     * 自定义Task的重试逻辑。
     */
    private IRetryStrategy taskRetryStrategy;

    /**
     * 自定义Lease操作的重试逻辑。
     */
    private IRetryStrategy leaseManagerRetryStrategy;

    /**
     * 用于执行Renew操作的thread pool的大小。
     */
    private int renewThreadPoolSize = 20;

    public ClientConfig() {
    }

    public long getLeaseDurationMillis() {
        return leaseDurationMillis;
    }

    public void setLeaseDurationMillis(long leaseDurationMillis) {
        this.leaseDurationMillis = leaseDurationMillis;
    }

    public long getRenewerIntervalMillis() {
        return renewerIntervalMillis;
    }

    public void setRenewerIntervalMillis(long renewerIntervalMillis) {
        this.renewerIntervalMillis = renewerIntervalMillis;
    }

    public long getTakerIntervalMillis() {
        return takerIntervalMillis;
    }

    public void setTakerIntervalMillis(long takerIntervalMillis) {
        this.takerIntervalMillis = takerIntervalMillis;
    }

    public long getParentShardPollIntervalMillis() {
        return parentShardPollIntervalMillis;
    }

    public void setParentShardPollIntervalMillis(long parentShardPollIntervalMillis) {
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
    }

    public long getWorkerIdleTimeMillis() {
        return workerIdleTimeMillis;
    }

    public void setWorkerIdleTimeMillis(long workerIdleTimeMillis) {
        this.workerIdleTimeMillis = workerIdleTimeMillis;
    }

    public int getStatusTableReadCapacity() {
        return statusTableReadCapacity;
    }

    public void setStatusTableReadCapacity(int statusTableReadCapacity) {
        this.statusTableReadCapacity = statusTableReadCapacity;
    }

    public int getStatusTableWriteCapacity() {
        return statusTableWriteCapacity;
    }

    public void setStatusTableWriteCapacity(int statusTableWriteCapacity) {
        this.statusTableWriteCapacity = statusTableWriteCapacity;
    }

    public IRetryStrategy getTaskRetryStrategy() {
        return taskRetryStrategy;
    }

    public void setTaskRetryStrategy(IRetryStrategy taskRetryStrategy) {
        this.taskRetryStrategy = taskRetryStrategy;
    }

    public IRetryStrategy getLeaseManagerRetryStrategy() {
        return leaseManagerRetryStrategy;
    }

    public void setLeaseManagerRetryStrategy(IRetryStrategy leaseManagerRetryStrategy) {
        this.leaseManagerRetryStrategy = leaseManagerRetryStrategy;
    }

    public long getMaxWaitTableReadyTimeMillis() {
        return maxWaitTableReadyTimeMillis;
    }

    public void setMaxWaitTableReadyTimeMillis(long maxWaitTableReadyTimeMillis) {
        this.maxWaitTableReadyTimeMillis = maxWaitTableReadyTimeMillis;
    }

    public long getCheckTableReadyIntervalMillis() {
        return checkTableReadyIntervalMillis;
    }

    public void setCheckTableReadyIntervalMillis(long checkTableReadyIntervalMillis) {
        this.checkTableReadyIntervalMillis = checkTableReadyIntervalMillis;
    }

    public boolean isAutoStealLease() {
        return autoStealLease;
    }

    public void setAutoStealLease(boolean autoStealLease) {
        this.autoStealLease = autoStealLease;
    }

    public long getSyncShardIntervalMillis() {
        return syncShardIntervalMillis;
    }

    public void setSyncShardIntervalMillis(long syncShardIntervalMillis) {
        this.syncShardIntervalMillis = syncShardIntervalMillis;
    }

    public long getMaxDurationBeforeLastSuccessfulRenewOrTakeLease() {
        return maxDurationBeforeLastSuccessfulRenewOrTakeLease;
    }

    public void setMaxDurationBeforeLastSuccessfulRenewOrTakeLease(long maxDurationBeforeLastSuccessfulRenewOrTakeLease) {
        this.maxDurationBeforeLastSuccessfulRenewOrTakeLease = maxDurationBeforeLastSuccessfulRenewOrTakeLease;
    }

    public int getRenewThreadPoolSize() {
        return renewThreadPoolSize;
    }

    public void setRenewThreadPoolSize(int renewThreadPoolSize) {
        this.renewThreadPoolSize = renewThreadPoolSize;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LeaseDurationMillis: ").append(leaseDurationMillis)
                .append(", RenewerIntervalMillis: ").append(renewerIntervalMillis)
                .append(", TakerIntervalMillis: ").append(takerIntervalMillis)
                .append(", ParentShardPollIntervalMillis: ").append(parentShardPollIntervalMillis)
                .append(", WorkerIdleTimeMillis: ").append(workerIdleTimeMillis)
                .append(", MaxWaitTableReadyTimeMillis: ").append(maxWaitTableReadyTimeMillis)
                .append(", CheckTableReadyIntervalMillis: ").append(checkTableReadyIntervalMillis)
                .append(", SyncShardIntervalMillis: ").append(syncShardIntervalMillis)
                .append(", MaxDurationBeforeLastSuccessfulRenewOrTakeLease: ").append(maxDurationBeforeLastSuccessfulRenewOrTakeLease)
                .append(", StatusTableReadCapacity: ").append(statusTableReadCapacity)
                .append(", StatusTableWriteCapacity: ").append(statusTableWriteCapacity)
                .append(", AutoStealLease: ").append(autoStealLease)
                .append(", RenewThreadPoolSize: ").append(renewThreadPoolSize)
                .append(", TaskRetryStrategy: ").append(taskRetryStrategy == null? "null" : taskRetryStrategy.getClass().getName())
                .append(", LeaseManagerRetryStrategy: ").append(leaseManagerRetryStrategy == null? "null" : leaseManagerRetryStrategy.getClass().getName());
        return sb.toString();
    }
}
