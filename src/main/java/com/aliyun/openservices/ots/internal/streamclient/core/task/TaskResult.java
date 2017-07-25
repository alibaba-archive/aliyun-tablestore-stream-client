package com.aliyun.openservices.ots.internal.streamclient.core.task;

/**
 * Task的执行结果。
 */
public class TaskResult {

    /**
     * 该阶段是否已经完成，用于决定Shard的处理状态。
     * 比如BlockOnParentShardTask，若仍有ParentShard未处理完，为false，即代表该Shard仍处于BlockOnParent的阶段。
     */
    private boolean isPhaseCompleted;

    /**
     * Task执行过程中抛出的异常，会Catch住并保存在这里。
     */
    private Exception exception;

    public TaskResult(Exception e) {
        this(e, false);
    }

    public TaskResult(boolean isPhaseCompleted) {
        this(null, isPhaseCompleted);
    }

    public TaskResult(Exception e, boolean isPhaseCompleted) {
        this.exception = e;
        this.isPhaseCompleted = isPhaseCompleted;
    }

    public Exception getException() {
        return exception;
    }

    public boolean isPhaseCompleted() {
        return isPhaseCompleted;
    }

    public void setIsPhaseCompleted(boolean isPhaseCompleted) {
        this.isPhaseCompleted = isPhaseCompleted;
    }
}
