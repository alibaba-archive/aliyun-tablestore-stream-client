package com.aliyun.openservices.ots.internal.streamclient.model;

public class ShutdownInput {

    private ShutdownReason shutdownReason;
    private IRecordProcessorCheckpointer checkpointer;

    public ShutdownInput() {

    }

    public ShutdownReason getShutdownReason() {
        return shutdownReason;
    }

    public void setShutdownReason(ShutdownReason shutdownReason) {
        this.shutdownReason = shutdownReason;
    }

    public IRecordProcessorCheckpointer getCheckpointer() {
        return checkpointer;
    }

    public void setCheckpointer(IRecordProcessorCheckpointer checkpointer) {
        this.checkpointer = checkpointer;
    }
}
