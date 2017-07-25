package com.aliyun.openservices.ots.internal.streamclient.model;

public class InitializationInput {

    private ShardInfo shardInfo;
    private String initialCheckpoint;
    private IRecordProcessorCheckpointer checkpointer;
    private IShutdownMarker shutdownMarker;

    public InitializationInput() {

    }

    public String getInitialCheckpoint() {
        return initialCheckpoint;
    }

    public void setInitialCheckpoint(String initialCheckpoint) {
        this.initialCheckpoint = initialCheckpoint;
    }

    public IRecordProcessorCheckpointer getCheckpointer() {
        return checkpointer;
    }

    public void setCheckpointer(IRecordProcessorCheckpointer checkpointer) {
        this.checkpointer = checkpointer;
    }

    public IShutdownMarker getShutdownMarker() {
        return shutdownMarker;
    }

    public void setShutdownMarker(IShutdownMarker shutdownMarker) {
        this.shutdownMarker = shutdownMarker;
    }

    public ShardInfo getShardInfo() {
        return shardInfo;
    }

    public void setShardInfo(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
    }
}
