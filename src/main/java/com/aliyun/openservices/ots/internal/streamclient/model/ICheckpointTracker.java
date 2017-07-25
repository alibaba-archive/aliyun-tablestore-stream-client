package com.aliyun.openservices.ots.internal.streamclient.model;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;

public interface ICheckpointTracker {

    void setCheckpoint(String shardId, String checkpointValue, String leaseIdentifier) throws ShutdownException, StreamClientException, DependencyException;

    String getCheckpoint(String shardId) throws StreamClientException, DependencyException;
}
