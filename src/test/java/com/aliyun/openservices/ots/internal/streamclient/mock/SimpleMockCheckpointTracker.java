package com.aliyun.openservices.ots.internal.streamclient.mock;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.ShutdownException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.model.ICheckpointTracker;

import java.util.HashMap;
import java.util.Map;

public class SimpleMockCheckpointTracker implements ICheckpointTracker {

    private Map<String, String> checkpointMap = new HashMap<String, String>();

    public void setCheckpoint(String shardId, String checkpointValue, String leaseIdentifier) throws ShutdownException, StreamClientException, DependencyException {
        checkpointMap.put(shardId, checkpointValue);
    }

    public String getCheckpoint(String shardId) throws StreamClientException, DependencyException {
        return checkpointMap.get(shardId);
    }
}