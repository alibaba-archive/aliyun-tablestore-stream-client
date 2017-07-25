package com.aliyun.openservices.ots.internal.streamclient.utils;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;

import java.util.ArrayList;
import java.util.List;

public class OTSHelper {

    public static StreamDetails getStreamDetails(SyncClientInterface ots, String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest(tableName);
        DescribeTableResponse result = ots.describeTable(describeTableRequest);
        return result.getStreamDetails();
    }

    public static List<StreamShard> listShard(SyncClientInterface ots, String streamId) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest(streamId);
        DescribeStreamResponse describeStreamResult = ots.describeStream(describeStreamRequest);
        List<StreamShard> shardList = new ArrayList<StreamShard>();
        shardList.addAll(describeStreamResult.getShards());
        while (describeStreamResult.getNextShardId() != null) {
            describeStreamRequest.setInclusiveStartShardId(describeStreamResult.getNextShardId());
            describeStreamResult = ots.describeStream(describeStreamRequest);
            shardList.addAll(describeStreamResult.getShards());
        }
        return shardList;
    }

    public static List<Stream> listStream(SyncClientInterface ots, String tableName) {
        ListStreamRequest listStreamRequest = new ListStreamRequest(tableName);
        ListStreamResponse listStreamResult = ots.listStream(listStreamRequest);
        return listStreamResult.getStreams();
    }

}
