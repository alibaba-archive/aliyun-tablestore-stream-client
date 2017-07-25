package com.aliyun.openservices.ots.internal.streamclient;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;

public class StreamConfig {

    /**
     * 一次请求最多获取的Records数。
     */
    private int maxRecords = 1000;

    /**
     * 用于访问OTS。
     */
    private SyncClientInterface otsClient;

    /**
     * 开启Stream的数据表的表名。
     */
    private String dataTableName;

    /**
     * 记录Lease信息的状态表的表名。
     */
    private String statusTableName;

    public int getMaxRecords() {
        return maxRecords;
    }

    public void setMaxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
    }

    public SyncClientInterface getOTSClient() {
        return otsClient;
    }

    public void setOTSClient(SyncClientInterface otsClient) {
        this.otsClient = otsClient;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    public void setDataTableName(String dataTableName) {
        this.dataTableName = dataTableName;
    }

    public String getStatusTableName() {
        return statusTableName;
    }

    public void setStatusTableName(String statusTableName) {
        this.statusTableName = statusTableName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MaxRecords: ").append(maxRecords)
                .append(", DataTableName: ").append(dataTableName)
                .append(", StatusTableName: ").append(statusTableName);
        return sb.toString();
    }
}
