package com.aliyun.openservices.ots.internal.streamclient.model;

import com.alicloud.openservices.tablestore.model.StreamRecord;

import java.util.List;

public class ProcessRecordsInput {

    private List<StreamRecord> records;
    private IRecordProcessorCheckpointer checkpointer;
    private IShutdownMarker shutdownMarker;

    public ProcessRecordsInput() {

    }

    public List<StreamRecord> getRecords() {
        return records;
    }

    public void setRecords(List<StreamRecord> records) {
        this.records = records;
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
}
