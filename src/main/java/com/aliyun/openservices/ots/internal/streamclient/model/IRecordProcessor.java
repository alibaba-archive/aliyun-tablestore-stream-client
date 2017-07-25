package com.aliyun.openservices.ots.internal.streamclient.model;

public interface IRecordProcessor {

    void initialize(InitializationInput initializationInput);

    void processRecords(ProcessRecordsInput processRecordsInput);

    void shutdown(ShutdownInput shutdownInput);
}
