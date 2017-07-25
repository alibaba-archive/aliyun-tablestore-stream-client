package com.aliyun.openservices.ots.internal.streamclient.smoketest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.ClientConfig;
import com.aliyun.openservices.ots.internal.streamclient.StreamConfig;
import com.aliyun.openservices.ots.internal.streamclient.Worker;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLeaseSerializer;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Daniel on 2017/4/25.
 */
public class StreamConsumerSample {
    String dataTable = "streamtable1";
    String statusTable = "StreamClientStatusTable3";
    String targetTable = "TargetTable3";

    AtomicLong totalCount = new AtomicLong(0);

    class RecordProcessor implements IRecordProcessor {
        private String workerId;
        private SyncClient client;
        private long lastUpdateTime = System.currentTimeMillis();

        public RecordProcessor(String workerId, SyncClient client) {
            this.workerId = workerId;
            this.client = client;
        }

        public void initialize(InitializationInput initializationInput) {
            System.out.println("Record processor '" + workerId + "' started." + initializationInput.getShardInfo());
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            try {
                int rowCount = 0;
                BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
                HashSet<PrimaryKey> set = new HashSet<PrimaryKey>();
                for (StreamRecord record : processRecordsInput.getRecords()) {
                    switch (record.getRecordType()) {
                        case PUT: {
                            RowPutChange rpc = new RowPutChange(targetTable, record.getPrimaryKey());
                            for (RecordColumn column : record.getColumns()) {
                                rpc.addColumn(column.getColumn());
                            }
                            if (set.contains(record.getPrimaryKey())) {
                                client.batchWriteRow(batchWriteRowRequest);
                                batchWriteRowRequest = new BatchWriteRowRequest();
                            }
                            else {
                                batchWriteRowRequest.addRowChange(rpc);
                                rowCount++;
                                set.add(record.getPrimaryKey());
                            }

                            //PutRowRequest request = new PutRowRequest();
                            //request.setRowChange(rpc);
                            //client.putRow(request);
                            break;
                        }
                        case UPDATE: {
                            RowUpdateChange ruc = new RowUpdateChange(targetTable, record.getPrimaryKey());

                            for (RecordColumn column : record.getColumns()) {
                                if (column.getColumnType() == RecordColumn.ColumnType.PUT) {
                                    ruc.put(column.getColumn());
                                } else if (column.getColumnType() == RecordColumn.ColumnType.DELETE_ONE_VERSION) {
                                    ruc.deleteColumn(column.getColumn().getName(), column.getColumn().getTimestamp());
                                } else {
                                    ruc.deleteColumns(column.getColumn().getName());
                                }
                            }

                            if (set.contains(record.getPrimaryKey())) {
                                client.batchWriteRow(batchWriteRowRequest);
                                batchWriteRowRequest = new BatchWriteRowRequest();
                            }
                            else {
                                batchWriteRowRequest.addRowChange(ruc);
                                rowCount++;
                                set.add(record.getPrimaryKey());
                            }
                            /*UpdateRowRequest request = new UpdateRowRequest();
                            request.setRowChange(ruc);
                            client.updateRow(request);*/
                            break;
                        }
                        case DELETE: {
                            RowDeleteChange rdc = new RowDeleteChange(targetTable, record.getPrimaryKey());

                            if (set.contains(record.getPrimaryKey())) {
                                client.batchWriteRow(batchWriteRowRequest);
                                batchWriteRowRequest = new BatchWriteRowRequest();
                            }
                            else {
                                batchWriteRowRequest.addRowChange(rdc);
                                rowCount++;
                                set.add(record.getPrimaryKey());
                            }
                            /*DeleteRowRequest request = new DeleteRowRequest();
                            request.setRowChange(rdc);
                            client.deleteRow(request);*/
                            break;
                        }
                    }

                    if(rowCount >= 99){
                        client.batchWriteRow(batchWriteRowRequest);
                        batchWriteRowRequest = new BatchWriteRowRequest();
                        rowCount = 0;
                        set = new HashSet<PrimaryKey>();
                    }
                    totalCount.incrementAndGet();
                }

                if(rowCount > 0){
                    client.batchWriteRow(batchWriteRowRequest);
                }

                long now = System.currentTimeMillis();
                if (now - lastUpdateTime > 5000) {
                    processRecordsInput.getCheckpointer().checkpoint();
                    lastUpdateTime = now;
                    System.out.println("Process count: " + totalCount);
                }


            } catch (Exception e) {
                System.out.println("current work id " + workerId);
                e.printStackTrace();
            }
        }

        public void shutdown(ShutdownInput shutdownInput) {

        }
    }

    class RecordProcessorFactory implements IRecordProcessorFactory {

        private final String workerIdentifier;
        private final SyncClient client;

        public RecordProcessorFactory(String workerIdentifier, SyncClient client) {
            this.workerIdentifier = workerIdentifier;
            this.client = client;
        }

        public IRecordProcessor createProcessor() {
            return new StreamConsumerSample.RecordProcessor(workerIdentifier, client);
        }
    }

    class RetryStrategy implements IRetryStrategy {

        public boolean shouldRetry(RetryableAction actionName, Exception ex, int retries) {
            return true;
        }

        public long getBackoffTimeMillis(RetryableAction actionName, Exception ex, int retries) {
            return 100;
        }
    }

    public Worker newWorker(SyncClient client, String dataTableName, String statusTableName, String streamId, String workerId) {
        StreamConfig streamConfig = new StreamConfig();
        streamConfig.setOTSClient(client);
        streamConfig.setDataTableName(dataTableName);
        streamConfig.setStatusTableName(statusTableName);

        ShardLeaseSerializer leaseSerializer = new ShardLeaseSerializer(statusTableName, streamId);
        ILeaseManager<ShardLease> leaseManager = new LeaseManager<ShardLease>(client, streamConfig.getStatusTableName(), leaseSerializer, new StreamConsumerSample.RetryStrategy(), 100);
        Worker worker = new Worker(workerId, new ClientConfig(), streamConfig, new StreamConsumerSample.RecordProcessorFactory(workerId, client), Executors.newCachedThreadPool(), leaseManager);
        return worker;
    }

    public void start(String workerPrefix) {
        SyncClient client = new SyncClient("", "", "", "");
        prepareTargetTable(client, targetTable);

        try {
            ListStreamRequest request = new ListStreamRequest(dataTable);
            ListStreamResponse response = client.listStream(request);
            String streamId = response.getStreams().get(0).getStreamId();

            //int workerCount = 15;
            int workerCount = 5;
            List<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < workerCount; i++) {
                Thread thread = new Thread(newWorker(client, dataTable, statusTable, streamId, workerPrefix + i));
                threads.add(thread);
            }

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            client.shutdown();
        }

    }

    private void prepareTargetTable(SyncClient client, String targetTable) {
        TableMeta tableMeta = new TableMeta(targetTable);
        tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("name", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("class", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("time", PrimaryKeyType.INTEGER);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        try {
            client.createTable(request);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equalsIgnoreCase("OTSObjectAlreadyExist")) {
                throw e;
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("starting");
        StreamConsumerSample sc = new StreamConsumerSample();
        sc.start("Worker1_");
        System.out.println("started");
    }
}

