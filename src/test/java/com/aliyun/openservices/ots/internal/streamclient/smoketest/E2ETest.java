package com.aliyun.openservices.ots.internal.streamclient.smoketest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.aliyun.openservices.ots.internal.streamclient.*;
import com.aliyun.openservices.ots.internal.streamclient.lease.LeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Daniel on 2017/4/20.
 */
public class E2ETest {
    ILeaseManager<ShardLease> leaseManager;
    AtomicLong totalCount = new AtomicLong(0);
    class RecordProcessor implements IRecordProcessor {

        private long creationTime = System.currentTimeMillis();
        private String workerIdentifier;

        private long lastUpdateTime = System.currentTimeMillis();

        public RecordProcessor(String workerIdentifier) {
            this.workerIdentifier = workerIdentifier;
        }

        public void initialize(InitializationInput initializationInput) {
            // Trace some info before start the query like stream info etc.
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            List<StreamRecord> records = processRecordsInput.getRecords();
            System.out.println("One Round process" + records.size());
            if(records.size() == 0) {
                // No more records we can wait for the next query
                // System.out.println("no more records");
                TimeUtils.sleepMillis(1000 * 5);
            }
            for (int i = 0; i < records.size(); i++) {
                if (i % 500 == 1) {
                    System.out.println("records:" + records.get(i) + "; shard info:" + workerIdentifier);
                }

                totalCount.incrementAndGet();
            }
            try {
                processRecordsInput.getCheckpointer().checkpoint();
                System.out.println("Process count: " + totalCount);
                /*long now = System.currentTimeMillis();
                if (now - lastUpdateTime > 5000) {
                    processRecordsInput.getCheckpointer().checkpoint();
                    lastUpdateTime = now;
                    System.out.println("Process count: " + totalCount);
                }*/
            } catch (Exception e) {
                e.printStackTrace();
            }
            /*TimeUtils.sleepMillis(1000);
            System.out.println(processRecordsInput.getCheckpointer().getLargestPermittedCheckpointValue());
            try {
                processRecordsInput.getCheckpointer().checkpoint();
            } catch (ShutdownException e) {
                e.printStackTrace();
            } catch (StreamClientException e) {
                e.printStackTrace();
            } catch (DependencyException e) {
                e.printStackTrace();
            }*/
        }

        public void shutdown(ShutdownInput shutdownInput) {
            // finish the query task and trace the shutdown reason
            System.out.println(shutdownInput.getShutdownReason());
        }
    }

    class RecordProcessorFactory implements IRecordProcessorFactory {

        private final String workerIdentifier;

        public RecordProcessorFactory(String workerIdentifier) {
            this.workerIdentifier = workerIdentifier;
        }

        public IRecordProcessor createProcessor() {
            return new E2ETest.RecordProcessor(workerIdentifier);
        }
    }

    public Worker getNewWorker(String workerIdentifier) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        StreamConfig streamConfig = new StreamConfig();
        streamConfig.setOTSClient(new SyncClient(endPoint, accessId, accessKey,
                instanceName));
        streamConfig.setDataTableName("StressTable");
        streamConfig.setStatusTableName("statusTable");

        Worker worker = new Worker(workerIdentifier, new ClientConfig(), streamConfig,
                new E2ETest.RecordProcessorFactory(workerIdentifier), Executors.newCachedThreadPool(), null);
        return worker;
    }

    public static void main(String[] args) throws InterruptedException {
        E2ETest test = new E2ETest();
        Worker worker1 = test.getNewWorker("worker1");
        Thread thread1 = new Thread(worker1);
        thread1.start();
    }
}
