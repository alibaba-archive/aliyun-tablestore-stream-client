package com.aliyun.openservices.ots.internal.streamclient.smoketest;

import com.aliyun.openservices.ots.internal.streamclient.*;
import com.aliyun.openservices.ots.internal.streamclient.lease.MemoryLeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.lease.ShardLease;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.mock.MockOTS;
import com.aliyun.openservices.ots.internal.streamclient.model.*;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;

import java.util.concurrent.Executors;

public class Test {

    ILeaseManager<ShardLease> leaseManager = new MemoryLeaseManager();

    class RecordProcessor implements IRecordProcessor {

        private long creationTime = System.currentTimeMillis();
        private String workerIdentifier;

        public RecordProcessor(String workerIdentifier) {
            this.workerIdentifier = workerIdentifier;
        }

        public void initialize(InitializationInput initializationInput) {
   //         System.out.println(workerIdentifier + ": " + initializationInput.getShardId());
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
   //         System.out.println(workerIdentifier + ": " + 0.001 * (System.currentTimeMillis() - creationTime));
            TimeUtils.sleepMillis(2000);
            System.out.println(processRecordsInput.getCheckpointer().getLargestPermittedCheckpointValue());
            try {
                processRecordsInput.getCheckpointer().checkpoint();
            } catch (ShutdownException e) {
                e.printStackTrace();
            } catch (StreamClientException e) {
                e.printStackTrace();
            } catch (DependencyException e) {
                e.printStackTrace();
            }
        }

        public void shutdown(ShutdownInput shutdownInput) {
            System.out.println(shutdownInput.getShutdownReason());
        }
    }

    class RecordProcessorFactory implements IRecordProcessorFactory {

        private final String workerIdentifier;

        public RecordProcessorFactory(String workerIdentifier) {
            this.workerIdentifier = workerIdentifier;
        }

        public IRecordProcessor createProcessor() {
            return new RecordProcessor(workerIdentifier);
        }
    }

    public Worker getNewWorker(String workerIdentifier) {
        StreamConfig streamConfig = new StreamConfig();
        streamConfig.setOTSClient(new MockOTS());
        streamConfig.setDataTableName("dataTable");
        streamConfig.setStatusTableName("statusTable");
        Worker worker = new Worker(workerIdentifier, new ClientConfig(), streamConfig,
                new RecordProcessorFactory(workerIdentifier), Executors.newCachedThreadPool(), leaseManager);
        return worker;
    }

    public static void main(String[] args) throws InterruptedException {
        Test test = new Test();
        Worker worker1 = test.getNewWorker("worker1");
        Thread thread1 = new Thread(worker1);
        thread1.start();
        TimeUtils.sleepMillis(10 * 1000);

        for (int i = 2; i <= 3; i++) {
            Worker worker = test.getNewWorker("worker" + i);
            Thread thread = new Thread(worker);
   //         thread.start();
        }
        TimeUtils.sleepMillis(10 * 1000);
  //      worker1.shutdown();
  //      System.out.println("Worker1 ShutDown.");
        TimeUtils.sleepMillis(1000 * 1000);
    }
}
