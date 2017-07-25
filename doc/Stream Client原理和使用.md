### Stream Client原理和使用
基于TableStore Stream API以及TableStore SDK，我们可以使用API或者SDK读取Stream的记录。通过Stream API和SDK的介绍我们不难发现，在实时获取增量数据的时候，我们需要注意分区的信息不是静态的。分区可能会分裂，合并，当分区发生变化后，我们需要处理分区之间的依赖关系确保单主键上数据顺序读取。同时如果我们的数据是由多客户端并发生成，为了提高导出增量数据的效率，我们也需要有多消费端并行读取各个分区的增量记录。Stream Client的设计目的就是是为了解决这些用户在消费Stream数据时的常见的问题，包括如何做负载均衡，故障恢复，Checkpoint，分区信息同步确保分区信息消费顺序等。在使用了Stream Client后，只需要关心每条记录的处理逻辑即可。下面具体介绍下Client的原理和如何使用Stream client高效打造适合自身业务的数据通道。

#### Stream Client使用
安装下载[最新的JAR包](https://oss.sonatype.org/service/local/artifact/maven/redirect?r=releases&g=com.aliyun.openservices&a=tablestore-streamclient&v=1.0.0&e=jar)或者通过Maven：

``` POM
<dependency>
  <groupId>com.aliyun.openservices</groupId>
  <artifactId>tablestore-streamclient</artifactId>
  <version>1.0.0</version>
</dependency>
```

#### Stream Client 原理

下面我们对Stream Client内部的逻辑做一个简单的介绍，我们也开源了Stream Client的代码，有兴趣的同学可以在[这里](https://github.com/aliyun/aliyun-tablestore-stream-client) 下载源码了解原理，也可以给我们贡献好的基于Stream的Sample代码。
为了方便做任务调度，以及记录当前每个分区的读取进度，Stream Client使用了TableStore的一张表来记录这些信息，表名用户可以自行选定，需要注意的是要确保该表名没有其他业务在使用。

Stream Client中为每个分区定义了一个租约（lease），租约用来记录一个分区的增量数据消费者和读取进度信息，每个租约的拥有者叫做worker，当一个新的消费端启动后，worker会做初始化，初始化的时候会检查分区和租约信息。对没有相应租约的分区创建租约。当有新的分区因分裂合并产生，Stream Client也会在上面提到的状态表中插入一条租约纪录。新的纪录会被某一个StreamClient的worker抢到并不断处理，如果有新的worker而加入可能会做负载均衡，调度至新worker处理。租约记录的schema如下：

* 主键StreamId，当前处理Stream的Id
* 主键StatusType，LeaseKey
* 主键StatusValue，表示当前Lease所对应的分区的Id
* 属性Checkpoint，记录当前分区Stream数据消费的位置（用户故障恢复）
* 属性LeaseCounter，是乐观锁，每个lease的owner会持续更新这个counter值，用来续租表示继续占有当前的lease。
* 属性LeaseOwner，表示拥有当前租约的worker名。
* 属性LeaseStealer，在负载均衡的时候，表示准备挪至哪个worker。
* 属性ParentShardIds，表示当前shard的父分区信息，在worker消费当前shard时会保证父分区的stream数据以已经被消费。

下图显示了一个典型的使用Stream Client消费增量数据的分布式架构
![streamclient1](http://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/assets/pic/57167/cn_zh/1500899293887/streamclient.png?x-oss-process=image/resize,m_lfit,h_300)

在这张图中，worker1和worker2是两个基于Stream Client的消费端，比如可以是启动在ECS上的进程。数据源不断的读写TableStore的中的表，这张表初期有分区P1，P2和P3。随着访问量，数据量增大，分区P2发生了一次分裂产生了P4和P5。我们的worker1在初始阶段会消费P1的数据，worker2消费P2和p3的数据，当P2发生分裂后，新的分区P4会被分配给worker1，分区5分配给worker2。但我们Stream Client会确保P2的R5记录已经被消费完成后，再开始消费P4和P5的数据。如果这时部署了一个新的消费端worker3，那可能发生的一件事情是worker2上的某个分区会被worker3调度走，负载均衡就发生了。
在上述常见下，StreamClient会在状态表中生成如下的租约信息：
![streamclient2](http://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/assets/pic/57167/cn_zh/1500899383109/leasemapping.png?x-oss-process=image/resize,m_lfit,h_300)


Stream Client中的worker是抽象消费Stream数据的载体，每一个分区会被分配一个worker也就是Lease的拥有者，拥有者会不断的通过心跳也就是更新LeaseCounter来续约当前的分区租约，通常我们在每一个Stream消费端会起一个worker，worker在完成初始化后获取当前需要处理的分区信息，同时worker会维护自己的线程池，并发的循环拉取所拥有的各分区的增量数据，worker初始化流程如下：

* 读取TableStore的配置，初始化内部访问TableStore的客户端。
* 获取对应表的Stream信息，并初始化租约管理类。租约管理类会同步租约信息，为新分区创建租约纪录。
* 初始化分区同步类，分区同步类会维持当前持有的分区心跳。
* 循环获取当前worker持有的分区的增量数据。


#### Stream Client 接口
为了方便用户使用Stream client消费Stream数据，隐藏分区读取逻辑和调度逻辑。Stream Client提供了IRecordProcessor接口，Stream Client的worker会在拉去到stream数据后调用processRecords函数来触发用户的处理数据逻辑。

```Java
public interface IRecordProcessor {

    void initialize(InitializationInput initializationInput);

    void processRecords(ProcessRecordsInput processRecordsInput);

    void shutdown(ShutdownInput shutdownInput);
}
```
 void initialize(InitializationInput initializationInput);
 用来初始化一个读取任务，表示我们的调度stream client准备开始读取某个shard的数据。
 
 void processRecords(ProcessRecordsInput processRecordsInput);表示具体读取到数据后用户希望如何处理这批记录。在ProcessRecordsInput中有一个getCheckpointer函数可以得到一个IRecordProcessorCheckpointer。这个接口是框架提供给用户用来做checkpoint的接口，用户可以自行决定多久做一次checkpoint。
 
 void shutdown(ShutdownInput shutdownInput); 表示结束某个shard的读区任务。
 
 需要注意的是，因为读区任务所在机器，进程可能会遇到各种类型的错误，例如因为环境因素重启，我们需要定期对处理完的数据做记录(checkpoint)。当任务重启后，会接着上次的checkpoint继续往后做，也就是说Stream client不保证ProcessRecordsInput里传给用户的记录一次。我们只会保证数据至少传一次（用户需要保证局部数据重复发送情况下的正确处理）。如果我们希望减少在出错情况下数据的重复处理，我们可以增加做checkpoint的频率，当然过于频繁的checkpoint也会降低系统的吞吐量。所以用户可以根据自身业务特点决定多久做一次checkpoint。当我们发现增量数据不能及时被消费的时候，我们可以增加消费端的资源，例如用更多的节点来读取stream记录。
 
 下面给出了一个最简单的示例使用stream client来实时获取增量数据，并在控制台输出增量数据。
 
 ```StreamSample
 public class StreamSample {
    class RecordProcessor implements IRecordProcessor {

        private long creationTime = System.currentTimeMillis();
        private String workerIdentifier;

        public RecordProcessor(String workerIdentifier) {
            this.workerIdentifier = workerIdentifier;
        }

        public void initialize(InitializationInput initializationInput) {
            // Trace some info before start the query like stream info etc.
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            List<StreamRecord> records = processRecordsInput.getRecords();

            if(records.size() == 0) {
                // No more records we can wait for the next query
                System.out.println("no more records");
            }
            for (int i = 0; i < records.size(); i++) {
                System.out.println("records:" + records.get(i));
            }
            
            // Since we don't persist the stream record we can skip blow step 
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
            return new StreamSample.RecordProcessor(workerIdentifier);
        }
    }

    public Worker getNewWorker(String workerIdentifier) {
        // Please replace with your table info
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        StreamConfig streamConfig = new StreamConfig();
        streamConfig.setOTSClient(new SyncClient(endPoint, accessId, accessKey,
                instanceName));
        streamConfig.setDataTableName("teststream");
        streamConfig.setStatusTableName("statusTable");

        Worker worker = new Worker(workerIdentifier, new ClientConfig(), streamConfig,
                new StreamSample.RecordProcessorFactory(workerIdentifier), Executors.newCachedThreadPool(), null);
        return worker;
    }

    public static void main(String[] args) throws InterruptedException {
        StreamSample test = new StreamSample();
        Worker worker1 = test.getNewWorker("worker1");
        Thread thread1 = new Thread(worker1);
        thread1.start();
    }
}
 ```

