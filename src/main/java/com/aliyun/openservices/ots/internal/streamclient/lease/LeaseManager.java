package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.interfaces.ILeaseManager;
import com.aliyun.openservices.ots.internal.streamclient.model.IRetryStrategy;
import com.aliyun.openservices.ots.internal.streamclient.model.RetryingCallableDecorator;
import com.aliyun.openservices.ots.internal.streamclient.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * ILeaseManager的实现类。
 * @param <T>
 */
public class LeaseManager<T extends Lease> implements ILeaseManager<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class);

    private static final String OBJECT_ALREADY_EXIST = "OTSObjectAlreadyExist";
    private static final String TABLE_NOT_READY = "OTSTableNotReady";
    private static final String OTS_PARTITION_UNAVAILABLE = "OTSPartitionUnavailable";
    private static final String OTS_CONDITION_CHECK_FAIL = "OTSConditionCheckFail";

    private final SyncClientInterface ots;
    private final String tableName;
    private final AbstractLeaseSerializer<T> serializer;
    private final IRetryStrategy retryStrategy;
    private final long checkTableReadyIntervalMillis;

    public LeaseManager(SyncClientInterface ots,
                        String tableName,
                        AbstractLeaseSerializer<T> serializer,
                        IRetryStrategy retryStrategy,
                        long checkTableReadyIntervalMillis) {
        this.ots = ots;
        this.tableName = tableName;
        this.serializer = serializer;
        this.retryStrategy = retryStrategy;
        this.checkTableReadyIntervalMillis = checkTableReadyIntervalMillis;
    }

    public boolean createLeaseTableIfNotExists(int readCU, int writeCU, final int ttl) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<Boolean>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_CREATE_TABLE_IF_NOT_EXISTS,
                retryStrategy,
                createLeaseTableIfNotExistsCallable(readCU, writeCU, ttl)).call();
    }

    public boolean waitUntilTableReady(long maxWaitTimeMillis) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<Boolean>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_WAIT_UNTIL_TABLE_READY,
                retryStrategy,
                waitUntilTableReadyCallable(maxWaitTimeMillis)).call();
    }

    public List<T> listLeases() throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<List<T>>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_LIST_LEASES,
                retryStrategy,
                listLeasesCallable()).call();
    }

    public void createLease(T lease) throws StreamClientException, DependencyException {
        new RetryingCallableDecorator<Void>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_CREATE_LEASE,
                retryStrategy,
                createLeaseCallable(lease)).call();
    }

    public T getLease(String leaseKey) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<T>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_GET_LEASE,
                retryStrategy,
                getLeaseCallable(leaseKey)).call();
    }

    public boolean renewLease(T lease) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<Boolean>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_RENEW_LEASE,
                retryStrategy,
                renewLeaseCallable(lease)).call();
    }

    public boolean takeLease(T lease, String owner) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<Boolean>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_TAKE_LEASE,
                retryStrategy,
                takeLeaseCallable(lease, owner)).call();
    }

    public boolean stealLease(T lease, String stealer) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<Boolean>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_STEAL_LEASE,
                retryStrategy,
                stealLeaseCallable(lease, stealer)).call();
    }

    public boolean transferLease(T lease) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<Boolean>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_TRANSFER_LEASE,
                retryStrategy,
                transferLeaseCallable(lease)).call();
    }

    public void deleteLease(String leaseKey) throws StreamClientException, DependencyException {
        new RetryingCallableDecorator<Void>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_DELETE_LEASE,
                retryStrategy,
                deleteLeaseCallable(leaseKey)).call();
    }

    public boolean updateLease(T lease) throws StreamClientException, DependencyException {
        return new RetryingCallableDecorator<Boolean>(
                IRetryStrategy.RetryableAction.LEASE_ACTION_UPDATE_LEASE,
                retryStrategy,
                updateLeaseCallable(lease)).call();
    }

    private Callable<Boolean> createLeaseTableIfNotExistsCallable(final int readCU, final int writeCU, final int ttl) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                /**
                 * 先通过ListTable检查，因为当“表已存在且表的个数达到上限”时，createTable会抛出OTSQuotaExhausted，而此时表是存在的。
                 */
                ListTableResponse listTableResult = ots.listTable();
                if (listTableResult.getTableNames().contains(tableName)) {
                    return false;
                }

                boolean tableNotExist = true;
                try {
                    ots.createTable(serializer.getCreateTableRequest(readCU, writeCU, ttl));
                } catch (TableStoreException ex) {
                    if (ex.getErrorCode().equals(OBJECT_ALREADY_EXIST)) {
                        tableNotExist = false;
                    } else {
                        throw new DependencyException(ex.getMessage(), ex);
                    }
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
                return tableNotExist;
            }
        };
    }

    public Callable<Boolean> waitUntilTableReadyCallable(final long maxWaitTimeMillis) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                long startTime = System.currentTimeMillis();

                while (System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
                    String leaseKeyToGet = "NotExistLease";
                    GetRowRequest getRowRequest = serializer.getGetRowRequest(leaseKeyToGet);
                    try {
                        ots.getRow(getRowRequest);
                        return true;
                    } catch (TableStoreException ex) {
                        if (!ex.getErrorCode().equals(OTS_PARTITION_UNAVAILABLE) &&
                                !ex.getErrorCode().equals(TABLE_NOT_READY)) {
                            throw new DependencyException(ex.getMessage(), ex);
                        }
                    } catch (ClientException ex) {
                        throw new DependencyException(ex.getMessage(), ex);
                    }
                    TimeUtils.sleepMillis(checkTableReadyIntervalMillis);
                }
                return false;
            }
        };
    }

    public Callable<List<T>> listLeasesCallable() {
        return new Callable<List<T>>() {
            public List<T> call() throws Exception {
                try {
                    Iterator<Row> iterator = ots.createRangeIterator(serializer.getRangeIteratorParameter());
                    List<T> list = new ArrayList<T>();
                    while (iterator.hasNext()) {
                        T lease = serializer.fromOTSRow(iterator.next());
                        if (lease != null) {
                            list.add(lease);
                        }
                    }
                    return list;
                } catch (TableStoreException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<Void> createLeaseCallable(final T lease) {
        return new Callable<Void>() {
            public Void call() throws Exception {
                try {
                    PutRowRequest putRowRequest = serializer.getPutRowRequest(lease);
                    ots.putRow(putRowRequest);
                    return null;
                } catch (TableStoreException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<T> getLeaseCallable(final String leaseKey) {
        return new Callable<T>() {
            public T call() throws Exception {
                try {
                    GetRowRequest getRowRequest = serializer.getGetRowRequest(leaseKey);
                    Row row = ots.getRow(getRowRequest).getRow();
                    if (row == null) {
                        return null;
                    } else {
                        return serializer.fromOTSRow(row);
                    }
                } catch (TableStoreException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<Boolean> renewLeaseCallable(final T lease) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                try {
                    UpdateRowRequest updateRowRequest = serializer.getUpdateRowRequestForRenew(lease);
                    ots.updateRow(updateRowRequest);
                    lease.setLeaseCounter(lease.getLeaseCounter() + 1);
                    return true;
                } catch (TableStoreException ex) {
                    if (ex.getErrorCode().equals(OTS_CONDITION_CHECK_FAIL)) {
                        return false;
                    }

                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<Boolean> takeLeaseCallable(final T lease, final String owner) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                try {
                    UpdateRowRequest updateRowRequest = serializer.getUpdateRowRequestForTake(lease, owner);
                    ots.updateRow(updateRowRequest);
                    lease.setLeaseCounter(lease.getLeaseCounter() + 1);
                    lease.setLeaseOwner(owner);
                    lease.setLeaseStealer("");
                    return true;
                } catch (TableStoreException ex) {
                    if (ex.getErrorCode().equals(OTS_CONDITION_CHECK_FAIL)) {
                        return false;
                    }

                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<Boolean> stealLeaseCallable(final T lease, final String stealer) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                try {
                    UpdateRowRequest updateRowRequest = serializer.getUpdateRowRequestForSteal(lease, stealer);
                    ots.updateRow(updateRowRequest);
                    lease.setLeaseStealer(stealer);
                    return true;
                } catch (TableStoreException ex) {
                    if (ex.getErrorCode().equals(OTS_CONDITION_CHECK_FAIL)) {
                        return false;
                    }

                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<Boolean> transferLeaseCallable(final T lease) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                if (lease.getLeaseStealer() == null || lease.getLeaseStealer().equals("")) {
                    return false;
                }

                try {
                    UpdateRowRequest updateRowRequest = serializer.getUpdateRowRequestForTransfer(lease);
                    ots.updateRow(updateRowRequest);
                    lease.setLeaseCounter(lease.getLeaseCounter() + 1);
                    lease.setLeaseOwner(lease.getLeaseStealer());
                    return true;
                } catch (TableStoreException ex) {
                    if (ex.getErrorCode().equals(OTS_CONDITION_CHECK_FAIL)) {
                        return false;
                    }

                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<Void> deleteLeaseCallable(final String leaseKey) {
        return new Callable<Void>() {
            public Void call() throws Exception {
                try {
                    DeleteRowRequest deleteRowRequest = serializer.getDeleteRowRequest(leaseKey);
                    ots.deleteRow(deleteRowRequest);
                    return null;
                } catch (TableStoreException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }

    public Callable<Boolean> updateLeaseCallable(final T lease) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                try {
                    UpdateRowRequest updateRowRequest = serializer.getUpdateRowRequestForUpdate(lease);
                    ots.updateRow(updateRowRequest);
                    lease.setLeaseCounter(lease.getLeaseCounter() + 1);
                    return true;
                } catch (TableStoreException ex) {
                    if (ex.getErrorCode().equals(OTS_CONDITION_CHECK_FAIL)) {
                        return false;
                    }

                    throw new DependencyException(ex.getMessage(), ex);
                } catch (ClientException ex) {
                    throw new DependencyException(ex.getMessage(), ex);
                }
            }
        };
    }
}

