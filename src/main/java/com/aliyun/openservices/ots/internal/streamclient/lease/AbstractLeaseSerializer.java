package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;

import java.util.Arrays;

/**
 * 构造对应各种Lease操作的OTS请求。
 * Lease信息会保存在OTS的一张表中，表的结构为：
 * PrimaryKey：
 *     StreamId:string,    // dataTable的streamId
 *     StatusType:string,  // 保存Lease信息时恒为“LeaseKey”。
 *     StatusValue:string  // 对应Lease的LeaseKey。
 * Columns:
 *     LeaseOwner:string
 *     LeaseCounter:long
 *     LeaseStealer:string
 *     ...
 *
 * @param <T>
 */
abstract class AbstractLeaseSerializer<T extends Lease> {

    protected static final String STREAM_ID = "StreamId";
    protected static final String STATUS_TYPE = "StatusType";
    protected static final String STATUS_VALUE = "StatusValue";
    protected static final String LEASE_KEY = "LeaseKey";
    protected static final String LEASE_OWNER = "LeaseOwner";
    protected static final String LEASE_COUNTER = "LeaseCounter";
    protected static final String LEASE_STEALER = "LeaseStealer";

    protected final String statusTableName;
    protected final String dataTableStreamId;

    AbstractLeaseSerializer(String statusTableName, String dataTableStreamId) {
        this.statusTableName = statusTableName;
        this.dataTableStreamId = dataTableStreamId;
    }

    TableMeta getTableMeta() {
        TableMeta tableMeta = new TableMeta(statusTableName);
        tableMeta.addPrimaryKeyColumn(
                new PrimaryKeySchema(STREAM_ID, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(
                new PrimaryKeySchema(STATUS_TYPE, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(
                new PrimaryKeySchema(STATUS_VALUE, PrimaryKeyType.STRING));
        return tableMeta;
    }

    CreateTableRequest getCreateTableRequest(int readCU, int writeCU, int ttl) {
        TableMeta meta = getTableMeta();
        TableOptions options = new TableOptions(ttl, 1);
        CreateTableRequest request = new CreateTableRequest(meta, options);
        request.setReservedThroughput(new ReservedThroughput(readCU, writeCU));
        return request;
    }


    PrimaryKey getPrimaryKey(PrimaryKeyValue leaseKey) {
        PrimaryKey primaryKey = new PrimaryKey(
                Arrays.asList(new PrimaryKeyColumn(STREAM_ID, PrimaryKeyValue.fromString(dataTableStreamId)),
                        new PrimaryKeyColumn(STATUS_TYPE, PrimaryKeyValue.fromString(LEASE_KEY)),
                        new PrimaryKeyColumn(STATUS_VALUE, leaseKey)));
        return primaryKey;
    }

    RangeIteratorParameter getRangeIteratorParameter() {
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(statusTableName);
        criteria.setMaxVersions(1);
        criteria.setInclusiveStartPrimaryKey(getPrimaryKey(PrimaryKeyValue.INF_MIN));
        criteria.setExclusiveEndPrimaryKey(getPrimaryKey(PrimaryKeyValue.INF_MAX));
        RangeIteratorParameter parameter = new RangeIteratorParameter(criteria);
        return parameter;
    }

    DeleteRowRequest getDeleteRowRequest(String leaseKey) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(leaseKey));
        RowDeleteChange change = new RowDeleteChange(statusTableName, pk);
        DeleteRowRequest request = new DeleteRowRequest();
        request.setRowChange(change);
        return request;
    }

    GetRowRequest getGetRowRequest(String leaseKey) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(leaseKey));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(statusTableName, pk);
        criteria.setMaxVersions(1);
        GetRowRequest request = new GetRowRequest(criteria);
        return request;
    }

    UpdateRowRequest getUpdateRowRequestForRenew(T lease) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(lease.getLeaseKey()));
        RowUpdateChange change = new RowUpdateChange(statusTableName, pk);
        change.put(LEASE_COUNTER, ColumnValue.fromLong(lease.getLeaseCounter() + 1));

        change.setCondition(getCounterCheckCondition(lease.getLeaseCounter()));
        UpdateRowRequest request = new UpdateRowRequest(change);
        return request;
    }

    UpdateRowRequest getUpdateRowRequestForTake(T lease, String newOwner) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(lease.getLeaseKey()));
        RowUpdateChange change = new RowUpdateChange(statusTableName, pk);
        change.put(LEASE_COUNTER, ColumnValue.fromLong(lease.getLeaseCounter() + 1));
        change.put(LEASE_OWNER, ColumnValue.fromString(newOwner));
        change.put(LEASE_STEALER, ColumnValue.fromString(""));

        change.setCondition(getCounterCheckCondition(lease.getLeaseCounter()));
        UpdateRowRequest request = new UpdateRowRequest(change);
        return request;
    }

    UpdateRowRequest getUpdateRowRequestForSteal(T lease, String stealer) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(lease.getLeaseKey()));
        RowUpdateChange change = new RowUpdateChange(statusTableName, pk);
        change.put(LEASE_STEALER, ColumnValue.fromString(stealer));

        // check owner not change
        SingleColumnValueCondition ownerCondition = new SingleColumnValueCondition(
                LEASE_OWNER,
                SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString(lease.getLeaseOwner()));
        ownerCondition.setPassIfMissing(false);
        ownerCondition.setLatestVersionsOnly(true);

        SingleColumnValueCondition stealerCondition = new SingleColumnValueCondition(
                LEASE_STEALER,
                SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString(""));
        stealerCondition.setPassIfMissing(true);
        stealerCondition.setLatestVersionsOnly(true);

        CompositeColumnValueCondition compositeCond = new CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator.AND);
        compositeCond.addCondition(ownerCondition);
        compositeCond.addCondition(stealerCondition);

        Condition condition = new Condition();
        condition.setColumnCondition(compositeCond);
        change.setCondition(condition);
        UpdateRowRequest request = new UpdateRowRequest(change);
        return request;
    }

    UpdateRowRequest getUpdateRowRequestForTransfer(T lease) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(lease.getLeaseKey()));
        RowUpdateChange change = new RowUpdateChange(statusTableName, pk);
        change.put(LEASE_COUNTER, ColumnValue.fromLong(lease.getLeaseCounter() + 1));
        change.put(LEASE_OWNER, ColumnValue.fromString(lease.getLeaseStealer()));

        change.setCondition(getCounterCheckCondition(lease.getLeaseCounter()));
        UpdateRowRequest request = new UpdateRowRequest(change);
        return request;
    }

    Condition getCounterCheckCondition(long counter) {
        SingleColumnValueCondition counterCondition = new SingleColumnValueCondition(
                LEASE_COUNTER,
                SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromLong(counter));
        counterCondition.setPassIfMissing(false);
        counterCondition.setLatestVersionsOnly(true);

        Condition condition = new Condition();
        condition.setColumnCondition(counterCondition);
        return condition;
    }

    abstract PutRowRequest getPutRowRequest(T lease);

    abstract UpdateRowRequest getUpdateRowRequestForUpdate(T lease);

    abstract T fromOTSRow(Row row);

}
