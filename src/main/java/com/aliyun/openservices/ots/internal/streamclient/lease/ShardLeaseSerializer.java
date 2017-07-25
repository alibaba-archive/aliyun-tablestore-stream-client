package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ShardLeaseSerializer extends AbstractLeaseSerializer<ShardLease> {

    private static final String CHECKPOINT = "Checkpoint";
    private static final String PARENT_SHARD_IDS = "ParentShardIds";
    private static final String SEPARATOR = "\n";

    public ShardLeaseSerializer(String statusTableName, String dataTableStreamId) {
        super(statusTableName, dataTableStreamId);
    }

    @Override
    PutRowRequest getPutRowRequest(ShardLease lease) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(lease.getLeaseKey()));
        if (!lease.getStreamId().equals(dataTableStreamId)) {
            throw new IllegalArgumentException("The streamId mismatch.");
        }
        RowPutChange change = new RowPutChange(statusTableName, pk);
        change.addColumn(LEASE_COUNTER, ColumnValue.fromLong(lease.getLeaseCounter()));
        change.addColumn(LEASE_OWNER, ColumnValue.fromString(lease.getLeaseOwner()));
        change.addColumn(LEASE_STEALER, ColumnValue.fromString(lease.getLeaseStealer()));
        change.addColumn(CHECKPOINT, ColumnValue.fromString(lease.getCheckpoint()));
        String parentShardIds = serializeParentShardIds(lease.getParentShardIds());
        change.addColumn(PARENT_SHARD_IDS, ColumnValue.fromString(parentShardIds));

        PutRowRequest request = new PutRowRequest();
        request.setRowChange(change);
        return request;
    }

    @Override
    UpdateRowRequest getUpdateRowRequestForUpdate(ShardLease lease) {
        PrimaryKey pk = getPrimaryKey(PrimaryKeyValue.fromString(lease.getLeaseKey()));
        RowUpdateChange change = new RowUpdateChange(statusTableName, pk);
        change.put(LEASE_COUNTER, ColumnValue.fromLong(lease.getLeaseCounter() + 1));
        change.put(CHECKPOINT, ColumnValue.fromString(lease.getCheckpoint()));

        change.setCondition(getCounterCheckCondition(lease.getLeaseCounter()));
        UpdateRowRequest request = new UpdateRowRequest(change);
        request.setRowChange(change);
        return request;
    }

    @Override
    ShardLease fromOTSRow(Row row) {
        /**
         * 删除过期的Lease后，可能仍有Worker对该Lease进行Renew等操作，会重新创建该Lease（列数不全）。
         * ConditionalUpdate上线后可以解决该问题。
         */
        if (row.getColumns().length < 5) {
            return null;
        }
        String streamId = row.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asString();
        String leaseKey = row.getPrimaryKey().getPrimaryKeyColumn(2).getValue().asString();
        ShardLease result = new ShardLease(leaseKey);
        result.setStreamId(streamId);
        result.setLeaseOwner(row.getColumn(LEASE_OWNER).get(0).getValue().asString());
        result.setLeaseCounter(row.getColumn(LEASE_COUNTER).get(0).getValue().asLong());
        result.setLeaseStealer(row.getColumn(LEASE_STEALER).get(0).getValue().asString());
        result.setCheckpoint(row.getColumn(CHECKPOINT).get(0).getValue().asString());
        String parentShardIds = row.getColumn(PARENT_SHARD_IDS).get(0).getValue().asString();
        result.setParentShardIds(deserializeParentShardIds(parentShardIds));
        return result;
    }

    private String serializeParentShardIds(Set<String> parentShardIds) {
        StringBuilder builder = new StringBuilder();
        for (String parentShardId : parentShardIds) {
            builder.append(parentShardId);
            builder.append(SEPARATOR);
        }
        return builder.toString();
    }

    private Set<String> deserializeParentShardIds(String parentShardIds) {
        Set<String> parentShardsList = new HashSet<String>();
        for (String parentShardId : parentShardIds.split(SEPARATOR)) {
            if (!parentShardId.isEmpty()) {
                parentShardsList.add(parentShardId);
            }
        }
        return parentShardsList;
    }
}
