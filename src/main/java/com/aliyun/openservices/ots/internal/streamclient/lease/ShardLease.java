package com.aliyun.openservices.ots.internal.streamclient.lease;

import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.aliyun.openservices.ots.internal.streamclient.model.ShardInfo;

import java.util.HashSet;
import java.util.Set;

public class ShardLease extends Lease {

    private String streamId = "";
    private Set<String> parentShardIds = new HashSet<String>();
    private String checkpoint = CheckpointPosition.TRIM_HORIZON;

    public ShardLease(String leaseKey) {
        super(leaseKey);
    }

    public ShardLease(ShardLease shardLease) {
        super(shardLease);
        this.streamId = shardLease.getStreamId();
        this.checkpoint = shardLease.getCheckpoint();
        this.parentShardIds.addAll(shardLease.getParentShardIds());
    }

    @Override
    public <T extends Lease> void update(T other) {
        if (!(other instanceof ShardLease)) {
            throw new IllegalArgumentException();
        }
        super.update(other);
        setCheckpoint(((ShardLease) other).getCheckpoint());
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public Set<String> getParentShardIds() {
        return parentShardIds;
    }

    public void setParentShardIds(Set<String> parentShardIds) {
        this.parentShardIds = parentShardIds;
    }

    public String getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    public ShardInfo toShardInfo() {
        return new ShardInfo(getLeaseKey(), getStreamId(),
                getParentShardIds(), getLeaseIdentifier());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Lease> T copy() {
        return (T) new ShardLease(this);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (streamId != null ? streamId.hashCode() : 0);
        result = 31 * result + (parentShardIds != null ? parentShardIds.hashCode() : 0);
        result = 31 * result + (checkpoint != null ? checkpoint.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ShardLease)) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }

        ShardLease other = (ShardLease) obj;

        if (streamId == null){
            if (other.streamId != null) {
                return false;
            }
        } else if (!streamId.equals(other.streamId)) {
                return false;
        }

        if (checkpoint == null){
            if (other.checkpoint != null) {
                return false;
            }
        } else if (!checkpoint.equals(other.checkpoint)) {
                return false;
        }

        if (parentShardIds == null){
            if (other.parentShardIds != null) {
                return false;
            }
        } else if (!parentShardIds.equals(other.parentShardIds)) {
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ShardLease [leaseKey=");
        sb.append(getLeaseKey());
        sb.append(", leaseIdentifier=");
        sb.append(getLeaseIdentifier());
        sb.append(", leaseOwner=");
        sb.append(getLeaseOwner());
        sb.append(", leaseCounter=");
        sb.append("" + getLeaseCounter());
        sb.append(", lastCounterIncrementMillis=");
        sb.append("" + getLastCounterIncrementMillis());
        sb.append(", leaseStealer=");
        sb.append(getLeaseStealer());
        sb.append(", checkpoint=");
        sb.append(checkpoint);
        sb.append(", streamId=");
        sb.append(streamId);
        sb.append(", parentShardIds=");
        sb.append(parentShardIds);
        sb.append("]");
        return sb.toString();
    }
}
