package com.aliyun.openservices.ots.internal.streamclient.model;

import java.util.Set;

public class ShardInfo {
    private String shardId;
    private String streamId;
    private Set<String> parentShardIds;
    private String leaseIdentifier;

    public ShardInfo(String shardId, String streamId,
                     Set<String> parentShardIds, String leaseIdentifier) {
        setShardId(shardId);
        setStreamId(streamId);
        setParentShardIds(parentShardIds);
        setLeaseIdentifier(leaseIdentifier);
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public Set<String> getParentShardIds() {
        return parentShardIds;
    }

    public void setParentShardIds(Set<String> parentShardIds) {
        this.parentShardIds = parentShardIds;
    }

    public String getLeaseIdentifier() {
        return leaseIdentifier;
    }

    public void setLeaseIdentifier(String leaseIdentifier) {
        this.leaseIdentifier = leaseIdentifier;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((leaseIdentifier == null) ? 0 : leaseIdentifier.hashCode());
        result = prime * result + ((parentShardIds == null) ? 0 : parentShardIds.hashCode());
        result = prime * result + ((streamId == null) ? 0 : streamId.hashCode());
        result = prime * result + ((shardId == null) ? 0 : shardId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ShardInfo)) {
            return false;
        }
        ShardInfo other = (ShardInfo) obj;

        if (shardId == null) {
            if (other.shardId != null) {
                return false;
            }
        } else if (!shardId.equals(other.shardId)) {
            return false;
        }

        if (streamId == null) {
            if (other.streamId != null) {
                return false;
            }
        } else if (!streamId.equals(other.streamId)) {
            return false;
        }

        if (leaseIdentifier == null) {
            if (other.leaseIdentifier != null) {
                return false;
            }
        } else if (!leaseIdentifier.equals(other.leaseIdentifier)) {
            return false;
        }

        if (parentShardIds == null) {
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
        sb.append("ShardInfo [shardId=");
        sb.append(shardId);
        sb.append(", leaseIdentifier=");
        sb.append(leaseIdentifier);
        sb.append(", streamId=");
        sb.append(streamId);
        sb.append(", parentShardIds=");
        sb.append(parentShardIds);
        sb.append("]");
        return sb.toString();
    }
}
