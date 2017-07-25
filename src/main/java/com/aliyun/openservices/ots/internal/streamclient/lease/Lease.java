package com.aliyun.openservices.ots.internal.streamclient.lease;

/**
 * Lease会被并发的访问，而Lease本身提供的函数均不是线程安全。
 * 所以需要在所有要对Lease并发访问的地方，访问之前加锁。
 */
public class Lease {
    private String leaseKey;
    private String leaseOwner = "";
    private long leaseCounter = 0;
    private String leaseStealer = "";

    // below fields won't be stored into status table
    private long lastCounterIncrementMillis = 0;
    private String leaseIdentifier = "";

    public Lease(String leaseKey) {
        this.leaseKey = leaseKey;
    }

    public Lease(Lease lease) {
        this.leaseKey = lease.getLeaseKey();
        this.leaseOwner = lease.getLeaseOwner();
        this.leaseCounter = lease.getLeaseCounter();
        this.lastCounterIncrementMillis = lease.getLastCounterIncrementMillis();
        this.leaseIdentifier = lease.getLeaseIdentifier();
        this.leaseStealer = lease.getLeaseStealer();
    }

    public <T extends Lease> void update(T other) {
        // has nothing to do
    }

    public String getLeaseKey() {
        return leaseKey;
    }

    public void setLeaseKey(String leaseKey) {
        this.leaseKey = leaseKey;
    }

    public String getLeaseOwner() {
        return leaseOwner;
    }

    public void setLeaseOwner(String leaseOwner) {
        this.leaseOwner = leaseOwner;
    }

    public long getLeaseCounter() {
        return leaseCounter;
    }

    public void setLeaseCounter(long leaseCounter) {
        this.leaseCounter = leaseCounter;
    }

    public long getLastCounterIncrementMillis() {
        return lastCounterIncrementMillis;
    }

    public void setLastCounterIncrementMillis(long lastCounterIncrementMillis) {
        this.lastCounterIncrementMillis = lastCounterIncrementMillis;
    }

    public String getLeaseIdentifier() {
        return leaseIdentifier;
    }

    public void setLeaseIdentifier(String leaseIdentifier) {
        this.leaseIdentifier = leaseIdentifier;
    }

    public boolean isExpired(long leaseDurationMillis, long now) {
        return (now - lastCounterIncrementMillis) > leaseDurationMillis;
    }

    public String getLeaseStealer() {
        return leaseStealer;
    }

    public void setLeaseStealer(String leaseStealer) {
        this.leaseStealer = leaseStealer;
    }

    @SuppressWarnings("unchecked")
    public <T extends Lease> T copy() {
        return (T) new Lease(this);
    }

    @Override
    public int hashCode() {
        int result = leaseKey != null ? leaseKey.hashCode() : 0;
        result = 31 * result + (leaseOwner != null ? leaseOwner.hashCode() : 0);
        result = 31 * result + (int) (leaseCounter ^ (leaseCounter >>> 32));
        result = 31 * result + (leaseIdentifier != null ? leaseIdentifier.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Lease)) {
            return false;
        }
        if (!getLeaseKey().equals(((Lease) obj).getLeaseKey())) {
            return false;
        }
        if (!getLeaseOwner().equals(((Lease) obj).getLeaseOwner())) {
            return false;
        }
        if (getLeaseCounter() != ((Lease) obj).getLeaseCounter()) {
            return false;
        }
        if (!getLeaseIdentifier().equals(((Lease) obj).getLeaseIdentifier())) {
            return false;
        }
        return true;
    }
}
