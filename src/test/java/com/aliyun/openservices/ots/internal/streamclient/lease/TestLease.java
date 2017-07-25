package com.aliyun.openservices.ots.internal.streamclient.lease;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLease {

    @Test
    public void testHashcode() {
        Lease lease = new Lease("leaseKey");
        Lease lease2 = new Lease(lease);

        assertEquals(lease.hashCode(), lease2.hashCode());

        lease2.setLastCounterIncrementMillis(10);
        assertEquals(lease.hashCode(), lease2.hashCode());

        lease2.setLeaseStealer("stealer");
        assertEquals(lease.hashCode(), lease2.hashCode());

        lease2.setLeaseOwner("owner");
        assertTrue(lease.hashCode() != lease2.hashCode());

        lease2.setLeaseOwner(lease.getLeaseOwner());
        lease2.setLeaseCounter(lease.getLeaseCounter() + 1);
        assertTrue(lease.hashCode() != lease2.hashCode());

        lease2.setLeaseCounter(lease.getLeaseCounter());
        lease2.setLeaseIdentifier("lease identifier");
        assertTrue(lease.hashCode() != lease2.hashCode());

        lease2.setLeaseIdentifier(lease.getLeaseIdentifier());
        lease2.setLeaseKey("leasekey2");
        assertTrue(lease.hashCode() != lease2.hashCode());
    }

    @Test
    public void testEqual() {
        Lease lease = new Lease("leaseKey");
        Lease lease2 = new Lease(lease);

        assertTrue(lease.equals(lease2));

        lease2.setLastCounterIncrementMillis(10);
        assertTrue(lease.equals(lease2));

        lease2.setLeaseStealer("stealer");
        assertTrue(lease.equals(lease2));

        lease2.setLeaseOwner("owner");
        assertTrue(!lease.equals(lease2));

        lease2.setLeaseOwner(lease.getLeaseOwner());
        lease2.setLeaseCounter(lease.getLeaseCounter() + 1);
        assertTrue(!lease.equals(lease2));

        lease2.setLeaseCounter(lease.getLeaseCounter());
        lease2.setLeaseIdentifier("lease identifier");
        assertTrue(!lease.equals(lease2));

        lease2.setLeaseIdentifier(lease.getLeaseIdentifier());
        lease2.setLeaseKey("leasekey2");
        assertTrue(!lease.equals(lease2));
    }

    @Test
    public void testInitialize() {
        Lease lease = new Lease("leaseKey");
        assertEquals(lease.getLeaseKey(), "leaseKey");
        assertEquals(lease.getLeaseCounter(), 0);
        assertEquals(lease.getLastCounterIncrementMillis(), 0);
        assertEquals(lease.getLeaseIdentifier(), "");
        assertEquals(lease.getLeaseStealer(), "");
        assertEquals(lease.getLeaseOwner(), "");
    }

    @Test
    public void testCopy() {
        Lease lease = new Lease("leaseKey1");
        lease.setLastCounterIncrementMillis(System.currentTimeMillis());
        lease.setLeaseCounter(111);
        lease.setLeaseIdentifier("lease identifier");
        lease.setLeaseOwner("lease owner 1");
        lease.setLeaseStealer("lease stealer");

        Lease leaseCopy = new Lease(lease);
        assertTrue(leaseCopy != lease);
        assertEquals(leaseCopy.getLeaseKey(), lease.getLeaseKey());
        assertEquals(leaseCopy.getLeaseStealer(), lease.getLeaseStealer());
        assertEquals(leaseCopy.getLeaseIdentifier(), lease.getLeaseIdentifier());
        assertEquals(leaseCopy.getLeaseCounter(), lease.getLeaseCounter());
        assertEquals(leaseCopy.getLeaseOwner(), lease.getLeaseOwner());
    }

    @Test
    public void testIsExpired() {
        long now = System.currentTimeMillis();

        Lease lease = new Lease("leaseKey1");
        lease.setLastCounterIncrementMillis(now);
        lease.setLeaseCounter(111);
        lease.setLeaseIdentifier("lease identifier");
        lease.setLeaseOwner("lease owner 1");
        lease.setLeaseStealer("lease stealer");

        long maxDuration = 5000;
        lease.setLastCounterIncrementMillis(now - maxDuration);
        assertTrue(!lease.isExpired(maxDuration, now));

        lease.setLastCounterIncrementMillis(now - maxDuration + 1);
        assertTrue(!lease.isExpired(maxDuration, now));

        lease.setLastCounterIncrementMillis(now - maxDuration - 1);
        assertTrue(lease.isExpired(maxDuration, now));
    }
}
