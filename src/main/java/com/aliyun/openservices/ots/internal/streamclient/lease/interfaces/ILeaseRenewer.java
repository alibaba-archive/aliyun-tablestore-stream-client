package com.aliyun.openservices.ots.internal.streamclient.lease.interfaces;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.Lease;

import java.util.Collection;
import java.util.Map;

/**
 * Lease续约。
 * @param <T>
 */
public interface ILeaseRenewer<T extends Lease> {

    public void initialize() throws StreamClientException, DependencyException;

    /**
     * 对当前持有的所有Lease进行续约。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public void renewLeases() throws StreamClientException, DependencyException;

    /**
     * 获取当前持有的全部Lease的信息。
     * @return
     */
    public Map<String, T> getCurrentlyHeldLeases();

    /**
     * 在当前持有的Lease中获取指定Lease的信息。
     * @param leaseKey
     * @return
     */
    public T getCurrentlyHeldLease(String leaseKey);

    /**
     * LeaseTaker获取到Lease后，通过该方法将这些Lease加入Renewer中进行续约。
     * @param newLeases
     */
    public void addLeasesToRenew(Collection<T> newLeases);

    /**
     * 清除当前持有的全部Lease。
     */
    public void clearCurrentlyHeldLeases();

    /**
     * Update指定的Lease，检查LeaseIdentifier是否一致。
     *
     * @param lease
     * @param leaseIdentifier
     * @return 更新成功，返回true。若Lease已经被抢占，返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean updateLease(T lease, String leaseIdentifier) throws StreamClientException, DependencyException;

    /**
     * Transfer指定的Lease，检查LeaseIdentifier是否一致。
     *
     * @param leaseKey
     * @param leaseIdentifier
     * @return 转交成功，返回true，否则返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean transferLease(String leaseKey, String leaseIdentifier) throws StreamClientException, DependencyException;
}
