package com.aliyun.openservices.ots.internal.streamclient.lease.interfaces;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.Lease;

import java.util.List;

/**
 * Lease的CURD等操作。
 */
public interface ILeaseManager<T extends Lease> {

    /**
     * 如果Lease表未创建，则创建Lease表。
     * @param readCU
     * @param writeCU
     * @param ttl
     * @return 若该表已经存在，返回false，若该表之前不存在（即本次创建），返回true。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean createLeaseTableIfNotExists(int readCU, int writeCU, int ttl) throws StreamClientException, DependencyException;

    /**
     * 等待Lease表Ready。
     * @param maxWaitTimeMillis 最长的等待时间。
     * @return 若表已经Ready，返回true，否则返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean waitUntilTableReady(long maxWaitTimeMillis) throws StreamClientException, DependencyException;

    /**
     * 获取Lease列表。
     * @return
     * @throws StreamClientException
     * @throws DependencyException
     */
    public List<T> listLeases() throws StreamClientException, DependencyException;

    /**
     * 创建Lease。
     * @param lease
     * @throws StreamClientException
     * @throws DependencyException
     */
    public void createLease(T lease) throws StreamClientException, DependencyException;

    /**
     * 获取指定Lease的信息。
     * @param leaseKey
     * @return
     * @throws StreamClientException
     * @throws DependencyException
     */
    public T getLease(String leaseKey) throws StreamClientException, DependencyException;

    /**
     * 尝试续租Lease，条件更新LeaseCounter。
     * @param lease
     * @return 续租成功，返回true，否则返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean renewLease(T lease) throws StreamClientException, DependencyException;

    /**
     * Owner尝试获得一个Lease，条件更新LeaseCounter，设置Owner。
     * @param lease
     * @param owner
     * @return 获取成功，返回true，否则返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean takeLease(T lease, String owner) throws StreamClientException, DependencyException;

    /**
     * Stealer尝试向其他Worker偷一个Lease，条件更新Stealer。
     * @param lease
     * @param stealer
     * @return 若更新Stealer成功，返回true，否则返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean stealLease(T lease, String stealer) throws StreamClientException, DependencyException;

    /**
     * Owner尝试将Lease转交给Stealer，条件更新Counter，设置Owner为Stealer，设置Stealer为空。
     * @param lease
     * @return 若转交成功，返回true，否则返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean transferLease(T lease) throws StreamClientException, DependencyException;

    /**
     * 删除一个Lease。
     * @param leaseKey
     * @throws StreamClientException
     * @throws DependencyException
     */
    public void deleteLease(String leaseKey) throws StreamClientException, DependencyException;

    /**
     * 尝试更新Lease中的属性，条件更新Counter。
     * @param lease
     * @return 若更新成功，返回true，否则返回false。
     * @throws StreamClientException
     * @throws DependencyException
     */
    public boolean updateLease(T lease) throws StreamClientException, DependencyException;
}
