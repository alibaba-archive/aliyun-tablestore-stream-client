package com.aliyun.openservices.ots.internal.streamclient.lease.interfaces;

import com.aliyun.openservices.ots.internal.streamclient.DependencyException;
import com.aliyun.openservices.ots.internal.streamclient.StreamClientException;
import com.aliyun.openservices.ots.internal.streamclient.lease.Lease;

import java.util.Map;

/**
 * Lease获取。
 * @param <T>
 */
public interface ILeaseTaker<T extends Lease> {

    /**
     * 尝试获取新的Lease并返回。
     *
     * @return
     * @throws StreamClientException
     * @throws DependencyException
     */
    public Map<String, T> takeLeases() throws StreamClientException, DependencyException;

}
