package com.aliyun.openservices.ots.internal.streamclient.core.exceptions;


/**
 * 该异常目前只用于ProcessTask中，用来表示已经读完一个Shard。
 */
public class ShardEndReachedException extends RuntimeException {

    public ShardEndReachedException(String message) {
        super(message);
    }

    public ShardEndReachedException(String message, Throwable e) {
        super(message, e);
    }
}
