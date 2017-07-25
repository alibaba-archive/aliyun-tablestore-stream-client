package com.aliyun.openservices.ots.internal.streamclient.core.exceptions;

/**
 * 在RecordProcessor中由用户代码抛出的异常会被包装为ApplicationException。
 */
public class ApplicationException extends RuntimeException {

    public ApplicationException(String message) {
        super(message);
    }

    public ApplicationException(String message, Throwable e) {
        super(message, e);
    }
}
