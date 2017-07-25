package com.aliyun.openservices.ots.internal.streamclient;

import java.io.IOException;

/**
 * 依赖环境导致的异常，如访问OTS异常等。
 */
public class DependencyException extends IOException {

    public DependencyException(String message) {
        super(message);
    }

    public DependencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
