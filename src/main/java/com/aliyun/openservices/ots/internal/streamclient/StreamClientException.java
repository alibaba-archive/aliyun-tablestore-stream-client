package com.aliyun.openservices.ots.internal.streamclient;

import java.io.IOException;

public class StreamClientException extends IOException {

    public StreamClientException(String message) {
        super(message);
    }

    /**
     * 构造函数。
     *
     * @param message    错误消息
     * @param cause      错误原因
     */
    public StreamClientException(String message, Throwable cause) {
        super(message, cause);
    }

}
