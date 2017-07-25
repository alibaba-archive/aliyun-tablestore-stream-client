package com.aliyun.openservices.ots.internal.streamclient;

import java.io.IOException;

/**
 * 表示Worker此时已经不再持有该Shard的Lease，RecordProcessor将会Shutdown。
 * 触发该异常的原因可能有：Worker未及时更新租约、该Shard已经过期被清理等。
 */
public class ShutdownException extends IOException {

    public ShutdownException(String message) {
        super(message);
    }

    public ShutdownException(String message, Throwable cause) {
        super(message, cause);
    }
}
