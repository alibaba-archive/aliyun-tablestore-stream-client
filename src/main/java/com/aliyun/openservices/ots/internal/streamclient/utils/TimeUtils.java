package com.aliyun.openservices.ots.internal.streamclient.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TimeUtils.class);

    public static long sleepMillis(long timeToSleepMillis) {
        if (timeToSleepMillis <= 0) {
            return 0;
        }
        long startTime = System.currentTimeMillis();
        try {
            Thread.sleep(timeToSleepMillis);
        } catch (InterruptedException ex) {
            Thread.interrupted();
            LOG.warn("Interrupted while sleeping");
        }
        return System.currentTimeMillis() - startTime;
    }
}
