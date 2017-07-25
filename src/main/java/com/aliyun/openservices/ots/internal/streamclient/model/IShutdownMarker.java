package com.aliyun.openservices.ots.internal.streamclient.model;

public interface IShutdownMarker {

    void markForProcessDone();

    void markForProcessRestart();

}
