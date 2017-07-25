package com.aliyun.openservices.ots.internal.streamclient.core.task;

import java.util.concurrent.Callable;

public interface ITask extends Callable<TaskResult> {

    TaskResult call();

    TaskType getTaskType();
}
