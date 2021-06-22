package io.arenadata.dtm.query.execution.plugin.adb.query.service.verticle;

public enum AdbExecutorTopic {
    EXECUTE("adbExecute"),
    EXECUTE_WITH_CURSOR("adbExecuteWithCursor"),
    EXECUTE_WITH_PARAMS("adbExecuteWithParams"),
    EXECUTE_UPDATE("adbExecuteUpdate"),
    EXECUTE_IN_TRANSACTION("adbExecuteInTransaction");

    private final String topic;

    AdbExecutorTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
