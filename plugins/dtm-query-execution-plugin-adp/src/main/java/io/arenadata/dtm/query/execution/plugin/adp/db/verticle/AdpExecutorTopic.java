package io.arenadata.dtm.query.execution.plugin.adp.db.verticle;

public enum AdpExecutorTopic {
    EXECUTE("adpExecute"),
    EXECUTE_WITH_CURSOR("adpExecuteWithCursor"),
    EXECUTE_WITH_PARAMS("adpExecuteWithParams"),
    EXECUTE_UPDATE("adpExecuteUpdate"),
    EXECUTE_IN_TRANSACTION("adpExecuteInTransaction");

    private final String topic;

    AdpExecutorTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
