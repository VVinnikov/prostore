package ru.ibs.dtm.common.eventbus;

public enum DataTopic {
    STATUS_EVENT_PUBLISH("status.event.publish"),
    START_WORKER_TASK("start.worker.task");

    private final String value;

    DataTopic(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
