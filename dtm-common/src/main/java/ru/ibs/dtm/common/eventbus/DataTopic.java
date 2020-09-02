package ru.ibs.dtm.common.eventbus;

public enum DataTopic {
    STATUS_EVENT_PUBLISH("status.event.publish"),
    START_WORKER_TASK("start.worker.task"),
    MPPW_START("mppw.start"),
    MPPW_KAFKA_START("mppw.kafka.start"),
    MPPW_KAFKA_STOP("mppw.kafka.stop");

    private final String value;

    DataTopic(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
