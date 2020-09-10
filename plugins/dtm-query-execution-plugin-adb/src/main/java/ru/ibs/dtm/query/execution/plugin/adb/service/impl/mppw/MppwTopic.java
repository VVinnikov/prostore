package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

public enum MppwTopic {

    KAFKA_START("kafka.start"),
    KAFKA_STOP("kafka.stop"),
    KAFKA_TRANSFER_DATA("kafka.transfer.data");

    private final String value;

    MppwTopic(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
