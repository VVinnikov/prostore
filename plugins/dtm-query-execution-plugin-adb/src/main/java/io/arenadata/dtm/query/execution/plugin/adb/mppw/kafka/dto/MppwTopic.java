package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto;

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
