package io.arenadata.dtm.query.execution.core.edml.mppw.dto;

public enum WriteOperationStatus {

    EXECUTING(0),
    SUCCESS(1),
    ERROR(2),
    ERASED(3);

    private final int value;

    WriteOperationStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
