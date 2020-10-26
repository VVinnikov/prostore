package io.arenadata.dtm.common.delta;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
public enum DeltaLoadStatus {
    IN_PROCESS,
    SUCCESS,
    ERROR
}
