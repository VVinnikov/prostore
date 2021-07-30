package io.arenadata.dtm.query.execution.core.edml.mppw.dto;

public enum MppwStopReason {
    OFFSET_RECEIVED,
    CHANGE_OFFSET_TIMEOUT,
    FIRST_OFFSET_TIMEOUT,
    ERROR_RECEIVED,
    BREAK_MPPW_RECEIVED,
    UNABLE_TO_START
}
