package io.arenadata.dtm.query.execution.core.delta.dto.query;

public enum DeltaAction {
    BEGIN_DELTA,
    COMMIT_DELTA,
    ROLLBACK_DELTA,
    GET_DELTA_OK,
    GET_DELTA_HOT,
    GET_DELTA_BY_DATETIME,
    GET_DELTA_BY_NUM
}
