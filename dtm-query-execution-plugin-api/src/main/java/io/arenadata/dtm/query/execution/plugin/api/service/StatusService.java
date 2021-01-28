package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;

public interface StatusService<T> extends DatamartExecutionService<StatusRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.STATUS;
    }
}
