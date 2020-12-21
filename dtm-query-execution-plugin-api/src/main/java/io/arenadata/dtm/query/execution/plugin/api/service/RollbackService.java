package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

public interface RollbackService<T> extends DatamartExecutionService<RollbackRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
