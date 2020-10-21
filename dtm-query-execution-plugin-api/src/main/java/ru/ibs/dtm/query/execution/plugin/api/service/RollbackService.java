package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

public interface RollbackService<T> extends DatamartExecutionService<RollbackRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
