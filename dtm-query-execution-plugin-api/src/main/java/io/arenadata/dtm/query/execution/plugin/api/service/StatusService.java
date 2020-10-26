package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;

public interface StatusService<T> extends DatamartExecutionService<StatusRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.STATUS;
    }
}
