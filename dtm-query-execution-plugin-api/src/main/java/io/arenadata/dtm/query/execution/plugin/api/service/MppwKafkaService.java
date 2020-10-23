package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.vertx.core.AsyncResult;

public interface MppwKafkaService<T> extends DatamartExecutionService<MppwRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.MPPW;
    }
}
