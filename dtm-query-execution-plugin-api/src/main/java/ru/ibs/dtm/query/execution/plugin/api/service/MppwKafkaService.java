package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

public interface MppwKafkaService<T> extends DatamartExecutionService<MppwRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.MPPW;
    }
}
