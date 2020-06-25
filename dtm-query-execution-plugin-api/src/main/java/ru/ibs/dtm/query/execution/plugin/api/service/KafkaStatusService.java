package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.status.KafkaStatusRequestContext;

public interface KafkaStatusService<T> extends DatamartExecutionService<KafkaStatusRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.STATUS;
    }
}
