package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import io.vertx.core.AsyncResult;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.DELTA;

public interface DeltaService<T> extends DatamartExecutionService<DeltaRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return DELTA;
    }
}