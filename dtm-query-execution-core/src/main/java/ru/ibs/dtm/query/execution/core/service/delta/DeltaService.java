package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DELTA;

public interface DeltaService<T> extends DatamartExecutionService<DeltaRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return DELTA;
    }
}
