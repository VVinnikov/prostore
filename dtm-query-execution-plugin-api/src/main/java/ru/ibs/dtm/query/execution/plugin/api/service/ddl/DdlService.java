package ru.ibs.dtm.query.execution.plugin.api.service.ddl;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;


public interface DdlService<T> extends DatamartExecutionService<DdlRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.DDL;
    }

    void addExecutor(DdlExecutor<T> executor);
}
