package io.arenadata.dtm.query.execution.plugin.api.service.ddl;

import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import io.vertx.core.AsyncResult;


public interface DdlService<T> extends DatamartExecutionService<DdlRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.DDL;
    }

    void addExecutor(DdlExecutor<T> executor);

    default void addPostExecutor(DdlPostExecutor executor) {}
}
