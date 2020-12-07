package io.arenadata.dtm.query.execution.plugin.api.service.check;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.vertx.core.AsyncResult;

public interface CheckService extends DatamartExecutionService<CheckContext, AsyncResult<QueryResult>> {
    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CHECK;
    }
    void addExecutor(CheckExecutor executor);
}
