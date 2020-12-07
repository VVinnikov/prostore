package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;

public interface TruncateService extends DatamartExecutionService<TruncateContext, AsyncResult<QueryResult>> {
    Future<Void> truncateHistory(TruncateHistoryParams params);
    void addExecutor(TruncateExecutor executor);
}
