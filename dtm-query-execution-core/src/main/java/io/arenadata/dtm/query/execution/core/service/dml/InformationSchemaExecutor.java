package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Service for execution information schema queries
 */
public interface InformationSchemaExecutor {

    void execute(QuerySourceRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler);
}
