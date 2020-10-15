package ru.ibs.dtm.query.execution.core.service.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.QuerySourceRequest;

/**
 * Service for execution information schema queries
 */
public interface InformationSchemaExecutor {

    void execute(QuerySourceRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler);
}
