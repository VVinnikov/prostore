package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.vertx.core.Future;

/**
 * Service for execution information schema queries
 */
public interface InformationSchemaExecutor {

    Future<QueryResult> execute(QuerySourceRequest request);
}
