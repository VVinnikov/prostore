package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.vertx.core.Future;

/**
 * Service for target database definition
 */
public interface TargetDatabaseDefinitionService {

    /**
     * Get target source type
     *
     * @param request request
     * @return future object
     */
    Future<QuerySourceRequest> getTargetSource(QuerySourceRequest request);
}
