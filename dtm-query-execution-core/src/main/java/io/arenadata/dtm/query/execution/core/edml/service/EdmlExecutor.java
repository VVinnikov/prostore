package io.arenadata.dtm.query.execution.core.edml.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlAction;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.vertx.core.Future;

public interface EdmlExecutor {

    Future<QueryResult> execute(EdmlRequestContext context);

    /**
     * Get edml action type
     *
     * @return action type
     */
    EdmlAction getAction();
}
