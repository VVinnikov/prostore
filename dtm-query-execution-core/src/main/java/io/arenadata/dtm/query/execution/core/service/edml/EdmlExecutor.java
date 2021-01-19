package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
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
