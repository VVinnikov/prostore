package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface EdmlExecutor {

    void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * Get edml action type
     *
     * @return action type
     */
    EdmlAction getAction();
}
