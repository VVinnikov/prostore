package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

public interface EdmlExecutor {

    void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * Get edml action type
     *
     * @return action type
     */
    EdmlAction getAction();
}
