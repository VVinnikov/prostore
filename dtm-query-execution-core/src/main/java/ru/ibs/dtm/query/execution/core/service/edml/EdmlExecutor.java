package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

public interface EdmlExecutor {

    void execute(EdmlRequestContext context, @Deprecated  EdmlQuery edmlQuery, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * Получить тип запроса
     *
     * @return тип запроса
     */
    EdmlAction getAction();
}
