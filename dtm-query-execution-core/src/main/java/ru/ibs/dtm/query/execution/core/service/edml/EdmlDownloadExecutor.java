package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ExternalTableLocationType;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

public interface EdmlDownloadExecutor {

    void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler);

    ExternalTableLocationType getDownloadType();
}
