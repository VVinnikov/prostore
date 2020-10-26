package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface EdmlUploadExecutor {

    void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler);

    ExternalTableLocationType getUploadType();
}
