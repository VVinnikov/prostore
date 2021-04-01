package io.arenadata.dtm.query.execution.core.edml.mppr.service;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.vertx.core.Future;

public interface EdmlDownloadExecutor {

    Future<QueryResult> execute(EdmlRequestContext context);

    ExternalTableLocationType getDownloadType();
}
