package io.arenadata.dtm.query.execution.core.edml.mppw.service;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.vertx.core.Future;

public interface EdmlUploadExecutor {

    Future<QueryResult> execute(EdmlRequestContext context);

    ExternalTableLocationType getUploadType();
}
