package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.vertx.core.Future;

public interface EdmlDownloadExecutor {

    Future<QueryResult> execute(EdmlRequestContext context);

    ExternalTableLocationType getDownloadType();
}
