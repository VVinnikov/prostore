package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.Future;

public interface EdmlUploadFailedExecutor {

    Future<Void> execute(EdmlRequestContext context);
}
