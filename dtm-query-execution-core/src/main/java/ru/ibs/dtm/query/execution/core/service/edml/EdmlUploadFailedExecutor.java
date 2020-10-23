package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.Future;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

public interface EdmlUploadFailedExecutor {

    Future<Void> execute(EdmlRequestContext context);
}
