package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.vertx.core.Future;

public interface EdmlUploadFailedExecutor {

    Future<Void> execute(EdmlRequestContext context);

    Future<Void> eraseWriteOp(RollbackRequestContext context);
}
