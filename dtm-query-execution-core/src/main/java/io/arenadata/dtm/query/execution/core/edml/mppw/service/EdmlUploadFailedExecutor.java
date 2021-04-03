package io.arenadata.dtm.query.execution.core.edml.mppw.service;

import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequestContext;
import io.vertx.core.Future;

public interface EdmlUploadFailedExecutor {

    Future<Void> execute(EdmlRequestContext context);

    Future<Void> eraseWriteOp(RollbackRequestContext context);
}
