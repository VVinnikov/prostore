package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;

public interface AdbMppwStartHandler {

    void handle(MppwKafkaRequestContext requestContext, Handler<AsyncResult<Void>> asyncHandler);
}
