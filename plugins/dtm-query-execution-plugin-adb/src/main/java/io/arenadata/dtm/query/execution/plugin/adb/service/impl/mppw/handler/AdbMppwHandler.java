package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.handler;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.vertx.core.Future;

public interface AdbMppwHandler {

    Future<Void> handle(MppwKafkaRequestContext requestContext);
}
