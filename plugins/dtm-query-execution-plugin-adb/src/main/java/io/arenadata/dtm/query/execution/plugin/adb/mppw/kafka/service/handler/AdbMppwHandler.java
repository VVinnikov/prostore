package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.handler;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import io.vertx.core.Future;

public interface AdbMppwHandler {

    Future<Void> handle(MppwKafkaRequestContext requestContext);
}
