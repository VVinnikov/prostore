package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.handler;

import io.vertx.core.Future;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;

public interface AdbMppwHandler {

    Future<Void> handle(MppwKafkaRequestContext requestContext);
}
