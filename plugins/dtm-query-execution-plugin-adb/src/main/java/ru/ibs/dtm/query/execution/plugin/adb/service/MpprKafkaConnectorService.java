package ru.ibs.dtm.query.execution.plugin.adb.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;

/**
 * Сервис выполнения вызова обращения MpprKafkaConnector
 */
public interface MpprKafkaConnectorService {

  void call(MpprKafkaConnectorRequest request, Handler<AsyncResult<QueryResult>> handler);
}
