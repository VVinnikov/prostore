package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Сервис выполнения вызова обращения MpprKafkaConnector
 */
public interface MpprKafkaConnectorService {

    void call(MpprKafkaConnectorRequest request, Handler<AsyncResult<QueryResult>> handler);
}
