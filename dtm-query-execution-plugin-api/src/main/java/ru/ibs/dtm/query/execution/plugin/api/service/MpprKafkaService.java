package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;

/**
 * Сервис выгрузки данных в kafka.
 */
public interface MpprKafkaService {

  void execute(MpprKafkaRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler);
}
