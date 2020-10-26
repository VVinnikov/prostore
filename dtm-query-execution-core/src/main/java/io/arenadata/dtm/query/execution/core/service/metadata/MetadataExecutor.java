package io.arenadata.dtm.query.execution.core.service.metadata;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Сервис исполнения ddl запросов в плагинах
 */
public interface MetadataExecutor<Request> {

  /**
   * Применить физическую модель на БД через плагин
   *  @param request dto-обертка для физическая модели
   * @param handler обработчик
   */
  void execute(Request request, Handler<AsyncResult<Void>> handler);
}
