package ru.ibs.dtm.query.execution.core.service.metadata;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;

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
