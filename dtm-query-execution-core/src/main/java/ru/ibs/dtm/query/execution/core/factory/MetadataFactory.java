package ru.ibs.dtm.query.execution.core.factory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;

/**
 * Генератор и исполнитель метаданных
 */
public interface MetadataFactory<Request> {
  /**
   * Получить отражение физической модели таблицы в классы.
   *
   * @param table   таблица
   * @param handler обработчик
   */
  void reflect(String table, Handler<AsyncResult<ClassTable>> handler);

  /**
   * Применить физическую модель на БД через плагин
   *  @param request dto-обертка для физическая модели
   * @param handler обработчик
   */
  void apply(Request request, Handler<AsyncResult<Void>> handler);
}
