package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

/**
 * Сервис генерации данных для сохранения в служебной БД.
 */
public interface MetaStorageGeneratorService {
  /**
   * Сохранение метаданных всех витринах
   *  @param context         модель таблицы служебной БД
   * @param resultHandler обработчик запроса
   */
  void save(DdlRequestContext context, Handler<AsyncResult<Void>> resultHandler);
}
