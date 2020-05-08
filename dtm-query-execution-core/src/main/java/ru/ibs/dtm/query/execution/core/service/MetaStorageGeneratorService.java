package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassTable;

/**
 * Сервис генерации данных для сохранения в служебной БД.
 */
public interface MetaStorageGeneratorService {
  /**
   * Сохранение метаданных всех витринах
   *
   * @param table         модель таблицы служебной БД
   * @param resultHandler обработчик запроса
   */
  void save(ClassTable table, Handler<AsyncResult<Void>> resultHandler);
}
