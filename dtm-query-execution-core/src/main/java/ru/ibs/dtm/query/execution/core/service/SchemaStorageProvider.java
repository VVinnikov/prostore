package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

/**
 * Поставщик схемы из хранилища
 */
public interface SchemaStorageProvider {
  /**
   * <p>Получить логическую схему</p>
   *
   * @param handler хэндлер асинхронной обработки результата
   */
  void getLogicalSchema(Handler<AsyncResult<JsonObject>> handler);
}
