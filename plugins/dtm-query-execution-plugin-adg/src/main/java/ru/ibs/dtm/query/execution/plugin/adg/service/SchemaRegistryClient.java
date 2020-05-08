package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adg.model.schema.SchemaReq;

/**
 * Сервис общения с Schema Registry
 */
public interface SchemaRegistryClient {

  /**
   * Зарегистрировать схему
   *
   * @param subject название объекта схемы
   * @param schema схема
   * @param handler обработчик
   */
  void register(String subject, SchemaReq schema, Handler<AsyncResult<Void>> handler);

  /**
   * Удалить схему из Schema Registry
   *
   * @param subject название объекта схемы
   * @param handler обработчик
   */
  void unregister(String subject, Handler<AsyncResult<Void>> handler);
}
