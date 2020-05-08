package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;

/**
 * Сервис синхронизации физической модели из служебной БД к Greenplum и Tarantool
 */
public interface DatabaseSynchronizeService {
  /**
   * Синхронизация таблицы, запись в целевую БД осуществляется через интерфейс плагина
   *
   * @param request           запрос
   * @param table             имя таблицы
   * @param completionHandler обработчик результата
   * @param createTopics      создать топики Kafka
   * @see MetadataFactory#apply(DdlRequest, Handler)
   */
  void putForRefresh(QueryRequest request,
                     String table,
                     boolean createTopics,
                     Handler<AsyncResult<Void>> completionHandler);

  /**
   * Удаление таблицы, удаление из целевой БД осуществляется через интерфейс плагина
   *
   * @param request           запрос
   * @param datamartId        идентификатор схемы
   * @param tableName         имя таблицы
   * @param completionHandler обработчик результата
   * @see MetadataFactory#apply(DdlRequest, Handler)
   */
  void removeTable(QueryRequest request,
                   Long datamartId,
                   String tableName,
                   Handler<AsyncResult<Void>> completionHandler);
}
