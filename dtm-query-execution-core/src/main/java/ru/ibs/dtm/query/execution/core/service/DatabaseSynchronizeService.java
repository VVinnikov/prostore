package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

/**
 * Сервис синхронизации физической модели из служебной БД к Greenplum и Tarantool
 */
public interface DatabaseSynchronizeService {
  /**
   * Синхронизация таблицы, запись в целевую БД осуществляется через интерфейс плагина
   *
   * @param context           запрос
   * @param table             имя таблицы
   * @param createTopics      создать топики Kafka
   * @param completionHandler обработчик результата
   * @see MetadataFactory#apply(RequestContext, Handler)
   */
  void putForRefresh(DdlRequestContext context,
                     String table,
                     boolean createTopics,
                     Handler<AsyncResult<Void>> completionHandler);

  /**
   * Удаление таблицы, удаление из целевой БД осуществляется через интерфейс плагина
   *
   * @param context           запрос
   * @param datamartId        идентификатор схемы
   * @param tableName         имя таблицы
   * @param completionHandler обработчик результата
   * @see MetadataFactory#apply(RequestContext, Handler)
   */
  void removeTable(DdlRequestContext context,
				   Long datamartId,
				   String tableName,
				   Handler<AsyncResult<Void>> completionHandler);
}
