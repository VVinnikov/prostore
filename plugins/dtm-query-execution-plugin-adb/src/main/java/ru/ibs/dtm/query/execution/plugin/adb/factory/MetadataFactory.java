package ru.ibs.dtm.query.execution.plugin.adb.factory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.Entity;

/**
 * Исполнитель метаданных
 */
public interface MetadataFactory {
  /**
   * Применить физическую модель на БД
   *
   * @param entity физическая модель
   * @param handler    обработчик
   */
  void apply(Entity entity, Handler<AsyncResult<Void>> handler);

  /**
   * Удалить физическую модель из БД
   *
   * @param entity физическая модель
   * @param handler    обработчик
   */
  void purge(Entity entity, Handler<AsyncResult<Void>> handler);
}
