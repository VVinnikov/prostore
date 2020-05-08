package ru.ibs.dtm.query.execution.plugin.adb.factory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassTable;

/**
 * Исполнитель метаданных
 */
public interface MetadataFactory {
  /**
   * Применить физическую модель на БД
   *
   * @param classTable физическая модель
   * @param handler    обработчик
   */
  void apply(ClassTable classTable, Handler<AsyncResult<Void>> handler);

  /**
   * Удалить физическую модель из БД
   *
   * @param classTable физическая модель
   * @param handler    обработчик
   */
  void purge(ClassTable classTable, Handler<AsyncResult<Void>> handler);
}
