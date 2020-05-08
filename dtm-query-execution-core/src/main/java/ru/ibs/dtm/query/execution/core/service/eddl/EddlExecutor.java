package ru.ibs.dtm.query.execution.core.service.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlAction;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;

/**
 * Исполнитель eddl запросов
 */
public interface EddlExecutor {

  /**
   * <p>Выполнить eddl запрос</p>
   *
   * @param query              запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void execute(EddlQuery query, Handler<AsyncResult<Void>> asyncResultHandler);

  /**
   * Получить тип запроса
   *
   * @return тип запроса
   */
  EddlAction getAction();
}
