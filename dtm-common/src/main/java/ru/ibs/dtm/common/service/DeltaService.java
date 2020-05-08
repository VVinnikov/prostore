package ru.ibs.dtm.common.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;

import java.util.List;

/**
 * Работа с дельтами
 */
public interface DeltaService {
  /**
   * Получение DELTA_OK на дату, см.
   * <a href="https://conf.ibs.ru/pages/viewpage.action?pageId=113451710">постановку</a>
   * <p>
   * Передаёт в {@code resultHandler} максимальный номер загруженной в datamart дельты,
   * дата-время загрузки которой не больше заданной.
   * <p>
   * Дельта может оказаться NULL. Например, если на дату ещё не было загрузок или витрина указана неверно.
   *
   * @param actualDeltaRequest параметры поиска
   * @param resultHandler      async-обработчик, в который будет передана дельта
   */
  void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler);

  /**
   * Получение нескольких DELTA_OK за раз.
   * Это множественный вариант {@link #getDeltaOnDateTime(ActualDeltaRequest, Handler)}
   *
   * @param actualDeltaRequests список параметров поиска
   * @param resultHandler       async-обработчик, в который будет передан список дельт
   */
  void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler);
}
