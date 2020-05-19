package ru.ibs.dtm.query.execution.plugin.api;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.plugin.core.Plugin;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;

/**
 * Интерфейс взаимодействия с плагинами источников данных
 */
public interface DtmDataSourcePlugin extends Plugin<SourceType> {

  /**
   * <p>Поддержка типа источника</p>
   *
   * @param sourceType тип источника
   * @return поддерживается или нет
   */
  default boolean supports(SourceType sourceType) {
    return getSourceType() == sourceType;
  }

  /**
   * <p>Получить тип Источника</p>
   *
   * @return тип Источника
   */
  SourceType getSourceType();

  /**
   * <p>Применение DDL по созданию базы дынных</p>
   *  @param request            запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void ddl(DdlRequestContext request, Handler<AsyncResult<Void>> asyncResultHandler);

  /**
   * <p>Получение данных с помощью выполнения Low Latency запроса</p>
   *
   * @param request            запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void llr(LlrRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler);

  /**
   * <p>Выполнить извлечение данных</p>
   *
   * @param request            запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void mpprKafka(MpprKafkaRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler);

  /**
   * <p>Получить оценку стоимости выполнения запроса</p>
   *
   * @param request            запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void calcQueryCost(CalcQueryCostRequest request, Handler<AsyncResult<Integer>> asyncResultHandler);
}
