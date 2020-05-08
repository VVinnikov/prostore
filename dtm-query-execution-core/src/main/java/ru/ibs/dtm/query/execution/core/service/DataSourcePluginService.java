package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;

import java.util.Set;

/**
 * Сервис взаимодействия ядра с плагинами источников данных
 */
public interface DataSourcePluginService {

  /**
   * <p>Получить тип Источника</p>
   *
   * @return пооддерживаемые типы источников
   */
  Set<SourceType> getSourceTypes();

  /**
   * <p>Применение физической модели на БД</p>
   *
   * @param sourceType тип источника
   * @param request запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void ddl(SourceType sourceType,
           DdlRequest request,
           Handler<AsyncResult<Void>> asyncResultHandler);

  /**
   * <p>Выполнить получение данных</p>
   *
   * @param sourceType тип источника
   * @param request запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void llr(SourceType sourceType,
           LlrRequest request,
           Handler<AsyncResult<QueryResult>> asyncResultHandler);

  /**
   * <p>Выполнить извлечение данных</p>
   *
   * @param sourceType тип источника
   * @param request запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void mpprKafka(SourceType sourceType,
                 MpprKafkaRequest request,
                 Handler<AsyncResult<QueryResult>> asyncResultHandler);

  /**
   * <p>Получить оценку стоимости выполнения запроса</p>
   *
   * @param sourceType тип источника
   * @param request запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void calcQueryCost(SourceType sourceType,
                     CalcQueryCostRequest request,
                     Handler<AsyncResult<Integer>> asyncResultHandler);
}
