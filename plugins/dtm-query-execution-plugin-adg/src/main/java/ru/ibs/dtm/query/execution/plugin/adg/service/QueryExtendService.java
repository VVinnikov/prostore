package ru.ibs.dtm.query.execution.plugin.adg.service;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;

/**
 * Интерфейс сервиса для обогащения запроса
 * */
public interface QueryExtendService {

  /**
   * Расширение дерева запроса
   *
   * @param queryTree исходное дерево объектов
   * @return модифицированное дерево запроса
   */
  RelNode extendQuery(QueryRequest queryRequest, RelNode queryTree);

  /**
   * Инициализация билдера дерева
   *
   * @param relBuilder билдер дерева
   * @param clearOptions очистка опцией
   */
  void setRequestBuilder(RelBuilder relBuilder, boolean clearOptions);

  /**
   * Добавление опции для процесса расширения запроса
   *
   * @param option опция для механизма расширения
   */
  void addOption(Object option);

}
