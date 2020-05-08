package ru.ibs.dtm.query.execution.plugin.adb.service;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * Интерфейс сервиса для обогащения запроса
 */
public interface QueryExtendService {

  /**
   * Расширение дерева запроса
   *
   * @param queryTree исходное дерево объектов
   * @return модифицированное дерево запроса
   */
  RelNode extendQuery(RelNode queryTree);

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
