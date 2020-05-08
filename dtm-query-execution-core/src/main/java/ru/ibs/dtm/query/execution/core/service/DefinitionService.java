package ru.ibs.dtm.query.execution.core.service;

/**
 * Сервис работы с Sql
 *
 * @param <T> тип результата
 */
public interface DefinitionService<T> {
  T processingQuery(String sql) throws Exception;
}
