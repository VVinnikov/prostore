package io.arenadata.dtm.query.calcite.core.service;

import lombok.SneakyThrows;

/**
 * Сервис работы с Sql
 *
 * @param <T> тип результата
 */
public interface DefinitionService<T> {
  @SneakyThrows
  T processingQuery(String sql);
}
