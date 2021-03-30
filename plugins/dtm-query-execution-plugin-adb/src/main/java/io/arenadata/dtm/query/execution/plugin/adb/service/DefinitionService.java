package io.arenadata.dtm.query.execution.plugin.adb.service;

/**
 * Service for working with ddl query
 *
 * @param <T> result type
 */
public interface DefinitionService<T> {
  T processingQuery(String sql) throws Exception;
}
