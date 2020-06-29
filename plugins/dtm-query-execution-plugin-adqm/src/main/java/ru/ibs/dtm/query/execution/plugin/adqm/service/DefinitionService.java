package ru.ibs.dtm.query.execution.plugin.adqm.service;

/**
 * Сервис работы с DDL
 *
 * @param <T> тип результата
 */
public interface DefinitionService<T> {
    T processingQuery(String sql) throws Exception;
}
