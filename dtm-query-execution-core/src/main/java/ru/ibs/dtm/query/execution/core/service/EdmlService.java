package ru.ibs.dtm.query.execution.core.service;

/**
 * Сервис выполнения EDML
 */
public interface EdmlService extends QueryExecuteService {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.EDML;
  }
}
