package ru.ibs.dtm.query.execution.core.service;

/**
 * Сервис выполнения DML
 */
public interface DmlService extends QueryExecuteService {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DML;
  }
}
