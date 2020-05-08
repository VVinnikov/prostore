package ru.ibs.dtm.query.execution.core.service;

/**
 * Сервис выполнения DDL
 */
public interface DdlService extends QueryExecuteService {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DDL;
  }
}
