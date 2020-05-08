package ru.ibs.dtm.query.execution.core.service;

/**
 * Сервис выполнения EDDL
 */
public interface EddlService extends QueryExecuteService {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.EDDL;
  }
}
