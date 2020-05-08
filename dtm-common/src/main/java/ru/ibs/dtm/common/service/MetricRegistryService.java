package ru.ibs.dtm.common.service;

import java.util.function.Supplier;

/**
 * Сервис сборка метрик и выдачи результата
 */
public interface MetricRegistryService {
  void append(String operation, Long duration);
  <T> T measureTimeMillis(String operation, Supplier<T> block);
  String printSummaryStatistics();
}
