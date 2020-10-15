package ru.ibs.dtm.common.service;

import io.vertx.core.Future;
import ru.ibs.dtm.common.delta.SelectOnInterval;

import java.time.LocalDateTime;

/**
 * Delta processing service
 */
public interface DeltaService {

  Future<Long> getCnToByDeltaDatetime(String datamart, LocalDateTime dateTime);

  Future<Long> getCnToByDeltaNum(String datamart, long num);

  Future<Long> getCnToDeltaHot(String datamart);

  Future<SelectOnInterval> getCnFromCnToByDeltaNums(String datamart, long deltaFrom, long deltaTo);
}
