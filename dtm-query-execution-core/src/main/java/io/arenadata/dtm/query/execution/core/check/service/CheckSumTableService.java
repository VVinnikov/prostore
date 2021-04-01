package io.arenadata.dtm.query.execution.core.check.service;

import io.arenadata.dtm.query.execution.core.check.dto.CheckSumRequestContext;
import io.vertx.core.Future;

import java.util.List;

public interface CheckSumTableService {

    Future<Long> calcCheckSumTable(CheckSumRequestContext request);

    Long convertCheckSumsToLong(List<String> sysCnHashList);

    Future<Long> calcCheckSumForAllTables(CheckSumRequestContext request);
}
