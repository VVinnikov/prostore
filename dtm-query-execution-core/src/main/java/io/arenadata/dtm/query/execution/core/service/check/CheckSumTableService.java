package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.query.execution.core.dto.check.CheckSumRequestContext;
import io.vertx.core.Future;

public interface CheckSumTableService {

    Future<Long> calcCheckSumTable(CheckSumRequestContext request);

    Future<Long> calcCheckSumForAllTables(CheckSumRequestContext request);
}
