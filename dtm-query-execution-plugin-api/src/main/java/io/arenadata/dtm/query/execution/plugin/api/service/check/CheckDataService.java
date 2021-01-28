package io.arenadata.dtm.query.execution.plugin.api.service.check;

import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.vertx.core.Future;

public interface CheckDataService {
    Future<Long> checkDataByCount(CheckDataByCountParams params);
    Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params);
}
