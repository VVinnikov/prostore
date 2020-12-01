package io.arenadata.dtm.query.execution.plugin.api.service.check;

import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.vertx.core.Future;

public interface CheckDataService {
    Future<Long> checkDataByCount(CheckContext context);
    Future<Long> checkDataByHashInt32(CheckContext context);
}
