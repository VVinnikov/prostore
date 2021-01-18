package io.arenadata.dtm.query.execution.plugin.api.service.check;

import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.vertx.core.Future;

public interface CheckDataService {
    Future<Long> checkDataByCount(CheckDataByCountRequest params);
    Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request params);
}
