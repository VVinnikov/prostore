package io.arenadata.dtm.query.execution.plugin.adp.check.service;

import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpCheckDataService")
public class AdpCheckDataService implements CheckDataService {
    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
