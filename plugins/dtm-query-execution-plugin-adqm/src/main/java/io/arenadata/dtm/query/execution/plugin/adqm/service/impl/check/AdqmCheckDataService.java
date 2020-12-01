package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adqmCheckDataService")
public class AdqmCheckDataService implements CheckDataService {
    @Override
    public Future<Long> checkDataByCount(CheckDataByCountParams params) {
        return Future.failedFuture("Implementation not done yet");
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params) {
        return Future.failedFuture("Implementation not done yet");
    }
}
